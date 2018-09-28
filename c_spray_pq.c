/* A spray-list priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and has relaxed correctness semantics.
 */

#include "c_spray_pq.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <assert.h>
#include <stdio.h>
#include <pthread.h>
#include <math.h>

enum STATE {PADDING, ACTIVE, DELETED};

typedef enum STATE state_t;
typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct config_t config_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  _Atomic(state_t) state;
  _Atomic(node_ptr) next[N];
};

struct config_t {
  int64_t thread_count, start_height, max_jump, descend_amount, padding_amount;
};

struct c_spray_pq_t {
  config_t config;
  node_ptr padding_head;
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel, state_t state){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  atomic_store_explicit(&node->state, state, memory_order_relaxed);
  return node;
}

static node_ptr node_unmark(node_ptr node){
  return (node_ptr)(((size_t)node) & (~0x1));
}

static node_ptr node_mark(node_ptr node){
  return (node_ptr)((size_t)node | 0x1);
}

static bool node_is_marked(node_ptr node){
  return node_unmark(node) != node;
}

static config_t c_spray_pq_config_paper(int64_t threads) {
  int64_t log_arg = threads;
  if(threads == 1) { log_arg = 2; }
  return (config_t) {
    .thread_count = threads,
    .start_height = log2(threads) + 1,
    .max_jump = log2(threads) + 1,
    .descend_amount = 1,
    .padding_amount = (threads * log2(log_arg)) / 2
  };
}

static void print_config(config_t *config) {
  printf("Thread Count: %ld\n", config->thread_count);
  printf("Start Height: %ld\n", config->start_height);
  printf("Max Jump: %ld\n", config->max_jump);
  printf("Descend Amount: %ld\n", config->descend_amount);
  printf("Padding Amount: %ld\n", config->padding_amount);
}

/** Print out the contents of the skip list along with node heights.
 */
void c_spray_pq_print (c_spray_pq_t *pqueue) {
  node_ptr node = atomic_load_explicit(&pqueue->head.next[0], memory_order_consume);
  while(node_unmark(node) != &pqueue->tail) {
    node_ptr next = atomic_load_explicit(&node->next[0], memory_order_consume);
    if(!node_is_marked(next)) {
      node = node_unmark(node);
      printf("node[%d]: %ld\n", node->toplevel, node->key);
    }
    node = next;
  }
}

/** Return a new fixed-height skip list.
 */
c_spray_pq_t* c_spray_pq_create(int64_t threads) {
  c_spray_pq_t* spray_pq = forkscan_malloc(sizeof(c_spray_pq_t));
  spray_pq->config = c_spray_pq_config_paper(threads);
  spray_pq->head.key = INT64_MIN;
  atomic_store_explicit(&spray_pq->head.state, PADDING, memory_order_relaxed);
  spray_pq->tail.key = INT64_MAX;
  atomic_store_explicit(&spray_pq->tail.state, PADDING, memory_order_relaxed);
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&spray_pq->head.next[i], &spray_pq->tail, memory_order_relaxed);
    atomic_store_explicit(&spray_pq->tail.next[i], NULL, memory_order_relaxed);
  }
  spray_pq->padding_head = &spray_pq->head;
  for(int64_t i = 1; i < spray_pq->config.padding_amount; i++) {
    node_ptr node = node_create(INT64_MIN, N - 1, PADDING);
    for(int64_t j = 0; j < N; j++) {
      atomic_store_explicit(&node->next[j], spray_pq->padding_head, memory_order_relaxed);
    }
    spray_pq->padding_head = node;
  }
  return spray_pq;
}

static uint64_t fast_rand (uint64_t *seed) {
  uint64_t val = *seed;
  if(val == 0) {
    val = 1;
  }
  val ^= val << 6;
  val ^= val >> 21;
  val ^= val << 7;
  *seed = val;
  return val;
}

static int32_t random_level (uint64_t *seed, int32_t max) {
  int32_t level = 1;
  while(fast_rand(seed) % 2 == 0 && level < max) {
    level++;
  }
  return level - 1;
}

static bool find(c_spray_pq_t *pqueue, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  bool marked, snip;
  node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    pred = &pqueue->head;
    for(int64_t level = N - 1; level >= 0; --level) {
      curr = node_unmark(atomic_load_explicit(&pred->next[level], memory_order_consume));
      while(true) {
        node_ptr raw_node = atomic_load_explicit(&curr->next[level], memory_order_consume);
        marked = node_is_marked(raw_node);
        succ = node_unmark(raw_node);
        while(marked) {
          snip = atomic_compare_exchange_weak_explicit(&pred->next[level], &curr, succ, memory_order_release, memory_order_consume);
          if(!snip) {
            goto retry;
          }
          // Curr is reloaded from CAS.
          raw_node = atomic_load_explicit(&curr->next[level], memory_order_consume);
          marked = node_is_marked(raw_node);
          succ = node_unmark(raw_node);
        }
        if(curr->key < key) {
          pred = curr;
          curr = succ;
        } else {
          break;
        }
      }
      preds[level] = pred;
      succs[level] = curr;
    }
    return curr->key == key;
  }
}

/** Add a node, lock-free, to the skiplist.
 */
int c_spray_pq_add(uint64_t *seed, c_spray_pq_t *pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find(pqueue, key, preds, succs)) {
      forkscan_free((void*)node);
      return false;
    }
    if(node == NULL) { node = node_create(key, toplevel, ACTIVE); }
    for(int64_t i = 0; i <= toplevel; ++i) {
      atomic_store_explicit(&node->next[i], succs[i], memory_order_release);
    }
    node_ptr pred = preds[0], succ = succs[0];
    if(!atomic_compare_exchange_weak_explicit(&pred->next[0], &succ, node, memory_order_release, memory_order_relaxed)) {
      continue;
    }
    for(int64_t i = 1; i <= toplevel; i++) {
      while(true) {
        pred = preds[i], succ = succs[i];
        if(atomic_compare_exchange_weak_explicit(&pred->next[i],
          &succ, node, memory_order_release, memory_order_relaxed)) {
          break;
        }
        bool _ = find(pqueue, key, preds, succs);
      }
    }
    return true;
  }
}

/** Remove a node, lock-free, from the skiplist.
 */
static int c_spray_pq_remove_leaky(c_spray_pq_t *pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr succ = NULL;
  while(true) {
    if(!find(pqueue, key, preds, succs)) {
      return false;
    }
    node_ptr node_to_remove = succs[0];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
      marked = node_is_marked(succ);
      while(!marked) {
        bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level],
          &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
        marked = node_is_marked(succ);
      }
    }
    succ = atomic_load_explicit(&node_to_remove->next[0], memory_order_relaxed);
    marked = node_is_marked(succ);
    while(true) {
      bool i_marked_it = atomic_compare_exchange_weak_explicit(&node_to_remove->next[0],
        &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
      marked = node_is_marked(succ);
      if(i_marked_it) {
        bool _ = find(pqueue, key, preds, succs);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

/** Remove a node, lock-free, from the skiplist.
 */
static int c_spray_pq_remove(c_spray_pq_t *pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr succ = NULL;
  while(true) {
    if(!find(pqueue, key, preds, succs)) {
      return false;
    }
    node_ptr node_to_remove = succs[0];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
      marked = node_is_marked(succ);
      while(!marked) {
        bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level],
          &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
        marked = node_is_marked(succ);
      }
    }
    succ = atomic_load_explicit(&node_to_remove->next[0], memory_order_relaxed);
    marked = node_is_marked(succ);
    while(true) {
      bool i_marked_it = atomic_compare_exchange_weak_explicit(&node_to_remove->next[0],
        &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
      marked = node_is_marked(succ);
      if(i_marked_it) {
        bool _ = find(pqueue, key, preds, succs);
        forkscan_retire(node_to_remove);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

static node_ptr spray(uint64_t * seed, c_spray_pq_t * pqueue) {
  node_ptr cur_node = pqueue->padding_head;
  int64_t D = pqueue->config.descend_amount;
  for(int64_t H = pqueue->config.start_height; H >= 0; H = H - D) {
    int64_t jump = fast_rand(seed) % (pqueue->config.max_jump + 1);
    while(jump-- > 0) {
      node_ptr next = node_unmark(atomic_load_explicit(&cur_node->next[H], memory_order_consume));
      if(next == &pqueue->tail || next == NULL) {
        break;
      }
      cur_node = next;
    }
  }
  return cur_node;
}

/** Pop the front node from the list.  Return true iff there was a node to pop.
 */
int c_spray_pq_leaky_pop_min(uint64_t *seed, c_spray_pq_t *pqueue) {
  node_ptr node = spray(seed, pqueue);
  node_ptr original_node = node;
  for(uint64_t i = 0; node != &pqueue->tail; node = node_unmark(atomic_load_explicit(&node->next[0], memory_order_relaxed)), i++) {
    state_t state = atomic_load_explicit(&node->state, memory_order_relaxed);
    if(state == PADDING || state == DELETED) {
      continue;
    }
    if(state == ACTIVE && 
      (atomic_exchange_explicit(&node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
      bool _ = c_spray_pq_remove_leaky(pqueue, node->key);
      return true;
    }
  }
  return false;
}


int c_spray_pq_pop_min(uint64_t *seed, c_spray_pq_t *pqueue) {
  node_ptr node = spray(seed, pqueue);
  for(uint64_t i = 0; node != &pqueue->tail; node = node_unmark(atomic_load_explicit(&node->next[0], memory_order_relaxed)), i++) {
    state_t state = atomic_load_explicit(&node->state, memory_order_relaxed);
    if(state == PADDING || state == DELETED) {
      continue;
    }
    if(state == ACTIVE && 
      (atomic_exchange_explicit(&node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
      bool _ = c_spray_pq_remove(pqueue, node->key);
      return true;
    }
  }
  return false;
  // node_ptr preds[N], succs[N];
  // node_ptr succ = NULL;
  // while(true) {
  //   node_ptr node_to_remove = atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed);
  //   if (node_to_remove == &pqueue->tail) {
  //     return false;
  //   }
  //   for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
  //     preds[level] = &pqueue->head;
  //     succs[level] = node_to_remove;
  //   }

  //   for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
  //     succ = node_to_remove->next[level];
  //     bool marked = node_is_marked(succ);
  //     while(!marked) {
  //       bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level], &succ,
  //                     node_mark(succ), memory_order_relaxed, memory_order_relaxed);
  //       succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
  //       marked = node_is_marked(succ);
  //    }
  //   }
  //   succ = node_unmark(atomic_load_explicit(&node_to_remove->next[0], memory_order_relaxed));

  //   if (atomic_compare_exchange_weak_explicit(&node_to_remove->next[0], &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed)) {
  //     bool _ = find(pqueue, node_to_remove->key, preds, succs);
  //     forkscan_retire(node_to_remove);
  //     return true;
  //   }
  // }
}
