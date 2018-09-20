/* A spray-list priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and has relaxed correctness semantics.
 */

#include "c_spray_pq.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <math.h>


enum STATE {PADDING, ACTIVE, DELETED};

typedef enum STATE state_t;
typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct config_t config_t;
typedef struct node_unpacked_t node_unpacked_t;

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

struct node_unpacked_t {
  bool marked;
  node_ptr address;
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

static node_unpacked_t c_spray_pqueue_node_unpack(node_ptr node){
  return (node_unpacked_t){
    .marked = node_is_marked(node),
    .address = node_unmark(node)
    };
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

/** Return a new spray list with parameters tuned to some specified thread count.
 */
c_spray_pq_t* c_spray_pq_create(int64_t threads) {
  c_spray_pq_t* spray_pqueue = forkscan_malloc(sizeof(c_spray_pq_t));
  spray_pqueue->config = c_spray_pq_config_paper(threads);
  spray_pqueue->head.key = INT64_MIN;
  spray_pqueue->head.state = PADDING;
  spray_pqueue->tail.key = INT64_MAX;
  spray_pqueue->tail.state = PADDING;
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&spray_pqueue->head.next[i], &spray_pqueue->tail, memory_order_relaxed);
    atomic_store_explicit(&spray_pqueue->tail.next[i], NULL, memory_order_relaxed);
  }
  spray_pqueue->padding_head = &spray_pqueue->head;
  printf("Padding amount %ld\n", spray_pqueue->config.padding_amount);
  for(int64_t i = 1; i < spray_pqueue->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      atomic_store_explicit(&node->next[j], spray_pqueue->padding_head, memory_order_relaxed);
    }
    spray_pqueue->padding_head = node;
  }
  print_config(&spray_pqueue->config);
  return spray_pqueue;
}

static uint64_t fast_rand (uint64_t *seed){
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


/* Perform the spray operation in the skiplist, looking for a node to attempt
 * to dequeue from the priority queue.
 */
static node_ptr spray(uint64_t * seed, c_spray_pq_t * set) {
  node_ptr cur_node = set->padding_head;
  int64_t D = set->config.descend_amount;
  for(int64_t H = set->config.start_height; H >= 0; H = H - D) {
    int64_t jump = fast_rand(seed) % (set->config.max_jump + 1);
    while(jump-- > 0) {
      node_ptr next = node_unmark(atomic_load_explicit(&cur_node->next[H], memory_order_relaxed));
      // For some reason I need to check against the tail...
      if(next == &set->tail) {
        break;
      }
      if(next == NULL) {
        break;
      }
      cur_node = next;
    }
  }
  return cur_node;
}

static void print_node(node_ptr node) {
  printf("node[%d]: %ld", node->toplevel, node->key);
  if(node->state == DELETED) {
    printf(" state: DELETED\n");
  } else if(node->state == PADDING) {
    printf(" state: PADDING\n");
  } else {
    printf(" state: ACTIVE\n");
  }
}

void c_spray_pq_print (c_spray_pq_t *set) {
  // Padding
  for(node_ptr curr = set->padding_head;
    curr != &set->head;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Head
  print_node(&set->head);
  // Everything in between
  for(node_ptr curr = node_unmark(set->head.next[0]);
    curr != &set->tail;
    curr = node_unmark(curr->next[0])) {
    print_node(curr);
  }
  // Tail
  print_node(&set->tail);
}

static bool find(c_spray_pq_t *set, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  bool marked, snip;
  node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    pred = &set->head;
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

/** Add a node, lock-free, to the spraylist's skiplist.
 */
int c_spray_pq_add(uint64_t *seed, c_spray_pq_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find(set, key, preds, succs)) {
      if(node != NULL) {
        forkscan_free((void*)node);
      }
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
        find(set, key, preds, succs);
      }
    }
    return true;
  }
}

/** Remove a node, lock-free, from the spray-list's skiplist.
 */
static int c_spray_pq_remove_leaky(c_spray_pq_t *set, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr succ = NULL;
  while(true) {
    if(!find(set, key, preds, succs)) {
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
        bool _ = find(set, key, preds, succs);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

/** Remove a the relaxed min node, lock-free, from the spray-list's skiplist
 * using the underlying spray method for node selection.
 */
int c_spray_pq_leaky_pop_min(uint64_t *seed, c_spray_pq_t *set) {
  // bool cleaner = (fast_rand(seed) % (set->config.thread_count + 1)) == 0;
  // if(cleaner) {
  //   // FIXME: Figure out the best constant/value here.
  //   size_t dist = 0, limit = set->config.padding_amount;
  //   limit = limit < 30 ? 30 : limit;
  //   for(node_ptr curr =
  //     node_unmark(set->head.next[0]);
  //     curr != &set->tail;
  //     curr = node_unmark(curr->next[0]), dist++){
  //     if(curr->state == DELETED) {
  //       if(__sync_bool_compare_and_swap(&curr->state, DELETED, REMOVING)) {
  //         c_spray_pq_remove_leaky(set, curr->key);
  //       }
  //     }
  //     if(dist == limit) {
  //       break;
  //     }
  //   }
  //   //c_spray_pq_print(set);
  // }
  // bool empty = set->head.next[0] == &set->tail;
  // if(empty) {
  //   return false;
  // }

  node_ptr node = spray(seed, set);
  for(; node != &set->tail; node = node_unmark(atomic_load_explicit(&node->next[0], memory_order_relaxed))) {
    state_t state = atomic_load_explicit(&node->state, memory_order_relaxed);
    if(state == PADDING || state == DELETED) {
      continue;
    }
    if(state == ACTIVE && 
      (atomic_exchange_explicit(&node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
      bool _ = c_spray_pq_remove_leaky(set, node->key);
      return true;
    }
  }
  return false;
}
