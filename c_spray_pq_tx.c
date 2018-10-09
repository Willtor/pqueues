/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_spray_pq_tx.h"
#include "elided_lock.h"

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
  _Atomic(state_t)* state;
  node_ptr next[N];
};

struct config_t {
  int64_t thread_count, start_height, max_jump, descend_amount, padding_amount;
};


struct c_spray_pq_tx_t {
  config_t config;
  elided_lock_t *lock;
  node_ptr padding_head;
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel, state_t state){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
  atomic_store_explicit(node->state, state, memory_order_relaxed);
  return node;
}

static void print_node(node_ptr node) {
  printf("node[%d]: %ld", node->toplevel, node->key);
  state_t state = atomic_load_explicit(node->state, memory_order_relaxed);
  if(state == DELETED) {
    printf(" state: DELETED\n");
  } else if(state == PADDING) {
    printf(" state: PADDING\n");
  } else {
    printf(" state: ACTIVE\n");
  }
}

void c_spray_pq_tx_print (c_spray_pq_tx_t *pqueue) {
  // Padding
  for(node_ptr curr = pqueue->padding_head;
    curr != &pqueue->head;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Head
  print_node(&pqueue->head);
  // Everything in between
  for(node_ptr curr = pqueue->head.next[0];
    curr != &pqueue->tail;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Tail
  print_node(&pqueue->tail);
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

/** Return a new fixed-height skip list.
 */
c_spray_pq_tx_t * c_spray_pq_tx_create(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.key = INT64_MAX;
  spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  return spray_pq_tx;
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

static bool find(c_spray_pq_tx_t *pqueue, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  node_ptr prev = &pqueue->head;
  node_ptr curr = prev;
  for(int64_t i = N - 1; i >= 0; i--) {
    curr = prev->next[i];
    while(curr->key < key) {
      prev = curr;
      curr = curr->next[i];
    }
    preds[i] = prev;
    succs[i] = curr;
  }
  return curr->key == key;
}

/* Perform the spray operation in the skiplist, looking for a node to attempt
 * to dequeue from the priority queue.
 */
static node_ptr spray(uint64_t * seed, c_spray_pq_tx_t * pqueue) {
  node_ptr cur_node = pqueue->padding_head;
  int64_t D = pqueue->config.descend_amount;
  for(int64_t H = pqueue->config.start_height; H >= 0; H = H - D) {
    int64_t jump = fast_rand(seed) % (pqueue->config.max_jump + 1);
    while(jump-- > 0) {
      node_ptr next = cur_node->next[H];
      if(next == &pqueue->tail || next == NULL) {
        break;
      }
      cur_node = next;
    }
  }
  return cur_node;
}

int c_spray_pq_tx_add(uint64_t *seed, c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = node_create(key, toplevel, ACTIVE);
  bool added = false;
  lock(pqueue->lock);
  if(!find(pqueue, key, preds, succs)) {
    for(int64_t i = 0; i <= toplevel; i++) {
      node->next[i] = succs[i];
      preds[i]->next[i] = node;
    }
    added = true;
  }
  unlock(pqueue->lock);
  if(!added) { forkscan_free(node); }
  return added;
}


static int c_spray_pq_tx_remove_leaky(c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(pqueue->lock);
  if(find(pqueue, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(pqueue->lock);
  return node != NULL;
}

static int c_spray_pq_tx_remove(c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(pqueue->lock);
  if(find(pqueue, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(pqueue->lock);
  forkscan_retire((void*)node);
  return node != NULL;
}


int c_spray_pq_tx_pop_min_leaky(uint64_t *seed, c_spray_pq_tx_t *pqueue) {
  node_ptr node_popped = NULL;
  lock(pqueue->lock);
  node_ptr head_node = pqueue->head.next[0];
  if(head_node != &pqueue->tail) {
    node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      pqueue->head.next[i] = node_popped->next[i];
    }
  }
  unlock(pqueue->lock);
  // forkscan_retire((void*)node_popped);
  return node_popped != NULL;
//   lock(pqueue->lock);
//   node_ptr node = spray(seed, pqueue);
//   unlock(pqueue->lock);

//   for(; node != &pqueue->tail; node = node->next[0]) {
//     state_t state = atomic_load_explicit(node->state, memory_order_relaxed);
//     if(state == PADDING || state == DELETED) {
//       continue;
//     }
//     if(state == ACTIVE && 
//       (atomic_exchange_explicit(node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
//       bool _ = c_spray_pq_tx_remove_leaky(pqueue, node->key);
//       return true;
//     }
//   }
//   return false;
}

// int c_spray_pq_tx_pop_min(c_spray_pq_tx_t *pqueue) {
//   node_ptr node_popped = NULL;
//   lock(pqueue->lock);
//   node_ptr head_node = pqueue->head.next[0];
//   if(head_node != &pqueue->tail) {
//     node_popped = head_node;
//     int64_t toplevel = node_popped->toplevel;
//     for(int64_t i = 0; i <= toplevel; i++) {
//       pqueue->head.next[i] = node_popped->next[i];
//     }
//   }
//   unlock(pqueue->lock);
//   forkscan_retire((void*)node_popped);
//   return node_popped != NULL;
// }