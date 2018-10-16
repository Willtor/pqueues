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
  // node_ptr node_popped = NULL;
  // lock(pqueue->lock);
  // node_ptr node = spray(seed, pqueue);
  // unlock(pqueue->lock);

  // bool cleaner = true;//false;//((fast_rand(seed) % pqueue->config.thread_count) == 0);
  // retry:
  // if(cleaner) {
  //   // node_ptr preds[N], succs[N];
  //   // find(pqueue, INT64_MAX - 1, preds, succs);
  //   // cleaner = false;
  //   // goto retry;

  //   // Here. We. Go.
  //   node_ptr lefts[N], left_nexts[N];
  //   bool needs_swing[N];
  //   lock(pqueue->lock);
  //   for(int64_t i = N - 1; i >= BOTTOM; i--) {
  //     lefts[i] = &pqueue->head;
  //     left_nexts[i] = pqueue->head.next[i];
  //     needs_swing[i] = false;
  //   }
  //   unlock(pqueue->lock);

  //   bool claimed_node = false, in_prefix = false;
  //   while(true) {
  //     int64_t prefix_height = 0, valid_height = -1;
  //     node_ptr right = node_unmark(left_nexts[BOTTOM]);
  //     if(right == NULL) {
  //       unlock(pqueue->lock);
  //       return false;
  //     }
  //     in_prefix = false;
  //     // TODO: Unmark?
  //     node_ptr right_next = right->next[BOTTOM];
  //     // Node scan...
  //     while(true) {
  //       node_ptr unmarked_right = node_unmark(right);
  //       if(unmarked_right == &pqueue->tail) {
  //         bool done = true;
  //         for(int64_t i = 0; i < N; i++) {
  //           if(needs_swing[i]) { done = false;}
  //         }
  //         // We don't need to swing any pointers and we've seen the tail.
  //         if(done) { return false; }
  //         valid_height = unmarked_right->toplevel;
  //         break; 
  //       }
  //       state_t state = unmarked_right->state;
  //       if(state == ACTIVE) {
  //         if(!claimed_node) {
  //           // Treat as another node to be discarded.
  //           claimed_node = &unmarked_right->state, DELETED, memory_order_relaxed) == ACTIVE);
  //           mark_pointers(unmarked_right);
  //         } else {
  //           // If we're in a prefix save the height of the found node
  //           // so that we can swing it.
  //           valid_height = unmarked_right->toplevel;
  //           if(!in_prefix) {
  //             // We're not in a prefix and we've found the most rightward
  //             // valid node. Save it's address and the values of it's nexts.
  //             // printf("Not in prefix, found a valid node.\n");
  //             for(int64_t i = unmarked_right->toplevel; i >= BOTTOM; i--) {
  //               if(!needs_swing[i]) {
  //                 lefts[i] = unmarked_right;
  //                 left_nexts[i] = atomic_load_explicit(&unmarked_right->next[i], memory_order_consume);
  //               }
  //             }
  //           }
  //           in_prefix = false;
  //           break;
  //         }
  //       } else {
  //         mark_pointers(right);
  //       }
  //       // In deleted prefix.
  //       in_prefix = true;
  //       // mark_pointers(right);
  //       // The pointers at these levels need to move around this node.
  //       for(int64_t i = 0; i <= unmarked_right->toplevel; i++) {
  //         needs_swing[i] = true; 
  //       }
       
  //       right = node_unmark(right_next);
  //       right_next = atomic_load_explicit(&right->next[BOTTOM], memory_order_consume);
  //     }
  //     // Did we skip over anyone?
  //     // Or, did we find someone or interest?
  //     if(left_nexts[BOTTOM] != right_next || valid_height >= 0) {
  //       // printf("SWINGING POINTER BOIS AROUND\n");
  //       // c_spray_pq_print(pqueue);
  //       // printf("LEFTS\n");
  //       // for(int64_t i = 0; i < N; i++) {
  //       //   print_node(lefts[i]);
  //       // }
  //       // printf("LEFT NEXT\n");
  //       // for(int64_t i = 0; i < N; i++) {
  //       //   print_node(left_nexts[i]);
  //       // }
  //       // printf("REPLACEMENT\n");
  //       // print_node(right);
  //       // printf("Valid height %ld\n", valid_height);
  //       for(int64_t level = valid_height; level >= BOTTOM; level--) {
  //         // assert(claimed_node);
  //         if(needs_swing[level]) {
  //           if(atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed) != DELETED) {
  //             printf("%ld, %d, %ld\n", atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed), left_nexts[BOTTOM] != right_next, valid_height);
  //           }
  //           // assert(atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed) == DELETED);
  //           bool success = atomic_compare_exchange_weak_explicit(&lefts[level]->next[level], &left_nexts[level], right,
  //             memory_order_release, memory_order_relaxed);
  //           if(success) {
  //             lefts[level] = right;
  //             left_nexts[level] =  atomic_load_explicit(&right->next[level], memory_order_consume);
  //             needs_swing[level] = false;
  //           } else if(claimed_node) {
  //             // printf("AFTER failed CAS at level%ld\n", level);
  //             // c_spray_pq_print(pqueue);
  //             // assert(false);
  //             return true;
  //           } else {
  //             // Someone interrupted our swinging in this deleted prefix 
  //             // AND we don't have a claimed node. Try again.
  //             goto retry;
  //           }
  //         }
  //       }
  //     } else { /*printf("NO CHANGES\n");*/ }
  //     valid_height = -1;
  //     bool done = true;
  //     for(int64_t i = 0; i < N; i++) {
  //       if(needs_swing[i]) { done = false;}
  //     }
  //     if(!done) { printf("More work to do\n"); continue; }

  //     if(claimed_node) {
  //       // printf("AFTER\n");
  //       // c_spray_pq_print(pqueue);
  //       // assert(false);
  //       return true;
  //     }
  //   }
  // } else {
  //   node_ptr node = spray(seed, pqueue);
  //   // If we're not passed the head yet, start just after there.
  //   if(atomic_load_explicit(&node->state, memory_order_relaxed) == PADDING) {
  //     node = atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed);
  //   }
  //   for(uint64_t i = 0; node != &pqueue->tail; node = node_unmark(atomic_load_explicit(&node->next[0], memory_order_relaxed)), i++) {
  //     state_t state = atomic_load_explicit(&node->state, memory_order_relaxed);
  //     if(state == PADDING || state == DELETED) {
  //       continue;
  //     }
  //     if(state == ACTIVE && 
  //       (atomic_exchange_explicit(&node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
  //       mark_pointers(node);
  //       return true;
  //     }
  //   }
  //   return false;
  // }

  // for(; node != &pqueue->tail; node = node->next[0]) {
  //   state_t state = atomic_load_explicit(node->state, memory_order_relaxed);
  //   if(state == PADDING || state == DELETED) {
  //     continue;
  //   }
  //   if(state == ACTIVE && 
  //     (atomic_exchange_explicit(node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
  //     bool _ = c_spray_pq_tx_remove_leaky(pqueue, node->key);
  //     return true;
  //   }
  // }
  // return false;
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