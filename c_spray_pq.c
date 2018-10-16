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
  atomic_bool cleaner_lock;
  pthread_spinlock_t lock;
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

static int64_t max(int64_t arg1, int64_t arg2) {
  return arg1 > arg2 ? arg1 : arg2;
}

static void print_config(config_t *config) {
  printf("Thread Count: %ld\n", config->thread_count);
  printf("Start Height: %ld\n", config->start_height);
  printf("Max Jump: %ld\n", config->max_jump);
  printf("Descend Amount: %ld\n", config->descend_amount);
  printf("Padding Amount: %ld\n", config->padding_amount);
}

void print_node(node_ptr node) {
  node_ptr unmarked_node = node_unmark(node);
  node_ptr next = atomic_load_explicit(&unmarked_node->next[BOTTOM], memory_order_relaxed);
  if(node_is_marked(next)) {
    printf("marked ");
  }
  printf("node[%d] key[%ld] state: ", unmarked_node->toplevel, unmarked_node->key);
  state_t state = atomic_load_explicit(&unmarked_node->state, memory_order_relaxed);
  if(state == ACTIVE) {
    printf("ACTIVE");
  } else if(state == PADDING) {
    printf("PADDING");
  } else {
    printf("DELETED");
  }
  printf("\n");
}

/** Print out the contents of the skip list along with node heights.
 */
void c_spray_pq_print (c_spray_pq_t *pqueue) {
  printf("**************************\n");
  node_ptr node = atomic_load_explicit(&pqueue->head.next[BOTTOM], memory_order_consume);
  assert(!node_is_marked(node));
  while(node_unmark(node) != &pqueue->tail) {
    print_node(node);
    node_ptr next = atomic_load_explicit(&node_unmark(node)->next[BOTTOM], memory_order_consume);
    node = next;
  }
  printf("**************************\n");
}

/** Return a new fixed-height skip list.
 */
c_spray_pq_t* c_spray_pq_create(int64_t threads) {
  c_spray_pq_t* spray_pq = forkscan_malloc(sizeof(c_spray_pq_t));
  spray_pq->config = c_spray_pq_config_paper(threads);
  spray_pq->head.key = INT64_MIN;
  spray_pq->head.toplevel = N - 1;
  atomic_store_explicit(&spray_pq->head.state, PADDING, memory_order_relaxed);
  spray_pq->tail.key = INT64_MAX;
  spray_pq->tail.toplevel = N - 1;
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
  atomic_store_explicit(&spray_pq->cleaner_lock, false, memory_order_relaxed);
  pthread_spin_init(&spray_pq->lock, PTHREAD_PROCESS_PRIVATE);
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

static void mark_pointers(node_ptr node) {
  node_ptr unmarked_node = node_unmark(node);
  assert(atomic_load_explicit(&node->state, memory_order_relaxed) == DELETED);
  for(int64_t level = unmarked_node->toplevel; level >= BOTTOM; --level) {
    while(true) {
      node_ptr succ = atomic_load_explicit(&unmarked_node->next[level], memory_order_relaxed);
      bool marked = node_is_marked(succ);
      if(marked) { break; }
      node_ptr temp = succ;
      bool success = atomic_compare_exchange_weak_explicit(&unmarked_node->next[level],
        &temp, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
      if(success) { break; }
    }
  }
}

static bool find(c_spray_pq_t *pqueue, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  bool marked, snip;
  // node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    node_ptr left = &pqueue->head, right = NULL;
    for(int64_t level = N - 1; level >= BOTTOM; --level) {
      node_ptr left_next = atomic_load_explicit(&left->next[level], memory_order_consume);
      // Is our current node invalid?
      if(node_is_marked(left_next)) { goto retry; }
      node_ptr right = left_next;
      // Find two nodes to put into preds and succs.
      while(true) {
        // Scan to the right so long as we find deleted nodes.
        node_ptr right_next = atomic_load_explicit(&right->next[level], memory_order_consume);
        while(node_is_marked(right_next)) {
          right = node_unmark(right_next);
          right_next = atomic_load_explicit(&right->next[level], memory_order_consume);
        }

        // Has the right not gone far enough?        
        if(right->key < key) {
          left = right;
          left_next = right_next;
          right = right_next;
        } else {
          // Right node is greater than our key, he's our succ, break.
          break;
        }
      }
      // Ensure the left node points to the right node, they must be adjacent.
      if(left_next != right) {
        bool success = atomic_compare_exchange_weak_explicit(&left->next[level], &left_next, right,
          memory_order_release, memory_order_relaxed);
        if(!success) { goto retry; }
      }
      preds[level] = left;
      succs[level] = right;
    }
    return succs[BOTTOM]->key == key;
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
      node_ptr found_node = succs[BOTTOM];
      state_t found_state = atomic_load_explicit(&found_node->state, memory_order_relaxed);
      if(found_state == DELETED) {
        mark_pointers(found_node);
        continue;
      }
      forkscan_free((void*)node);
      return false;
    }
    if(node == NULL) { node = node_create(key, toplevel, ACTIVE); }
    for(int64_t i = BOTTOM; i <= toplevel; ++i) {
      atomic_store_explicit(&node->next[i], succs[i], memory_order_release);
    }
    node_ptr pred = preds[BOTTOM], succ = succs[BOTTOM];
    if(!atomic_compare_exchange_weak_explicit(&pred->next[BOTTOM], &succ, node, memory_order_release, memory_order_relaxed)) {
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
    node_ptr node_to_remove = succs[BOTTOM];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level > BOTTOM; --level) {
      succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
      marked = node_is_marked(succ);
      while(!marked) {
        bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level],
          &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
        marked = node_is_marked(succ);
      }
    }
    succ = atomic_load_explicit(&node_to_remove->next[BOTTOM], memory_order_relaxed);
    marked = node_is_marked(succ);
    while(true) {
      bool i_marked_it = atomic_compare_exchange_weak_explicit(&node_to_remove->next[BOTTOM],
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
    node_ptr node_to_remove = succs[BOTTOM];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level > BOTTOM; --level) {
      succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
      marked = node_is_marked(succ);
      while(!marked) {
        bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level],
          &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed);
        marked = node_is_marked(succ);
      }
    }
    succ = atomic_load_explicit(&node_to_remove->next[BOTTOM], memory_order_relaxed);
    marked = node_is_marked(succ);
    while(true) {
      bool i_marked_it = atomic_compare_exchange_weak_explicit(&node_to_remove->next[BOTTOM],
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
  for(int64_t H = pqueue->config.start_height; H >= BOTTOM; H = H - D) {
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

  bool cleaner = ((fast_rand(seed) % (pqueue->config.thread_count)) == 0);
  retry:
  if(cleaner) {
    // bool locked = atomic_load_explicit(&pqueue->cleaner_lock, memory_order_acquire);
    // if(locked){ cleaner = false; goto retry; }
    // if(atomic_exchange_explicit(&pqueue->cleaner_lock, true, memory_order_acquire) != false) { cleaner = false; goto retry; }
    if(pthread_spin_trylock(&pqueue->lock) != 0) {
      cleaner = false;
      goto retry;
    }
    // Here. We. Go.
    node_ptr lefts[N], left_nexts[N];
    bool needs_swing[N];
    for(int64_t i = N - 1; i >= BOTTOM; i--) {
      lefts[i] = &pqueue->head;
      left_nexts[i] = atomic_load_explicit(&pqueue->head.next[i], memory_order_consume);
      needs_swing[i] = false;
    }

    // printf("BEFORE\n");
    // c_spray_pq_print(pqueue);

    bool claimed_node = false, in_prefix = false;
    while(true) {
      int64_t prefix_height = 0, valid_height = -1;
      node_ptr right = node_unmark(left_nexts[BOTTOM]);
      if(right == NULL) {
        // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
        pthread_spin_unlock(&pqueue->lock);
        return false; 
      }
      in_prefix = false;
      // TODO: Unmark?
      node_ptr right_next = atomic_load_explicit(&right->next[BOTTOM], memory_order_consume);
      // Node scan...
      while(true) {
        node_ptr unmarked_right = node_unmark(right);
        if(unmarked_right == &pqueue->tail) {
          bool done = true;
          for(int64_t i = 0; i < N; i++) {
            if(needs_swing[i]) { done = false;}
          }
          // We don't need to swing any pointers and we've seen the tail.
          if(done) {
            // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
            pthread_spin_unlock(&pqueue->lock);
            return false;
          }
          valid_height = unmarked_right->toplevel;
          break; 
        }
        state_t state = atomic_load_explicit(&unmarked_right->state, memory_order_relaxed);
        if(state == ACTIVE) {
          if(!claimed_node) {
            // Treat as another node to be discarded.
            claimed_node = (atomic_exchange_explicit(&unmarked_right->state, DELETED, memory_order_relaxed) == ACTIVE);
            mark_pointers(unmarked_right);
          } else {
            // If we're in a prefix save the height of the found node
            // so that we can swing it.
            valid_height = unmarked_right->toplevel;
            if(!in_prefix) {
              // We're not in a prefix and we've found the most rightward
              // valid node. Save it's address and the values of it's nexts.
              // printf("Not in prefix, found a valid node.\n");
              for(int64_t i = unmarked_right->toplevel; i >= BOTTOM; i--) {
                if(!needs_swing[i]) {
                  lefts[i] = unmarked_right;
                  left_nexts[i] = atomic_load_explicit(&unmarked_right->next[i], memory_order_consume);
                }
              }
            }
            in_prefix = false;
            break;
          }
        } else {
          mark_pointers(right);
        }
        // In deleted prefix.
        in_prefix = true;
        // mark_pointers(right);
        // The pointers at these levels need to move around this node.
        for(int64_t i = 0; i <= unmarked_right->toplevel; i++) {
          needs_swing[i] = true; 
        }
       
        right = node_unmark(right_next);
        right_next = atomic_load_explicit(&right->next[BOTTOM], memory_order_consume);
      }
      // Did we skip over anyone?
      // Or, did we find someone or interest?
      if(left_nexts[BOTTOM] != right_next || valid_height >= 0) {
        // printf("SWINGING POINTER BOIS AROUND\n");
        // c_spray_pq_print(pqueue);
        // printf("LEFTS\n");
        // for(int64_t i = 0; i < N; i++) {
        //   print_node(lefts[i]);
        // }
        // printf("LEFT NEXT\n");
        // for(int64_t i = 0; i < N; i++) {
        //   print_node(left_nexts[i]);
        // }
        // printf("REPLACEMENT\n");
        // print_node(right);
        // printf("Valid height %ld\n", valid_height);
        for(int64_t level = valid_height; level >= BOTTOM; level--) {
          // assert(claimed_node);
          if(needs_swing[level]) {
            // if(atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed) != DELETED) {
            //   printf("%ld, %d, %ld\n", atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed), left_nexts[BOTTOM] != right_next, valid_height);
            // }
            // assert(atomic_load_explicit(&node_unmark(left_nexts[level])->state, memory_order_relaxed) == DELETED);
            bool success = atomic_compare_exchange_weak_explicit(&lefts[level]->next[level], &left_nexts[level], right,
              memory_order_release, memory_order_relaxed);
            if(success) {
              lefts[level] = right;
              left_nexts[level] =  atomic_load_explicit(&right->next[level], memory_order_consume);
              needs_swing[level] = false;
            } else if(claimed_node) {
              // printf("AFTER failed CAS at level%ld\n", level);
              // c_spray_pq_print(pqueue);
              // assert(false);
              // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
              pthread_spin_unlock(&pqueue->lock);
              return true;
            } else {
              // Someone interrupted our swinging in this deleted prefix 
              // AND we don't have a claimed node. Try again.
              // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
              pthread_spin_unlock(&pqueue->lock);
              cleaner = false;
              goto retry;
            }
          }
        }
      } else { /*printf("NO CHANGES\n");*/ }
      valid_height = -1;
      bool done = true;
      for(int64_t i = 0; i < N; i++) {
        if(needs_swing[i]) { done = false;}
      }
      if(!done) { continue; }

      if(claimed_node) {
        // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
        pthread_spin_unlock(&pqueue->lock);
        return true;
      }
    }
  } else {
    node_ptr node = spray(seed, pqueue);
    // If we're not passed the head yet, start just after there.
    if(atomic_load_explicit(&node->state, memory_order_relaxed) == PADDING) {
      node = atomic_load_explicit(&pqueue->head.next[BOTTOM], memory_order_relaxed);
    }
    for(uint64_t i = 0; node != &pqueue->tail; node = node_unmark(atomic_load_explicit(&node->next[BOTTOM], memory_order_relaxed)), i++) {
      state_t state = atomic_load_explicit(&node->state, memory_order_relaxed);
      if(state == PADDING || state == DELETED) {
        continue;
      }
      if(state == ACTIVE && 
        (atomic_exchange_explicit(&node->state, DELETED, memory_order_relaxed) == ACTIVE)) {
        mark_pointers(node);
        return true;
      }
    }
    return false;
  }
}


int c_spray_pq_pop_min(uint64_t *seed, c_spray_pq_t *pqueue) {
  node_ptr node = spray(seed, pqueue);
  // If we're not passed the head yet, start just after there.
  if(atomic_load_explicit(&node->state, memory_order_relaxed) == PADDING) {
    node = atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed);
  }
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
}
