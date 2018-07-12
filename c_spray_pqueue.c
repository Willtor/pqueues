/* A spray-list priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and has relaxed correctness semantics.
 */

#include "c_spray_pqueue.h"

#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>
#include <math.h>


c_spray_pqueue_node_ptr c_spray_pqueue_node_create(int64_t key, int32_t toplevel, state_t state){
  c_spray_pqueue_node_ptr node = forkscan_malloc(sizeof(c_spray_pqueue_node_t));
  node->key = key;
  node->toplevel = toplevel;
  node->state = state;
  return node;
}

c_spray_pqueue_node_ptr c_spray_pqueue_node_unmark(c_spray_pqueue_node_ptr node){
  return (c_spray_pqueue_node_ptr)(((size_t)node) & (~0x1));
}

c_spray_pqueue_node_ptr c_spray_pqueue_node_mark(c_spray_pqueue_node_ptr node){
  return (c_spray_pqueue_node_ptr)((size_t)node | 0x1);
}

bool c_spray_pqueue_node_is_marked(c_spray_pqueue_node_ptr node){
  return c_spray_pqueue_node_unmark(node) != node;
}


typedef struct _c_spray_pqueue_node_unpacked_t {
  bool marked;
  c_spray_pqueue_node_ptr address;
} c_spray_pqueue_node_unpacked_t;

c_spray_pqueue_node_unpacked_t c_spray_pqueue_node_unpack(c_spray_pqueue_node_ptr node){
  return (c_spray_pqueue_node_unpacked_t){
    .marked = c_spray_pqueue_node_is_marked(node),
    .address = c_spray_pqueue_node_unmark(node)
    };
}


c_spray_pqueue_config_t c_spray_pqueue_config_paper(int64_t threads) {
  int64_t log_arg = threads;
  if(threads == 1) { log_arg = 2; }
  return (c_spray_pqueue_config_t) {
    .thread_count = threads,
    .start_height = log2(threads) + 1,
    .max_jump = log2(threads) + 1,
    .descend_amount = 1,
    .padding_amount = log2(log_arg)
  };
}

/** Return a new spray list with parameters tuned to some specified thread count.
 */
c_spray_pqueue_t* c_spray_pqueue_create(int64_t threads) {
  c_spray_pqueue_t* spray_pqueue = forkscan_malloc(sizeof(c_spray_pqueue_t));
  spray_pqueue->config = c_spray_pqueue_config_paper(threads);
  spray_pqueue->head.key = INT64_MIN;
  spray_pqueue->head.state = PADDING;
  spray_pqueue->tail.key = INT64_MAX;
  spray_pqueue->tail.state = PADDING;
  for(int64_t i = 0; i < N; i++) {
    spray_pqueue->head.next[i] = &spray_pqueue->tail;
    spray_pqueue->tail.next[i] = NULL;
  }
  spray_pqueue->padding_head = &spray_pqueue->head;
  for(int64_t i = 1; i < spray_pqueue->config.padding_amount; i++) {
    c_spray_pqueue_node_ptr node = forkscan_malloc(sizeof(c_spray_pqueue_node_t));
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) { 
      node->next[j] = spray_pqueue->padding_head;
    }
    spray_pqueue->padding_head = node;
  }
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
static c_spray_pqueue_node_ptr spray(uint64_t * seed, c_spray_pqueue_t * set) {
  c_spray_pqueue_node_ptr cur_node = set->padding_head;
  int64_t D = set->config.descend_amount;
  for(int64_t H = set->config.start_height; H >= 0; H = H - D) {
    int64_t jump = (fast_rand(seed) % set->config.max_jump) + 1;
    while(jump-- > 0) {
      c_spray_pqueue_node_ptr next = c_spray_pqueue_node_unmark(cur_node->next[H]);
      if(next == NULL) {
        break;
      }
      cur_node = next;
    }
  }
  return cur_node;
}

static void print_node(c_spray_pqueue_node_ptr node) {
  printf("node[%d]: %ld", node->toplevel, node->key);
  if(node->state == DELETED) {
    printf(" state: DELETED\n");
  } else if(node->state == PADDING) {
    printf(" state: PADDING\n");
  } else {
    printf(" state: ACTIVE\n");
  }

}

void c_spray_pqueue_print (c_spray_pqueue_t *set) {
  // Padding
  for(c_spray_pqueue_node_ptr curr = set->padding_head;
    curr != &set->head;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Head
  print_node(&set->head);
  // Everything in between
  for(c_spray_pqueue_node_ptr curr = c_spray_pqueue_node_unmark(set->head.next[0]);
    curr != &set->tail;
    curr = c_spray_pqueue_node_unmark(curr->next[0])) {
    print_node(curr);
  }
  // Tail
  print_node(&set->tail);
}

static bool find(c_spray_pqueue_t *set, int64_t key, 
  c_spray_pqueue_node_ptr preds[N], c_spray_pqueue_node_ptr succs[N]) {
  bool marked, snip;
  c_spray_pqueue_node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    pred = &set->head;
    for(int64_t level = N - 1; level >= 0; --level) {
      curr = c_spray_pqueue_node_unmark(pred->next[level]);
      while(true) {
        c_spray_pqueue_node_unpacked_t unpacked_node = c_spray_pqueue_node_unpack(curr->next[level]);
        succ = unpacked_node.address;
        marked = unpacked_node.marked;
        while(unpacked_node.marked) {
          snip = __sync_bool_compare_and_swap(&pred->next[level], curr, succ);
          if(!snip) {
            goto retry;
          }
          curr = c_spray_pqueue_node_unmark(pred->next[level]);
          unpacked_node = c_spray_pqueue_node_unpack(curr->next[level]);
          succ = unpacked_node.address;
          marked = unpacked_node.marked;
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
int c_spray_pqueue_add(uint64_t *seed, c_spray_pqueue_t *set, int64_t key) {
  c_spray_pqueue_node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  c_spray_pqueue_node_ptr node = NULL;
  while(true) {
    if(find(set, key, preds, succs)) {
      if(node != NULL) {
        forkscan_free((void*)node);
      }
      return false;
    }
    if(node == NULL) { node = c_spray_pqueue_node_create(key, toplevel, ACTIVE); }
    for(int64_t i = 0; i <= toplevel; ++i) {
      node->next[i] = c_spray_pqueue_node_unmark(succs[i]);
    }
    c_spray_pqueue_node_ptr pred = preds[0], succ = succs[0];
    if(!__sync_bool_compare_and_swap(&pred->next[0], c_spray_pqueue_node_unmark(succ), node)) {
      continue;
    }
    for(int64_t i = 1; i <= toplevel; i++) {
      while(true) {
        pred = preds[i], succ = succs[i];
        if(__sync_bool_compare_and_swap(&pred->next[i],
          c_spray_pqueue_node_unmark(succ), node)) {
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
int c_spray_pqueue_remove_leaky(c_spray_pqueue_t *set, int64_t key) {
  c_spray_pqueue_node_ptr preds[N], succs[N];
  c_spray_pqueue_node_ptr succ = NULL;
  while(true) {
    if(!find(set, key, preds, succs)) {
      return false;
    }
    c_spray_pqueue_node_ptr node_to_remove = succs[0];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = node_to_remove->next[level];
      marked = c_spray_pqueue_node_is_marked(succ);
      while(!marked) {
        bool _ = __sync_bool_compare_and_swap(&node_to_remove->next[level],
          c_spray_pqueue_node_unmark(succ), c_spray_pqueue_node_mark(succ));
        succ = node_to_remove->next[level];
        marked = c_spray_pqueue_node_is_marked(succ);
      }
    }
    succ = node_to_remove->next[0];
    marked = c_spray_pqueue_node_is_marked(succ);
    while(true) {
      bool i_marked_it = __sync_bool_compare_and_swap(&node_to_remove->next[0],
        c_spray_pqueue_node_unmark(succ), c_spray_pqueue_node_mark(succ));
      succ = succs[0]->next[0];
      marked = c_spray_pqueue_node_is_marked(succ);
      if(i_marked_it) {
        find(set, key, preds, succs);
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
int c_spray_pqueue_leaky_pop_min(uint64_t *seed, c_spray_pqueue_t *set) {
  while(true) {
    bool cleaner = (fast_rand(seed) % (set->config.thread_count + 1)) == 0;
    if(cleaner) {
      // TODO: Shouldn't scan the whole list.
      // Just the max length of the spray.
      size_t dist = 0, limit = set->config.padding_amount * set->config.padding_amount;
      limit = limit < 5 ? 5 : limit;
      for(c_spray_pqueue_node_ptr curr =
        c_spray_pqueue_node_unmark(set->head.next[0]);
        curr != &set->tail;
        curr = c_spray_pqueue_node_unmark(curr->next[0]), dist++){
        if(curr->state == DELETED) {
          c_spray_pqueue_remove_leaky(set, curr->key);
        }
        if(dist == limit) {
          break;
        }
      }
      //c_spray_pqueue_print(set);
    }
    bool empty = set->head.next[0] == &set->tail;
    if(empty) {
      return false;
    }

    c_spray_pqueue_node_ptr node = spray(seed, set);
    if(node->state == PADDING || node->state == DELETED){
      continue;
    }

    if(__sync_bool_compare_and_swap(&node->state, ACTIVE, DELETED)) {
      return true;
    }
  }
  return false;
}
