/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include "c_shavit_lotan_pqueue.h"

#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>


c_shavit_lotan_pq_node_ptr c_shavit_lotan_pq_node_create(int64_t key, int32_t toplevel){
  c_shavit_lotan_pq_node_ptr node = forkscan_malloc(sizeof(c_shavit_lotan_pq_node_t));
  node->key = key;
  node->toplevel = toplevel;
  node->deleted = false;
  return node;
}

c_shavit_lotan_pq_node_ptr sl_pqueue_node_unmark(c_shavit_lotan_pq_node_ptr node){
  return (c_shavit_lotan_pq_node_ptr)(((size_t)node) & (~0x1));
}

c_shavit_lotan_pq_node_ptr sl_pqueue_node_mark(c_shavit_lotan_pq_node_ptr node){
  return (c_shavit_lotan_pq_node_ptr)((size_t)node | 0x1);
}

bool sl_pqueue_node_is_marked(c_shavit_lotan_pq_node_ptr node){
  return sl_pqueue_node_unmark(node) != node;
}


typedef struct _c_shavit_lotan_pqueue_node_unpacked_t {
  bool marked;
  c_shavit_lotan_pq_node_ptr address;
} c_shavit_lotan_pqueue_node_unpacked_t;

c_shavit_lotan_pqueue_node_unpacked_t c_shavit_lotan_pqueue_node_unpack(c_shavit_lotan_pq_node_ptr node){
  return (c_shavit_lotan_pqueue_node_unpacked_t){
    .marked = sl_pqueue_node_is_marked(node),
    .address = sl_pqueue_node_unmark(node)
    };
}

void c_shavit_lotan_pqueue_print (c_shavit_lotan_pqueue_t *set){
  c_shavit_lotan_pq_node_ptr node = set->head.next[0];
  while(sl_pqueue_node_unmark(node) != &set->tail) {
    if(sl_pqueue_node_is_marked(node->next[0])) {
      node = sl_pqueue_node_unmark(node)->next[0];
    } else {
      node = sl_pqueue_node_unmark(node);
      printf("node[%d]: %ld\n", node->toplevel, node->key);
      node = node->next[0];
    }
  }
}

/** Return a new shavit lotan priority queue.
 */
c_shavit_lotan_pqueue_t* c_shavit_lotan_pqueue_create() {
  c_shavit_lotan_pqueue_t* sl_pqueue = forkscan_malloc(sizeof(c_shavit_lotan_pqueue_t));
  sl_pqueue->head.key = INT64_MIN;
  sl_pqueue->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    sl_pqueue->head.next[i] = &sl_pqueue->tail;
    sl_pqueue->tail.next[i] = NULL;
  }
  return sl_pqueue;
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

static bool find(c_shavit_lotan_pqueue_t *set, int64_t key, 
  c_shavit_lotan_pq_node_ptr preds[N], c_shavit_lotan_pq_node_ptr succs[N]) {
  bool marked, snip;
  c_shavit_lotan_pq_node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    pred = &set->head;
    for(int64_t level = N - 1; level >= 0; --level) {
      curr = sl_pqueue_node_unmark(pred->next[level]);
      while(true) {
        c_shavit_lotan_pqueue_node_unpacked_t unpacked_node = c_shavit_lotan_pqueue_node_unpack(curr->next[level]);
        succ = unpacked_node.address;
        marked = unpacked_node.marked;
        while(unpacked_node.marked) {
          snip = __sync_bool_compare_and_swap(&pred->next[level], curr, succ);
          if(!snip) {
            goto retry;
          }
          curr = sl_pqueue_node_unmark(pred->next[level]);
          unpacked_node = c_shavit_lotan_pqueue_node_unpack(curr->next[level]);
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

/** Add a node, lock-free, to the Shavit Lotan priority queue.
 */
int c_shavit_lotan_pqueue_add(uint64_t *seed, c_shavit_lotan_pqueue_t * set, int64_t key) {
  c_shavit_lotan_pq_node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  c_shavit_lotan_pq_node_ptr node = NULL;
  while(true) {
    if(find(set, key, preds, succs)) {
      if(node != NULL) {
        forkscan_free((void*)node);
      }
      return false;
    }
    if(node == NULL) { node = c_shavit_lotan_pq_node_create(key, toplevel); }
    for(int64_t i = 0; i <= toplevel; ++i) {
      node->next[i] = sl_pqueue_node_unmark(succs[i]);
    }
    c_shavit_lotan_pq_node_ptr pred = preds[0], succ = succs[0];
    if(!__sync_bool_compare_and_swap(&pred->next[0], sl_pqueue_node_unmark(succ), node)) {
      continue;
    }
    for(int64_t i = 1; i <= toplevel; i++) {
      while(true) {
        pred = preds[i], succ = succs[i];
        if(__sync_bool_compare_and_swap(&pred->next[i],
          sl_pqueue_node_unmark(succ), node)){
          break;
        }
        find(set, key, preds, succs);
      }
    }
    return true;
  }
}

/** Remove a node, lock-free, from the skiplist.
 */
int c_shavit_lotan_pqueue_remove_leaky(c_shavit_lotan_pqueue_t * set, int64_t key) {
  c_shavit_lotan_pq_node_ptr preds[N], succs[N];
  c_shavit_lotan_pq_node_ptr succ = NULL;
  while(true) {
    if(!find(set, key, preds, succs)) {
      return false;
    }
    c_shavit_lotan_pq_node_ptr node_to_remove = succs[0];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = node_to_remove->next[level];
      marked = sl_pqueue_node_is_marked(succ);
      while(!marked) {
        bool _ = __sync_bool_compare_and_swap(&node_to_remove->next[level],
          sl_pqueue_node_unmark(succ), sl_pqueue_node_mark(succ));
        succ = node_to_remove->next[level];
        marked = sl_pqueue_node_is_marked(succ);
      }
    }
    succ = node_to_remove->next[0];
    marked = sl_pqueue_node_is_marked(succ);
    while(true) {
      bool i_marked_it = __sync_bool_compare_and_swap(&node_to_remove->next[0],
        sl_pqueue_node_unmark(succ), sl_pqueue_node_mark(succ));
      succ = succs[0]->next[0];
      marked = sl_pqueue_node_is_marked(succ);
      if(i_marked_it) {
        find(set, key, preds, succs);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

/** Remove the minimum element in the Shavit Lotan priority queue.
 */
int c_shavit_lotan_pqueue_leaky_pop_min(c_shavit_lotan_pqueue_t * set) {

  for(c_shavit_lotan_pq_node_ptr curr = sl_pqueue_node_unmark(set->head.next[0]);
    curr != &set->tail;
    curr = sl_pqueue_node_unmark(curr->next[0])) {
    if(curr->deleted) {
      continue;
    }
    if(__sync_bool_compare_and_swap(&curr->deleted, false, true)){
      return c_shavit_lotan_pqueue_remove_leaky(set, curr->key);
    }
  }
  return false;
}
