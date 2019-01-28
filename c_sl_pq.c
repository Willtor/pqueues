/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include "c_sl_pq.h"
#include "utils.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <assert.h>

#define N 20
#define BOTTOM 0

typedef struct node_t node_t; 
typedef node_t *node_ptr;
typedef struct node_unpacked_t node_unpacked_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  atomic_bool deleted;
  _Atomic(node_ptr) next[N];
};

struct c_sl_pq_t {
  node_t head, tail;
};

struct node_unpacked_t {
  bool marked;
  node_ptr address;
};

static node_ptr node_create(int64_t key, int32_t toplevel){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  atomic_store_explicit(&node->deleted, false, memory_order_relaxed);
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

static node_unpacked_t node_unpack(node_ptr node){
  return (node_unpacked_t){
    .marked = node_is_marked(node),
    .address = node_unmark(node)
    };
}

void c_sl_pq_print (c_sl_pq_t *pqueue){
  node_ptr node = atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed);
  while(node_unmark(node) != &pqueue->tail) {
    node_ptr next = atomic_load_explicit(&node->next[0], memory_order_consume);
    if(!node_is_marked(next)) {
      printf("node[%d]: %ld\n", node->toplevel, node->key);
    }
    node = next;
  }
}

/** Return a new shavit lotan priority queue.
 */
c_sl_pq_t* c_sl_pq_create() {
  c_sl_pq_t* sl_pqueue = forkscan_malloc(sizeof(c_sl_pq_t));
  sl_pqueue->head.key = INT64_MIN;
  sl_pqueue->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&sl_pqueue->head.next[i], &sl_pqueue->tail, memory_order_relaxed);
    atomic_store_explicit(&sl_pqueue->tail.next[i], NULL, memory_order_relaxed);
  }
  return sl_pqueue;
}


static void mark_pointers(node_ptr node) {
  node_ptr unmarked_node = node_unmark(node);
  assert(atomic_load_explicit(&node->deleted, memory_order_relaxed));
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

static bool find(c_sl_pq_t *pqueue, int64_t key, 
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

/** Add a node, lock-free, to the Shavit Lotan priority queue.
 */
int c_sl_pq_add(uint64_t *seed, c_sl_pq_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find(pqueue, key, preds, succs)) {
      if(succs[BOTTOM]->deleted) {
        mark_pointers(succs[BOTTOM]);
        continue;
      }
      forkscan_free((void*)node);
      return false;
    }
    if(node == NULL) { node = node_create(key, toplevel); }
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
int c_sl_pq_remove_leaky(c_sl_pq_t * pqueue, int64_t key) {
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
int c_sl_pq_remove(c_sl_pq_t * pqueue, int64_t key) {
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

/** Remove the minimum element in the Shavit Lotan priority queue.
 */
int c_sl_pq_leaky_pop_min(c_sl_pq_t * pqueue) {
  node_ptr left_next = node_unmark(atomic_load_explicit(&pqueue->head.next[BOTTOM], memory_order_consume));
  if(left_next == &pqueue->tail) { return false; }
  node_ptr curr = left_next;
  for(; curr != &pqueue->tail; curr = node_unmark(atomic_load_explicit(&curr->next[BOTTOM], memory_order_consume))) {
    if(atomic_load_explicit(&curr->deleted, memory_order_relaxed)) {
      mark_pointers(curr);
      continue;
    }
    if(!atomic_exchange_explicit(&curr->deleted, true, memory_order_relaxed)){
      mark_pointers(curr);
      return true;
    }
  }
  return false;
}

/** Remove the minimum element in the Shavit Lotan priority queue.
 */
int c_sl_pq_pop_min(c_sl_pq_t * pqueue) {
  while(true) {
    node_ptr curr = node_unmark(atomic_load_explicit(&pqueue->head.next[0], memory_order_consume));
    if(curr == &pqueue->tail) {
      return false;
    }
    for(; curr != &pqueue->tail; curr = node_unmark(atomic_load_explicit(&curr->next[0], memory_order_consume))) {
      if(atomic_load_explicit(&curr->deleted, memory_order_relaxed)) {
        continue;
      }
      if(!atomic_exchange_explicit(&curr->deleted, true, memory_order_relaxed)){
        mark_pointers(curr);
        forkscan_retire(curr);
        return true;
      }
    }
  }
}
