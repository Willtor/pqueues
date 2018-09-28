/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include "c_sl_pq.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>


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

static bool find(c_sl_pq_t *pqueue, int64_t key, 
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

/** Add a node, lock-free, to the Shavit Lotan priority queue.
 */
int c_sl_pq_add(uint64_t *seed, c_sl_pq_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find(pqueue, key, preds, succs)) {
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
  while(true) {
    node_ptr curr = node_unmark(atomic_load_explicit(&pqueue->head.next[0], memory_order_consume));
    if(curr == &pqueue->tail) {
      return false;
    }
    for(; curr != &pqueue->tail; curr = node_unmark(atomic_load_explicit(&curr->next[0], memory_order_consume))) {
      if(curr->deleted) {
        continue;
      }
      if(!atomic_exchange_explicit(&curr->deleted, true, memory_order_relaxed)){
        bool res = c_sl_pq_remove_leaky(pqueue, curr->key);
        return true;
      }
    }
  }
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
        bool res = c_sl_pq_remove(pqueue, curr->key);
        return true;
      }
    }
  }
}
