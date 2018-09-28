/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl_lf.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>


typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct node_unpacked_t node_unpacked_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  _Atomic(node_ptr) next[N];
};

struct c_fhsl_lf_t {
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
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

/** Print out the contents of the skip list along with node heights.
 */
void c_fhsl_lf_print (c_fhsl_lf_t *set){
  node_ptr node = atomic_load_explicit(&set->head.next[0], memory_order_consume);
  while(node_unmark(node) != &set->tail) {
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
c_fhsl_lf_t * c_fhsl_lf_create() {
  c_fhsl_lf_t* fhsl_lf = forkscan_malloc(sizeof(c_fhsl_lf_t));
  fhsl_lf->head.key = INT64_MIN;
  fhsl_lf->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&fhsl_lf->head.next[i], &fhsl_lf->tail, memory_order_relaxed);
    atomic_store_explicit(&fhsl_lf->tail.next[i], NULL, memory_order_relaxed);
  }
  return fhsl_lf;
}

/** Return whether the skip list contains the value.
 */
int c_fhsl_lf_contains(c_fhsl_lf_t *set, int64_t key) {
  node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    node_ptr next = node_unmark(atomic_load_explicit(&node->next[i], memory_order_consume));
    while(next->key <= key) {
      node = next; 
      next = node_unmark(atomic_load_explicit(&node->next[i], memory_order_consume));
    }
    if(node->key == key) {
      return !node_is_marked(atomic_load_explicit(&node->next[0], memory_order_relaxed));
    }
  }
  return false;
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

static bool find(c_fhsl_lf_t *set, int64_t key, 
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
int c_fhsl_lf_add(uint64_t *seed, c_fhsl_lf_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    if(find(set, key, preds, succs)) {
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
        bool _ = find(set, key, preds, succs);
      }
    }
    return true;
  }
}

/** Remove a node, lock-free, from the skiplist.
 */
int c_fhsl_lf_remove_leaky(c_fhsl_lf_t * set, int64_t key) {
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
    if(marked) { return false; }
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

/** Remove a node, lock-free, from the skiplist.
 */
int c_fhsl_lf_remove(c_fhsl_lf_t * set, int64_t key) {
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
        forkscan_retire(node_to_remove);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

/** Pop the front node from the list.  Return true iff there was a node to pop.
 */
int c_fhsl_lf_pop_min_leaky (c_fhsl_lf_t *set) {
    node_ptr preds[N], succs[N];
    node_ptr succ = NULL;
    while(true) {
        node_ptr node_to_remove = atomic_load_explicit(&set->head.next[0], memory_order_relaxed);
        if (node_to_remove == &set->tail) {
            return false;
        }
        for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
          preds[level] = &set->head;
          succs[level] = node_to_remove;
        }

        for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
            succ = node_to_remove->next[level];
            bool marked = node_is_marked(succ);
            while(!marked) {
                bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level], &succ,
                              node_mark(succ), memory_order_relaxed, memory_order_relaxed);
                succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
                marked = node_is_marked(succ);
            }
        }
        succ = node_unmark(atomic_load_explicit(&node_to_remove->next[0], memory_order_relaxed));

        if (atomic_compare_exchange_weak_explicit(&node_to_remove->next[0], &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed)) {
            bool _ = find(set, node_to_remove->key, preds, succs);
            return true;
        }
    }
}


int c_fhsl_lf_pop_min(c_fhsl_lf_t *set) {
    node_ptr preds[N], succs[N];
    node_ptr succ = NULL;
    while(true) {
        node_ptr node_to_remove = atomic_load_explicit(&set->head.next[0], memory_order_relaxed);
        if (node_to_remove == &set->tail) {
            return false;
        }
        for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
          preds[level] = &set->head;
          succs[level] = node_to_remove;
        }

        for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
            succ = node_to_remove->next[level];
            bool marked = node_is_marked(succ);
            while(!marked) {
                bool _ = atomic_compare_exchange_weak_explicit(&node_to_remove->next[level], &succ,
                              node_mark(succ), memory_order_relaxed, memory_order_relaxed);
                succ = atomic_load_explicit(&node_to_remove->next[level], memory_order_relaxed);
                marked = node_is_marked(succ);
            }
        }
        succ = node_unmark(atomic_load_explicit(&node_to_remove->next[0], memory_order_relaxed));

        if (atomic_compare_exchange_weak_explicit(&node_to_remove->next[0], &succ, node_mark(succ), memory_order_relaxed, memory_order_relaxed)) {
            bool _ = find(set, node_to_remove->key, preds, succs);
            forkscan_retire(node_to_remove);
            return true;
        }
    }
}