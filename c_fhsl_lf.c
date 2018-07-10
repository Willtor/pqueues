/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl_lf.h"

#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>


fhsl_lf_node_ptr c_fhsl_node_create(int64_t key, int32_t toplevel){
  fhsl_lf_node_ptr node = forkscan_malloc(sizeof(fhsl_lf_node_t));
  node->key = key;
  node->toplevel = toplevel;
  return node;
}

fhsl_lf_node_ptr fhsl_lf_node_unmark(fhsl_lf_node_ptr node){
  return (fhsl_lf_node_ptr)(((size_t)node) & (~0x1));
}

fhsl_lf_node_ptr fhsl_lf_node_mark(fhsl_lf_node_ptr node){
  return (fhsl_lf_node_ptr)((size_t)node | 0x1);
}

bool fhsl_lf_node_is_marked(fhsl_lf_node_ptr node){
  return fhsl_lf_node_unmark(node) != node;
}


typedef struct _c_fhsl_lf_node_unpacked_t {
  bool marked;
  fhsl_lf_node_ptr address;
} c_fhsl_lf_node_unpacked_t;

c_fhsl_lf_node_unpacked_t fhsl_lf_node_unpack(fhsl_lf_node_ptr node){
  return (c_fhsl_lf_node_unpacked_t){
    .marked = fhsl_lf_node_is_marked(node),
    .address = fhsl_lf_node_unmark(node)
    };
}

/** Print out the contents of the skip list along with node heights.
 */
void c_fhsl_lf_print (c_fhsl_lf_t *set){
  fhsl_lf_node_ptr node = set->head.next[0];
  while(fhsl_lf_node_unmark(node) != &set->tail) {
    if(fhsl_lf_node_is_marked(node->next[0])) {
      node = fhsl_lf_node_unmark(node)->next[0];
    } else {
      node = fhsl_lf_node_unmark(node);
      printf("node[%d]: %ld\n", node->toplevel, node->key);
      node = node->next[0];
    }
  }
}

/** Return a new fixed-height skip list.
 */
c_fhsl_lf_t * c_fhsl_lf_create() {
  c_fhsl_lf_t* fhsl_lf = forkscan_malloc(sizeof(c_fhsl_lf_t));
  fhsl_lf->head.key = INT64_MIN;
  fhsl_lf->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    fhsl_lf->head.next[i] = &fhsl_lf->tail;
    fhsl_lf->tail.next[i] = NULL;
  }
  return fhsl_lf;
}

/** Return whether the skip list contains the value.
 */
int c_fhsl_lf_contains(c_fhsl_lf_t *set, int64_t key) {
  fhsl_lf_node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    fhsl_lf_node_ptr next = fhsl_lf_node_unmark(node->next[i]);
    while(next->key <= key) {
      node = next;
      next = fhsl_lf_node_unmark(node->next[i]);
    }
    if(node->key == key) {
      return fhsl_lf_node_is_marked(node->next[0]);
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
  fhsl_lf_node_ptr preds[N], fhsl_lf_node_ptr succs[N]) {
  bool marked, snip;
  fhsl_lf_node_ptr pred = NULL, curr = NULL, succ = NULL;
retry:
  while(true) {
    pred = &set->head;
    for(int64_t level = N - 1; level >= 0; --level) {
      curr = fhsl_lf_node_unmark(pred->next[level]);
      while(true) {
        c_fhsl_lf_node_unpacked_t unpacked_node = fhsl_lf_node_unpack(curr->next[level]);
        succ = unpacked_node.address;
        marked = unpacked_node.marked;
        while(unpacked_node.marked) {
          snip = __sync_bool_compare_and_swap(&pred->next[level], curr, succ);
          if(!snip) {
            goto retry;
          }
          curr = fhsl_lf_node_unmark(pred->next[level]);
          unpacked_node = fhsl_lf_node_unpack(curr->next[level]);
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

/** Add a node, lock-free, to the skiplist.
 */
int c_fhsl_lf_add(uint64_t *seed, c_fhsl_lf_t * set, int64_t key) {
  fhsl_lf_node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  fhsl_lf_node_ptr node = NULL;
  while(true) {
    if(find(set, key, preds, succs)) {
      if(node != NULL) {
        forkscan_free((void*)node);
      }
      return false;
    }
    if(node == NULL) { node = c_fhsl_node_create(key, toplevel); }
    for(int64_t i = 0; i <= toplevel; ++i) {
      node->next[i] = fhsl_lf_node_unmark(succs[i]);
    }
    fhsl_lf_node_ptr pred = preds[0], succ = succs[0];
    if(!__sync_bool_compare_and_swap(&pred->next[0], fhsl_lf_node_unmark(succ), node)) {
      continue;
    }
    for(int64_t i = 1; i <= toplevel; i++) {
      while(true) {
        pred = preds[i], succ = succs[i];
        if(__sync_bool_compare_and_swap(&pred->next[i],
          fhsl_lf_node_unmark(succ), node)){
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
int c_fhsl_lf_remove_leaky(c_fhsl_lf_t * set, int64_t key) {
  fhsl_lf_node_ptr preds[N], succs[N];
  fhsl_lf_node_ptr succ = NULL;
  while(true) {
    if(!find(set, key, preds, succs)) {
      return false;
    }
    fhsl_lf_node_ptr node_to_remove = succs[0];
    bool marked;
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = node_to_remove->next[level];
      marked = fhsl_lf_node_is_marked(succ);
      while(!marked) {
        bool _ = __sync_bool_compare_and_swap(&node_to_remove->next[level],
          fhsl_lf_node_unmark(succ), fhsl_lf_node_mark(succ));
        succ = node_to_remove->next[level];
        marked = fhsl_lf_node_is_marked(succ);
      }
    }
    succ = node_to_remove->next[0];
    marked = fhsl_lf_node_is_marked(succ);
    while(true) {
      bool i_marked_it = __sync_bool_compare_and_swap(&node_to_remove->next[0],
        fhsl_lf_node_unmark(succ), fhsl_lf_node_mark(succ));
      succ = succs[0]->next[0];
      marked = fhsl_lf_node_is_marked(succ);
      if(i_marked_it) {
        find(set, key, preds, succs);
        return true;
      } else if(marked) {
        return false;
      }
    }
  }
}

/** Pop the front node from the list.  Return true iff there was a node to pop.
 *  Leak the memory.
 */
int c_fhsl_lf_leaky_pop_min(c_fhsl_lf_t * set) {
  fhsl_lf_node_ptr preds[N], succs[N];
  fhsl_lf_node_ptr succ;
  while(true) {
    fhsl_lf_node_ptr node_to_remove = set->head.next[0];
    if(node_to_remove == &set->tail) {
      return false;
    }
    for(int64_t level = node_to_remove->toplevel; level >= 0; --level) {
      preds[level] = &set->head;
      succs[level] = node_to_remove;
    }
    for(int64_t level = node_to_remove->toplevel; level >= 1; --level) {
      succ = node_to_remove->next[level];
      bool marked = fhsl_lf_node_is_marked(succ);
      while(!marked) {
        __sync_bool_compare_and_swap(&node_to_remove->next[level],
          fhsl_lf_node_unmark(succ), fhsl_lf_node_mark(succ));
        succ = node_to_remove->next[level];
        marked = fhsl_lf_node_is_marked(succ);
      }
    }
    succ = node_to_remove->next[0];
    if(__sync_bool_compare_and_swap(&node_to_remove->next[0],
      fhsl_lf_node_unmark(succ), fhsl_lf_node_mark(succ))) {
        find(set, node_to_remove->key, preds, succs);
        return true;
      }
  }
}
