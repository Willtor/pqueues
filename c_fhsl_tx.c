/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl_tx.h"
#include "elided_lock.h"

#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>


typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct node_unpacked_t node_unpacked_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  node_ptr next[N];
};

struct c_fhsl_tx_t {
  elided_lock_t *lock;
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  return node;
}

/** Print out the contents of the skip list along with node heights.
 */
void c_fhsl_tx_print (c_fhsl_tx_t *set){
  lock(set->lock);
  node_ptr node = set->head.next[0];
  while(node != &set->tail) {
    node_ptr next = node->next[0];
    printf("node[%d]: %ld\n", node->toplevel, node->key);
    node = next;
  }
  unlock(set->lock);
}

/** Return a new fixed-height skip list.
 */
c_fhsl_tx_t * c_fhsl_tx_create() {
  c_fhsl_tx_t* fhsl_tx = forkscan_malloc(sizeof(c_fhsl_tx_t));
  fhsl_tx->head.key = INT64_MIN;
  fhsl_tx->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    fhsl_tx->head.next[i] = &fhsl_tx->tail;
    fhsl_tx->tail.next[i] = NULL;
  }
  fhsl_tx->lock = create_elided_lock();
  return fhsl_tx;
}

int c_fhsl_tx_contains(c_fhsl_tx_t *set, int64_t key) {
  lock(set->lock);
  node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    node_ptr next = node->next[i];
    while(next->key <= key) {
      node = next;
      next = node->next[i];
    }
    if(node->key == key) {
      unlock(set->lock);
      return true;
    }
  }
  unlock(set->lock);
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

static bool find(c_fhsl_tx_t *set, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  node_ptr prev = &set->head;
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

int c_fhsl_tx_add(uint64_t *seed, c_fhsl_tx_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = node_create(key, toplevel);
  bool added = false;
  lock(set->lock);
  if(!find(set, key, preds, succs)) {
    for(int64_t i = 0; i <= toplevel; i++) {
      node->next[i] = succs[i];
      preds[i]->next[i] = node;
    }
    added = true;
  }
  unlock(set->lock);
  if(!added) { forkscan_free(node); }
  return added;
}


int c_fhsl_tx_remove_leaky(c_fhsl_tx_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(set->lock);
  if(find(set, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(set->lock);  
  return node != NULL;
}

int c_fhsl_tx_remove(c_fhsl_tx_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(set->lock);
  if(find(set, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(set->lock);
  forkscan_retire((void*)node);
  return node != NULL;
}


int c_fhsl_tx_pop_min_leaky(c_fhsl_tx_t *set) {
  node_ptr node_popped = NULL;
  lock(set->lock);
  node_ptr head_node = set->head.next[0];
  if(head_node != &set->tail) {
    node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      set->head.next[i] = node_popped->next[i];
    }
  }
  unlock(set->lock);
  return node_popped != NULL;
}

int c_fhsl_tx_pop_min(c_fhsl_tx_t *set) {
  node_ptr node_popped = NULL;
  lock(set->lock);
  node_ptr head_node = set->head.next[0];
  if(head_node != &set->tail) {
    node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      set->head.next[i] = node_popped->next[i];
    }
  }
  unlock(set->lock);
  forkscan_retire((void*)node_popped);
  return node_popped != NULL;
}