/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_lj_pq.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <assert.h>

typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct unpacked_t unpacked_t; 

enum LJ_STATE { INSERT_PENDING, INSERTED };
typedef enum LJ_STATE state_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  _Atomic(state_t) insert_state;
  _Atomic(node_ptr) next[N];
};

struct c_lj_pq_t {
  uint32_t boundoffset;
  node_t head, tail;
};

static node_ptr node_create(int64_t key, int32_t toplevel){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  atomic_store_explicit(&node->insert_state, INSERT_PENDING, memory_order_relaxed);
  return node;
}

static node_ptr unmark(node_ptr node){
  return (node_ptr)(((size_t)node) & (~0x1));
}

static node_ptr mark(node_ptr node){
  return (node_ptr)((size_t)node | 0x1);
}

static bool is_marked(node_ptr node){
  return unmark(node) != node;
}


/** Print out the contents of the skip list along with node heights.
 */
void c_lj_pq_print (c_lj_pq_t *pqueue){
  node_ptr node = atomic_load_explicit(&pqueue->head.next[0], memory_order_consume);
  while(unmark(node) != &pqueue->tail) {
    node_ptr unmarked_node = unmark(node);
    printf("node[%d]: %ld deleted: %d\n", unmarked_node->toplevel, unmarked_node->key, is_marked(node));
    node = atomic_load_explicit(&unmarked_node->next[0], memory_order_relaxed);
  }
}

/** Return a new fixed-height skip list.
 */
c_lj_pq_t * c_lj_pq_create(uint32_t boundoffset) {
  c_lj_pq_t* lj_pqueue = forkscan_malloc(sizeof(c_lj_pq_t));
  lj_pqueue->boundoffset = boundoffset;
  lj_pqueue->head.key = INT64_MIN;
  atomic_store_explicit(&lj_pqueue->head.insert_state, INSERTED, memory_order_relaxed);
  lj_pqueue->tail.key = INT64_MAX;
  atomic_store_explicit(&lj_pqueue->tail.insert_state, INSERTED, memory_order_relaxed);
  for(int64_t i = 0; i < N; i++) {
    atomic_store_explicit(&lj_pqueue->head.next[i], &lj_pqueue->tail, memory_order_relaxed);
    atomic_store_explicit(&lj_pqueue->tail.next[i], NULL, memory_order_relaxed);
  }
  return lj_pqueue;
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


static node_ptr locate_preds(
  c_lj_pq_t *pqueue, 
  int64_t key,
  node_ptr preds[N],
  node_ptr succs[N]) {
  node_ptr cur = &pqueue->head, next = NULL, del = NULL;
  int32_t level = N - 1;
  bool deleted = false;
  while(level >= 0) {
    next = atomic_load_explicit(&cur->next[level], memory_order_consume);
    deleted = is_marked(next);
    next = unmark(next);

    while(next->key < key || 
      is_marked(atomic_load_explicit(&next->next[0], memory_order_relaxed)) || 
      ((level == 0) && deleted)) {
      if(level == 0 && deleted) {
        del = next;
      }
      cur = next;
      next = atomic_load_explicit(&next->next[level], memory_order_relaxed);
      deleted = is_marked(next);
      next = unmark(next);
    }
    preds[level] = cur;
    succs[level] = next;
    level--;
  }
  return del;
}

/** Add a node, lock-free, to the skiplist.
 */
int c_lj_pq_add(uint64_t *seed, c_lj_pq_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = NULL;
  while(true) {
    node_ptr del = locate_preds(pqueue, key, preds, succs);
    node_ptr pred_next = atomic_load_explicit(&preds[0]->next[0], memory_order_relaxed);
    if(succs[0]->key == key &&
      !is_marked(pred_next) &&
      pred_next == succs[0]) {
      if(node != NULL) { forkscan_free((void*)node); }
      return false;
    }

    if(node == NULL) { node = node_create(key, toplevel); }
    for(int64_t i = 0; i <= toplevel; ++i) { atomic_store_explicit(&node->next[i], succs[i], memory_order_release); }
    node_ptr pred = preds[0], succ = succs[0];
    if(!atomic_compare_exchange_weak_explicit(&pred->next[0], &succ, node, memory_order_release, memory_order_relaxed)) { continue; }

    for(int64_t i = 1; i <= toplevel; i++) {

      if(is_marked(atomic_load_explicit(&node->next[0], memory_order_relaxed)) ||
        is_marked(atomic_load_explicit(&succs[i]->next[0], memory_order_relaxed)) ||
        del == succs[i]) {
        node->insert_state = INSERTED;
        return true;
      }

      atomic_store_explicit(&node->next[i], succs[i], memory_order_release);

      if(!atomic_compare_exchange_weak_explicit(&preds[i]->next[i], &succs[i], node, memory_order_release, memory_order_relaxed)) {
        del = locate_preds(pqueue, key, preds, succs);
        if(succs[0] != node) {
          atomic_store_explicit(&node->insert_state, INSERTED, memory_order_relaxed);
          return true;
        }
      }
    }
    atomic_store_explicit(&node->insert_state, INSERTED, memory_order_relaxed);
    return true;
  }
}


static void restructure(c_lj_pq_t *pqueue) {
  node_ptr pred = NULL, cur = NULL, head = NULL;
  int32_t level = N - 1;
  pred = &pqueue->head;
  while(level > 0) {
    head = atomic_load_explicit(&pqueue->head.next[level], memory_order_consume);
    cur = atomic_load_explicit(&pred->next[level], memory_order_consume);
    if(!is_marked(atomic_load_explicit(&head->next[0], memory_order_consume))) {
      level--;
      continue;
    }
    while(is_marked(atomic_load_explicit(&cur->next[0], memory_order_consume))) {
      pred = cur;
      cur = atomic_load_explicit(&pred->next[level], memory_order_consume);
    }
    if(atomic_compare_exchange_weak_explicit(&pqueue->head.next[level], 
      &head, cur, memory_order_release, memory_order_consume)){
      level--;
    }
  }
}


/** Pop the front node from the list.  Return true iff there was a node to pop.
 *  Leak the memory.
 */
int c_lj_pq_leaky_pop_min(c_lj_pq_t * pqueue) {
  node_ptr cur = &pqueue->head, next = NULL, newhead = NULL,
    obs_head = atomic_load_explicit(&cur->next[0], memory_order_relaxed);
  int32_t offset = 0;
  do {
    offset++;
    next = atomic_load_explicit(&cur->next[0], memory_order_consume);
    if(unmark(next) == &pqueue->tail) { return false; }
    if(newhead == NULL && atomic_load_explicit(&cur->insert_state, memory_order_relaxed) == INSERT_PENDING) { newhead = cur; }
    if(is_marked(next)) { continue; }
    // Yuck
    next = atomic_fetch_or_explicit((_Atomic(uintptr_t)*)&cur->next[0], 1, memory_order_relaxed);
  } while((cur = unmark(next)) && is_marked(next));
  

  if(newhead == NULL) { newhead = cur; }
  if(offset <= pqueue->boundoffset) { return true; }
  if(atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed) != obs_head) { return true; }

  if(atomic_compare_exchange_weak_explicit(&pqueue->head.next[0], &obs_head, mark(newhead), memory_order_release, memory_order_relaxed)) {
    restructure(pqueue);
  }
  return true;
}

/** Pop the front node from the list.  Return true iff there was a node to pop.
 */
int c_lj_pq_pop_min(c_lj_pq_t * pqueue) {
  node_ptr cur = &pqueue->head, next = NULL, newhead = NULL,
    obs_head = NULL;
  int32_t offset = 0;
  obs_head = atomic_load_explicit(&cur->next[0], memory_order_consume);
  do {
    offset++;
    next = atomic_load_explicit(&cur->next[0], memory_order_consume);
    if(unmark(next) == &pqueue->tail) { return false; }
    if(newhead == NULL && atomic_load_explicit(&cur->insert_state, memory_order_relaxed) == INSERT_PENDING) { newhead = cur; }
    if(is_marked(next)) { continue; }
    // Yuck
    next = atomic_fetch_or_explicit((_Atomic(uintptr_t)*)&cur->next[0], 1, memory_order_relaxed);
  } while((cur = unmark(next)) && is_marked(next));
  

  if(newhead == NULL) { newhead = cur; }
  if(offset <= pqueue->boundoffset) { return true; }
  if(atomic_load_explicit(&pqueue->head.next[0], memory_order_relaxed) != obs_head) { return true; }

  if(atomic_compare_exchange_weak_explicit(&pqueue->head.next[0], &obs_head, mark(newhead), memory_order_release, memory_order_relaxed)) {
    restructure(pqueue);
    cur = unmark(obs_head);
    while (cur != unmark(newhead)) {
      next = unmark(cur->next[0]);
      forkscan_retire((void*) cur);
      cur = next;
    }
  }
  return true;
}