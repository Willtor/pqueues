/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_linden_jonsson_pqueue.h"

#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>
#include <assert.h>


c_linden_jonsson_node_ptr c_linden_jonsson_create(int64_t key, int32_t toplevel){
  c_linden_jonsson_node_ptr node = forkscan_malloc(sizeof(c_linden_jonsson_node_t));
  node->key = key;
  node->toplevel = toplevel;
  node->insert_state = INSERT_PENDING;
  return node;
}

c_linden_jonsson_node_ptr c_linden_jonsson_unmark(c_linden_jonsson_node_ptr node){
  return (c_linden_jonsson_node_ptr)(((size_t)node) & (~0x1));
}

c_linden_jonsson_node_ptr c_linden_jonsson_mark(c_linden_jonsson_node_ptr node){
  return (c_linden_jonsson_node_ptr)((size_t)node | 0x1);
}

bool c_linden_jonsson_is_marked(c_linden_jonsson_node_ptr node){
  return c_linden_jonsson_unmark(node) != node;
}


typedef struct _c_linden_jonsson_unpacked_t {
  bool marked;
  c_linden_jonsson_node_ptr address;
} c_linden_jonsson_unpacked_t;

c_linden_jonsson_unpacked_t c_linden_jonsson_unpack(c_linden_jonsson_node_ptr node) {
  return (c_linden_jonsson_unpacked_t){
    .marked = c_linden_jonsson_is_marked(node),
    .address = c_linden_jonsson_unmark(node)
    };
}

/** Print out the contents of the skip list along with node heights.
 */
void c_linden_jonsson_pqueue_print (c_linden_jonsson_pqueue_t *set){
  c_linden_jonsson_node_ptr node = set->head.next[0];
  while(c_linden_jonsson_unmark(node) != &set->tail) {
    c_linden_jonsson_node_ptr unmarked_node = c_linden_jonsson_unmark(node);
    printf("node[%d]: %ld deleted: %d\n", unmarked_node->toplevel, unmarked_node->key, c_linden_jonsson_is_marked(node));
    node = unmarked_node->next[0];
  }
}

/** Return a new fixed-height skip list.
 */
c_linden_jonsson_pqueue_t * c_linden_jonsson_pqueue_create(uint32_t boundoffset) {
  c_linden_jonsson_pqueue_t* lj_pqueue = forkscan_malloc(sizeof(c_linden_jonsson_pqueue_t));
  lj_pqueue->boundoffset = boundoffset;
  lj_pqueue->head.key = INT64_MIN;
  lj_pqueue->head.insert_state = INSERTED;
  lj_pqueue->tail.key = INT64_MAX;
  lj_pqueue->tail.insert_state = INSERTED;
  for(int64_t i = 0; i < N; i++) {
    lj_pqueue->head.next[i] = &lj_pqueue->tail;
    lj_pqueue->tail.next[i] = NULL;
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


static c_linden_jonsson_node_ptr locate_preds(
  c_linden_jonsson_pqueue_t *set, 
  int64_t key,
  c_linden_jonsson_node_ptr preds[N],
  c_linden_jonsson_node_ptr succs[N]) {
  c_linden_jonsson_node_ptr cur = &set->head, next = NULL, del = NULL;
  int32_t level = N - 1;
  bool deleted = false;
  while(level >= 0) {
    next = cur->next[level];
    deleted = c_linden_jonsson_is_marked(next);
    next = c_linden_jonsson_unmark(next);

    while(next->key < key || 
      c_linden_jonsson_is_marked(next->next[0]) || 
      ((level == 0) && deleted)) {
      if(level == 0 && deleted) {
        del = next;
      }
      cur = next;
      next = next->next[level];
      deleted = c_linden_jonsson_is_marked(next);
      next = c_linden_jonsson_unmark(next);
    }
    preds[level] = cur;
    succs[level] = next;
    level--;
  }
  return del;
}

/** Add a node, lock-free, to the skiplist.
 */
int c_linden_jonsson_pqueue_add(uint64_t *seed, c_linden_jonsson_pqueue_t * set, int64_t key) {
  c_linden_jonsson_node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  c_linden_jonsson_node_ptr node = NULL;
  while(true) {
    c_linden_jonsson_node_ptr del = locate_preds(set, key, preds, succs);
    if(succs[0]->key == key &&
      !c_linden_jonsson_is_marked(preds[0]->next[0]) &&
      preds[0]->next[0] == succs[0]) {
      if(node != NULL) {
        forkscan_free((void*)node);
      }
      return false;
    }

    if(node == NULL) { node = c_linden_jonsson_create(key, toplevel); }
    for(int64_t i = 0; i <= toplevel; ++i) { node->next[i] = succs[i]; }
    c_linden_jonsson_node_ptr pred = preds[0], succ = succs[0];
    if(!__sync_bool_compare_and_swap(&pred->next[0], succ, node)) { continue; }

    for(int64_t i = 1; i <= toplevel; i++) {

      if(c_linden_jonsson_is_marked(node->next[0]) ||
        c_linden_jonsson_is_marked(succs[i]->next[0]) ||
        del == succs[i]) {
        node->insert_state = INSERTED;
        return true;
      }

      node->next[i] = succs[i];

      if(!__sync_bool_compare_and_swap(&preds[i]->next[i], succs[i], node)) {
        del = locate_preds(set, key, preds, succs);
        if(succs[0] != node) {
          node->insert_state = INSERTED;
          return true;
        }
      }
    }
    node->insert_state = INSERTED;
    return true;
  }
}


static void restructure(c_linden_jonsson_pqueue_t *set) {
  c_linden_jonsson_node_ptr pred = NULL, cur = NULL, head = NULL;
  int32_t level = N - 1;
  pred = &set->head;
  while(level > 0) {
    head = set->head.next[level];
    cur = pred->next[level];
    if(!c_linden_jonsson_is_marked(head->next[0])) {
      level--;
      continue;
    }
    while(c_linden_jonsson_is_marked(cur->next[0])) {
      pred = cur;
      cur = pred->next[level];
    }
    if(__sync_bool_compare_and_swap(&set->head.next[level], head, cur)){
      level--;
    }
  }
}


/** Pop the front node from the list.  Return true iff there was a node to pop.
 *  Leak the memory.
 */
int c_linden_jonsson_pqueue_leaky_pop_min(c_linden_jonsson_pqueue_t * set) {
  c_linden_jonsson_node_ptr cur = NULL, next = NULL, newhead = NULL,
    obs_head = NULL;
  int32_t offset = 0;
  cur = &set->head;
  obs_head = cur->next[0];
  do {
    offset++;
    next = cur->next[0];
    if(c_linden_jonsson_unmark(next) == &set->tail) { return false; }
    if(newhead == NULL && cur->insert_state == INSERT_PENDING) { newhead = cur; }
    if(c_linden_jonsson_is_marked(next)) { continue; }
    next = (c_linden_jonsson_node_ptr)__sync_fetch_and_or((uintptr_t*)&cur->next[0], (uintptr_t)1);
  } while((cur = c_linden_jonsson_unmark(next)) && c_linden_jonsson_is_marked(next));
  
  assert(!c_linden_jonsson_is_marked(cur));

  if(newhead == NULL) { newhead = cur; }
  if(offset <= set->boundoffset) { return true; }
  if(set->head.next[0] != obs_head) { return true; }

  if(__sync_bool_compare_and_swap(&set->head.next[0], 
    obs_head, c_linden_jonsson_mark(newhead))) {
    restructure(set);
    cur = c_linden_jonsson_unmark(obs_head);
    // while(cur != c_linden_jonsson_unmark(newhead)) {
    //   next = c_linden_jonsson_unmark(cur->next[0]);
    //   cur = next;
    // }
  }
  return true;
}