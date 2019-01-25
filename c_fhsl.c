/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl.h"
#include "utils.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>


#define N 20
#define BOTTOM 0

typedef struct node_t node_t;
typedef node_t* node_ptr;

struct node_t {
  int64_t key;
  int32_t toplevel;
  node_ptr next[N];
};

struct c_fhsl_t {
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
void c_fhsl_print (c_fhsl_t *set){
  node_ptr node = set->head.next[BOTTOM];
  while(node != &set->tail) {
    node_ptr next = node->next[BOTTOM];
    printf("node[%d]: %ld\n", node->toplevel, node->key);
    node = next;
  }
}

/** Return a new fixed-height skip list.
 */
c_fhsl_t * c_fhsl_create() {
  c_fhsl_t* fhsl = forkscan_malloc(sizeof(c_fhsl_t));
  fhsl->head.key = INT64_MIN;
  fhsl->tail.key = INT64_MAX;
  for(int64_t i = 0; i < N; i++) {
    fhsl->head.next[i] = &fhsl->tail;
    fhsl->tail.next[i] = NULL;
  }
  return fhsl;
}

/** Return whether the skip list contains the value.
 */
int c_fhsl_contains(c_fhsl_t *set, int64_t key) {
  node_ptr node = &set->head;
  for(int64_t i = N - 1; i >= 0; i--) {
    node_ptr next = node->next[i];
    while(next->key <= key) {
      node = next; 
      next = node->next[i];
    }
    if(node->key == key) {
      return true;
    }
  }
  return false;
}

static bool find(c_fhsl_t *set, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  node_ptr left = &set->head, right = left;
  for(int64_t level = N - 1; level >= BOTTOM; --level) {
    right = left->next[level];
    // Has the right not gone far enough?        
    while(right->key < key) {
      left = right;
      right = right->next[level];
    }
    preds[level] = left;
    succs[level] = right;
  }
  return succs[BOTTOM]->key == key;
  
}

/** Add a node to the skiplist.
 */
int c_fhsl_add(uint64_t *seed, c_fhsl_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = -1;
  node_ptr node = NULL;
  if(find(set, key, preds, succs)) {
    forkscan_free((void*)node);
    return false;
  }
  if(node == NULL) { 
    toplevel = random_level(seed, N);
    node = node_create(key, toplevel); 
  }
  for(int64_t i = BOTTOM; i <= toplevel; ++i) {
    node->next[i] = succs[i];
    preds[i]->next[i] = node;
  }
  return true;
}

/** Remove a node from the skiplist.
 */
int c_fhsl_remove(c_fhsl_t * set, int64_t key) {
  node_ptr preds[N], succs[N];
  if(!find(set, key, preds, succs)) {
    return false;
  }
  node_ptr node = succs[BOTTOM];
  for(int64_t i = BOTTOM; i <= node->toplevel; ++i) {
    preds[i]->next[i] = node->next[i];
  }
  forkscan_free(node);
  return true;
}

/** Pop the front node from the list.  Return true iff there was a node to pop.
 */
int c_fhsl_pop_min (c_fhsl_t *set) {
  node_ptr head_node = set->head.next[BOTTOM];
  if(head_node != &set->tail) {
    node_ptr node_popped = head_node;
    int64_t toplevel = node_popped->toplevel;
    for(int64_t i = BOTTOM; i <= toplevel; i++) {
      set->head.next[i] = node_popped->next[i];
    }
    forkscan_free(node_popped);
    return true;
  }
  return false;
}