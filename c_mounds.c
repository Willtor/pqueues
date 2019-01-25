/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include "c_mounds.h"
#include "utils.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <pthread.h>

#define THRESHOLD 10
#define ROOT 1

typedef struct list_node_t list_node_t;
typedef struct mound_node_t mound_node_t;
typedef struct bit_reversed_counter_t bit_reversed_counter_t;

struct list_node_t {
  int64_t priority;
  list_node_t *next;
};

struct mound_node_t {
  pthread_mutex_t lock;
  _Atomic(list_node_t *) list;
};

list_node_t *list_node_create(list_node_t *list, int64_t priority) {
  list_node_t *new_node = forkscan_malloc(sizeof(list_node_t));
  new_node->priority = priority;
  new_node->next = list;
  return new_node;
}

void mound_node_init(mound_node_t *node, list_node_t *list) {
  atomic_store_explicit(&node->list, list, memory_order_relaxed);
}

bool is_leaf(uintmax_t depth, size_t i) {
  uintmax_t lower = UINTMAX_C(1) << (depth - 1);
  if(i < lower) return false;
  uintmax_t upper = (UINTMAX_C(1) << depth) - 1;
  if(i > upper) return false;
  return true;  
}

size_t get_parent(size_t i) {
  return i / 2;
}

int64_t get_val(list_node_t *list) {
  return (list == NULL) ? INT64_MAX : list->priority;
}

struct c_mound_pq_t {
  pthread_spinlock_t *locks;
  mound_node_t *tree;
  uintmax_t max_depth;
  atomic_uintmax_t depth;
};


/** Return a new mound priority queue.
 */
c_mound_pq_t* c_mound_pq_create(size_t size) {
  c_mound_pq_t* mound_pqueue = forkscan_malloc(sizeof(c_mound_pq_t));
  mound_pqueue->locks = forkscan_malloc(sizeof(pthread_spinlock_t) * size);
  mound_pqueue->tree = forkscan_malloc(sizeof(mound_node_t) * size);
  for(size_t i = 0; i < size; i++) {
    pthread_spin_init(mound_pqueue->locks + i, PTHREAD_PROCESS_SHARED);
    mound_node_init(mound_pqueue->tree + i, NULL);
  }
  mound_pqueue->max_depth = size;
  atomic_store_explicit(&mound_pqueue->depth, log2(size) - 1, memory_order_relaxed);
  return mound_pqueue;
}

static mound_node_t* lock(c_mound_pq_t *pqueue, size_t i) {
  mound_node_t *mound = &pqueue->tree[i];
  pthread_spin_lock(pqueue->locks + i);
  return mound;
}

static void unlock(c_mound_pq_t *pqueue, size_t i) {
  pthread_spin_unlock(pqueue->locks + i);
}

static uintmax_t rand_leaf(uintmax_t depth, uint64_t *seed) {
  uintmax_t lower = UINTMAX_C(1) << (depth - 1);
  // Missing "- 1" at the end of upper here. For the mod.
  uintmax_t upper = (UINTMAX_C(1) << depth);
  uintmax_t diff = (upper - lower) + 1;
  return lower + (fast_rand(seed) % diff);
}

static size_t binary_search(c_mound_pq_t *pqueue, uintmax_t leaf, uintmax_t root, int64_t priority) {
  return ROOT;
}

static size_t linear_search(c_mound_pq_t *pqueue, uintmax_t leaf, uintmax_t root, int64_t priority) {
  uintmax_t last_index = leaf;
  for(uintmax_t  parent = leaf / 2; parent != 0; last_index = parent, parent /= 2) {
    list_node_t *list = atomic_load_explicit(&pqueue->tree[parent].list, memory_order_seq_cst);
    int64_t parent_priority = get_val(list);
    if(parent_priority < priority) {
      return last_index;
    }
  }
  return last_index;
}

static uintmax_t find_insert_point(uint64_t *seed, c_mound_pq_t *pqueue, int64_t priority) {
  while(true) {
    uintmax_t depth = atomic_load_explicit(&pqueue->depth, memory_order_relaxed);
    for(uintmax_t i = 0; i < THRESHOLD; i++) {
      uintmax_t random_leaf = rand_leaf(depth, seed);
      list_node_t *list = atomic_load_explicit(&pqueue->tree[random_leaf].list, memory_order_seq_cst);
      int64_t leaf_priority = get_val(list);
      if(leaf_priority >= priority) return linear_search(pqueue, random_leaf, ROOT, priority);
    }
    if(depth == atomic_load_explicit(&pqueue->depth, memory_order_relaxed)) {
      atomic_compare_exchange_weak_explicit(&pqueue->depth, &depth, depth + 1, memory_order_relaxed, memory_order_relaxed);
    }
  }
}


static void moundify(c_mound_pq_t *pqueue, size_t i) {
  while(true) {
    list_node_t *current = atomic_load_explicit(&pqueue->tree[i].list, memory_order_seq_cst);
    uintmax_t depth = atomic_load_explicit(&pqueue->depth, memory_order_relaxed);
    if(is_leaf(depth, i)) {
      unlock(pqueue, i);
      return;
    }
    size_t left_index = i * 2, right_index = (i * 2) + 1;
    mound_node_t *left_mound = lock(pqueue, left_index);
    mound_node_t *right_mound = lock(pqueue, right_index);
    list_node_t *left = atomic_load_explicit(&left_mound->list, memory_order_seq_cst);
    list_node_t *right = atomic_load_explicit(&right_mound->list, memory_order_seq_cst);    
    int64_t left_val = get_val(left), right_val = get_val(right), current_val = get_val(current);
    if(left_val <= right_val && left_val < current_val) {
      unlock(pqueue, right_index);
      atomic_store_explicit(&pqueue->tree[i].list, left, memory_order_seq_cst);
      unlock(pqueue, i);
      atomic_store_explicit(&pqueue->tree[left_index].list, current, memory_order_seq_cst);
      moundify(pqueue, left_index);
      return;
    } else if(right_val < left_val && right_val < current_val) {
      unlock(pqueue, left_index);
      atomic_store_explicit(&pqueue->tree[i].list, right, memory_order_seq_cst);
      unlock(pqueue, i);
      atomic_store_explicit(&pqueue->tree[right_index].list, current, memory_order_seq_cst);
      moundify(pqueue, right_index);
      return;
    } else {
      unlock(pqueue, i);
      unlock(pqueue, left_index);
      unlock(pqueue, right_index);
      return;
    }
  }
}

/** Add an item to the mound priority queue.
 */
int c_mound_pq_add(uint64_t *seed, c_mound_pq_t * pqueue, int64_t priority) {
  // printf("Add\n");
  while(true) {
    uintmax_t insertion_point = find_insert_point(seed, pqueue, priority);
    if(insertion_point == ROOT) {
      mound_node_t *root = lock(pqueue, ROOT);
      list_node_t *list = atomic_load_explicit(&root->list, memory_order_seq_cst);
      if(get_val(list) >= priority) {
        list_node_t *new_node = list_node_create(list, priority);
        atomic_store_explicit(&root->list, new_node, memory_order_seq_cst);
        assert(atomic_load_explicit(&root->list, memory_order_relaxed) != NULL);
        unlock(pqueue, ROOT);
        return true;
      }
      unlock(pqueue, ROOT);
      continue;
    }
    uintmax_t parent_point = get_parent(insertion_point);
    mound_node_t *parent = lock(pqueue, parent_point);
    mound_node_t *child = lock(pqueue, insertion_point);
    list_node_t *parent_list = atomic_load_explicit(&parent->list, memory_order_seq_cst);
    list_node_t *child_list = atomic_load_explicit(&child->list, memory_order_seq_cst);
    if(get_val(child_list) >= priority && get_val(parent_list) <= priority) {
      list_node_t *new_node = list_node_create(child_list, priority);
      atomic_store_explicit(&child->list, new_node, memory_order_seq_cst);
      assert(atomic_load_explicit(&child->list, memory_order_relaxed) != NULL);
      unlock(pqueue, insertion_point);
      unlock(pqueue, parent_point);
      return true;
    } else {
      unlock(pqueue, parent_point);
      unlock(pqueue, insertion_point);
    }
  }
}


/** Remove the minimum element in the mound priority queue.
 */
int c_mound_pq_leaky_pop_min(c_mound_pq_t * pqueue) {
  // printf("Pop min\n");
  mound_node_t *root = lock(pqueue, ROOT);
  list_node_t *list = atomic_load_explicit(&pqueue->tree[ROOT].list, memory_order_seq_cst);
  if(list == NULL) {
    unlock(pqueue, ROOT);
    return false;
  }
  atomic_store_explicit(&root->list, list->next, memory_order_seq_cst);
  // Leak list node. forkscan_retire(list);
  moundify(pqueue, ROOT);
  return true;
}

/** Remove the minimum element in the mound priority queue.
 */
int c_mound_pq_pop_min(c_mound_pq_t * pqueue) {
  mound_node_t *root = lock(pqueue, ROOT);
  list_node_t *list = atomic_load_explicit(&root->list, memory_order_seq_cst);
  if(list == NULL) {
    unlock(pqueue, ROOT);
    return false;
  }
  atomic_store_explicit(&root->list, list->next, memory_order_seq_cst);
  forkscan_retire(list);
  moundify(pqueue, ROOT);
  return true;
}
