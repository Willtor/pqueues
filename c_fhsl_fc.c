/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_fhsl_fc.h"
#include "c_fhsl.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>
#include <immintrin.h>
#include <pthread.h>


#define N 20
#define BOTTOM 0


typedef struct op_t op_t;
typedef enum op_type op_type_t;

enum op_type {CONTAINS, ADD, REMOVE, POP_MIN, NONE};

// Please forgive me...
struct op_t {
  _Atomic(op_type_t) pending_op;
  union {
  _Atomic(uint64_t) contains, add, remove;
  } op_arg;
  union{
    atomic_bool contains, add, remove, pop_min;
  } op_ret;
  char padding[128 - (sizeof(_Atomic(op_type_t)) + sizeof(_Atomic(uint64_t)) + sizeof(atomic_bool))];
};

struct c_fhsl_fc_t {
  size_t num_threads;
  op_t *pending_ops;
  pthread_t server_thread;
  c_fhsl_t *inner_set;
};


/** Print out the contents of the skip list along with node heights.
 */
void c_fhsl_fc_print (c_fhsl_fc_t *set){
  c_fhsl_print(set->inner_set);
}

static void* server_thread_func(void *set) {
  c_fhsl_fc_t* fhsl_fc = set;
  size_t num_threads = fhsl_fc->num_threads;
  uint64_t seed = time(NULL);
  while(true) {
    for(size_t i = 0; i < num_threads; i++) {
      op_type_t op = atomic_load_explicit(&fhsl_fc->pending_ops[i].pending_op, memory_order_acquire);
      if(op == NONE) {
        continue;
      } else if(op == CONTAINS) {
        uint64_t arg = atomic_load_explicit(&fhsl_fc->pending_ops[i].op_arg.contains, memory_order_relaxed);
        bool ans = c_fhsl_contains(fhsl_fc->inner_set, arg);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].op_ret.contains, ans, memory_order_relaxed);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == ADD) {
        uint64_t arg = atomic_load_explicit(&fhsl_fc->pending_ops[i].op_arg.add, memory_order_relaxed);
        bool ans = c_fhsl_add(&seed, fhsl_fc->inner_set, arg);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].op_ret.add, ans, memory_order_relaxed);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == REMOVE) {
        uint64_t arg = atomic_load_explicit(&fhsl_fc->pending_ops[i].op_arg.remove, memory_order_relaxed);
        bool ans = c_fhsl_remove(fhsl_fc->inner_set, arg);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].op_ret.remove, ans, memory_order_relaxed);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == POP_MIN) {
        bool ans = c_fhsl_pop_min(fhsl_fc->inner_set);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].op_ret.pop_min, ans, memory_order_relaxed);
        atomic_store_explicit(&fhsl_fc->pending_ops[i].pending_op, NONE, memory_order_release);
      }
    }
    _mm_pause();
  }
}

static void wait(c_fhsl_fc_t *set, size_t thread_id) {
  while(atomic_load_explicit(&set->pending_ops[thread_id].pending_op, memory_order_acquire) != NONE) { _mm_pause(); }
}

/** Return a new fixed-height skip list.
 */
c_fhsl_fc_t * c_fhsl_fc_create(size_t num_threads) {
  c_fhsl_fc_t* fhsl_fc = forkscan_malloc(sizeof(c_fhsl_fc_t));
  fhsl_fc->num_threads = num_threads;
  fhsl_fc->pending_ops = forkscan_malloc(sizeof(op_t) * num_threads);
  for(size_t i = 0; i < num_threads; i++) {
    atomic_store_explicit(&fhsl_fc->pending_ops[i].pending_op, NONE, memory_order_relaxed); // Important, rest is not.
    atomic_store_explicit(&fhsl_fc->pending_ops[i].op_arg.contains, UINT64_MAX, memory_order_relaxed);
    atomic_store_explicit(&fhsl_fc->pending_ops[i].op_ret.contains, false, memory_order_relaxed);
  }
  pthread_create(&fhsl_fc->server_thread, NULL, server_thread_func, fhsl_fc);
  fhsl_fc->inner_set = c_fhsl_create();
  return fhsl_fc;
}

/** Return whether the skip list contains the value.
 */
int c_fhsl_fc_contains(c_fhsl_fc_t *set, int64_t key, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].op_arg.contains, key, memory_order_relaxed);
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, CONTAINS, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.contains, memory_order_relaxed);
}

/** Add a node to the skiplist.
 */
int c_fhsl_fc_add(c_fhsl_fc_t * set, int64_t key, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].op_arg.add, key, memory_order_relaxed);
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, ADD, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.add, memory_order_relaxed);
}

/** Remove a node from the skiplist.
 */
int c_fhsl_fc_remove(c_fhsl_fc_t * set, int64_t key, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].op_arg.remove, key, memory_order_relaxed);
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, REMOVE, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.remove, memory_order_relaxed);
}

int c_fhsl_fc_pop_min(c_fhsl_fc_t *set, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, POP_MIN, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.pop_min, memory_order_relaxed);
}