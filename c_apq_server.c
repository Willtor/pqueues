#include "c_apq_server.h"
#include "c_fhsl.h"
#include "c_fhsl_b.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <forkscan.h>
#include <stdio.h>
#include <immintrin.h>
#include <pthread.h>

typedef struct op_t op_t;
typedef enum op_type op_type_t;

enum op_type {CONTAINS, ADD, REMOVE, REMOVE_LEAKY, POP_MIN, POP_MIN_LEAKY, NONE};

// Please forgive me...
struct op_t {
  _Atomic(op_type_t) pending_op;
  union {
  _Atomic(int64_t) contains, add, remove;
  } op_arg;
  union{
    atomic_bool contains, add, remove, pop_min;
  } op_ret;
  char padding[128 - (sizeof(_Atomic(op_type_t)) + sizeof(_Atomic(int64_t)) + sizeof(atomic_bool))];
};

struct c_apq_server_t {
  op_t *pending_ops;
  char padding1[128];
  _Atomic(int64_t) cutoff_key;
  char padding2[128];
  size_t num_threads;
  uint64_t seed;
  uintmax_t fc_size, fc_size_threshold, fc_transfer_amount;
  c_fhsl_b_t *fc_set;
  c_fhsl_b_t *p_set;
  pthread_t server_thread;
};


/** Print out the contents of the skip list along with node heights.
 */
void c_apq_server_print (c_apq_server_t *set){
  c_fhsl_b_print(set->fc_set);
}

static void* server_thread_func(void *set) {
  c_apq_server_t* apq = set;
  size_t num_threads = apq->num_threads;
  while(true) {
    for(size_t i = 0; i < num_threads; i++) {
      op_type_t op = atomic_load_explicit(&apq->pending_ops[i].pending_op, memory_order_acquire);
      if(op == NONE) {
        continue;
      } else if(op == CONTAINS) {
        int64_t arg = atomic_load_explicit(&apq->pending_ops[i].op_arg.contains, memory_order_relaxed);
        bool ans = c_fhsl_b_contains_serial(apq->fc_set, arg);
        atomic_store_explicit(&apq->pending_ops[i].op_ret.contains, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == ADD) {
        int64_t arg = atomic_load_explicit(&apq->pending_ops[i].op_arg.add, memory_order_relaxed);
        bool ans = c_fhsl_b_add_serial(&apq->seed, apq->fc_set, arg);
        if(ans) { apq->fc_size++; }
        atomic_store_explicit(&apq->pending_ops[i].op_ret.add, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == REMOVE) {
        int64_t arg = atomic_load_explicit(&apq->pending_ops[i].op_arg.remove, memory_order_relaxed);
        bool ans = c_fhsl_b_remove_serial(apq->fc_set, arg);
        atomic_store_explicit(&apq->pending_ops[i].op_ret.remove, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == REMOVE_LEAKY) {
        int64_t arg = atomic_load_explicit(&apq->pending_ops[i].op_arg.remove, memory_order_relaxed);
        bool ans = c_fhsl_b_remove_leaky_serial(apq->fc_set, arg);
        atomic_store_explicit(&apq->pending_ops[i].op_ret.remove, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == POP_MIN_LEAKY) {
        bool ans = c_fhsl_b_pop_min_leaky_serial(apq->fc_set);
        if(ans) { apq->fc_size--; }
        atomic_store_explicit(&apq->pending_ops[i].op_ret.pop_min, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      } else if(op == POP_MIN) {
        bool ans = c_fhsl_b_pop_min_serial(apq->fc_set);
        if(ans) { apq->fc_size--; }
        atomic_store_explicit(&apq->pending_ops[i].op_ret.pop_min, ans, memory_order_relaxed);
        atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_release);
      }
    }
    if(apq->fc_size < apq->fc_size_threshold) {
      // Try take nodes from parallel skiplist.
      size_t transfer_count = apq->fc_transfer_amount;
      node_ptr head = NULL, tail = NULL;
      size_t approx_seen = c_fhsl_b_bulk_pop(apq->p_set, transfer_count, &head, &tail);
      // Reintegrate nodes into serial list.
      if(head != NULL && tail != NULL) {
        c_fhsl_b_bulk_push(apq->fc_set, head, tail);
        apq->fc_size += approx_seen;
        atomic_store_explicit(&apq->cutoff_key, tail->key, memory_order_relaxed);
      }
    }
    _mm_pause();
  }
}

static void wait(c_apq_server_t *set, size_t thread_id) {
  while(atomic_load_explicit(&set->pending_ops[thread_id].pending_op, memory_order_acquire) != NONE) { _mm_pause(); }
}

/** Return a new fixed-height skip list.
 */
c_apq_server_t * c_apq_server_create(size_t num_threads, int64_t cutoff_key) {
  c_apq_server_t* apq = forkscan_malloc(sizeof(c_apq_server_t));
  apq->num_threads = num_threads;
  apq->pending_ops = forkscan_malloc(sizeof(op_t) * num_threads);
  for(size_t i = 0; i < num_threads; i++) {
    atomic_store_explicit(&apq->pending_ops[i].pending_op, NONE, memory_order_relaxed); // Important, rest is not.
    atomic_store_explicit(&apq->pending_ops[i].op_arg.contains, UINT64_MAX, memory_order_relaxed);
    atomic_store_explicit(&apq->pending_ops[i].op_ret.contains, false, memory_order_relaxed);
  }
  apq->seed = time(NULL);
  atomic_store_explicit(&apq->cutoff_key, cutoff_key, memory_order_relaxed);
  apq->fc_size_threshold = (num_threads * 4) > cutoff_key ? cutoff_key : num_threads * 4;
  apq->fc_size = 0;
  // Parameter
  apq->fc_transfer_amount = apq->fc_size_threshold;
  apq->fc_set = c_fhsl_b_create();
  apq->p_set = c_fhsl_b_create();
  pthread_create(&apq->server_thread, NULL, server_thread_func, apq);
  return apq;
}

/** Add a node to the skiplist.
 */
int c_apq_server_add(uint64_t *seed, c_apq_server_t *set, int64_t key, size_t thread_id) {
  int64_t cutoff_key = atomic_load_explicit(&set->cutoff_key, memory_order_relaxed);
  if(key < cutoff_key) {
    atomic_store_explicit(&set->pending_ops[thread_id].op_arg.add, key, memory_order_relaxed);
    atomic_store_explicit(&set->pending_ops[thread_id].pending_op, ADD, memory_order_release);
    wait(set, thread_id);
    return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.add, memory_order_relaxed);
  } else {
    return c_fhsl_b_add(seed, set->p_set, key);
  }
}


int c_apq_server_pop_min_leaky(c_apq_server_t *set, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, POP_MIN_LEAKY, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.pop_min, memory_order_relaxed);
}

int c_apq_server_pop_min(c_apq_server_t *set, size_t thread_id) {
  atomic_store_explicit(&set->pending_ops[thread_id].pending_op, POP_MIN, memory_order_release);
  wait(set, thread_id);
  return atomic_load_explicit(&set->pending_ops[thread_id].op_ret.pop_min, memory_order_relaxed);
}