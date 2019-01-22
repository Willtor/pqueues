/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include "c_hunt_heap.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <immintrin.h>
#include <sys/syscall.h>
#include <unistd.h>

#define EMPTY UINTMAX_MAX
#define AVAILABLE (UINTMAX_MAX - 1)

typedef struct bucket_t bucket_t;
typedef struct bit_reversed_counter_t bit_reversed_counter_t;

struct bucket_t {
  pthread_spinlock_t lock;
  atomic_uintmax_t tag;
  int64_t priority;
};

void bucket_init(bucket_t *bucket) {
  pthread_spin_init(&bucket->lock, PTHREAD_PROCESS_PRIVATE);
  bucket->tag = EMPTY;
}

struct bit_reversed_counter_t {
  uintmax_t count, reversed;
  int32_t high_bit;
};

void bit_reversed_counter_init(bit_reversed_counter_t *brc) {
  brc->count = 0;
  brc->reversed = 0;
  brc->high_bit = -1;
}

uintmax_t bit_reversed_counter_increment(bit_reversed_counter_t *brc) {
  brc->count++;
  int32_t bit = brc->high_bit;
  for(; bit >= 0; bit--) {
    uintmax_t mask = UINTMAX_C(1) << bit;
    uintmax_t is_set = brc->reversed & mask;
    brc->reversed ^= mask;
    if(is_set != 0) {
      break;
    }
  }
  if(bit < 0) {
    brc->reversed = brc->count;
    brc->high_bit++;
  }
  return brc->reversed;
}

uintmax_t bit_reversed_counter_decrement(bit_reversed_counter_t *brc) {
  brc->count--;
  int32_t bit = brc->high_bit;
  for(; bit >= 0; bit--) {
    uintmax_t mask = UINTMAX_C(1) << bit;
    uintmax_t is_set = brc->reversed & mask;
    brc->reversed ^= mask;
    if(is_set == 0) {
      break;
    }
  }
  if(bit < 0) {
    brc->reversed = brc->count;
    brc->high_bit--;
  }
  return brc->reversed;
}

struct c_hunt_pq_t {
  pthread_spinlock_t lock;
  bit_reversed_counter_t counter;
  size_t size;
  bucket_t * buckets;
};


void c_hunt_pq_print (c_hunt_pq_t *pqueue){}

/** Return a new Hunt priority queue.
 */
c_hunt_pq_t* c_hunt_pq_create(size_t size) {
  c_hunt_pq_t* hunt_pqueue = forkscan_malloc(sizeof(c_hunt_pq_t));
  pthread_spin_init(&hunt_pqueue->lock, PTHREAD_PROCESS_PRIVATE);
  bit_reversed_counter_init(&hunt_pqueue->counter);
  hunt_pqueue->size = size;
  hunt_pqueue->buckets = forkscan_malloc(sizeof(bucket_t) * size);
  for(size_t i = 0; i < size; i++) {
    bucket_init(hunt_pqueue->buckets + i);
  }
  return hunt_pqueue;
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


static void lock(pthread_spinlock_t *lock) {
  pthread_spin_lock(lock);
}

static void unlock(pthread_spinlock_t *lock) {
  pthread_spin_unlock(lock);
}

static void swap_buckets(bucket_t *b1, bucket_t *b2) {
  atomic_uintmax_t tag1 = atomic_load_explicit(&b1->tag, memory_order_relaxed),
    tag2 = atomic_load_explicit(&b2->tag, memory_order_relaxed);
  int64_t priority1 = b1->priority, priority2 = b2->priority;
  atomic_store_explicit(&b1->tag, tag2, memory_order_relaxed);
  atomic_store_explicit(&b2->tag, tag1, memory_order_relaxed);
  b1->priority = priority2;
  b2->priority = priority1;
}

/** Add an item to the Hunt priority queue.
 */
int c_hunt_pq_add(c_hunt_pq_t * pqueue, int64_t priority) {
  int tid = syscall(SYS_gettid);
  lock(&pqueue->lock);
  uintmax_t i = bit_reversed_counter_increment(&pqueue->counter);
  lock(&pqueue->buckets[i].lock);
  unlock(&pqueue->lock);
  pqueue->buckets[i].priority = priority;
  pqueue->buckets[i].tag = tid;

  unlock(&pqueue->buckets[i].lock);
  while (i > 1) {
    uintmax_t parent = i / 2;
    lock(&pqueue->buckets[parent].lock);
    lock(&pqueue->buckets[i].lock);
    uintmax_t old_i = i;
    if((atomic_load_explicit(&pqueue->buckets[parent].tag, memory_order_relaxed) == AVAILABLE) 
      && (atomic_load_explicit(&pqueue->buckets[i].tag, memory_order_relaxed) == tid)){
      if(pqueue->buckets[i].priority > pqueue->buckets[parent].priority) {
        swap_buckets(pqueue->buckets + i, pqueue->buckets + parent);
        i = parent;
      }
      else {
        atomic_store_explicit(&pqueue->buckets[i].tag, AVAILABLE, memory_order_relaxed);
        i = 0;
      }
    }
    else if(atomic_load_explicit(&pqueue->buckets[parent].tag, memory_order_relaxed) == EMPTY) {
      i = 0;
    }
    else if(atomic_load_explicit(&pqueue->buckets[i].tag, memory_order_relaxed) != tid) {
      i = parent;
    }

    unlock(&pqueue->buckets[old_i].lock);
    unlock(&pqueue->buckets[parent].lock);
  }
  if(i == 1) {
    lock(&pqueue->buckets[i].lock);
    if(atomic_load_explicit(&pqueue->buckets[i].tag, memory_order_relaxed) == tid) {
      atomic_store_explicit(&pqueue->buckets[i].tag, AVAILABLE, memory_order_relaxed);
    }
    unlock(&pqueue->buckets[i].lock);
  }
  return true;
}


/** Remove the minimum element in the Hunt priority queue.
 */
int c_hunt_pq_leaky_pop_min(c_hunt_pq_t * pqueue) {
  lock(&pqueue->lock);
  uintmax_t bottom = bit_reversed_counter_decrement(&pqueue->counter);
  lock(&pqueue->buckets[bottom].lock);
  unlock(&pqueue->lock);

  int64_t priority = pqueue->buckets[bottom].priority;
  pqueue->buckets[bottom].tag = EMPTY;
  unlock(&pqueue->buckets[bottom].lock);

  lock(&pqueue->buckets[1].lock);
  if(atomic_load_explicit(&pqueue->buckets[1].tag, memory_order_relaxed) == EMPTY) {
    unlock(&pqueue->buckets[1].lock);
    return true;
  }

  pqueue->buckets[bottom].priority = pqueue->buckets[1].priority;
  pqueue->buckets[1].priority = priority;
  atomic_store_explicit(&pqueue->buckets[1].tag, AVAILABLE, memory_order_relaxed);

  uintmax_t i = 1;
  size_t size = pqueue->size;
  while (i < (size / 2)) {
    uintmax_t left = i * 2, right = (i * 2) + 1, child = i * 2;
    lock(&pqueue->buckets[left].lock);
    lock(&pqueue->buckets[right].lock);
    if(atomic_load_explicit(&pqueue->buckets[left].tag, memory_order_relaxed) == EMPTY) {
      unlock(&pqueue->buckets[right].lock);
      unlock(&pqueue->buckets[left].lock);
      break;
    }
    else if((atomic_load_explicit(&pqueue->buckets[right].tag, memory_order_relaxed) == EMPTY) || 
      (pqueue->buckets[left].priority > pqueue->buckets[right].priority)) {
      unlock(&pqueue->buckets[right].lock);
      child = left;
    }
    else {
      unlock(&pqueue->buckets[left].lock);
      child = right;
    }

    if(pqueue->buckets[child].priority > pqueue->buckets[i].priority) {
      swap_buckets(pqueue->buckets + child, pqueue->buckets + i);
      unlock(&pqueue->buckets[i].lock);
      i = child;
    } else {
      unlock(&pqueue->buckets[child].lock);
      break;
    }
  }
  unlock(&pqueue->buckets[i].lock);
  return true;
}

/** Remove the minimum element in the Hunt priority queue.
 */
int c_hunt_pq_pop_min(c_hunt_pq_t * pqueue) {
  return c_hunt_pq_leaky_pop_min(pqueue);
}
