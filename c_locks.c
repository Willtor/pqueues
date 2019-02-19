#include "c_locks.h"
#include <sys/syscall.h>
#include <unistd.h>
#include <immintrin.h>
#include <stdatomic.h>
#include <assert.h>
#include <stdbool.h>

#define NO_OWNER UINTMAX_MAX
#define LOCK_SUCCEEDED 0
#define LOCK_FAILED 1

/*
A simple spinlock.
*/
void spinlock_init(spinlock_t *lock) {
  atomic_store_explicit(&lock->lock_state, false, memory_order_relaxed);
}

int spinlock_is_locked(spinlock_t *lock) {
  return atomic_load_explicit(&lock->lock_state, memory_order_relaxed);
}

int spinlock_trylock(spinlock_t *lock){
  bool state = atomic_load_explicit(&lock->lock_state, memory_order_acquire);
  if(state) {
    return LOCK_FAILED;
  } else {
    return atomic_exchange_explicit(&lock->lock_state, true, memory_order_acquire) ? LOCK_FAILED : LOCK_SUCCEEDED;
  }
}

void spinlock_lock(spinlock_t *lock){
  while(true) {
    if(spinlock_trylock(lock) == LOCK_FAILED) {
      _mm_pause();
      continue;
    } else {
      return;
    }
  }
}

void spinlock_unlock(spinlock_t *lock) {
  atomic_store_explicit(&lock->lock_state, false, memory_order_release);
}

/*
An owner spinlock. Lock and unlock record the thread id.
*/
void owned_spinlock_init(owned_spinlock_t *owned_lock) {
  spinlock_init(&owned_lock->lock);
  atomic_store_explicit(&owned_lock->owner, NO_OWNER, memory_order_relaxed);
}

int owned_spinlock_trylock(owned_spinlock_t *owned_lock) {
  if(spinlock_trylock(&owned_lock->lock) == LOCK_SUCCEEDED) {
    uintmax_t owner = atomic_load_explicit(&owned_lock->owner, memory_order_relaxed);
    assert(owner == NO_OWNER);
    int tid = syscall(SYS_gettid);
    atomic_store_explicit(&owned_lock->owner, tid, memory_order_relaxed);
    return LOCK_SUCCEEDED;
  }
  return LOCK_FAILED;
}

void owned_spinlock_lock(owned_spinlock_t *owned_lock) {
  spinlock_lock(&owned_lock->lock);
  uintmax_t owner = atomic_load_explicit(&owned_lock->owner, memory_order_relaxed);
  assert(owner == NO_OWNER);
  int tid = syscall(SYS_gettid);
  atomic_store_explicit(&owned_lock->owner, tid, memory_order_relaxed);
}

void owned_spinlock_unlock(owned_spinlock_t *owned_lock) {
  int tid = syscall(SYS_gettid);
  uintmax_t owner = atomic_load_explicit(&owned_lock->owner, memory_order_relaxed);
  assert(owner == tid);
  atomic_store_explicit(&owned_lock->owner, NO_OWNER, memory_order_relaxed);
}