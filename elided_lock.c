#include "elided_lock.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <immintrin.h>
#include <stdio.h>

struct elided_lock_t {
  atomic_bool locked;
};

// Inspiration: https://software.intel.com/en-us/articles/tsx-anti-patterns-in-lock-elision-code

#define MAX_RETRIES 5

static void wait_for_lock(elided_lock_t *lock) {
  while(atomic_load_explicit(&lock->locked, memory_order_relaxed)) {
    _mm_pause();
  }
}

static void slow_path(elided_lock_t *lock) {
  while(true) {
    wait_for_lock(lock);
    bool previous = atomic_exchange_explicit(&lock->locked, true, memory_order_acquire);
    // If previous wasn't locked, return.
    if(!previous){
      return;
    }
  }
}

static void init_elided_lock(elided_lock_t *lock) {
  atomic_store_explicit(&lock->locked, false, memory_order_relaxed);
}

elided_lock_t * create_elided_lock() {
  elided_lock_t *lock = forkscan_malloc(sizeof(elided_lock_t));
  init_elided_lock(lock);
  return lock;
}

void lock(elided_lock_t *lock) {
  for (size_t i = 0; i < MAX_RETRIES; i++) {
    wait_for_lock(lock);
    unsigned int status = _xbegin();
    if (status == _XBEGIN_STARTED) {
      // Still unlocked?
      if (!atomic_load_explicit(&lock->locked, memory_order_relaxed)) {
        return;
      } else {
        _xabort(0xff);
      }
    }
    if ((status & _XABORT_EXPLICIT) && _XABORT_CODE(status) == 0xff) {
      wait_for_lock(lock);
    }
    // Transaction is too big, or something.
    if (!(status & _XABORT_RETRY) &&
          !((status & _XABORT_EXPLICIT) && _XABORT_CODE(status) == 0xff)) {
        break;
    }
  }
  slow_path(lock);
}

void unlock(elided_lock_t *lock) {
  if(!atomic_load_explicit(&lock->locked, memory_order_relaxed) && _xtest()) {
    _xend();
  } else {
    atomic_store_explicit(&lock->locked, false, memory_order_release);
  }
}