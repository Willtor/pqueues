#pragma once

#include <stdatomic.h>
#include <stdalign.h>

typedef struct spinlock_t spinlock_t;
typedef struct owned_spinlock_t owned_spinlock_t;

struct spinlock_t {
  alignas(128) atomic_bool lock_state;
};

struct owned_spinlock_t {
  spinlock_t lock;
  atomic_uintmax_t owner;
};

void spinlock_init(spinlock_t *);
int spinlock_is_locked(spinlock_t *);
int spinlock_trylock(spinlock_t *);
void spinlock_lock(spinlock_t *);
void spinlock_unlock(spinlock_t *);

void owned_spinlock_init(owned_spinlock_t *);
int owned_spinlock_trylock(owned_spinlock_t *);
void owned_spinlock_lock(owned_spinlock_t *);
void owned_spinlock_unlock(owned_spinlock_t *);