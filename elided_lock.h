#pragma once

typedef struct elided_lock_t elided_lock_t;

elided_lock_t *create_elided_lock();
void lock(elided_lock_t *lock);
void unlock(elided_lock_t *lock);