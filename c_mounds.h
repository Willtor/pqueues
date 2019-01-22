#pragma once

/* A Hunt et al priority queue written in C.
 * Array based heap with multiple locks.
 */

#include <stdint.h>
#include <stddef.h>

#define N 20

typedef struct c_mound_pq_t c_mound_pq_t;

c_mound_pq_t *c_mound_pq_create(size_t size);

int c_mound_pq_add(uint64_t *seed, c_mound_pq_t *pqueue, int64_t priority);
int c_mound_pq_leaky_pop_min(c_mound_pq_t *pqueue);
int c_mound_pq_pop_min(c_mound_pq_t * pqueue);