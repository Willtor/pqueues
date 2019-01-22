#pragma once

/* A Hunt et al priority queue written in C.
 * Array based heap with multiple locks.
 */

#include <stdint.h>
#include <stddef.h>

#define N 20

typedef struct c_hunt_pq_t c_hunt_pq_t;

c_hunt_pq_t *c_hunt_pq_create(size_t size);

int c_hunt_pq_add(c_hunt_pq_t *pqueue, int64_t priority);
int c_hunt_pq_leaky_pop_min(c_hunt_pq_t *pqueue);
int c_hunt_pq_pop_min(c_hunt_pq_t * pqueue);
void c_hunt_pq_print (c_hunt_pq_t *pqueue);