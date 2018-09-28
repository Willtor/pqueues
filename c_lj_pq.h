#pragma once

#include <stdint.h>

#define N 20

typedef struct c_lj_pq_t c_lj_pq_t;

c_lj_pq_t * c_lj_pq_create(uint32_t boundoffset);

int c_lj_pq_add(uint64_t *seed, c_lj_pq_t * pqueue, int64_t key);
int c_lj_pq_leaky_pop_min(c_lj_pq_t * pqueue);
void c_lj_pq_print(c_lj_pq_t *pqueue);