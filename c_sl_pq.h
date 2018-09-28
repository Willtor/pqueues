#pragma once

/* A Shavit Lotan priority queue written in C.
 * Like the original description in the paper this queue has a skiplist
 * as the underlying data-structure for ordering elements.
 * The data-structure is lock-free and quiescently consistent.
 */

#include <stdint.h>

#define N 20

typedef struct c_sl_pq_t c_sl_pq_t;

c_sl_pq_t * c_sl_pq_create();

int c_sl_pq_add(uint64_t *seed, c_sl_pq_t *pqueue, int64_t key);
int c_sl_pq_leaky_pop_min(c_sl_pq_t *pqueue);
int c_sl_pq_pop_min(c_sl_pq_t * pqueue);
void c_sl_pq_print (c_sl_pq_t *pqueue);