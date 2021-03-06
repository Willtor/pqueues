#pragma once

#include <stdint.h>

#define N 20
#define BOTTOM 0

typedef struct c_spray_pq_tx_t c_spray_pq_tx_t;

c_spray_pq_tx_t * c_spray_pq_tx_create(int64_t threads);
c_spray_pq_tx_t * c_spray_pq_tx_create_test1(int64_t threads);
c_spray_pq_tx_t * c_spray_pq_tx_create_test2(int64_t threads);
c_spray_pq_tx_t * c_spray_pq_tx_create_test3(int64_t threads);
c_spray_pq_tx_t * c_spray_pq_tx_create_test4(int64_t threads);


int find_external(c_spray_pq_tx_t *pqueue, int64_t key);
int c_spray_pq_tx_add(uint64_t *seed, c_spray_pq_tx_t * set, int64_t key);
int c_spray_pq_tx_pop_min_leaky(uint64_t *seed, c_spray_pq_tx_t *set);
void c_spray_pq_tx_print (c_spray_pq_tx_t *set);
void c_spray_pq_tx_test_print (c_spray_pq_tx_t *pqueue);