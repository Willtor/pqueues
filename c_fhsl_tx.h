#pragma once

#include <stdint.h>

#define N 20

typedef struct c_fhsl_tx_t c_fhsl_tx_t;

c_fhsl_tx_t * c_fhsl_tx_create();

int c_fhsl_tx_contains(c_fhsl_tx_t * set, int64_t key);
int c_fhsl_tx_add(uint64_t *seed, c_fhsl_tx_t * set, int64_t key);
int c_fhsl_tx_remove_leaky(c_fhsl_tx_t * set, int64_t key);
int c_fhsl_tx_remove(c_fhsl_tx_t * set, int64_t key);
int c_fhsl_tx_pop_min_leaky(c_fhsl_tx_t *set);
void c_fhsl_tx_print (c_fhsl_tx_t *set);