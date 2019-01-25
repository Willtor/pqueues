#pragma once

#include <stdint.h>

typedef struct c_fhsl_t c_fhsl_t;

c_fhsl_t * c_fhsl_create();

int c_fhsl_contains(c_fhsl_t * set, int64_t key);
int c_fhsl_add(uint64_t *seed, c_fhsl_t * set, int64_t key);
int c_fhsl_remove(c_fhsl_t * set, int64_t key);
int c_fhsl_pop_min(c_fhsl_t *set);
void c_fhsl_print (c_fhsl_t *set);