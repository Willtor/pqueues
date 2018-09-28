#pragma once

#include <stdint.h>

#define N 20

typedef struct c_fhsl_lf_t c_fhsl_lf_t;

c_fhsl_lf_t * c_fhsl_lf_create();

int c_fhsl_lf_contains(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_add(uint64_t *seed, c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove_leaky(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_pop_min_leaky(c_fhsl_lf_t *set);
int c_fhsl_lf_pop_min(c_fhsl_lf_t *set);
void c_fhsl_lf_print (c_fhsl_lf_t *set);