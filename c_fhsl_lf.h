#pragma once

#include <stdint.h>

#define N 20

typedef struct _fhsl_lf_node_t {
  int64_t key;
  int32_t toplevel;
  struct _fhsl_lf_node_t volatile * volatile next[N];
} fhsl_lf_node_t;

typedef fhsl_lf_node_t volatile * volatile fhsl_lf_node_ptr;

typedef struct _fhsl_lf_t {
  fhsl_lf_node_t head, tail;
} c_fhsl_lf_t;

c_fhsl_lf_t * c_fhsl_lf_create();

int c_fhsl_lf_contains(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_add(uint64_t *seed, c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove_leaky(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_leaky_pop_min(c_fhsl_lf_t * set);
void c_fhsl_lf_print (c_fhsl_lf_t *set);