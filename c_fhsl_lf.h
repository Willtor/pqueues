#pragma once

#include <stdint.h>
#include <stddef.h>

typedef struct c_fhsl_lf_t c_fhsl_lf_t;
typedef struct node_t node_t;
typedef node_t* node_ptr;

c_fhsl_lf_t * c_fhsl_lf_create();

int c_fhsl_lf_contains(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_contains_serial(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_add(uint64_t *seed, c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_add_serial(uint64_t *seed, c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove_leaky(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove_leaky_serial(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_remove_serial(c_fhsl_lf_t * set, int64_t key);
int c_fhsl_lf_pop_min_leaky(c_fhsl_lf_t *set);
int c_fhsl_lf_pop_min_leaky_serial(c_fhsl_lf_t *set);
int c_fhsl_lf_pop_min(c_fhsl_lf_t *set);
int c_fhsl_lf_pop_min_serial(c_fhsl_lf_t *set);
int c_fhsl_lf_bulk_pop(size_t amount, node_ptr *head, node_ptr *tail);
void c_fhsl_lf_print (c_fhsl_lf_t *set);