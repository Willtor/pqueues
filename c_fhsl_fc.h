#pragma once

#include <stdint.h>
#include <stddef.h>

typedef struct c_fhsl_fc_t c_fhsl_fc_t;

c_fhsl_fc_t * c_fhsl_fc_create(size_t num_threads);

int c_fhsl_fc_contains(c_fhsl_fc_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_add(c_fhsl_fc_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_remove(c_fhsl_fc_t * set, int64_t key, size_t thread_id);
int c_fhsl_fc_pop_min(c_fhsl_fc_t *set, size_t thread_id);
void c_fhsl_fc_print (c_fhsl_fc_t *set);