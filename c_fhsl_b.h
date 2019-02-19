#pragma once

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>

typedef struct c_fhsl_b_t c_fhsl_b_t;
typedef struct node_t node_t;
typedef node_t* node_ptr;

#define N 20

struct node_t {
  int64_t key;
  int32_t toplevel;
  atomic_bool marked, fully_linked;
  pthread_spinlock_t lock;
  _Atomic(node_ptr) next[N];
};

c_fhsl_b_t * c_fhsl_b_create();

int c_fhsl_b_contains(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_contains_serial(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_add(uint64_t *seed, c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_add_serial(uint64_t *seed, c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_remove_leaky(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_remove_leaky_serial(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_remove(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_remove_serial(c_fhsl_b_t * set, int64_t key);
int c_fhsl_b_pop_min_leaky(c_fhsl_b_t *set);
int c_fhsl_b_pop_min_leaky_serial(c_fhsl_b_t *set);
int c_fhsl_b_pop_min(c_fhsl_b_t *set);
int c_fhsl_b_pop_min_serial(c_fhsl_b_t *set);
int c_fhsl_b_bulk_pop(c_fhsl_b_t *set, size_t amount, node_ptr *head, node_ptr *tail);
void c_fhsl_b_bulk_push(c_fhsl_b_t *set, node_ptr head, node_ptr tail);
void c_fhsl_b_print (c_fhsl_b_t *set);