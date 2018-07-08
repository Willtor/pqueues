/* Lock-free binary tree..
 * From paper "Fast Concurrent Lock-Free Binary Search Trees"
 * Lock-free updates (add/remove) and wait-free reads (contains).
*/

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <forkscan.h>

typedef struct _c_bt_lf_node_t {
    int64_t key;
    volatile struct _c_bt_lf_node_t * volatile left, * volatile right;
} c_bt_lf_node_t;

typedef c_bt_lf_node_t volatile * volatile c_bt_lf_node_ptr;


typedef struct _c_bt_lf_t {
    c_bt_lf_node_ptr R, S;
} c_bt_lf_t;

c_bt_lf_t * c_bt_lf_create();

int c_bt_lf_contains(c_bt_lf_t * set, int64_t key);
int c_bt_lf_add(c_bt_lf_t * set, int64_t key);
int c_bt_lf_remove_leaky(c_bt_lf_t * set, int64_t key);
int c_bt_lf_remove_retire(c_bt_lf_t * set, int64_t key);