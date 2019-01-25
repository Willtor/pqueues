/* Fixed height skiplist: A skiplist implementation with an array of "next"
 * nodes of fixed height.
 */

#include "c_spray_pq_tx.h"
#include "elided_lock.h"
#include "utils.h"

#include <assert.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <forkscan.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>


enum STATE {PADDING, ACTIVE, DELETED};

typedef enum STATE state_t;
typedef struct node_t node_t;
typedef node_t* node_ptr;
typedef struct config_t config_t;
typedef struct node_unpacked_t node_unpacked_t;

struct node_t {
  int64_t key;
  int32_t toplevel;
  // _Atomic(state_t)* state;
  state_t state;
  node_ptr next[N];
};

struct config_t {
  int64_t thread_count, start_height, max_jump, descend_amount, padding_amount;
};


struct c_spray_pq_tx_t {
  config_t config;
  atomic_bool cleaner_lock;
  // pthread_spinlock_t cleaner_lock;
  elided_lock_t *lock;
  node_ptr padding_head;
  node_t head, tail;
};


static node_ptr node_create(int64_t key, int32_t toplevel, state_t state){
  node_ptr node = forkscan_malloc(sizeof(node_t));
  node->key = key;
  node->toplevel = toplevel;
  // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(node->state, state, memory_order_relaxed);
  node->state = state;
  return node;
}

static void print_node(node_ptr node) {
  if(node == NULL) {
    printf("NULL Node\n");
    return;
  }
  printf("node[%d] with key: %ld", node->toplevel, node->key);
  // state_t state = atomic_load_explicit(node->state, memory_order_relaxed);
  state_t state = node->state;
  if(state == DELETED) {
    printf(" state: DELETED\n");
  } else if(state == PADDING) {
    printf(" state: PADDING\n");
  } else {
    printf(" state: ACTIVE\n");
  }
}

static void print_node_test(node_ptr node) {
  if(node == NULL) {
    printf("NULL Node\n");
    return;
  }
  printf("node[%d] with key: %ld", node->toplevel, node->key);
  // state_t state = atomic_load_explicit(node->state, memory_order_relaxed);
  state_t state = node->state;
  if(state == DELETED) {
    printf(" state: DELETED\n");
  } else if(state == PADDING) {
    printf(" state: PADDING\n");
  } else {
    printf(" state: ACTIVE\n");
  }
  printf("Nexts\n");
  for(int64_t i = node->toplevel; i >= BOTTOM; i--) {
    printf("%ld - ", i);
    print_node(node->next[i]);
  }
  printf("**************\n");
}

void c_spray_pq_tx_print (c_spray_pq_tx_t *pqueue) {
  // Padding
  for(node_ptr curr = pqueue->padding_head;
    curr != &pqueue->head;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Head
  print_node(&pqueue->head);
  // Everything in between
  int64_t length = 0;
  for(node_ptr curr = pqueue->head.next[0];
    curr != &pqueue->tail;
    curr = curr->next[0],length++) {
    print_node(curr);
  }
  printf("Length %ld\n", length);
  // Tail
  print_node(&pqueue->tail);
}

void c_spray_pq_tx_test_print (c_spray_pq_tx_t *pqueue) {
  // Padding
  for(node_ptr curr = pqueue->padding_head;
    curr != &pqueue->head;
    curr = curr->next[0]) {
    print_node(curr);
  }
  // Head
  print_node_test(&pqueue->head);
  // Everything in between
  for(node_ptr curr = pqueue->head.next[0];
    curr != &pqueue->tail;
    curr = curr->next[0]) {
    print_node_test(curr);
  }
  // Tail
  print_node_test(&pqueue->tail);
}

static config_t c_spray_pq_config_paper(int64_t threads) {
  int64_t log_arg = threads;
  if(threads == 1) { log_arg = 2; }
  return (config_t) {
    .thread_count = threads,
    .start_height = log2(threads) + 1,
    .max_jump = log2(threads) + 1,
    .descend_amount = 1,
    .padding_amount = (threads * log2(log_arg)) / 2
  };
}

static void print_config(config_t *config) {
  printf("Thread Count: %ld\n", config->thread_count);
  printf("Start Height: %ld\n", config->start_height);
  printf("Max Jump: %ld\n", config->max_jump);
  printf("Descend Amount: %ld\n", config->descend_amount);
  printf("Padding Amount: %ld\n", config->padding_amount);
}

/** Return a new fixed-height skip list.
 */
c_spray_pq_tx_t * c_spray_pq_tx_create(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.toplevel = N - 1;
  // spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->head.state = PADDING;
  spray_pq_tx->tail.key = INT64_MAX;
  // spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.state = PADDING;
  spray_pq_tx->tail.toplevel = N - 1;
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    // atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  // pthread_spin_init(&spray_pq_tx->cleaner_lock, PTHREAD_PROCESS_PRIVATE);
  atomic_store_explicit(&spray_pq_tx->cleaner_lock, false, memory_order_relaxed);
  return spray_pq_tx;
}

c_spray_pq_tx_t * c_spray_pq_tx_create_test1(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.toplevel = N - 1;
  // spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->head.state = PADDING;
  spray_pq_tx->tail.key = INT64_MAX;
  // spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.state = PADDING;
  spray_pq_tx->tail.toplevel = N - 1;
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    // atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  // pthread_spin_init(&spray_pq_tx->cleaner_lock, PTHREAD_PROCESS_PRIVATE);
  atomic_store_explicit(&spray_pq_tx->cleaner_lock, false, memory_order_relaxed);


  node_ptr node1 = node_create(1, 0, DELETED);
  node_ptr node2 = node_create(2, 1, DELETED);
  node_ptr node3 = node_create(3, 2, ACTIVE);
  node_ptr node4 = node_create(4, 1, ACTIVE);

  spray_pq_tx->head.next[0] = node1;
  spray_pq_tx->head.next[1] = node2;
  spray_pq_tx->head.next[2] = node3;
  node1->next[0] = node2;
  node2->next[0] = node3;
  node2->next[1] = node3;
  node3->next[0] = node4;
  node3->next[1] = node4;
  node3->next[2] = &spray_pq_tx->tail;
  node4->next[0] = &spray_pq_tx->tail;
  node4->next[1] = &spray_pq_tx->tail;

  return spray_pq_tx;
}

c_spray_pq_tx_t * c_spray_pq_tx_create_test2(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.toplevel = N - 1;
  // spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->head.state = PADDING;
  spray_pq_tx->tail.key = INT64_MAX;
  // spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.state = PADDING;
  spray_pq_tx->tail.toplevel = N - 1;
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    // atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  // pthread_spin_init(&spray_pq_tx->cleaner_lock, PTHREAD_PROCESS_PRIVATE);
  atomic_store_explicit(&spray_pq_tx->cleaner_lock, false, memory_order_relaxed);

  // First prefix
  node_ptr node0 = node_create(0, 1, DELETED);
  node_ptr node1 = node_create(1, 0, DELETED);
  
  // Will be made part of first prefix by cleaner.
  node_ptr node2 = node_create(2, 1, ACTIVE);

  // Safe point 1
  node_ptr node3 = node_create(3, 2, ACTIVE);

  // Second prefix
  node_ptr node4 = node_create(4, 0, DELETED);
  node_ptr node5 = node_create(5, 1, DELETED);
  
  // Safe point 2
  node_ptr node6 = node_create(6, 3, ACTIVE);

  spray_pq_tx->head.next[0] = node0;
  spray_pq_tx->head.next[1] = node0;
  spray_pq_tx->head.next[2] = node3;
  spray_pq_tx->head.next[3] = node6;

  node0->next[0] = node1;
  node0->next[1] = node2;

  node1->next[0] = node2;

  node2->next[0] = node3;
  node2->next[1] = node3;

  node3->next[0] = node4;
  node3->next[1] = node5;
  node3->next[2] = node6;

  node4->next[0] = node5;

  node5->next[0] = node6;
  node5->next[1] = node6;

  node6->next[0] = &spray_pq_tx->tail;
  node6->next[1] = &spray_pq_tx->tail;
  node6->next[2] = &spray_pq_tx->tail;
  node6->next[3] = &spray_pq_tx->tail;


  return spray_pq_tx;
}

c_spray_pq_tx_t * c_spray_pq_tx_create_test3(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.toplevel = N - 1;
  // spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->head.state = PADDING;
  spray_pq_tx->tail.key = INT64_MAX;
  // spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.state = PADDING;
  spray_pq_tx->tail.toplevel = N - 1;
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    // atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  // pthread_spin_init(&spray_pq_tx->cleaner_lock, PTHREAD_PROCESS_PRIVATE);
  atomic_store_explicit(&spray_pq_tx->cleaner_lock, false, memory_order_relaxed);


  // First prefix
  node_ptr node0 = node_create(0, 1, DELETED);
  node_ptr node1 = node_create(1, 0, DELETED);
  node_ptr node2 = node_create(2, 1, DELETED);
  
  // Will be made part of first prefix by cleaner.
  node_ptr node3 = node_create(3, 2, ACTIVE);

  // Safe point 1
  node_ptr node4 = node_create(4, 0, ACTIVE);

  // Second prefix
  node_ptr node5 = node_create(5, 1, DELETED);
  node_ptr node6 = node_create(6, 3, DELETED);
  
  // Safe point 2
  node_ptr node7 = node_create(7, 4, ACTIVE);

  // Head
  spray_pq_tx->head.next[0] = node0;
  spray_pq_tx->head.next[1] = node0;
  spray_pq_tx->head.next[2] = node3;
  spray_pq_tx->head.next[3] = node6;
  spray_pq_tx->head.next[4] = node7;


  node0->next[0] = node1;
  node0->next[1] = node2;

  node1->next[0] = node2;

  node2->next[0] = node3;
  node2->next[1] = node3;

  node3->next[0] = node4;
  node3->next[1] = node5;
  node3->next[2] = node6;

  node4->next[0] = node5;

  node5->next[0] = node6;
  node5->next[1] = node6;

  node6->next[0] = node7;
  node6->next[1] = node7;
  node6->next[2] = node7;
  node6->next[3] = node7;

  node7->next[0] = &spray_pq_tx->tail;
  node7->next[1] = &spray_pq_tx->tail;
  node7->next[2] = &spray_pq_tx->tail;
  node7->next[3] = &spray_pq_tx->tail;
  node7->next[4] = &spray_pq_tx->tail;


  return spray_pq_tx;
}


c_spray_pq_tx_t * c_spray_pq_tx_create_test4(int64_t threads) {
  c_spray_pq_tx_t* spray_pq_tx = forkscan_malloc(sizeof(c_spray_pq_tx_t));
  spray_pq_tx->config = c_spray_pq_config_paper(threads);
  spray_pq_tx->head.key = INT64_MIN;
  spray_pq_tx->head.toplevel = N - 1;
  // spray_pq_tx->head.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->head.state, PADDING, memory_order_relaxed);
  spray_pq_tx->head.state = PADDING;
  spray_pq_tx->tail.key = INT64_MAX;
  // spray_pq_tx->tail.state = forkscan_malloc(sizeof(_Atomic(state_t)));
  // atomic_store_explicit(spray_pq_tx->tail.state, PADDING, memory_order_relaxed);
  spray_pq_tx->tail.state = PADDING;
  spray_pq_tx->tail.toplevel = N - 1;
  for(int64_t i = 0; i < N; i++) {
    spray_pq_tx->head.next[i] = &spray_pq_tx->tail;
    spray_pq_tx->tail.next[i] = NULL;
  }
  spray_pq_tx->lock = create_elided_lock();
  spray_pq_tx->padding_head = &spray_pq_tx->head;
  printf("Padding amount %ld\n", spray_pq_tx->config.padding_amount);
  for(int64_t i = 1; i < spray_pq_tx->config.padding_amount; i++) {
    node_ptr node = forkscan_malloc(sizeof(node_t));
    // node->state = forkscan_malloc(sizeof(_Atomic(state_t)));
    // atomic_store_explicit(node->state, PADDING, memory_order_relaxed);
    node->state = PADDING;
    for(int64_t j = 0; j < N; j++) {
      node->next[j] = spray_pq_tx->padding_head;
    }
    spray_pq_tx->padding_head = node;
  }
  // pthread_spin_init(&spray_pq_tx->cleaner_lock, PTHREAD_PROCESS_PRIVATE);
  atomic_store_explicit(&spray_pq_tx->cleaner_lock, false, memory_order_relaxed);


  // First prefix
  node_ptr node0 = node_create(0, 0, DELETED);
  node_ptr node1 = node_create(1, 1, DELETED);
  node_ptr node2 = node_create(2, 2, DELETED);
  
  // Will be made part of first prefix by cleaner.
  node_ptr node3 = node_create(3, 3, ACTIVE);

  // Safe point
  node_ptr node4 = node_create(4, 0, ACTIVE);
  node_ptr node5 = node_create(5, 1, ACTIVE);
  node_ptr node6 = node_create(6, 2, ACTIVE);
  node_ptr node7 = node_create(7, 3, ACTIVE);

  // Head
  spray_pq_tx->head.next[0] = node0;
  spray_pq_tx->head.next[1] = node1;
  spray_pq_tx->head.next[2] = node2;
  spray_pq_tx->head.next[3] = node3;


  node0->next[0] = node1;

  node1->next[0] = node2;
  node1->next[1] = node2;

  node2->next[0] = node3;
  node2->next[1] = node3;
  node2->next[2] = node3;

  node3->next[0] = node4;
  node3->next[1] = node5;
  node3->next[2] = node6;
  node3->next[3] = node7;
  

  node4->next[0] = node5;

  node5->next[0] = node6;
  node5->next[1] = node6;

  node6->next[0] = node7;
  node6->next[1] = node7;
  node6->next[2] = node7;

  node7->next[0] = &spray_pq_tx->tail;
  node7->next[1] = &spray_pq_tx->tail;
  node7->next[2] = &spray_pq_tx->tail;
  node7->next[3] = &spray_pq_tx->tail;  

  return spray_pq_tx;
}

static bool find(c_spray_pq_tx_t *pqueue, int64_t key, 
  node_ptr preds[N], node_ptr succs[N]) {
  node_ptr left = &pqueue->head;
  for(int64_t i = N - 1; i >= BOTTOM; i--) {
    node_ptr left_next = left->next[i];
    node_ptr right = left_next;
    while(true) {
      while(right->state == DELETED) {
        right = right->next[i];
      }
      if(left_next != right) {
        left->next[i] = right;
      }
      assert(left->state != DELETED);
      assert(right->state != DELETED);
      if(right->key < key) {
        left = right;
        right = right->next[i];
      } else {
        assert(left->state != DELETED);
        assert(right->state != DELETED);
        break;
      }
    }
    preds[i] = left;
    succs[i] = right;
  }
  return succs[BOTTOM]->key == key;
}

int find_external(c_spray_pq_tx_t *pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  return find(pqueue, key, preds, succs);
}


/* Perform the spray operation in the skiplist, looking for a node to attempt
 * to dequeue from the priority queue.
 */
static node_ptr spray(uint64_t * seed, c_spray_pq_tx_t * pqueue) {
  node_ptr cur_node = pqueue->padding_head;
  int64_t D = pqueue->config.descend_amount;
  for(int64_t H = pqueue->config.start_height; H >= 0; H = H - D) {
    int64_t jump = fast_rand(seed) % (pqueue->config.max_jump + 1);
    while(jump-- > 0) {
      node_ptr next = cur_node->next[H];
      if(next == &pqueue->tail || next == NULL) {
        break;
      }
      cur_node = next;
    }
  }
  return cur_node;
}

static bool verify_node(node_ptr node) {
  bool val = (node->state == ACTIVE);
  for(int64_t i = 0; i <= node->toplevel; i++) {
    val = val && (node->next[i]->state != DELETED);
  }
  return val;
}

bool c_spray_pq_tx_verify(c_spray_pq_tx_t * pqueue) {
  // Everything in between
  for(node_ptr curr = pqueue->head.next[BOTTOM];
    curr != &pqueue->tail;
    curr = curr->next[BOTTOM]) {
    assert(verify_node(curr));
    if(!verify_node(curr)) { return false; }
  }
  return true;
}

int c_spray_pq_tx_add(uint64_t *seed, c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  int32_t toplevel = random_level(seed, N);
  node_ptr node = node_create(key, toplevel, ACTIVE);
  bool added = false;
  // printf("Trying to add %ld\n", key);
retry:
  // printf("Beginning add\n");
  // c_spray_pq_tx_print(pqueue);
  lock(pqueue->lock);
  bool found = find(pqueue, key, preds, succs);
  if(found && succs[BOTTOM]->state == DELETED) {
    // If deleted remove and retry on.
    // TODO: Flip state back to active?
    for(int64_t i = 0; i <= succs[BOTTOM]->toplevel; i++) {
      if(preds[i]->next[i]->state == DELETED && preds[i]->next[i]->key <= key) {
        preds[i]->next[i] = succs[BOTTOM]->next[i];
      }
    }
    unlock(pqueue->lock);
    goto retry;
  } else if(!found){
    for(int64_t i = 0; i <= toplevel; i++) {
      node->next[i] = succs[i];
      preds[i]->next[i] = node;
    }
    added = true;
  }
  unlock(pqueue->lock);
  if(!added) {
    forkscan_free(node);
    // printf("Didn't add %ld found: %d\n", key, found);
    // print_node(succs[BOTTOM]);
    // printf("Key found: %ld with state: %ud\n", succs[BOTTOM]->key, succs[BOTTOM]->state);
  }
  // printf("After add\n");
  // c_spray_pq_tx_print(pqueue);
  return added;
}


static int c_spray_pq_tx_remove_leaky(c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(pqueue->lock);
  if(find(pqueue, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(pqueue->lock);
  return node != NULL;
}

static int c_spray_pq_tx_remove(c_spray_pq_tx_t * pqueue, int64_t key) {
  node_ptr preds[N], succs[N];
  node_ptr node = NULL;
  lock(pqueue->lock);
  if(find(pqueue, key, preds, succs)) {
    node = succs[0];
    int64_t toplevel = node->toplevel;
    for(int64_t i = 0; i <= toplevel; i++) {
      preds[i]->next[i] = node->next[i];
    }
  }
  unlock(pqueue->lock);
  forkscan_retire((void*)node);
  return node != NULL;
}


int c_spray_pq_tx_pop_min_leaky(uint64_t *seed, c_spray_pq_tx_t *pqueue) {

  bool cleaner = ((fast_rand(seed) % (pqueue->config.thread_count)) == 0);
retry:
  if(cleaner) {

    bool locked = atomic_load_explicit(&pqueue->cleaner_lock, memory_order_relaxed);
    if(locked){ cleaner = false; goto retry; }
    if(atomic_exchange_explicit(&pqueue->cleaner_lock, true, memory_order_acquire) == true) { cleaner = false; goto retry; }

    bool claimed_node = false;
    lock(pqueue->lock);
    node_ptr left = &pqueue->head;
    node_ptr right = pqueue->head.next[BOTTOM];
    for(; right != &pqueue->tail; right = right->next[BOTTOM]) {
      state_t state = right->state;
      if(state == DELETED) { continue; }
      if(state == ACTIVE) {
        if(!claimed_node) {
          right->state = DELETED;
          claimed_node = true;
          continue;
        }
        pqueue->head.next[BOTTOM] = right;
        unlock(pqueue->lock);
        atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
        return true;
      }
    }
    pqueue->head.next[BOTTOM] = &pqueue->tail;
    unlock(pqueue->lock);
    atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
    return claimed_node;
    
    
    // if(pthread_spin_trylock(&pqueue->cleaner_lock) != 0) { cleaner = false; goto retry; }
    // Here. We. Go.
    node_ptr lefts[N];
    bool needs_swing[N];
    lock(pqueue->lock);
    // Get our snapshot of rolling pointers.
    for(int64_t i = N - 1; i >= BOTTOM; i--) {
      lefts[i] = &pqueue->head;
      needs_swing[i] = false;
    }
    // printf("Beginning\n");
    // c_spray_pq_tx_print(pqueue);

    int64_t node_needing_swing = 0;
    // bool claimed_node = false;
    while(true) {
      // printf("Looping again!\n");
      int64_t valid_height = -1;
      node_ptr left_next = lefts[BOTTOM]->next[BOTTOM];
      node_ptr right = left_next;
      // Node scan...
      while(true) {
        // printf("Scanning for nodes!\n");
        if(right == &pqueue->tail) {
          // We don't need to swing any pointers and we've seen the tail.
          if(node_needing_swing == 0) {
            unlock(pqueue->lock);
            atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
            //pthread_spin_unlock(&pqueue->cleaner_lock);
            // c_spray_pq_tx_print(pqueue);
            // assert(c_spray_pq_tx_verify(pqueue));
            // assert(false);
            return claimed_node;
          }
          valid_height = right->toplevel;
        } else if(right->state == ACTIVE) {
          if(!claimed_node) {
            // printf("Claimed node %ld\n", right->key);
            // print_node(right);
            right->state = DELETED;
            claimed_node = true;
            // Node is now deleted, retry!
            continue;
          } else {
            // printf("Found valid node\n");
            // print_node(right);
            valid_height = right->toplevel;
            for(int64_t i = right->toplevel; i >= BOTTOM; i--) {
              if(!needs_swing[i]) {
                lefts[i] = right;
              }
            }
          }
        } else {
          // In deleted prefix.
          // The pointers at these levels need to move around this node.
          // printf("Found deleted node %ld\n", right->key);
          // print_node(right);
          for(int64_t i = 0; i <= right->toplevel; i++) {
            if(!needs_swing[i] && lefts[i]->next[i]->key <= right->key) {
              // printf("Need to swing %ld\n", i);
              node_needing_swing++;
              needs_swing[i] = true;
            }
          }
        }
        if(valid_height != -1) { break; }
        right = right->next[BOTTOM];
      }
      // Did we skip over anyone?
      // Or, did we find someone of interest?
      if(left_next != right || valid_height >= 0) {
        assert(right != NULL);
        assert(valid_height >= 0);
        // printf("Lefts before swing\n");
        for(int64_t i = N - 1; i >= BOTTOM; i--) {
          // print_node(lefts[i]);
        }
        // printf("Left nexts\n");
        for(int64_t i = N - 1; i >= BOTTOM; i--) {
          // print_node(lefts[i]->next[i]);
        }
        for(int64_t i = N - 1; i >= BOTTOM; i--) {
          // printf("%ld - %d\n", i, needs_swing[i]);
        }
        // printf("Replacement\n");
        // print_node(right);
        for(int64_t level = valid_height; level >= BOTTOM; level--) {
          assert(level <= right->toplevel);
          if(needs_swing[level]) {
            // printf("Swinging level %ld\n", level);
            assert(right->state != DELETED);
            assert(lefts[level]->next[level]->state == DELETED);
            lefts[level]->next[level] = right;
            lefts[level] = right;
            node_needing_swing--;
            needs_swing[level] = false;
          }
        }
        if(right == &pqueue->tail) {
          // printf("Tail exit\n");
          if(!claimed_node) {
            // printf("No node\n");
            // c_spray_pq_tx_print(pqueue);
          }
          unlock(pqueue->lock);
          // pthread_spin_unlock(&pqueue->cleaner_lock);
          atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
          return claimed_node;
        }
        // printf("Lefts after swing\n");
        for(int64_t i = N - 1; i >= BOTTOM; i--) {
          // print_node(lefts[i]);
        }
        // printf("Left nexts\n");
        for(int64_t i = N - 1; i >= BOTTOM; i--) {
          // print_node(lefts[i]->next[i]);
        }
      }
      if(node_needing_swing != 0) {
        bool done = true;
        for(int64_t l = 0; l < N; l++) {
          if(needs_swing[l]) {
            done = false;
          }
        }
        assert(!done);
        // printf("Not done yet!\n");
        continue;
      }

      if(claimed_node) {
        // atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
        unlock(pqueue->lock);
        // pthread_spin_unlock(&pqueue->cleaner_lock);
        atomic_store_explicit(&pqueue->cleaner_lock, false, memory_order_release);
        // c_spray_pq_tx_print(pqueue);
        // assert(c_spray_pq_tx_verify(pqueue));
        // assert(false);
        return true;
      }
    }
  } else {
    node_ptr node_popped = NULL;
    lock(pqueue->lock);
    node_ptr node = spray(seed, pqueue);
    // If we're not passed the head yet, start just after there.
    if(node->state == PADDING) {
      node = pqueue->head.next[BOTTOM];
    }
    // unlock(pqueue->lock);
    // lock(pqueue->lock);
    for(; node != &pqueue->tail; node = node->next[BOTTOM]) {
      state_t state = node->state;
      if(state == PADDING || state == DELETED) { continue; }
      if(state == ACTIVE) {
        node->state = DELETED;
        unlock(pqueue->lock);
        // c_spray_pq_tx_remove_leaky(pqueue, node->key);
        return true;
      }
    }
    unlock(pqueue->lock);
    return false;
  }
}
