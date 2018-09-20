#include <pthread.h>

typedef struct thread_pinner_t thread_pinner_t;

thread_pinner_t * thread_pinner_create();
int get_num_cores();
int pin_thread(thread_pinner_t *thread_pinner, pthread_t thread);