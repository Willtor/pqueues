#include "papi_interface.h"
#include <pthread.h>
#include <forkscan.h>

#define NUM_EVENTS 5

int library_init() {
  return PAPI_library_init(PAPI_VER_CURRENT) == PAPI_VER_CURRENT;
}
int register_thread() {
  return PAPI_thread_init(pthread_self) == PAPI_OK;
}

int start_counters() {
  int PAPI_events[NUM_EVENTS] = { PAPI_L1_TCM, 
    PAPI_L2_TCM, PAPI_STL_ICY, PAPI_TOT_INS, PAPI_L1_DCM /*PAPI_TOT_CYC*/};
  return PAPI_start_counters(PAPI_events, NUM_EVENTS) == PAPI_OK;
}

long long* stop_counters() {
  long long *counters = forkscan_malloc(sizeof(int64_t) * NUM_EVENTS);
  bool res = PAPI_stop_counters(counters, NUM_EVENTS) == PAPI_OK;
  if(res) {
    return counters;
  } else {
    forkscan_free(counters);
    return NULL;
  }
}