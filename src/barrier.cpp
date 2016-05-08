/*
 * barrier.cpp
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#include <map>
#include <iterator>
#include <string.h>
#include "barrier.h"
#include "messaging.h"
#include "threadpool.h"
#include "allreduce.h"
#include "pthread.h"
#include "misc.h"
#include "ndm.h"

static std::map<std::string, BarrierState*, UUIDStateComparitor> barrier_state;
static pthread_mutex_t barrier_state_mutex;

static void reductionCallback(void*, NDM_Metadata);

void initialise_ndmBarrier() { pthread_mutex_init(&barrier_state_mutex, NULL); }

void collective_ndmBarrier(Messaging messaging, ThreadPool threadPool, void (*callback)(NDM_Metadata), NDM_Group comm_group,
                           const char* unique_id) {
  BarrierState* new_state = new BarrierState(callback, unique_id);
  pthread_mutex_lock(&barrier_state_mutex);
  barrier_state.insert(std::pair<std::string, BarrierState*>(new_state->getUniqueId(), new_state));
  pthread_mutex_unlock(&barrier_state_mutex);
  collective_ndmAllReduce(messaging, threadPool, NULL, NDM_NOTYPE, 0, NDM_NO_OPERATION, reductionCallback, NDM_ANY_MYRANK, comm_group,
                          unique_id);
}

static void reductionCallback(void* data, NDM_Metadata metaData) {
  pthread_mutex_lock(&barrier_state_mutex);
  std::map<std::string, BarrierState*, UUIDStateComparitor>::iterator it = barrier_state.find(metaData.unique_id);
  if (it == barrier_state.end()) raiseError("Barrier state not found");
  BarrierState* specificState = it->second;
  barrier_state.erase(it);
  void (*callback)(NDM_Metadata) = specificState->getCallback();
  delete (specificState);
  pthread_mutex_unlock(&barrier_state_mutex);
  callback(metaData);
}
