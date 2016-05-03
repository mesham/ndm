/*
 * barrier.cpp
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#include <vector>
#include <iterator>
#include <string.h>
#include "barrier.h"
#include "messaging.h"
#include "threadpool.h"
#include "allreduce.h"
#include "pthread.h"
#include "misc.h"
#include "ndm.h"

static std::vector<BarrierState*> barrier_state;
static pthread_mutex_t barrier_state_mutex;

static void reductionCallback(void*, NDM_Metadata);

void initialise_ndmBarrier() { pthread_mutex_init(&barrier_state_mutex, NULL); }

void collective_ndmBarrier(Messaging messaging, ThreadPool threadPool, void (*callback)(NDM_Metadata), NDM_Group comm_group,
                           const char* unique_id) {
  BarrierState* new_state = new BarrierState(callback, unique_id);
  pthread_mutex_lock(&barrier_state_mutex);
  barrier_state.insert(barrier_state.begin(), new_state);
  pthread_mutex_unlock(&barrier_state_mutex);
  collective_ndmAllReduce(messaging, threadPool, NULL, NDM_NOTYPE, 0, NDM_NO_OPERATION, reductionCallback, NDM_ANY_MYRANK, comm_group,
                          unique_id);
}

static void reductionCallback(void* data, NDM_Metadata metaData) {
  std::string searchSalt = std::string(metaData.unique_id);
  size_t wildCardLocB = searchSalt.find('*');
  pthread_mutex_lock(&barrier_state_mutex);
  std::vector<BarrierState*>::iterator it;
  BarrierState* specificState = NULL;
  for (it = barrier_state.begin(); it != barrier_state.end(); it++) {
    size_t wildCardLocA = (*it)->getUniqueId().find('*');
    if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
      if (wildCardLocA == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocB).compare(searchSalt.substr(0, wildCardLocB)) == 0) specificState = (*it);
      } else if (wildCardLocB == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocA)) == 0) specificState = (*it);
      } else {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocB)) == 0) specificState = (*it);
      }
    } else {
      if ((*it)->getUniqueId().compare(searchSalt) == 0) specificState = (*it);
    }
    if (specificState != NULL) break;
  }
  if (specificState == NULL) raiseError("Barrier state not found");
  barrier_state.erase(it);
  void (*cb)(NDM_Metadata) = specificState->getCallback();
  delete (specificState);
  pthread_mutex_unlock(&barrier_state_mutex);
  cb(metaData);
}
