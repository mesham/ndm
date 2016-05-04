/*
 * allreduce.cpp
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#include <vector>
#include <iterator>
#include <string.h>
#include "allreduce.h"
#include "reduce.h"
#include "bcast.h"
#include "messaging.h"
#include "threadpool.h"
#include "pthread.h"
#include "misc.h"
#include "groups.h"
#include "ndm.h"

#define REDUCTION_ROOT 0

static std::vector<AllReduceState*> allreduce_state;
static pthread_mutex_t allreduce_state_mutex;
static Messaging messaging;
static ThreadPool threadPool;

static void reductionCallback(void*, NDM_Metadata);
static void broadcastCallback(void*, NDM_Metadata);
static AllReduceState* findAllReduceState(const char*, bool);

void initialise_ndmAllReduce(Messaging messaging_arg, ThreadPool threadPool_arg) {
  messaging = messaging_arg;
  threadPool = threadPool_arg;
  pthread_mutex_init(&allreduce_state_mutex, NULL);
}

void collective_ndmAllReduce(Messaging messaging_arg, ThreadPool threadPool_arg, void* data, int type, int size, NDM_Op operation,
                             void (*callback)(void*, NDM_Metadata), int my_rank, NDM_Group comm_group, const char* unique_id) {
  pthread_mutex_lock(&allreduce_state_mutex);
  AllReduceState* specificState = findAllReduceState(unique_id, false);
  bool newEntry = specificState == NULL;
  if (newEntry) {
    specificState = new AllReduceState(callback, getGroupLocalSize(comm_group), unique_id);
    specificState->incrementNumberEntriesRetrieved();
    allreduce_state.insert(allreduce_state.begin(), specificState);
  }
  pthread_mutex_unlock(&allreduce_state_mutex);
  collective_ndmReduce(&messaging, &threadPool, data, type, size, size, 1, 0, operation, reductionCallback, REDUCTION_ROOT, my_rank,
                       comm_group, unique_id);
  bool rankIsLocalToGroup =
      (my_rank >= 0 && my_rank == REDUCTION_ROOT) || (my_rank == NDM_ANY_MYRANK && isRankLocalToGroup(comm_group, REDUCTION_ROOT));
  if (!rankIsLocalToGroup) {
    collective_ndmBcast(messaging, threadPool, NULL, type, size, broadcastCallback, REDUCTION_ROOT, my_rank, comm_group, unique_id);
  } else if (!newEntry) {
    specificState->lock();
    int myRank = my_rank >= 0 ? my_rank : getLocalNthGroupRank(comm_group, specificState->getNumberEntriesRetrieved());
    specificState->incrementNumberEntriesRetrieved();
    specificState->unlock();
    collective_ndmBcast(messaging, threadPool, NULL, type, size, broadcastCallback, REDUCTION_ROOT, myRank, comm_group, unique_id);
  }
}

static void reductionCallback(void* data, NDM_Metadata metaData) {
  collective_ndmBcast(messaging, threadPool, data, metaData.data_type, metaData.number_elements, broadcastCallback, REDUCTION_ROOT,
                      REDUCTION_ROOT, metaData.comm_group, metaData.unique_id);
}

static void broadcastCallback(void* data, NDM_Metadata metaData) {
  pthread_mutex_lock(&allreduce_state_mutex);
  AllReduceState* specificState = findAllReduceState(metaData.unique_id, false);
  pthread_mutex_unlock(&allreduce_state_mutex);
  if (specificState == NULL) raiseError("Allreduce state not found");
  specificState->lock();
  specificState->incrementCalledBack();
  bool lastEntry = specificState->getCalledBack() >= specificState->getNumberExpectedLocalCallbacks();
  void (*cb)(void*, NDM_Metadata) = specificState->getCallback();
  if (lastEntry) {
    pthread_mutex_lock(&allreduce_state_mutex);
    findAllReduceState(metaData.unique_id, true);
    pthread_mutex_unlock(&allreduce_state_mutex);
    delete (specificState);
  }
  specificState->unlock();
  cb(data, metaData);
}

static AllReduceState* findAllReduceState(const char* uniqueId, bool eraseEntry) {
  std::string searchSalt = std::string(uniqueId);
  size_t wildCardLocB = searchSalt.find('*');
  AllReduceState* specificState = NULL;
  std::vector<AllReduceState*>::iterator it;
  for (it = allreduce_state.begin(); it != allreduce_state.end(); it++) {
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
  if (eraseEntry && specificState != NULL) allreduce_state.erase(it);
  return specificState;
}
