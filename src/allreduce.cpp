/*
 * allreduce.cpp
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#include <map>
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

static std::map<std::string, AllReduceState*, AllReduceStateComparitor> allreduce_state;
static pthread_mutex_t allreduce_state_mutex;
static Messaging messaging;
static ThreadPool threadPool;

static void reductionCallback(void*, NDM_Metadata);
static void broadcastCallback(void*, NDM_Metadata);

void initialise_ndmAllReduce(Messaging messaging_arg, ThreadPool threadPool_arg) {
  messaging = messaging_arg;
  threadPool = threadPool_arg;
  pthread_mutex_init(&allreduce_state_mutex, NULL);
}

void collective_ndmAllReduce(Messaging messaging_arg, ThreadPool threadPool_arg, void* data, int type, int size, NDM_Op operation,
                             void (*callback)(void*, NDM_Metadata), int my_rank, NDM_Group comm_group, const char* unique_id) {
  pthread_mutex_lock(&allreduce_state_mutex);
  std::map<std::string, AllReduceState*, AllReduceStateComparitor>::iterator it = allreduce_state.find(std::string(unique_id));
  AllReduceState* specificState;
  bool newEntry = it == allreduce_state.end();
  if (newEntry) {
    specificState = new AllReduceState(callback, getGroupLocalSize(comm_group), unique_id);
    specificState->incrementNumberEntriesRetrieved();
    allreduce_state.insert(std::pair<std::string, AllReduceState*>(specificState->getUniqueId(), specificState));
  } else {
    specificState = it->second;
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
  std::map<std::string, AllReduceState*, AllReduceStateComparitor>::iterator it = allreduce_state.find(metaData.unique_id);
  pthread_mutex_unlock(&allreduce_state_mutex);
  if (it == allreduce_state.end()) raiseError("Allreduce state not found");
  AllReduceState* specificState = it->second;
  specificState->lock();
  specificState->incrementCalledBack();
  bool lastEntry = specificState->getCalledBack() >= specificState->getNumberExpectedLocalCallbacks();
  void (*callback)(void*, NDM_Metadata) = specificState->getCallback();
  if (lastEntry) {
    pthread_mutex_lock(&allreduce_state_mutex);
    allreduce_state.erase(allreduce_state.find(metaData.unique_id));
    pthread_mutex_unlock(&allreduce_state_mutex);
    delete (specificState);
  }
  specificState->unlock();
  callback(data, metaData);
}
