/*
 * reduce.cpp
 *
 *  Created on: 2 Jan 2016
 *      Author: nick
 */

#include <vector>
#include <iterator>
#include <string.h>
#include <stdlib.h>
#include "messaging.h"
#include "reduce.h"
#include "ndm.h"
#include "data_types.h"
#include "pthread.h"
#include "misc.h"
#include "groups.h"

static int REDUCE_ACTION_ID = 2;

static std::vector<ReductionState*> reduction_state;
static pthread_mutex_t reduction_state_lock;
static Messaging messaging;
static ThreadPool threadPool;

static void reduction_callback_at_root(void*, NDM_Metadata);
static ReductionState* findReductionState(const char*);
static void sendToSpecificProcess(Messaging*, void*, int, int, int, int, NDM_Group, const char*);
static void applyOperation(ReductionState*, void*, int, int);
static void applyActualOperation(int*, int*, NDM_Op);
static void applyActualOperation(float*, float*, NDM_Op);
static void applyActualOperation(double*, double*, NDM_Op);

void initialise_ndmReduce(Messaging messaging_arg, ThreadPool threadPool_arg) {
  messaging = messaging_arg;
  threadPool = threadPool_arg;
  pthread_mutex_init(&reduction_state_lock, NULL);
}

void collective_ndmReduce(Messaging* messaging, ThreadPool* threadPool, void* data, int type, int size, int totalSize, int startPoint,
                          NDM_Op operation, void (*callback)(void*, NDM_Metadata), int root, int my_rank, NDM_Group comm_group,
                          const char* unique_id) {
  if (getGroupSize(comm_group) == 1) {
    callback(data, messaging->generateMetaData(type, totalSize, my_rank, root, comm_group, unique_id));
  } else {
    pthread_mutex_lock(&reduction_state_lock);
    ReductionState* state = findReductionState(unique_id);
    if (state == NULL) {
      state = new ReductionState(type, totalSize, callback, operation, root, comm_group, unique_id);
      reduction_state.insert(reduction_state.begin(), state);
    }
    applyOperation(state, data, size, startPoint);
    if (state->isDataSettingComplete()) {
      pthread_mutex_unlock(&reduction_state_lock);
      state->incrementContributedProcesses();
      if ((my_rank >= 0 && my_rank == root) || (my_rank == NDM_ANY_MYRANK && isRankLocalToGroup(comm_group, root))) {
        int i;
        for (i = 1; i < getNumberDistinctProcesses(comm_group); i++) {
          messaging->registerCommand(unique_id, NDM_ANY_MYRANK, root, comm_group, REDUCE_ACTION_ID, false, reduction_callback_at_root);
        }
      } else {
        sendToSpecificProcess(messaging, state->getData(), type, totalSize, my_rank, root, comm_group, unique_id);
      }
    } else {
      pthread_mutex_unlock(&reduction_state_lock);
    }
  }
}

static ReductionState* findReductionState(const char* uniqueId) {
  std::string searchSalt = std::string(uniqueId);
  size_t wildCardLocB = searchSalt.find('*');
  std::vector<ReductionState*>::iterator it;
  ReductionState* specificState = NULL;
  for (it = reduction_state.begin(); it != reduction_state.end(); it++) {
    size_t wildCardLocA = (*it)->getUniqueId().find('*');
    if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
      if (wildCardLocA == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocB).compare(searchSalt.substr(0, wildCardLocB)) == 0) return *it;
      } else if (wildCardLocB == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocA)) == 0) return *it;
      } else {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocB)) == 0) return *it;
      }
    } else {
      if ((*it)->getUniqueId().compare(searchSalt) == 0) return *it;
    }
  }
  return NULL;
}

static void sendToSpecificProcess(Messaging* messaging, void* data, int type, int size, int source, int target, NDM_Group comm_group,
                                  const char* unique_id) {
  int dataSize;
  char* buffer = messaging->packageMessage(data, type, size, source, target, comm_group, REDUCE_ACTION_ID, unique_id, &dataSize);
  messaging->sendMessage(buffer, dataSize, target, comm_group);
}

static void reduction_callback_at_root(void* buffer, NDM_Metadata metaData) {
  std::string searchSalt = std::string(metaData.unique_id);
  size_t wildCardLocB = searchSalt.find('*');
  pthread_mutex_lock(&reduction_state_lock);
  std::vector<ReductionState*>::iterator it;
  ReductionState* specificState = NULL;
  for (it = reduction_state.begin(); it != reduction_state.end(); it++) {
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
  if (specificState == NULL) raiseError("Reduction state not found");
  specificState->lock();
  specificState->incrementContributedProcesses();
  bool isComplete = specificState->hasCompletedAllProcessesReduction();
  if (isComplete) reduction_state.erase(it);
  pthread_mutex_unlock(&reduction_state_lock);
  int i;
  if (specificState->getSize() > 0 && specificState->getType() != NDM_NOTYPE && metaData.data_type != NDM_NOTYPE &&
      metaData.number_elements > 0) {
    applyOperation(specificState, buffer, specificState->getSize(), 0);
  }
  specificState->unlock();
  if (isComplete) {
    specificState->getCallback()(
        specificState->getData(),
        messaging.generateMetaData(specificState->getType(), specificState->getSize(), -1, specificState->getRoot(),
                                   specificState->getCommGroup(), specificState->getUniqueId().c_str()));
    delete (specificState);
  }
}

static void applyOperation(ReductionState* specificState, void* source, int dataSize, int startPoint) {
  int i, typeSize = specificState->getMyTypeSize(), actualElement;
  char* existing = (char*)specificState->getData();
  bool dataValues = source != NULL && specificState->getSize() > 0 && specificState->getType() != NDM_NOTYPE;
  for (i = 0; i < dataSize; i++) {
    actualElement = i + startPoint;
    if (dataValues) {
      if (!specificState->isDataElementEmpty(actualElement)) {
        if (specificState->getType() == NDM_INT) {
          applyActualOperation(&((int*)existing)[actualElement], &((int*)source)[i], specificState->getOperation());
        } else if (specificState->getType() == NDM_FLOAT) {
          applyActualOperation(&((float*)existing)[actualElement], &((float*)source)[i], specificState->getOperation());
        } else if (specificState->getType() == NDM_DOUBLE) {
          applyActualOperation(&((double*)existing)[actualElement], &((double*)source)[i], specificState->getOperation());
        }
      } else {
        memcpy(&existing[typeSize * actualElement], &((char*)source)[typeSize * i], typeSize);
      }
      specificState->setDataElementHasValue(actualElement);
    }
    specificState->incrementDataElementContributions(actualElement);
  }
}

static void applyActualOperation(int* existing, int* source, NDM_Op operation) {
  if (operation == NDM_SUM) *existing += *source;
  if (operation == NDM_PROD) *existing *= *source;
  if (operation == NDM_MIN && *source < *existing) *existing = *source;
  if (operation == NDM_MAX && *source > *existing) *existing = *source;
}

static void applyActualOperation(float* existing, float* source, NDM_Op operation) {
  if (operation == NDM_SUM) *existing += *source;
  if (operation == NDM_PROD) *existing *= *source;
  if (operation == NDM_MIN && *source < *existing) *existing = *source;
  if (operation == NDM_MAX && *source > *existing) *existing = *source;
}

static void applyActualOperation(double* existing, double* source, NDM_Op operation) {
  if (operation == NDM_SUM) *existing += *source;
  if (operation == NDM_PROD) *existing *= *source;
  if (operation == NDM_MIN && *source < *existing) *existing = *source;
  if (operation == NDM_MAX && *source > *existing) *existing = *source;
}
