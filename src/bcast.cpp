/*
 * bcast.cpp
 *
 *  Created on: 29 Dec 2015
 *      Author: nick
 */

#include <set>
#include <iterator>
#include "p2p.h"
#include "bcast.h"
#include "messaging.h"
#include "threadpool.h"
#include "data_types.h"
#include <string.h>
#include <stdlib.h>
#include "groups.h"
#include "ndm.h"
#include "pthread.h"

static int BCAST_ACTION_ID = 1;

struct PassToLocalCallBackThread {
  SpecificMessage* message;
  void (*callback)(void*, NDM_Metadata);
};

static Messaging messaging;
static ThreadPool threadPool;

static void localBcastCallback(void*);
static void sendToSpecificProcess(Messaging, void*, int, int, int, int, NDM_Group, const char*);
static BcastState* findExistingBcastState(const char*);

static std::vector<BcastState*> bcastState;
static pthread_mutex_t bcastState_mutex;

void initialise_ndmBcast(Messaging messaging_arg, ThreadPool threadPool_arg) {
  messaging = messaging_arg;
  threadPool = threadPool_arg;
  pthread_mutex_init(&bcastState_mutex, NULL);
}

// Removal of bcast state?

void collective_ndmBcast(Messaging messaging, ThreadPool threadPool, void* data, int type, int size,
                         void (*callback)(void*, NDM_Metadata), int root, int my_rank, NDM_Group comm_group, const char* unique_id) {
  bool rankIsLocalToGroup = (my_rank >= 0 && my_rank == root) || (my_rank == NDM_ANY_MYRANK && isRankLocalToGroup(comm_group, root));
  pthread_mutex_lock(&bcastState_mutex);
  BcastState* state = findExistingBcastState(unique_id);
  if (rankIsLocalToGroup && (my_rank != NDM_ANY_MYRANK || state == NULL)) {
    if (state == NULL) {
      state = new BcastState(unique_id);
      bcastState.push_back(state);
    }
    state->incrementNumberEntriesRetrieved();
    pthread_mutex_unlock(&bcastState_mutex);
    int i;
    for (i = 0; i < getGroupSize(comm_group); i++) {
      if (i != root) {
        sendToSpecificProcess(messaging, data, type, size, root, i, comm_group, unique_id);
      }
    }
    char* data_buffer = NULL;
    if (size * getTypeSize(type) > 0) {
      data_buffer = (char*)malloc(size * getTypeSize(type));
      memcpy(data_buffer, data, size * getTypeSize(type));
    }
    SpecificMessage* message =
        new SpecificMessage(root, root, comm_group, BCAST_ACTION_ID, size, type, new std::string(unique_id), data_buffer);
    PassToLocalCallBackThread* plcbt = (PassToLocalCallBackThread*)malloc(sizeof(PassToLocalCallBackThread));
    plcbt->message = message;
    plcbt->callback = callback;
    threadPool.startThread(localBcastCallback, plcbt);
  } else {
    if (state == NULL) {
      state = new BcastState(unique_id);
      bcastState.push_back(state);
    }
    int myRank = my_rank >= 0 ? my_rank : getLocalNthGroupRank(comm_group, state->getNumberEntriesRetrieved());
    state->incrementNumberEntriesRetrieved();
    pthread_mutex_unlock(&bcastState_mutex);
    messaging.registerCommand(unique_id, root, myRank, comm_group, BCAST_ACTION_ID, false, callback);
  }
}

static BcastState* findExistingBcastState(const char* uniqueId) {
  std::string searchSalt = std::string(uniqueId);
  size_t wildCardLocB = searchSalt.find('*');
  std::vector<BcastState*>::iterator it;
  for (it = bcastState.begin(); it != bcastState.end(); it++) {
    size_t wildCardLocA = (*it)->getUniqueId().find('*');
    if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
      if (wildCardLocA == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocB).compare(searchSalt.substr(0, wildCardLocB)) == 0) return (*it);
      } else if (wildCardLocB == std::string::npos) {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocA)) == 0) return (*it);
      } else {
        if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(searchSalt.substr(0, wildCardLocB)) == 0) return (*it);
      }
    } else {
      if ((*it)->getUniqueId().compare(searchSalt) == 0) return (*it);
    }
  }
  return NULL;
}

static void localBcastCallback(void* data) {
  PassToLocalCallBackThread* plcbt = (PassToLocalCallBackThread*)data;
  SpecificMessage* message = plcbt->message;
  plcbt->callback(message->getData(),
                  messaging.generateMetaData(message->getMessageType(), message->getMessageLength(), message->getSourcePid(),
                                             message->getTargetPid(), message->getCommGroup(), message->getUniqueId()->c_str()));
  free(plcbt->message);
  free(plcbt);
}

static void sendToSpecificProcess(Messaging messaging, void* data, int type, int size, int source, int target, NDM_Group comm_group,
                                  const char* unique_id) {
  int dataSize;
  char* buffer = messaging.packageMessage(data, type, size, source, target, comm_group, BCAST_ACTION_ID, unique_id, &dataSize);
  messaging.sendMessage(buffer, dataSize, target, comm_group);
}
