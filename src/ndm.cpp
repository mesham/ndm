/*
 * ndm.cpp
 *
 *  Created on: 22 Nov 2015
 *      Author: nick
 */

#include "ndm.h"
#include "threadpool.h"
#include "messaging.h"
#include "p2p.h"
#include "bcast.h"
#include "reduce.h"
#include "allreduce.h"
#include "barrier.h"
#include "groups.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "data_types.h"
#include <iostream>
#include <unistd.h>

ThreadPool threadPool;
Messaging messaging;

bool continue_polling;
static int listenerThreadId;

void threadEntryPoint(void*);
static void awaitGlobalCompletion();

static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

int ndmInit(void) {
  continue_polling = true;
  threadPool.initThreadPool();
  messaging.init();
  initialiseGroupDirectory();
  initialise_ndmBarrier();
  initialise_ndmReduce(messaging, threadPool);
  initialise_ndmAllReduce(messaging, threadPool);
  initialise_ndmBcast(messaging, threadPool);
  listenerThreadId = threadPool.startListenerThread(threadEntryPoint, NULL);
  return 0;
}

void threadEntryPoint(void* data) {
  while (continue_polling) {
    SpecificMessage* m = messaging.awaitCommand();  // do inside a thread
    if (m != NULL) threadPool.startThread(messaging.handleCommand, (void*)m);
  }
}

void ndmSend(void* data, int size, int type, int target, NDM_Group comm_group, const char* unique_id) {
  p2p_ndmSend(messaging, data, type, size, target, comm_group, unique_id);
}

int ndmRecv(void (*callback)(void*, NDM_Metadata), int source, NDM_Group comm_group, const char* unique_id) {
  p2p_ndmRecv(messaging, callback, source, NDM_ANY_MYRANK, false, comm_group, unique_id);
  return 0;
}

int ndmRecvStream(void (*callback)(void*, NDM_Metadata), int source, NDM_Group comm_group, const char* unique_id) {
  p2p_ndmRecv(messaging, callback, source, NDM_ANY_MYRANK, true, comm_group, unique_id);
  return 0;
}

int ndmRecvFromRank(void (*callback)(void*, NDM_Metadata), int source, int my_rank, NDM_Group comm_group, const char* unique_id) {
  p2p_ndmRecv(messaging, callback, source, my_rank, false, comm_group, unique_id);
  return 0;
}

int ndmBcast(void* data, int size, int type, void (*callback)(void*, NDM_Metadata), int root, NDM_Group comm_group,
             const char* unique_id) {
  collective_ndmBcast(messaging, threadPool, data, type, size, callback, root, NDM_ANY_MYRANK, comm_group, unique_id);
  return 0;
}

int ndmBcastFromRank(void* data, int size, int type, void (*callback)(void*, NDM_Metadata), int root, int my_rank,
                     NDM_Group comm_group, const char* unique_id) {
  collective_ndmBcast(messaging, threadPool, data, type, size, callback, root, my_rank, comm_group, unique_id);
  return 0;
}

int ndmReduce(void* data, int size, int type, NDM_Op operation, void (*callback)(void*, NDM_Metadata), int root, NDM_Group comm_group,
              const char* unique_id) {
  collective_ndmReduce(&messaging, &threadPool, data, type, size, size, 1, 0, operation, callback, root, NDM_ANY_MYRANK, comm_group,
                       unique_id);
  return 0;
}

int ndmReduceFromRank(void* data, int size, int type, NDM_Op operation, void (*callback)(void*, NDM_Metadata), int root, int my_rank,
                      NDM_Group comm_group, const char* unique_id) {
  collective_ndmReduce(&messaging, &threadPool, data, type, size, size, 1, 0, operation, callback, root, my_rank, comm_group,
                       unique_id);
  return 0;
}

int ndmReduceAdditive(void* data, int size, int type, int totalsize, int contributionsPerElement, int startelement, NDM_Op operation,
                      void (*callback)(void*, NDM_Metadata), int root, NDM_Group comm_group, const char* unique_id) {
  collective_ndmReduce(&messaging, &threadPool, data, type, size, totalsize, contributionsPerElement, startelement, operation,
                       callback, root, NDM_ANY_MYRANK, comm_group, unique_id);
  return 0;
}

int ndmAllReduce(void* data, int size, int type, NDM_Op operation, void (*callback)(void*, NDM_Metadata), NDM_Group comm_group,
                 const char* unique_id) {
  collective_ndmAllReduce(messaging, threadPool, data, type, size, operation, callback, NDM_ANY_MYRANK, comm_group, unique_id);
  return 0;
}

int ndmAllReduceFromRank(void* data, int size, int type, NDM_Op operation, void (*callback)(void*, NDM_Metadata), int my_rank,
                         NDM_Group comm_group, const char* unique_id) {
  collective_ndmAllReduce(messaging, threadPool, data, type, size, operation, callback, my_rank, comm_group, unique_id);
  return 0;
}

int ndmBarrier(void (*callback)(NDM_Metadata), NDM_Group comm_group, const char* unique_id) {
  collective_ndmBarrier(messaging, threadPool, callback, comm_group, unique_id);
  return 0;
}

int ndmGroupCreate(NDM_Group* newGroupHandle, int numberGroupEntries, int* groupGlobalRanks) {
  *newGroupHandle = addGroup(numberGroupEntries, groupGlobalRanks);
  return 0;
}

int ndmGroupCreateWithStride(NDM_Group* newGroupHandle, NDM_Group baseGroup, int numberContiguous, int stride) {
  *newGroupHandle = createContiguousGroupWithStride(baseGroup, numberContiguous, stride);
  return 0;
}

int ndmGroupExtractChunkContainingRank(NDM_Group* newGroupHandle, NDM_Group baseGroup, int extractionSize, int rank) {
  *newGroupHandle = extractGroupBasedOnSizeAndRank(baseGroup, extractionSize, rank);
  return 0;
}

int ndmGroupCreateDisjoint(NDM_Group* newGroupHandle, NDM_Group groupA, NDM_Group groupB) {
  *newGroupHandle = createDisjointGroup(groupA, groupB);
  return 0;
}

int ndmCreateVirtualRanks(NDM_Group* newGroupHandle, NDM_Group existingGroup, int virtualRanksPerProcess) {
  *newGroupHandle = createVirtualRanksInGroup(existingGroup, virtualRanksPerProcess);
  return 0;
}

int ndmGroupSize(NDM_Group groupId, int* targetSize) {
  *targetSize = getGroupSize(groupId);
  return 0;
}

int ndmGroupRank(NDM_Group groupId, int* targetRank) {
  *targetRank = getMyGroupRank(groupId);
  return 0;
}

int ndmIsRankLocal(NDM_Group groupId, int rank, int* flag) {
  *flag = isRankLocalToGroup(groupId, rank);
  return 0;
}

int ndmGroupLocalSize(NDM_Group groupId, int* flag) {
  *flag = getGroupLocalSize(groupId);
  return 0;
}

int ndmGetGroupsGlobalNthRank(NDM_Group groupId, int nthLocal, int* globalGroupRank) {
  *globalGroupRank = getLocalNthGroupRank(groupId, nthLocal);
  return 0;
}

void generateUUID(char* str, unsigned int random_num) {
  int i, random_stride = 1000;
  for (i = 0; i < NDM_AUTO_UUID_LENGTH; ++i) {
    str[i] = alphanum[random_num % (sizeof(alphanum) - 1)];
    random_num += random_stride;
    random_stride /= 2;
    if (random_stride <= 0) random_stride = 1000;
  }
  str[NDM_AUTO_UUID_LENGTH] = '\0';
}

int ndmFinalise(void) {
  awaitGlobalCompletion();
  continue_polling = false;
  messaging.ceasePolling();
  messaging.handleOutstandingRequests();
  while (!threadPool.isThreadPoolFinished()) {
  }
  return 0;
}

static void awaitGlobalCompletion() {
  bool continueToAwait = true;
  while (continueToAwait) {
    while (!threadPool.isThreadPoolFinishedApartFromListenerThread(listenerThreadId) || !messaging.readyToShutdown()) {
    }
    messaging.clearMessagingActive();
    sleep(2);
    continueToAwait = messaging.getMessagingActive();
  }
}
