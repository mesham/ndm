/*
 * groups.cpp
 *
 *  Created on: 4 Apr 2016
 *      Author: nick
 */

#include <map>
#include <iterator>
#include "misc.h"
#include "groups.h"
#include "mpi.h"
#include "ndm.h"
#include "pthread.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static std::map<int, SpecificGroup*> groups;
static NDM_Group groupCurrentId;
static int myGlobalRank, globalSize;
static pthread_mutex_t currentGroupId_mutex;

static void addGroupToDirectory(NDM_Group, int, int, int*);

void initialiseGroupDirectory() {
  pthread_mutex_init(&currentGroupId_mutex, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &myGlobalRank);
  MPI_Comm_size(MPI_COMM_WORLD, &globalSize);
  groupCurrentId = NDM_GLOBAL_GROUP + 1;
  int* groupRanks = (int*)malloc(sizeof(int) * globalSize);
  int i;
  for (i = 0; i < globalSize; i++) groupRanks[i] = i;
  addGroupToDirectory(NDM_GLOBAL_GROUP, globalSize, myGlobalRank, groupRanks);
}

static void addGroupToDirectory(NDM_Group group, int groupSize, int myGroupRank, int* groupRanks) {
  SpecificGroup* newGroup = new SpecificGroup(group, groupSize, myGroupRank, groupRanks, 1);
  groups.insert(groups.end(), std::pair<int, SpecificGroup*>(group, newGroup));
}

NDM_Group addGroup(int numberEntries, int* groupRanks) {
  int* allocatedGroupRanks = (int*)malloc(sizeof(int) * numberEntries);
  memcpy(allocatedGroupRanks, groupRanks, sizeof(int) * numberEntries);
  int myRank = -1, i;
  for (i = 0; i < numberEntries; i++) {
    if (groupRanks[i] == myGlobalRank) myRank = i;
  }
  pthread_mutex_lock(&currentGroupId_mutex);
  int specificGroupId = groupCurrentId++;
  pthread_mutex_unlock(&currentGroupId_mutex);
  addGroupToDirectory(specificGroupId, numberEntries, myRank, allocatedGroupRanks);
  return specificGroupId;
}

NDM_Group createVirtualRanksInGroup(NDM_Group baseGroup, int numberVirtualRanksPerGroup) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(baseGroup);
  if (it != groups.end()) {
    pthread_mutex_lock(&currentGroupId_mutex);
    int specificGroupId = groupCurrentId++;
    pthread_mutex_unlock(&currentGroupId_mutex);
    int* groupRanks = (int*)malloc(sizeof(int) * it->second->getGroupSize() * numberVirtualRanksPerGroup);
    int i, j;
    for (i = 0; i < it->second->getGroupSize(); i++) {
      for (j = 0; j < numberVirtualRanksPerGroup; j++) {
        groupRanks[(i * numberVirtualRanksPerGroup) + j] = it->second->getGroupEntries()[i];
      }
    }
    SpecificGroup* newGroup =
        new SpecificGroup(specificGroupId, it->second->getGroupSize() * numberVirtualRanksPerGroup,
                          it->second->getGroupEntries()[it->second->getMyGroupRank()], groupRanks, numberVirtualRanksPerGroup);
    groups.insert(groups.end(), std::pair<int, SpecificGroup*>(specificGroupId, newGroup));
    return specificGroupId;
  } else {
    raiseError("Existing base group not found");
    return -1;
  }
}

NDM_Group createContiguousGroupWithStride(NDM_Group baseGroup, int number_contiguous_ranks, int stride) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(baseGroup);
  if (it != groups.end()) {
    pthread_mutex_lock(&currentGroupId_mutex);
    int specificGroupId = groupCurrentId++;
    pthread_mutex_unlock(&currentGroupId_mutex);
    int myBaseRank = it->second->getMyGroupRank(), myRank = -1;
    int numberOfGroups = ceil((double)it->second->getGroupSize() / (number_contiguous_ranks + stride));
    int correction = ((number_contiguous_ranks + stride) * numberOfGroups) - it->second->getGroupSize();
    if (correction > number_contiguous_ranks) correction = number_contiguous_ranks;
    int numberOfProcesses = (numberOfGroups * number_contiguous_ranks) - correction;
    int* groupRanks = (int*)malloc(sizeof(int) * numberOfProcesses);
    int i, j = 0;
    for (i = 0; i < it->second->getGroupSize(); i++) {
      if (i % (number_contiguous_ranks + stride) >= stride) {
        groupRanks[j] = it->second->getGroupEntries()[i];
        if (i == myBaseRank) myRank = j;
        j++;
      }
    }
    SpecificGroup* newGroup = new SpecificGroup(specificGroupId, numberOfProcesses, myRank, groupRanks, 1);
    groups.insert(groups.end(), std::pair<int, SpecificGroup*>(specificGroupId, newGroup));
    return specificGroupId;
  } else {
    raiseError("Existing base group not found");
    return -1;
  }
}

NDM_Group createDisjointGroup(NDM_Group groupA, NDM_Group groupB) {
  std::map<int, SpecificGroup*>::iterator itA = groups.find(groupA);
  std::map<int, SpecificGroup*>::iterator itB = groups.find(groupB);
  if (itA != groups.end() && itB != groups.end()) {
    pthread_mutex_lock(&currentGroupId_mutex);
    int specificGroupId = groupCurrentId++;
    pthread_mutex_unlock(&currentGroupId_mutex);
    int* searchedRanks = (int*)malloc(sizeof(int) * (itA->second->getGroupSize() + itB->second->getGroupSize()));
    int i, j, actualSize = 0, myExistingRank = -1, myNewRank = -1;
    if (itA->second->getMyGroupRank() >= 0) {
      myExistingRank = itA->second->getGroupEntries()[itA->second->getMyGroupRank()];
    } else if (itB->second->getMyGroupRank() >= 0) {
      myExistingRank = itB->second->getGroupEntries()[itB->second->getMyGroupRank()];
    }
    for (i = 0; i < itA->second->getGroupSize(); i++) {
      for (j = 0; j < itB->second->getGroupSize(); j++) {
        if (itA->second->getGroupEntries()[i] == itB->second->getGroupEntries()[j]) break;
      }
      if (j == itB->second->getGroupSize()) {
        if (myExistingRank == itA->second->getGroupEntries()[i]) myNewRank = actualSize;
        searchedRanks[actualSize++] = itA->second->getGroupEntries()[i];
      }
    }

    for (i = 0; i < itB->second->getGroupSize(); i++) {
      for (j = 0; j < itA->second->getGroupSize(); j++) {
        if (itA->second->getGroupEntries()[i] == itB->second->getGroupEntries()[j]) break;
      }
      if (j == itA->second->getGroupSize()) {
        if (myExistingRank == itB->second->getGroupEntries()[i]) myNewRank = actualSize;
        searchedRanks[actualSize++] = itB->second->getGroupEntries()[i];
      }
    }

    searchedRanks = (int*)realloc(searchedRanks, sizeof(int) * actualSize);
    SpecificGroup* newGroup = new SpecificGroup(specificGroupId, actualSize, myNewRank, searchedRanks, 1);
    groups.insert(groups.end(), std::pair<int, SpecificGroup*>(specificGroupId, newGroup));
    return specificGroupId;
  } else {
    raiseError("Existing one of the groups provided not found");
    return -1;
  }
}

NDM_Group extractGroupBasedOnSizeAndRank(NDM_Group baseGroup, int size, int myrank) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(baseGroup);
  if (it != groups.end()) {
    pthread_mutex_lock(&currentGroupId_mutex);
    int specificGroupId = groupCurrentId++;
    pthread_mutex_unlock(&currentGroupId_mutex);

    int i, startingPoint = size * (myrank / size),
           endPoint = it->second->getGroupSize() < startingPoint + size ? it->second->getGroupSize() : startingPoint + size;
    int* groupRanks = (int*)malloc(sizeof(int) * (endPoint - startingPoint));
    for (i = startingPoint; i < endPoint; i++) {
      groupRanks[i - startingPoint] = it->second->getGroupEntries()[i];
    }
    SpecificGroup* newGroup = new SpecificGroup(specificGroupId, endPoint - startingPoint, myrank - startingPoint, groupRanks, 1);
    groups.insert(groups.end(), std::pair<int, SpecificGroup*>(specificGroupId, newGroup));
    return specificGroupId;
  } else {
    raiseError("Existing base group not found");
    return -1;
  }
}

int getLocalNthGroupRank(NDM_Group group, int n) {
  if (n < 0) raiseError("Negative group ranks is not allowed");
  std::map<int, SpecificGroup*>::iterator it = groups.find(group);
  if (it != groups.end()) {
    int myGroupRank = it->second->getMyGroupRank();
    int i, occurances = 0;
    for (i = 0; i < it->second->getGroupSize(); i++) {
      if (it->second->getGroupEntries()[i] == myGroupRank) {
        if (occurances == n) return i;
        occurances++;
      }
    }
  } else {
    raiseError("Group not found");
  }
  return -1;
}

int getNumberDistinctProcesses(NDM_Group group) {
  if (group == NDM_GLOBAL_GROUP) {
    return globalSize;
  } else {
    std::map<int, SpecificGroup*>::iterator it = groups.find(group);
    if (it != groups.end()) {
      return it->second->getGroupSize() / it->second->getRanksPerProcess();
    } else {
      raiseError("Group not found");
    }
  }
  return -1;
}

int translateRankFromGroup(NDM_Group group, int groupRank) {
  if (groupRank < 0) raiseError("Negative group ranks is not allowed");
  if (group == NDM_GLOBAL_GROUP) {
    if (groupRank >= globalSize) raiseError("Group rank exceeds group size");
    return groupRank;
  } else {
    std::map<int, SpecificGroup*>::iterator it = groups.find(group);
    if (it != groups.end()) {
      if (groupRank >= it->second->getGroupSize()) raiseError("Group rank exceeds group size");
      return it->second->getGroupToGlobalRank(groupRank);
    } else {
      raiseError("Group not found");
    }
  }
  return -1;
}

int isRankLocalToGroup(NDM_Group group, int rank) {
  if (rank < 0) raiseError("Negative group ranks is not allowed");
  std::map<int, SpecificGroup*>::iterator it = groups.find(group);
  if (it != groups.end()) {
    if (rank >= it->second->getGroupSize()) raiseError("Group rank exceeds group size");
    return it->second->getGroupToGlobalRank(rank) == it->second->getMyGroupRank();
  } else {
    raiseError("Group not found");
  }
  return 0;
}

int getGroupLocalSize(NDM_Group group) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(group);
  if (it != groups.end()) {
    return it->second->getRanksPerProcess();
  } else {
    raiseError("Group not found");
  }
  return -1;
}

int getMyGroupRank(NDM_Group group) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(group);
  if (it != groups.end()) {
    return it->second->getMyGroupRank();
  } else {
    raiseError("Group not found");
  }
  return -1;
}

int getGroupSize(NDM_Group group) {
  std::map<int, SpecificGroup*>::iterator it = groups.find(group);
  if (it != groups.end()) {
    return it->second->getGroupSize();
  } else {
    raiseError("Group not found");
  }
  return -1;
}
