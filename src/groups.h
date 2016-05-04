/*
 * groups.h
 *
 *  Created on: 4 Apr 2016
 *      Author: nick
 */

#ifndef SRC_GROUPS_H_
#define SRC_GROUPS_H_

#include "ndm.h"

class SpecificGroup {
  int groupId, groupSize, myRank, *groupEntries, ranksPerProcess;

 public:
  SpecificGroup(NDM_Group groupId, int groupSize, int myRank, int* groupEntries, int ranksPerProcess) {
    this->groupId = groupId;
    this->groupSize = groupSize;
    this->myRank = myRank;
    this->groupEntries = groupEntries;
    this->ranksPerProcess = ranksPerProcess;
  }

  const NDM_Group getGroupId() const { return groupId; }
  const int getGroupSize() const { return groupSize; }
  const int getMyGroupRank() const { return myRank; }
  const int getGroupToGlobalRank(int groupRank) const { return this->groupEntries[groupRank]; }
  const int getRanksPerProcess() const { return ranksPerProcess; }
  const int* getGroupEntries() const { return this->groupEntries; }
};

void initialiseGroupDirectory();
NDM_Group addGroup(int, int*);
int translateRankFromGroup(NDM_Group, int);
NDM_Group createVirtualRanksInGroup(NDM_Group, int);
NDM_Group createContiguousGroupWithStride(NDM_Group, int, int);
NDM_Group extractGroupBasedOnSizeAndRank(NDM_Group, int, int);
NDM_Group createDisjointGroup(NDM_Group, NDM_Group);
int getGroupSize(NDM_Group);
int getMyGroupRank(NDM_Group);
int isRankLocalToGroup(NDM_Group, int);
int getGroupLocalSize(NDM_Group);
int getLocalNthGroupRank(NDM_Group, int);
int getNumberDistinctProcesses(NDM_Group);

#endif /* SRC_GROUPS_H_ */
