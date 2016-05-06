/*
 * reduce.h
 *
 *  Created on: 2 Jan 2016
 *      Author: nick
 */

#ifndef SRC_REDUCE_H_
#define SRC_REDUCE_H_

#include "pthread.h"
#include "threadpool.h"
#include "messaging.h"
#include "ndm.h"
#include "groups.h"
#include "data_types.h"

struct ReductionStateComparitor {
  bool operator()(std::string a, std::string b) {
    size_t wildCardLocA = a.find('*');
    size_t wildCardLocB = b.find('*');
    if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
      if (wildCardLocA == std::string::npos) {
        return a.substr(0, wildCardLocB) < b.substr(0, wildCardLocB);
      } else if (wildCardLocB == std::string::npos) {
        return a.substr(0, wildCardLocA) < b.substr(0, wildCardLocA);
      } else {
        return a.substr(0, wildCardLocA) < b.substr(0, wildCardLocB);
      }
    } else {
      return a < b;
    }
  }
};

class ReductionState {
  int contributedProcesses, root, type, size, numberProcesses, *dataContributions, contributionsPerElement;
  NDM_Op operation;
  NDM_Group comm_group;
  pthread_mutex_t mutex;
  void* data;
  std::string unique_id;
  void (*callback)(void*, NDM_Metadata);
  int numberEntriesRetrieved, localGroupSize, typeSize;
  char* dataNotEmpty;

 public:
  ReductionState(int type, int size, int contributionsPerElement, void (*callback)(void*, NDM_Metadata), NDM_Op operation, int root,
                 NDM_Group comm_group, std::string unique_id) {
    this->typeSize = getTypeSize(type);
    this->data = malloc(this->typeSize * size);
    this->dataContributions = (int*)malloc(sizeof(int) * size);
    this->dataNotEmpty = (char*)malloc(size);
    this->operation = operation;
    this->root = root;
    this->type = type;
    this->size = size;
    this->callback = callback;
    this->contributionsPerElement = contributionsPerElement;
    this->unique_id = unique_id;
    this->numberProcesses = getNumberDistinctProcesses(comm_group);
    this->contributedProcesses = 0;
    this->comm_group = comm_group;
    this->numberEntriesRetrieved = 0;
    this->localGroupSize = getGroupLocalSize(comm_group);
    int i;
    for (i = 0; i < size; i++) {
      this->dataContributions[i] = 0;
      this->dataNotEmpty[i] = 0;
    }
    pthread_mutex_init(&mutex, NULL);
  }

  ~ReductionState() {
    pthread_mutex_destroy(&mutex);
    free(this->dataContributions);
    free(this->dataNotEmpty);
    free(this->data);
  }

  bool isDataElementEmpty(int elementNumber) { return this->dataNotEmpty[elementNumber] == 0; }

  void setDataElementHasValue(int elementNumber) { this->dataNotEmpty[elementNumber] = 1; }

  void incrementDataElementContributions(int elementNumber) { this->dataContributions[elementNumber]++; }

  bool isDataSettingComplete() {
    int i;
    for (i = 0; i < this->size; i++) {
      if (this->dataContributions[i] < this->localGroupSize * contributionsPerElement) return false;
    }
    return true;
  }

  int getMyTypeSize() { return this->typeSize; }

  void lock() { pthread_mutex_lock(&mutex); }

  void unlock() { pthread_mutex_unlock(&mutex); }

  int getContributedProcesses() const { return contributedProcesses; }

  void setContributedProcesses(int contributedProcesses) { this->contributedProcesses = contributedProcesses; }

  void incrementContributedProcesses() { this->contributedProcesses++; }

  void* getData() const { return data; }

  // void setData(void* data) { this->data = data; }

  NDM_Op getOperation() const { return operation; }

  void setOperation(NDM_Op operation) { this->operation = operation; }

  int getRoot() const { return root; }

  void setRoot(int root) { this->root = root; }

  int getNumberProcesses() const { return numberProcesses; }

  bool hasCompletedAllProcessesReduction() const { return numberProcesses <= contributedProcesses; }

  const std::string& getUniqueId() const { return unique_id; }

  void setUniqueId(const std::string& uniqueId) { unique_id = uniqueId; }

  int getSize() const { return size; }

  void setSize(int size) { this->size = size; }

  int getType() const { return type; }

  void setType(int type) { this->type = type; }

  NDM_Group getCommGroup() const { return this->comm_group; }

  typedef void (*FuncSig)(void*, NDM_Metadata);

  FuncSig getCallback() { return this->callback; }
};

void initialise_ndmReduce(Messaging, ThreadPool);
void collective_ndmReduce(Messaging*, ThreadPool*, void*, int, int, int, int, int, NDM_Op, void (*)(void*, NDM_Metadata), int, int,
                          NDM_Group, const char*);

#endif /* SRC_REDUCE_H_ */
