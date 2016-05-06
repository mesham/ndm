/*
 * allreduce.h
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#ifndef SRC_ALLREDUCE_H_
#define SRC_ALLREDUCE_H_

#include <stddef.h>
#include "threadpool.h"
#include "messaging.h"
#include "ndm.h"
#include <pthread.h>

struct AllReduceStateComparitor {
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

class AllReduceState {
  std::string unique_id;
  void (*callback)(void*, NDM_Metadata);
  int numberEntriesRetrieved;
  int calledBack, local_group_size;
  pthread_mutex_t mutex;

 public:
  AllReduceState(void (*callback)(void*, NDM_Metadata), int local_group_size, std::string unique_id) {
    this->callback = callback;
    this->unique_id = unique_id;
    this->numberEntriesRetrieved = 0;
    this->calledBack = 0;
    this->local_group_size = local_group_size;
    pthread_mutex_init(&mutex, NULL);
  }

  ~AllReduceState() { pthread_mutex_destroy(&mutex); }

  const std::string& getUniqueId() const { return unique_id; }

  int getNumberExpectedLocalCallbacks() const { return local_group_size; }

  int getNumberEntriesRetrieved() const { return numberEntriesRetrieved; }
  void incrementNumberEntriesRetrieved() { numberEntriesRetrieved++; }

  void lock() { pthread_mutex_lock(&mutex); }
  void unlock() { pthread_mutex_unlock(&mutex); }

  int getCalledBack() const { return calledBack; }
  void incrementCalledBack() { calledBack++; }

  typedef void (*FuncSig)(void*, NDM_Metadata);
  FuncSig getCallback() { return this->callback; }
};

void initialise_ndmAllReduce(Messaging, ThreadPool);
void collective_ndmAllReduce(Messaging, ThreadPool, void*, int, int, NDM_Op, void (*)(void*, NDM_Metadata), int, NDM_Group,
                             const char*);

#endif /* SRC_ALLREDUCE_H_ */
