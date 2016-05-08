/*
 * bcast.h
 *
 *  Created on: 29 Dec 2015
 *      Author: nick
 */

#ifndef SRC_BCAST_H_
#define SRC_BCAST_H_

#include "threadpool.h"
#include "messaging.h"
#include "ndm.h"

class BcastState {
  std::string unique_id;
  int numberEntriesRetrieved;

 public:
  BcastState(std::string unique_id) {
    this->numberEntriesRetrieved = 0;
    this->unique_id = unique_id;
  }
  int getNumberEntriesRetrieved() const { return numberEntriesRetrieved; }
  void incrementNumberEntriesRetrieved() { numberEntriesRetrieved++; }
  const std::string& getUniqueId() const { return unique_id; }
};

void initialise_ndmBcast(Messaging, ThreadPool);
void collective_ndmBcast(Messaging, ThreadPool, void*, int, int, void (*)(void*, NDM_Metadata), int, int, NDM_Group, const char*);

#endif /* SRC_BCAST_H_ */
