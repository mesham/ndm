/*
 * barrier.h
 *
 *  Created on: 31 Mar 2016
 *      Author: nick
 */

#ifndef SRC_BARRIER_H_
#define SRC_BARRIER_H_

#include "threadpool.h"
#include "messaging.h"
#include "ndm.h"

class BarrierState {
  std::string unique_id;
  void (*callback)(NDM_Metadata);

 public:
  BarrierState(void (*callback)(NDM_Metadata), std::string unique_id) {
    this->callback = callback;
    this->unique_id = unique_id;
  }

  const std::string& getUniqueId() const { return unique_id; }

  typedef void (*FuncSig)(NDM_Metadata);
  FuncSig getCallback() { return this->callback; }
};

void initialise_ndmBarrier();
void collective_ndmBarrier(Messaging, ThreadPool, void (*)(NDM_Metadata), NDM_Group, const char*);

#endif /* SRC_BARRIER_H_ */
