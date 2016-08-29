/*
 * messaging.h
 *
 *  Created on: 22 Nov 2015
 *      Author: nick
 */

#ifndef SRC_MESSAGING_H_
#define SRC_MESSAGING_H_

#include <stack>
#include <map>
#include <vector>
#include <string>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>
#include "ndm.h"

class RequestUniqueIdentifier {
  int source_pid, target_pid, action_id;
  NDM_Group comm_group;
  std::string unique_id;

 public:
  RequestUniqueIdentifier(int source_pid, int target_pid, NDM_Group comm_group, int action_id, std::string unique_id) {
    this->source_pid = source_pid;
    this->target_pid = target_pid;
    this->action_id = action_id;
    this->unique_id = unique_id;
    this->comm_group = comm_group;
  }

  bool operator<(const RequestUniqueIdentifier& rhs) const {
    if (this->source_pid != -1 && rhs.source_pid != -1) {
      if (this->source_pid < rhs.source_pid) return true;
      if (this->source_pid > rhs.source_pid) return false;
    }
    if (this->target_pid != -1 && rhs.target_pid != -1) {
      if (this->target_pid < rhs.target_pid) return true;
      if (this->target_pid > rhs.target_pid) return false;
    }
    if (this->comm_group != -1 && rhs.comm_group != -1) {
      if (this->comm_group < rhs.comm_group) return true;
      if (this->comm_group > rhs.comm_group) return false;
    }
    if (this->action_id < rhs.action_id) return true;
    if (this->action_id > rhs.action_id) return false;
    size_t wildCardLocA = unique_id.find('*');
    size_t wildCardLocB = rhs.unique_id.find('*');
    if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
      if (wildCardLocA == std::string::npos) {
        return unique_id.substr(0, wildCardLocB) < rhs.unique_id.substr(0, wildCardLocB);
      } else if (wildCardLocB == std::string::npos) {
        return unique_id.substr(0, wildCardLocA) < rhs.unique_id.substr(0, wildCardLocA);
      } else {
        return unique_id.substr(0, wildCardLocA) < rhs.unique_id.substr(0, wildCardLocB);
      }
    } else {
      return unique_id < rhs.unique_id;
    }
  }

  int getSourcePid() const { return source_pid; }
  int getTargetPid() const { return target_pid; }
  NDM_Group getCommGroup() const { return comm_group; }
  std::string getUniqueId() const { return unique_id; }
  int getActionId() const { return action_id; }
};

class SpecificMessage {
  int source_pid, target_pid, action_id, message_length, message_type;
  NDM_Group comm_group;
  char* data;
  std::string* unique_id;

 public:
  SpecificMessage(SpecificMessage* source) {
    this->source_pid = source->getSourcePid();
    this->target_pid = source->getTargetPid();
    this->comm_group = source->getCommGroup();
    this->unique_id = new std::string(*source->getUniqueId());
    this->message_length = source->getMessageLength();
    this->message_type = source->getMessageType();
    this->data = source->getData();
    this->action_id = source->getActionId();
  }
  SpecificMessage(int sourcePid, int targetPid, NDM_Group comm_group, int action_id, int message_length, int message_type,
                  std::string* unique_id, char* data) {
    this->source_pid = sourcePid;
    this->target_pid = targetPid;
    this->comm_group = comm_group;
    this->action_id = action_id;
    this->message_type = message_type;
    this->unique_id = unique_id;
    this->message_length = message_length;
    this->data = data;
  }

  char* getData() const { return data; }

  void setData(char* data) { this->data = data; }

  int getSourcePid() const { return source_pid; }
  int getTargetPid() const { return target_pid; }

  void setSourcePid(int sourcePid) { source_pid = sourcePid; }

  NDM_Group getCommGroup() const { return this->comm_group; }

  std::string* getUniqueId() { return this->unique_id; }

  int getMessageLength() { return this->message_length; }

  int getMessageType() { return this->message_type; }

  int getActionId() const { return action_id; }

  void setActionId(int actionId) { action_id = actionId; }
};

class RegisteredCommand {
  std::string unique_id;
  int source, target, action_id;
  NDM_Group comm_group;
  void (*callback)(void*, NDM_Metadata);
  bool recurring;

 public:
  RegisteredCommand(std::string unique_id, int source, int target, NDM_Group comm_group, int action_id, bool recurring,
                    void (*callback)(void*, NDM_Metadata)) {
    this->source = source;
    this->target = target;
    this->action_id = action_id;
    this->comm_group = comm_group;
    this->unique_id = unique_id;
    this->callback = callback;
    this->recurring = recurring;
  }

  bool getRecurring() const { return recurring; }
  void setRucurring(bool recurring) { this->recurring = recurring; }

  typedef void (*FuncSig)(void*, NDM_Metadata);

  FuncSig getCallback() { return this->callback; }

  void setCallback(void (*callback)(void*, NDM_Metadata)) { this->callback = callback; }

  const std::string getUniqueId() const { return unique_id; }

  void setUniqueId(const char* unique_id) { this->unique_id = unique_id; }

  int getSource() const { return source; }

  void setSource(int source) { this->source = source; }

  int getTarget() const { return target; }

  int getActionId() const { return action_id; }

  NDM_Group getCommGroup() const { return comm_group; }
};

class RegisterdCommandContainer {
  std::stack<RegisteredCommand*> commands;
  pthread_mutex_t mutex;

 public:
  RegisterdCommandContainer() { pthread_mutex_init(&mutex, NULL); }

  void lock() { pthread_mutex_lock(&mutex); }
  void unlock() { pthread_mutex_unlock(&mutex); }
  void pushCommand(RegisteredCommand* command) { commands.push(command); }
  void popCommand() { commands.pop(); }
  RegisteredCommand* getFirstCommand(RegisteredCommand* command) { return commands.top(); }
  RegisteredCommand* getFirstCommandKeepOnlyIfRecurring() {
    RegisteredCommand* topCommand = commands.top();
    if (!topCommand->getRecurring()) commands.pop();
    return topCommand;
  }
  int getSize() { return commands.size(); }
  bool isEmpty() { return commands.empty(); }
};

class Messaging {
  static std::vector<MPI_Request> outstandingSendRequests;
  static std::map<RequestUniqueIdentifier, RegisterdCommandContainer*> registeredCommands;
  static std::vector<SpecificMessage*> outstandingRequests;
  static pthread_mutex_t mutex_outstandingSendRequests, mutex_outstandingRequests, mutex_messagingActive, mutex_numRegisteredCommands,
      mutex_processingMsgOrCommand, mpi_mutex;
  static pthread_rwlock_t rwlock_registeredCommands;
  static bool continue_polling, messagingActive, protectMPI, mpiInitHere;
  static int rank, totalSize, numberRecurringCommands, srCleanIncrement, totalNumberCommands, totalNumberOutstandingMessages;
  static void runCommand(void*);
  static void cleanOutstandingSendRequests();
  static void localMessagingCallback(void*);
  static void waitAllForMPIRequest(int, MPI_Request*);

 public:
  static void init(int*, char***);
  static void finalise();
  static void lockMPI();
  static void unlockMPI();
  static bool getMessagingActive();
  static void clearMessagingActive();
  static int getMyRank() { return rank; }
  static int getNumberProcessors() { return totalSize; }
  static char* packageMessage(void*, int, int, int, int, NDM_Group, int, const char*, int*);
  static void sendMessage(char*, int, int, NDM_Group);
  static void registerCommand(const char*, int, int, NDM_Group, int, bool, void (*callback)(void*, NDM_Metadata));
  static NDM_Metadata generateMetaData(int, int, int, int, NDM_Group, const char*);
  static SpecificMessage* awaitCommand();
  static void handleOutstandingRequests();
  static void handleCommand(void*);
  static void ceasePolling() { continue_polling = false; }
  static bool readyToShutdown();
};

#endif /* SRC_MESSAGING_H_ */
