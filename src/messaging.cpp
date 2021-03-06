/*
 * messaging.cpp
 *
 *  Created on: 22 Nov 2015
 *      Author: nick
 */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <vector>
#include <stack>
#include <iterator>
#include <unistd.h>
#include "misc.h"
#include "messaging.h"
#include "data_types.h"
#include "groups.h"
#include "ndm.h"

#define MPI_TAG 16384
#define CLEAN_SR_EVERY 1000
#define WAIT_BETWEEN_MPI_TESTS 1000

std::vector<SpecificMessage*> Messaging::outstandingRequests;
std::vector<MPI_Request> Messaging::outstandingSendRequests;
std::map<RequestUniqueIdentifier, RegisterdCommandContainer*> Messaging::registeredCommands;

pthread_mutex_t Messaging::mutex_outstandingSendRequests, Messaging::mutex_outstandingRequests, Messaging::mutex_messagingActive,
    Messaging::mutex_numRegisteredCommands, Messaging::mutex_processingMsgOrCommand, Messaging::mpi_mutex;
pthread_rwlock_t Messaging::rwlock_registeredCommands;
bool Messaging::continue_polling = true, Messaging::messagingActive = true, Messaging::protectMPI, Messaging::mpiInitHere;
int Messaging::rank = -1, Messaging::totalSize = -1, Messaging::numberRecurringCommands = 0, Messaging::srCleanIncrement,
    Messaging::totalNumberCommands;

SpecificMessage* Messaging::awaitCommand() {
  int pending_message, message_size, data_size;
  char* buffer, *data_buffer;
  MPI_Status message_status;
  while (continue_polling) {
    lockMPI();
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_TAG, MPI_COMM_WORLD, &pending_message, &message_status);
    unlockMPI();
    if (pending_message) {
      if (++srCleanIncrement % CLEAN_SR_EVERY == 0) {
        cleanOutstandingSendRequests();
        srCleanIncrement = 0;
      }
      pthread_mutex_lock(&mutex_messagingActive);
      messagingActive = true;
      pthread_mutex_unlock(&mutex_messagingActive);
      lockMPI();
      MPI_Get_count(&message_status, MPI_BYTE, &message_size);
      unlockMPI();
      buffer = (char*)malloc(message_size);
      lockMPI();
      MPI_Recv(buffer, message_size, MPI_BYTE, message_status.MPI_SOURCE, MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      unlockMPI();
      int data_type = *((int*)buffer);
      int source_pid = *((int*)&buffer[4]);
      int target_pid = *((int*)&buffer[8]);
      NDM_Group comm_group = *((int*)&buffer[12]);
      int action_id = *((int*)&buffer[16]);
      int unique_id_length = strlen(&buffer[20]);
      data_size = message_size - (20 + unique_id_length + 1);
      if (data_size > 0) {
        data_buffer = (char*)malloc(data_size);
        memcpy(data_buffer, &buffer[20 + unique_id_length + 1], data_size);
      } else {
        data_buffer = NULL;
      }
      SpecificMessage* message =
          new SpecificMessage(source_pid, target_pid, comm_group, action_id, data_size == 0 ? 0 : data_size / getTypeSize(data_type),
                              data_type, new std::string(&buffer[20]), data_buffer);
      free(buffer);
      return message;
    }
  }
  return NULL;
}

void Messaging::cleanOutstandingSendRequests() {
  int i, flag;
  std::vector<MPI_Request>::iterator it;
  std::vector<std::vector<MPI_Request>::iterator> indexes_to_remove;
  if (pthread_mutex_trylock(&mutex_outstandingSendRequests) == 0) {
    for (it = outstandingSendRequests.begin(); it != outstandingSendRequests.end(); it++) {
      MPI_Request req = (*it);
      lockMPI();
      MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
      unlockMPI();
      if (flag) indexes_to_remove.push_back(it);
    }
    for (i = indexes_to_remove.size() - 1; i >= 0; i--) {
      outstandingSendRequests.erase(indexes_to_remove[i]);
    }
    pthread_mutex_unlock(&mutex_outstandingSendRequests);
  }
}

void Messaging::handleOutstandingRequests() {
  MPI_Request* arr;
  pthread_mutex_lock(&mutex_outstandingSendRequests);
  int number_sends = outstandingSendRequests.size();
  if (number_sends > 0) {
    arr = (MPI_Request*)malloc(sizeof(MPI_Request) * number_sends);
    std::copy(outstandingSendRequests.begin(), outstandingSendRequests.end(), arr);
    outstandingSendRequests.erase(outstandingSendRequests.begin(), outstandingSendRequests.end());
    pthread_mutex_unlock(&mutex_outstandingSendRequests);
    waitAllForMPIRequest(number_sends, arr);
    free(arr);
  } else {
    pthread_mutex_unlock(&mutex_outstandingSendRequests);
  }
}

void Messaging::waitAllForMPIRequest(int number_sends, MPI_Request* arr) {
  if (protectMPI) {
    int flag = 0;
    while (flag == 0) {
      lockMPI();
      MPI_Testall(number_sends, arr, &flag, MPI_STATUSES_IGNORE);
      unlockMPI();
      if (flag == 0) usleep(useconds_t(WAIT_BETWEEN_MPI_TESTS));
    }
  } else {
    MPI_Waitall(number_sends, arr, MPI_STATUSES_IGNORE);
  }
}

bool Messaging::readyToShutdown() {
  pthread_mutex_lock(&mutex_numRegisteredCommands);
  int currentNumC = totalNumberCommands;
  pthread_mutex_unlock(&mutex_numRegisteredCommands);
  pthread_mutex_lock(&mutex_outstandingRequests);
  bool outstandingReqEmpty = outstandingRequests.empty();
  pthread_mutex_unlock(&mutex_outstandingRequests);
  if (outstandingReqEmpty && currentNumC == numberRecurringCommands) {
    // handleOutstandingRequests();
    cleanOutstandingSendRequests();
    if (outstandingSendRequests.empty()) return true;
  }
  return false;
}

void Messaging::lockMPI() {
  if (protectMPI) pthread_mutex_lock(&mpi_mutex);
}

void Messaging::unlockMPI() {
  if (protectMPI) pthread_mutex_unlock(&mpi_mutex);
}

void Messaging::finalise() {
  if (mpiInitHere) MPI_Finalize();
}

void Messaging::init(int* argc, char*** argv) {
  pthread_rwlock_init(&rwlock_registeredCommands, NULL);
  pthread_mutex_init(&mutex_outstandingRequests, NULL);
  pthread_mutex_init(&mutex_processingMsgOrCommand, NULL);
  pthread_mutex_init(&mutex_outstandingSendRequests, NULL);
  pthread_mutex_init(&mutex_messagingActive, NULL);
  pthread_mutex_init(&mutex_numRegisteredCommands, NULL);
  pthread_mutex_init(&mpi_mutex, NULL);
  int is_mpi_init, provided;
  MPI_Initialized(&is_mpi_init);
  if (is_mpi_init) {
    mpiInitHere = false;
    MPI_Query_thread(&provided);
    if (provided != MPI_THREAD_MULTIPLE && provided != MPI_THREAD_SERIALIZED) {
      raiseError("You must initialise MPI in thread serialised or multiple, or let NDM do this for you");
    }
    protectMPI = provided == MPI_THREAD_SERIALIZED;
  } else {
    mpiInitHere = true;
    MPI_Init_thread(argc, argv, MPI_THREAD_SERIALIZED, &provided);
    protectMPI = true;
  }
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &totalSize);
  srCleanIncrement = 0;
  totalNumberCommands = 0;
}

void Messaging::registerCommand(const char* unique_id, int source, int target, NDM_Group comm_group, int action_id, bool recurring,
                                void (*callback)(void*, NDM_Metadata)) {
  pthread_mutex_lock(&mutex_messagingActive);
  messagingActive = true;
  pthread_mutex_unlock(&mutex_messagingActive);
  pthread_mutex_lock(&mutex_processingMsgOrCommand);
  pthread_mutex_lock(&mutex_outstandingRequests);
  std::vector<SpecificMessage*>::iterator it;
  std::vector<SpecificMessage*> outstandingRequestsToHandle;
  std::vector<std::vector<SpecificMessage*>::iterator> outstandingIteratorsToRemove;
  std::string uuid_string = std::string(unique_id);
  for (it = outstandingRequests.begin(); it < outstandingRequests.end(); it++) {
    if (((*it)->getSourcePid() == source || source == NDM_ANY_SOURCE) && (*it)->getActionId() == action_id &&
        (*it)->getCommGroup() == comm_group &&
        ((*it)->getTargetPid() == NDM_ANY_SOURCE || target == NDM_ANY_SOURCE || (*it)->getTargetPid() == target)) {

      if (*(*it)->getUniqueId() != uuid_string) {
        size_t wildCardLocA = uuid_string.find('*');
        size_t wildCardLocB = (*it)->getUniqueId()->find('*');
        if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
          if (wildCardLocA == std::string::npos &&
              uuid_string.substr(0, wildCardLocB) != (*it)->getUniqueId()->substr(0, wildCardLocB)) {
            continue;
          } else if (wildCardLocB == std::string::npos &&
                     uuid_string.substr(0, wildCardLocA) != (*it)->getUniqueId()->substr(0, wildCardLocA)) {
            continue;
          } else if (uuid_string.substr(0, wildCardLocA) != (*it)->getUniqueId()->substr(0, wildCardLocB)) {
            continue;
          }
        } else {
          continue;
        }
      }

      outstandingRequestsToHandle.push_back(*it);
      outstandingIteratorsToRemove.push_back(it);
    }
  }
  if (!outstandingIteratorsToRemove.empty()) {
    int i;
    for (i = outstandingIteratorsToRemove.size() - 1; i >= 0; i--) {
      outstandingRequests.erase(outstandingIteratorsToRemove[i]);
    }
  }
  pthread_mutex_unlock(&mutex_outstandingRequests);

  if (outstandingRequestsToHandle.empty() || recurring) {
    RequestUniqueIdentifier ruuid = RequestUniqueIdentifier(source, target, comm_group, action_id, std::string(unique_id));
    pthread_rwlock_rdlock(&rwlock_registeredCommands);
    std::map<RequestUniqueIdentifier, RegisterdCommandContainer*>::iterator it = registeredCommands.find(ruuid);
    if (it == registeredCommands.end()) {
      pthread_rwlock_unlock(&rwlock_registeredCommands);
      pthread_rwlock_wrlock(&rwlock_registeredCommands);
      it = registeredCommands.find(ruuid);
      if (it == registeredCommands.end()) {
        RegisterdCommandContainer* newCommandContainer = new RegisterdCommandContainer();
        it = registeredCommands.insert(std::pair<RequestUniqueIdentifier, RegisterdCommandContainer*>(ruuid, newCommandContainer))
                 .first;
      }
    }
    it->second->lock();
    pthread_rwlock_unlock(&rwlock_registeredCommands);
    it->second->pushCommand(new RegisteredCommand(unique_id, source, target, comm_group, action_id, recurring, callback));
    pthread_mutex_lock(&mutex_numRegisteredCommands);
    totalNumberCommands++;
    pthread_mutex_unlock(&mutex_numRegisteredCommands);
    it->second->unlock();
  }
  pthread_mutex_unlock(&mutex_processingMsgOrCommand);
  if (!outstandingRequestsToHandle.empty()) {
    for (it = outstandingRequestsToHandle.begin(); it < outstandingRequestsToHandle.end(); it++) {
      callback((*it)->getData(), generateMetaData((*it)->getMessageType(), (*it)->getMessageLength(), (*it)->getSourcePid(),
                                                  (*it)->getTargetPid(), (*it)->getCommGroup(), (*it)->getUniqueId()->c_str()));
      delete (*it);
    }
  }
  if (recurring) numberRecurringCommands++;
}

NDM_Metadata Messaging::generateMetaData(int dataType, int numberElements, int source, int my_rank, NDM_Group comm_group,
                                         const char* unique_id) {
  NDM_Metadata metaData;
  metaData.data_type = dataType;
  metaData.number_elements = numberElements;
  metaData.source = source;
  metaData.my_rank = my_rank;
  metaData.comm_group = comm_group;
  metaData.unique_id = unique_id;
  return metaData;
}

char* Messaging::packageMessage(void* data, int type, int size, int source_pid, int target_pid, NDM_Group comm_group, int action_id,
                                const char* unique_id, int* dataLength) {
  int uid_size = strlen(unique_id) + 1;
  int buffer_size = getTypeSize(type) * size;
  char* buffer = (char*)malloc((sizeof(int) * 4) + sizeof(NDM_Group) + uid_size + buffer_size);
  memcpy(buffer, &type, sizeof(int));
  int currentMemoryIndex = sizeof(int);
  memcpy(&buffer[currentMemoryIndex], &source_pid, sizeof(int));
  currentMemoryIndex += sizeof(int);
  memcpy(&buffer[currentMemoryIndex], &target_pid, sizeof(int));
  currentMemoryIndex += sizeof(int);
  memcpy(&buffer[currentMemoryIndex], &comm_group, sizeof(NDM_Group));
  currentMemoryIndex += sizeof(NDM_Group);
  memcpy(&buffer[currentMemoryIndex], &action_id, sizeof(int));
  currentMemoryIndex += sizeof(int);
  strcpy(&buffer[currentMemoryIndex], unique_id);
  memcpy(&buffer[currentMemoryIndex + uid_size], data, buffer_size);
  *dataLength = (sizeof(int) * 4) + sizeof(NDM_Group) + uid_size + buffer_size;
  return buffer;
}

void Messaging::sendMessage(char* buffer, int payload_size, int target, NDM_Group comm_group) {
  pthread_mutex_lock(&mutex_messagingActive);
  messagingActive = true;
  pthread_mutex_unlock(&mutex_messagingActive);
  MPI_Request request_handle;
  lockMPI();
  MPI_Isend(buffer, payload_size, MPI_BYTE, translateRankFromGroup(comm_group, target), MPI_TAG, MPI_COMM_WORLD, &request_handle);
  unlockMPI();
  pthread_mutex_lock(&mutex_outstandingSendRequests);
  outstandingSendRequests.push_back(request_handle);
  pthread_mutex_unlock(&mutex_outstandingSendRequests);
}

void Messaging::clearMessagingActive() {
  pthread_mutex_lock(&mutex_messagingActive);
  messagingActive = false;
  pthread_mutex_unlock(&mutex_messagingActive);
}

bool Messaging::getMessagingActive() {
  pthread_mutex_lock(&mutex_messagingActive);
  bool ma = messagingActive;
  pthread_mutex_unlock(&mutex_messagingActive);
  return ma;
}

void Messaging::handleCommand(void* data) {
  SpecificMessage* message = (SpecificMessage*)data;
  bool commandExecuted = false;
  pthread_mutex_lock(&mutex_processingMsgOrCommand);
  pthread_rwlock_rdlock(&rwlock_registeredCommands);
  std::map<RequestUniqueIdentifier, RegisterdCommandContainer*>::iterator it = registeredCommands.find(RequestUniqueIdentifier(
      message->getSourcePid(), message->getTargetPid(), message->getCommGroup(), message->getActionId(), *message->getUniqueId()));
  if (it != registeredCommands.end()) {
    it->second->lock();
    pthread_rwlock_unlock(&rwlock_registeredCommands);
    if (!it->second->isEmpty()) {
      RegisteredCommand* locatedCommand = it->second->getFirstCommandKeepOnlyIfRecurring();
      if (!locatedCommand->getRecurring()) {
        pthread_mutex_lock(&mutex_numRegisteredCommands);
        totalNumberCommands--;
        pthread_mutex_unlock(&mutex_numRegisteredCommands);
      }
      it->second->unlock();
      pthread_mutex_unlock(&mutex_processingMsgOrCommand);
      locatedCommand->getCallback()(
          message->getData(), generateMetaData(message->getMessageType(), message->getMessageLength(), message->getSourcePid(),
                                               message->getTargetPid(), message->getCommGroup(), message->getUniqueId()->c_str()));
      if (!locatedCommand->getRecurring()) delete locatedCommand;
      commandExecuted = true;
    } else {
      it->second->unlock();
    }
  } else {
    pthread_rwlock_unlock(&rwlock_registeredCommands);
  }
  if (!commandExecuted) {
    SpecificMessage* newMessage = new SpecificMessage(message);
    pthread_mutex_lock(&mutex_outstandingRequests);
    outstandingRequests.push_back(newMessage);
    pthread_mutex_unlock(&mutex_outstandingRequests);
    pthread_mutex_unlock(&mutex_processingMsgOrCommand);
  }
}
