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
#include "messaging.h"
#include "data_types.h"
#include "groups.h"
#include "ndm.h"

#define MPI_TAG 16384

std::vector<SpecificMessage*> Messaging::outstandingRequests;
std::vector<MPI_Request> Messaging::outstandingSendRequests;
std::vector<RegisteredCommand*> Messaging::registeredCommands;
pthread_mutex_t Messaging::mutex_outstandingSendRequests, Messaging::mutex_outstandingRequests, Messaging::mutex_registeredCommands;
bool Messaging::continue_polling = true, Messaging::messagingActive = true;
int Messaging::rank = -1, Messaging::totalSize = -1, Messaging::numberRecurringCommands = 0;

SpecificMessage* Messaging::awaitCommand() {
  int pending_message, message_size, data_size;
  char* buffer, *data_buffer;
  MPI_Status message_status;
  while (continue_polling) {
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_TAG, MPI_COMM_WORLD, &pending_message, &message_status);
    if (pending_message) {
      cleanOutstandingSendRequests();
      messagingActive = true;
      MPI_Get_count(&message_status, MPI_BYTE, &message_size);
      buffer = (char*)malloc(message_size);
      MPI_Recv(buffer, message_size, MPI_BYTE, message_status.MPI_SOURCE, MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
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
  pthread_mutex_lock(&mutex_outstandingSendRequests);
  for (it = outstandingSendRequests.begin(); it != outstandingSendRequests.end(); it++) {
    MPI_Request req = (*it);
    MPI_Test(&req, &flag, MPI_STATUS_IGNORE);
    if (flag) indexes_to_remove.push_back(it);
  }
  for (i = indexes_to_remove.size() - 1; i >= 0; i--) {
    outstandingSendRequests.erase(indexes_to_remove[i]);
  }
  pthread_mutex_unlock(&mutex_outstandingSendRequests);
}

void Messaging::handleOutstandingRequests() {
  int number_sends = outstandingSendRequests.size();
  pthread_mutex_lock(&mutex_outstandingSendRequests);
  MPI_Request* arr = (MPI_Request*)malloc(sizeof(MPI_Request) * number_sends);
  std::copy(outstandingSendRequests.begin(), outstandingSendRequests.end(), arr);
  pthread_mutex_unlock(&mutex_outstandingSendRequests);
  MPI_Waitall(number_sends, arr, MPI_STATUSES_IGNORE);
  free(arr);
}

bool Messaging::readyToShutdown() {
  if (outstandingRequests.empty() && registeredCommands.size() == numberRecurringCommands) {
    cleanOutstandingSendRequests();
    pthread_mutex_lock(&mutex_outstandingSendRequests);
    bool osREmpty = outstandingSendRequests.empty();
    pthread_mutex_unlock(&mutex_outstandingSendRequests);
    return osREmpty;
  }
  return false;
}

void Messaging::init() {
  pthread_mutex_init(&mutex_registeredCommands, NULL);
  pthread_mutex_init(&mutex_outstandingRequests, NULL);
  pthread_mutex_init(&mutex_outstandingSendRequests, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &totalSize);
}

void Messaging::registerCommand(const char* unique_id, int source, int target, NDM_Group comm_group, int action_id, bool recurring,
                                void (*callback)(void*, NDM_Metadata)) {
  messagingActive = true;
  pthread_mutex_lock(&mutex_registeredCommands);
  pthread_mutex_lock(&mutex_outstandingRequests);
  std::vector<SpecificMessage*>::iterator it;
  std::vector<SpecificMessage*> outstandingRequestsToHandle;
  std::vector<std::vector<SpecificMessage*>::iterator> outstandingIteratorsToRemove;
  for (it = outstandingRequests.begin(); it < outstandingRequests.end(); it++) {
    if ((*it)->getSourcePid() == source && (*it)->getActionId() == action_id && (*it)->getCommGroup() == comm_group &&
        ((*it)->getTargetPid() == NDM_ANY_SOURCE || target == NDM_ANY_SOURCE || (*it)->getTargetPid() == target) &&
        strcmp((*it)->getUniqueId()->c_str(), unique_id) == 0) {
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
  if (outstandingRequestsToHandle.empty() || recurring) {
    pthread_mutex_unlock(&mutex_outstandingRequests);
    registeredCommands.push_back(new RegisteredCommand(unique_id, source, target, comm_group, action_id, recurring, callback));
    pthread_mutex_unlock(&mutex_registeredCommands);
  } else {
    pthread_mutex_unlock(&mutex_outstandingRequests);
    pthread_mutex_unlock(&mutex_registeredCommands);
  }
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
  messagingActive = true;
  MPI_Request request_handle;
  MPI_Isend(buffer, payload_size, MPI_BYTE, translateRankFromGroup(comm_group, target), MPI_TAG, MPI_COMM_WORLD, &request_handle);
  pthread_mutex_lock(&mutex_outstandingSendRequests);
  outstandingSendRequests.push_back(request_handle);
  pthread_mutex_unlock(&mutex_outstandingSendRequests);
}

void Messaging::handleCommand(void* data) {
  SpecificMessage* message = (SpecificMessage*)data;

  pthread_mutex_lock(&mutex_registeredCommands);
  std::vector<RegisteredCommand*>::iterator it = locateCommand(message);
  if (it != registeredCommands.end()) {
    RegisteredCommand* locatedCommand = *it;
    if (!locatedCommand->getRecurring()) {
      registeredCommands.erase(it);
    }
    pthread_mutex_unlock(&mutex_registeredCommands);
    locatedCommand->getCallback()(message->getData(),
                                  generateMetaData(message->getMessageType(), message->getMessageLength(), message->getSourcePid(),
                                                   message->getTargetPid(), message->getCommGroup(), message->getUniqueId()->c_str()));
    if (!locatedCommand->getRecurring()) delete locatedCommand;
  } else {
    SpecificMessage* newMessage = new SpecificMessage(message);
    pthread_mutex_lock(&mutex_outstandingRequests);
    outstandingRequests.push_back(newMessage);
    pthread_mutex_unlock(&mutex_outstandingRequests);
    pthread_mutex_unlock(&mutex_registeredCommands);
  }
}

std::vector<RegisteredCommand*>::iterator Messaging::locateCommand(SpecificMessage* message) {
  size_t wildCardLocB = message->getUniqueId()->find('*');
  std::vector<RegisteredCommand*>::iterator it;
  for (it = registeredCommands.begin(); it != registeredCommands.end(); it++) {
    if (((*it)->getSource() == -1 || message->getSourcePid() == -1 || message->getSourcePid() == (*it)->getSource()) &&
        (message->getTargetPid() == -1 || (*it)->getTarget() == -1 || message->getTargetPid() == (*it)->getTarget()) &&
        message->getCommGroup() == (*it)->getCommGroup() && message->getActionId() == (*it)->getActionId()) {
      size_t wildCardLocA = (*it)->getUniqueId().find('*');
      if (wildCardLocA != std::string::npos || wildCardLocB != std::string::npos) {
        if (wildCardLocA == std::string::npos) {
          if ((*it)->getUniqueId().substr(0, wildCardLocB).compare(message->getUniqueId()->substr(0, wildCardLocB)) == 0) return it;
        } else if (wildCardLocB == std::string::npos) {
          if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(message->getUniqueId()->substr(0, wildCardLocA)) == 0) return it;
        } else {
          if ((*it)->getUniqueId().substr(0, wildCardLocA).compare(message->getUniqueId()->substr(0, wildCardLocB)) == 0) return it;
        }
      } else {
        if ((*it)->getUniqueId().compare(*message->getUniqueId()) == 0) return it;
      }
    }
  }
  return registeredCommands.end();
}
