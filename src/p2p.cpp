/*
 * p2p.cpp
 *
 *  Created on: 29 Dec 2015
 *      Author: nick
 */

#include "p2p.h"
#include "messaging.h"
#include "data_types.h"
#include <string.h>
#include "groups.h"
#include "ndm.h"

static int P2P_ACTION_ID = 0;

void p2p_ndmSend(Messaging messaging, void* data, int type, int size, int target, NDM_Group comm_group, const char* unique_id) {
  int dataSize;
  char* buffer =
      messaging.packageMessage(data, type, size, getMyGroupRank(comm_group), target, comm_group, P2P_ACTION_ID, unique_id, &dataSize);
  messaging.sendMessage(buffer, dataSize, target, comm_group);
}

void p2p_ndmRecv(Messaging messaging, void (*callback)(void*, NDM_Metadata), int source, int target, bool recurring,
                 NDM_Group comm_group, const char* unique_id) {
  messaging.registerCommand(unique_id, source, target, comm_group, P2P_ACTION_ID, recurring, callback);
}
