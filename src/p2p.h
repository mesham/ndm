/*
 * p2p.h
 *
 *  Created on: 29 Dec 2015
 *      Author: nick
 */

#ifndef SRC_COMMUNICATIONS_P2P_H_
#define SRC_COMMUNICATIONS_P2P_H_

#include "messaging.h"
#include "ndm.h"

void p2p_ndmSend(Messaging messaging, void*, int, int, int, NDM_Group, const char*);
void p2p_ndmRecv(Messaging messaging, void (*)(void*, NDM_Metadata), int, int, bool, NDM_Group, const char*);
#endif /* SRC_COMMUNICATIONS_P2P_H_ */
