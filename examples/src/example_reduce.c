/*
 * example2.c
 *
 *  Created on: 3 May 2016
 *      Author: nick
 */

#include "ndm.h"
#include <mpi.h>
#include <stdio.h>

void recvFunction(void*, NDM_Metadata);
void additiveRecvFunction(void*, NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();

  char uuid[10];
  int data = 10;
  ndmReduce(&data, 1, NDM_INT, NDM_SUM, recvFunction, 0, NDM_GLOBAL_GROUP, "a");
  ndmGroupRank(NDM_GLOBAL_GROUP, &data);
  ndmAllReduce(&data, 1, NDM_INT, NDM_MAX, recvFunction, NDM_GLOBAL_GROUP, "maxrank");
  data = 5;
  ndmReduceAdditive(&data, 1, NDM_INT, 12, 1, 0, NDM_SUM, additiveRecvFunction, 0, NDM_GLOBAL_GROUP, "additive");
  ndmReduceAdditive(&data, 1, NDM_INT, 12, 1, 0, NDM_SUM, additiveRecvFunction, 0, NDM_GLOBAL_GROUP, "additive");
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void recvFunction(void* buffer, NDM_Metadata metaData) {
  printf("Got reduction data '%d' with uuid %s on pid %d\n", *((int*)buffer), metaData.unique_id, metaData.my_rank);
}

void additiveRecvFunction(void* buffer, NDM_Metadata metaData) { printf("Got additive data '%d'\n", *((int*)buffer)); }
