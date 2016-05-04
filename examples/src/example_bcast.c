/*
 * example_bcast.c
 *
 *  Created on: 3 May 2016
 *      Author: nick
 */

#include "ndm.h"
#include <mpi.h>
#include <stdio.h>

void recvFunctionBcast(void*, NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided, my_rank, size;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();

  ndmGroupRank(NDM_GLOBAL_GROUP, &my_rank);
  ndmGroupSize(NDM_GLOBAL_GROUP, &size);

  char uuid[10];
  int i, data;
  for (i = 0; i < size; i++) {
    sprintf(uuid, "bcast_%d", i);
    if (my_rank == i) {
      data = i * 10;
      ndmBcast(&data, 1, NDM_INT, recvFunctionBcast, i, NDM_GLOBAL_GROUP, uuid);
    } else {
      ndmBcast(NULL, 1, NDM_INT, recvFunctionBcast, i, NDM_GLOBAL_GROUP, uuid);
    }
  }
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void recvFunctionBcast(void* buffer, NDM_Metadata metaData) {
  if (metaData.my_rank == 0) {
    printf("Got bcast data '%d' from %d with uuid %s\n", *((int*)buffer), metaData.source, metaData.unique_id);
  }
}
