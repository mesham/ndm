/*
 * example_barrier.c
 *
 *  Created on: 3 May 2016
 *      Author: nick
 */

#include "ndm.h"
#include <mpi.h>
#include <stdio.h>

void recvFunctionBarrier(NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided, my_rank;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  ndmGroupRank(NDM_GLOBAL_GROUP, &my_rank);

  ndmBarrier(recvFunctionBarrier, NDM_GLOBAL_GROUP, "a");
  if (my_rank == 0) printf("First barrier issued asynchronously, continuing to issue second barrier\n");
  ndmBarrier(recvFunctionBarrier, NDM_GLOBAL_GROUP, "b");
  if (my_rank == 0) printf("Second barrier issued asynchronously, awaiting all barrier callbacks\n");
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void recvFunctionBarrier(NDM_Metadata metaData) { printf("Barrier for '%s' at rank %d\n", metaData.unique_id, metaData.my_rank); }
