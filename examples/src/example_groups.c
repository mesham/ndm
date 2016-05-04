/*
 * example2.c
 *
 *  Created on: 3 May 2016
 *      Author: nick
 */

#include "ndm.h"
#include <mpi.h>
#include <stdio.h>

int main(int argc, char* argv[]) {
  int provided, my_rank, shrunk_rank, extracted_rank, disjoint_rank;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  ndmGroupRank(NDM_GLOBAL_GROUP, &my_rank);
  NDM_Group shrunkGroup, extractedGroup, disjointGroup;
  ndmGroupCreateWithStride(&shrunkGroup, NDM_GLOBAL_GROUP, 11, 1);
  ndmGroupRank(shrunkGroup, &shrunk_rank);
  ndmGroupExtractChunkContainingRank(&extractedGroup, NDM_GLOBAL_GROUP, 12, my_rank);
  ndmGroupRank(extractedGroup, &extracted_rank);
  ndmGroupCreateDisjoint(&disjointGroup, NDM_GLOBAL_GROUP, shrunkGroup);
  ndmGroupRank(disjointGroup, &disjoint_rank);
  printf("%d and %d and %d and %d\n", my_rank, shrunk_rank, extracted_rank, disjoint_rank);
  ndmFinalise();
  MPI_Finalize();
  return 0;
}
