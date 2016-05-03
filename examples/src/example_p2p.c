#include "ndm.h"
#include <mpi.h>
#include <stdio.h>

void recvFunctionTest(void*, NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided, my_rank;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  int data = 12;
  if (my_rank == 0) {
    ndmSend(&data, 1, NDM_INT, 1, NDM_GLOBAL_GROUP, "a");
  } else if (my_rank == 1) {
    ndmRecv(recvFunctionTest, NDM_ANY_SOURCE, NDM_GLOBAL_GROUP, "a");
  }

  NDM_Group threadedGroup;
  ndmCreateVirtualRanks(&threadedGroup, NDM_GLOBAL_GROUP, 4);

  int flag;
  ndmIsRankLocal(threadedGroup, 0, &flag);
  if (flag) {
    data = 1000;
    int i, size;
    ndmGroupSize(threadedGroup, &size);
    for (i = 0; i < size; i++) {
      ndmSend(&data, 1, NDM_INT, i, threadedGroup, "b");
    }
  }
  ndmRecvStream(recvFunctionTest, NDM_ANY_SOURCE, threadedGroup, "b");

  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void recvFunctionTest(void* buffer, NDM_Metadata metaData) {
  printf("Got value '%d' from %d with uid '%s', my rank is %d\n", ((int*)buffer)[0], metaData.source, metaData.unique_id,
         metaData.my_rank);
}
