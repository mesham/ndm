#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include "mpi.h"
#include "ndm.h"

#define HXRES 512
#define HYRES 512
#define MAX_ITERATIONS 100
#define MAGNIFICATION 1.0
#define VIRTUAL_RANKS_PER_PROCESS 4

void mandelbrotKernel(void*, NDM_Metadata);
void reducePointLine(void*, NDM_Metadata);
void runInHeterogeneousMode(void);
void runInProcessMode(void);
void distributeAndRun(NDM_Group);

int main(int argc, char* argv[]) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  runInProcessMode();
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void runInHeterogeneousMode(void) {
  NDM_Group threadedGroup;
  ndmCreateVirtualRanks(&threadedGroup, NDM_GLOBAL_GROUP, VIRTUAL_RANKS_PER_PROCESS);
  distributeAndRun(threadedGroup);
}

void runInProcessMode(void) { distributeAndRun(NDM_GLOBAL_GROUP); }

void distributeAndRun(NDM_Group specificGroup) {
  int size, flag;
  ndmIsRankLocal(specificGroup, 0, &flag);
  ndmGroupSize(specificGroup, &size);
  if (flag) {
    int i, currentStart = 1;
    for (i = 0; i < size; i++) {
      int dataToSend[2];
      dataToSend[0] = currentStart;
      dataToSend[1] = HXRES / size;
      if (i < HXRES % size) dataToSend[1]++;
      currentStart += dataToSend[1];
      ndmSend(dataToSend, 2, NDM_INT, i, specificGroup, "dist");
    }
  }
  ndmRecvStream(mandelbrotKernel, 0, specificGroup, "dist");
}

void mandelbrotKernel(void* buffer, NDM_Metadata metadata) {
  int hxstart = ((int*)buffer)[0], hxlen = ((int*)buffer)[1];
  double x, xx, y, cx, cy;
  int iteration, hx, hy, boundedPoints;
  char uid[20];
  for (hy = 1; hy <= HYRES; hy++) {
    boundedPoints = 0;
    for (hx = hxstart; hx < hxstart + hxlen; hx++) {
      cx = (((float)hx) / ((float)HXRES) - 0.5) / MAGNIFICATION * 3.0 - 0.7;
      cy = (((float)hy) / ((float)HYRES) - 0.5) / MAGNIFICATION * 3.0;
      x = 0.0;
      y = 0.0;
      for (iteration = 1; iteration < MAX_ITERATIONS; iteration++) {
        xx = x * x - y * y + cx;
        y = 2.0 * x * y + cy;
        x = xx;
        if (x * x + y * y > 100.0) {
          break;
        }
      }
      if (iteration == MAX_ITERATIONS) boundedPoints++;
    }
    sprintf(uid, "points_%d", hy);
    ndmReduceFromRank(&boundedPoints, 1, NDM_INT, NDM_SUM, reducePointLine, 0, metadata.my_rank, metadata.comm_group, uid);
  }
}

void reducePointLine(void* buffer, NDM_Metadata metadata) {
  int* data = (int*)buffer;
  printf("Points inside = %.0f%% %s\n", ((double)data[0] / HXRES) * 100, metadata.unique_id);
}
