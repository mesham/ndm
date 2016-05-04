#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "ndm.h"

#define HXRES 512
#define HYRES 512
#define MAX_ITERATIONS 100
#define MAGNIFICATION 1.0

NDM_Group computationGroup, analysisGroup, localGroup, analyticsGroup;
int num_comp_ranks;

void mandelbrotKernel(void*, NDM_Metadata);
void numberBoundedPoints(void*, NDM_Metadata);
void firstNonBoundedPoint(void*, NDM_Metadata);
void firstBoundedPoint(void*, NDM_Metadata);
void localDataDump(void*, NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  int size, my_global_rank, computation_group_rank;
  ndmGroupRank(NDM_GLOBAL_GROUP, &my_global_rank);

  ndmGroupCreateWithStride(&computationGroup, NDM_GLOBAL_GROUP, 11, 1);
  ndmGroupExtractChunkContainingRank(&localGroup, NDM_GLOBAL_GROUP, 12, my_global_rank);
  ndmGroupCreateDisjoint(&analyticsGroup, NDM_GLOBAL_GROUP, computationGroup);

  ndmGroupSize(computationGroup, &size);
  ndmGroupSize(localGroup, &num_comp_ranks);
  num_comp_ranks--;
  ndmGroupRank(computationGroup, &computation_group_rank);

  if (my_global_rank == 0) {
    int i, currentStart = 1;
    for (i = 0; i < size; i++) {
      int dataToSend[2];
      dataToSend[0] = currentStart;
      dataToSend[1] = HXRES / size;
      if (i < HXRES % size) dataToSend[1]++;
      currentStart += dataToSend[1];
      ndmSend(dataToSend, 2, NDM_INT, i, computationGroup, "dist");
    }
  }
  if (computation_group_rank >= 0) {
    ndmRecv(mandelbrotKernel, 0, computationGroup, "dist");
  } else {
    ndmRecvStream(localDataDump, NDM_ANY_SOURCE, localGroup, "datadump_*");
  }
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void mandelbrotKernel(void* buffer, NDM_Metadata metadata) {
  int hxstart = ((int*)buffer)[0], hxlen = ((int*)buffer)[1];
  double x, xx, y, cx, cy;
  int iteration, hx, hy, point_summary[hxlen];
  char uid[20];
  for (hy = 1; hy <= HYRES; hy++) {
    sprintf(uid, "datadump_%d", hy);
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
      if (iteration == MAX_ITERATIONS) {
        point_summary[hx - hxstart] = 1;
      } else {
        point_summary[hx - hxstart] = 0;
      }
    }
    ndmSend(point_summary, hxlen, NDM_INT, 0, localGroup, uid);
  }
}

void localDataDump(void* buffer, NDM_Metadata metadata) {
  char uid_bp[20], uid_firstnonbounded[20], uid_firstbounded[20];
  char* lineNumber = strstr(metadata.unique_id, "_");
  sprintf(uid_bp, "points%s", lineNumber);
  sprintf(uid_firstnonbounded, "nb%s", lineNumber);
  sprintf(uid_firstbounded, "b%s", lineNumber);
  int* data = (int*)buffer;
  int i, boundedPoints = 0, firstnb = 0, firstb = 0;
  for (i = 0; i < metadata.number_elements; i++) {
    if (data[i]) {
      boundedPoints++;
      if (!firstb) {
        firstb = 1;
        ndmReduceAdditive(&i, 1, NDM_INT, num_comp_ranks, 1, 0, NDM_MIN, firstBoundedPoint, 0, analyticsGroup, uid_firstbounded);
      }
    } else {
      if (!firstnb) {
        firstnb = 1;
        ndmReduceAdditive(&i, 1, NDM_INT, num_comp_ranks, 1, 0, NDM_MIN, firstNonBoundedPoint, 0, analyticsGroup, uid_firstnonbounded);
      }
    }
  }
  ndmReduceAdditive(&boundedPoints, 1, NDM_INT, num_comp_ranks, 1, 0, NDM_SUM, numberBoundedPoints, 0, analyticsGroup, uid_bp);
  if (!firstb)
    ndmReduceAdditive(NULL, 1, NDM_INT, num_comp_ranks, 1, 0, NDM_MIN, firstBoundedPoint, 0, analyticsGroup, uid_firstbounded);
  if (!firstnb)
    ndmReduceAdditive(NULL, 1, NDM_INT, num_comp_ranks, 1, 0, NDM_MIN, firstNonBoundedPoint, 0, analyticsGroup, uid_firstnonbounded);
}

void numberBoundedPoints(void* buffer, NDM_Metadata metadata) {
  int* data = (int*)buffer;
  printf("Points inside = %.0f%% %s\n", ((double)data[0] / HXRES) * 100, metadata.unique_id);
}

void firstNonBoundedPoint(void* buffer, NDM_Metadata metadata) {
  int* data = (int*)buffer;
  printf("First non-bounded point=%d %s\n", data[0], metadata.unique_id);
}

void firstBoundedPoint(void* buffer, NDM_Metadata metadata) {
  int* data = (int*)buffer;
  printf("First bounded point=%d %s\n", data[0], metadata.unique_id);
}
