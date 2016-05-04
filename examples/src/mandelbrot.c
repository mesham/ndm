#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include "mpi.h"
#include "ndm.h"

#define HXRES 512
#define HYRES 512
#define MAX_ITERATIONS 100
#define MAGNIFICATION 1.0

void mandelbrotKernel(void*, NDM_Metadata);
void numberBoundedPoints(void*, NDM_Metadata);
void firstNonBoundedPoint(void*, NDM_Metadata);
void firstBoundedPoint(void*, NDM_Metadata);

int main(int argc, char* argv[]) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  ndmInit();
  int size, myrank;
  ndmGroupRank(NDM_GLOBAL_GROUP, &myrank);
  ndmGroupSize(NDM_GLOBAL_GROUP, &size);
  if (myrank == 0) {
    int i, currentStart = 1;
    for (i = 0; i < size; i++) {
      int dataToSend[2];
      dataToSend[0] = currentStart;
      dataToSend[1] = HXRES / size;
      if (i < HXRES % size) dataToSend[1]++;
      currentStart += dataToSend[1];
      ndmSend(dataToSend, 2, NDM_INT, i, NDM_GLOBAL_GROUP, "dist");
    }
  }
  ndmRecv(mandelbrotKernel, 0, NDM_GLOBAL_GROUP, "dist");
  ndmFinalise();
  MPI_Finalize();
  return 0;
}

void mandelbrotKernel(void* buffer, NDM_Metadata metadata) {
  int hxstart = ((int*)buffer)[0], hxlen = ((int*)buffer)[1];
  double x, xx, y, cx, cy;
  int iteration, hx, hy, boundedPoints, firstnb, firstb;
  char uid_bp[20], uid_firstnonbounded[20], uid_firstbounded[20];
  for (hy = 1; hy <= HYRES; hy++) {
    boundedPoints = firstnb = firstb = 0;
    sprintf(uid_bp, "points_%d", hy);
    sprintf(uid_firstnonbounded, "nb_%d", hy);
    sprintf(uid_firstbounded, "b_%d", hy);
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
          if (!firstnb) {
            firstnb = 1;
            ndmReduce(&hx, 1, NDM_INT, NDM_MIN, firstNonBoundedPoint, 0, metadata.comm_group, uid_firstnonbounded);
          }
        }
      }
      if (iteration == MAX_ITERATIONS) {
        boundedPoints++;
        if (!firstb) {
          firstb = 1;
          ndmReduce(&hx, 1, NDM_INT, NDM_MIN, firstBoundedPoint, 0, metadata.comm_group, uid_firstbounded);
        }
      }
    }
    ndmReduce(&boundedPoints, 1, NDM_INT, NDM_SUM, numberBoundedPoints, 0, metadata.comm_group, uid_bp);
    if (!firstb) ndmReduce(NULL, 1, NDM_INT, NDM_MIN, firstBoundedPoint, 0, metadata.comm_group, uid_firstbounded);
    if (!firstnb) ndmReduce(NULL, 1, NDM_INT, NDM_MIN, firstNonBoundedPoint, 0, metadata.comm_group, uid_firstnonbounded);
  }
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
