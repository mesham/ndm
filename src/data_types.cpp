/*
 * data_types.cpp
 *
 *  Created on: 21 Dec 2015
 *      Author: nick
 */

#include "ndm.h"
#include "data_types.h"
#include <stdio.h>

int getTypeSize(int type) {
  if (type == NDM_INT) return 4;
  if (type == NDM_FLOAT) return 4;
  if (type == NDM_DOUBLE) return 8;
  if (type == NDM_NOTYPE) return 0;
  fprintf(stderr, "Error in type matching\n");
  return -1;
}
