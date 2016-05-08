/*
 * ndm.h
 *
 *  Created on: 22 Nov 2015
 *      Author: nick
 */

#ifndef SRC_NDM_H_
#define SRC_NDM_H_

#define NDM_NOTYPE 0
#define NDM_INT 1
#define NDM_FLOAT 2
#define NDM_DOUBLE 3
#define NDM_BYTE 4

#define NDM_ANY_SOURCE -1
#define NDM_ANY_MYRANK -1

#define NDM_NO_OPERATION -1
#define NDM_SUM 0
#define NDM_MIN 1
#define NDM_MAX 2
#define NDM_PROD 3

#define NDM_AUTO_UUID_LENGTH 15

#define NDM_GLOBAL_GROUP 0

#ifdef __cplusplus
extern "C" {
#endif

typedef int NDM_Group;
typedef int NDM_Op;

struct ndm_struct_metadata {
  int data_type, number_elements, source, my_rank;
  NDM_Group comm_group;
  const char *unique_id;
};

typedef struct ndm_struct_metadata NDM_Metadata;

int ndmInit(void);
void ndmSend(void *, int, int, int, NDM_Group, const char *);
int ndmRecv(void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmRecvStream(void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmRecvFromRank(void (*)(void *, NDM_Metadata), int, int, NDM_Group, const char *);
int ndmBcast(void *, int, int, void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmBcastFromRank(void *, int, int, void (*)(void *, NDM_Metadata), int, int, NDM_Group, const char *);
int ndmReduce(void *, int, int, NDM_Op, void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmReduceFromRank(void *, int, NDM_Op, int, void (*)(void *, NDM_Metadata), int, int, NDM_Group, const char *);
int ndmReduceAdditive(void *, int, int, int, int, int, NDM_Op, void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmAllReduce(void *, int, int, NDM_Op, void (*)(void *, NDM_Metadata), NDM_Group, const char *);
int ndmAllReduceFromRank(void *, int, int, NDM_Op, void (*)(void *, NDM_Metadata), int, NDM_Group, const char *);
int ndmBarrier(void (*)(NDM_Metadata), NDM_Group, const char *);
int ndmGroupCreate(NDM_Group *, int, int *);
int ndmGroupCreateWithStride(NDM_Group *, NDM_Group, int, int);
int ndmGroupExtractChunkContainingRank(NDM_Group *, NDM_Group, int, int);
int ndmGroupCreateDisjoint(NDM_Group *, NDM_Group, NDM_Group);
int ndmGroupSize(NDM_Group, int *);
int ndmGroupRank(NDM_Group, int *);
int ndmCreateVirtualRanks(NDM_Group *, NDM_Group, int);
int ndmIsRankLocal(NDM_Group, int, int *);
int ndmGroupLocalSize(NDM_Group, int *);
int ndmGetGroupsGlobalNthRank(NDM_Group, int, int *);
void generateUUID(char *, unsigned int);
int ndmFinalise(void);

#ifdef __cplusplus
}
#endif

#endif /* SRC_NDM_H_ */
