#ifndef RAFT_FORWARDER_H_
#define RAFT_FORWARDER_H_

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "unistd.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>

/**
 * Thread
 */

/**
 *
 * Forwarder related
 *
 */
#define SIMMODE_T int
#define SIMCONNTYPE_T int

#define SIMMODE_NONE 0
#define SIMMODE_ASYNC 1
#define SIMMODE_SYNC 2
#define SIMMODE_FILE 3

#define SIMCONNTYPE_TCP 0
#define SIMCONNTYPE_UNIX 1
#define SIMCONNTYPE_PIPE 2

typedef struct forwarder forwarder_t;

struct forwarder {
    SIMMODE_T MODE;
    SIMCONNTYPE_T CONNTYPE;

    int out_conn;
    int in_conn;

    pthread_mutex_t *mutex;
    pthread_cond_t *cond;
    uint64_t nextReqId;

    int OutConn; // fd
    int InConn; // fd

    // For FILE mode
    FILE* file;
    FILE* meta_file;
    char log_dir[64];
    long num_messages;
};


forwarder_t *forwarder_new();

void forwarder_send(forwarder_t *fwdr, const void *buf, size_t len);

void forwarder_recv(forwarder_t *fwdr, void *buf, size_t len);

void forwarder_close(forwarder_t *fwdr);

#endif
