#ifndef RAFT_FORWARDER_H_
#define RAFT_FORWARDER_H_

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/**
 * Forwarder: reports protocol messages to the Ellsberg simulator or to log
 * files. Configured through environment variables, read by forwarder_new():
 *
 *   SIMMODE      NONE (default), ASYNC, SYNC or FILE
 *   SIMCONNTYPE  UNIX (default; the only supported connection type)
 *   IID          instance id, used in socket and log file names
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
    char iid[64];

    /* ASYNC / SYNC: Unix domain socket connected to the simulator */
    int conn;

    /* Serializes writes to the socket or log file */
    pthread_mutex_t mutex;

    /* FILE mode */
    FILE *file;
    FILE *meta_file;
    char log_dir[128];
    long num_messages;
    long prev_num_messages;
    pthread_t rotate_thread;
    int closing;
};

/* Returns NULL when SIMMODE is NONE or unset (reporting disabled). */
forwarder_t *forwarder_new(void);

void forwarder_send(forwarder_t *fwdr, const void *buf, size_t len);

/* Safe to call with NULL. */
void forwarder_close(forwarder_t *fwdr);

#endif
