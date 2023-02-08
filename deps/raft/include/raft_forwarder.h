#ifndef RAFT_FORWARDER_H_
#define RAFT_FORWARDER_H_

#include "raft.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "unistd.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

forwarder_t *forwarder_new(char *server_path, char *client_path);

void forwarder_send(forwarder_t *fwdr, const void *buf, int len);

void forwarder_recv(forwarder_t *fwdr, void *buf, int len);

void forwarder_close(forwarder_t *fwdr);

#endif
