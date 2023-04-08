#ifndef REDISRAFT_INTERPRETER_H
#define REDISRAFT_INTERPRETER_H

#include "redisraft.h"

/* Client */
sds InterpreteCmdRaft(raft_server_t *raft, bool send, size_t req_id, RedisModuleString **rmargv, char **cargv, int argc);
sds InterpreteCmdRaftReply(raft_server_t *raft, bool send, size_t req_id, const char* reply_proto, size_t reply_proto_len, bool success);
sds InterpreteRaftNode(raft_server_t *raft, bool send, bool add, size_t req_id, RedisModuleString **rmargv, char **cargv, int argc);
sds InterpreteRaftNodeReply(raft_server_t *raft, bool send, bool add, size_t req_id, raft_node_id_t node_id, const char* dbid, bool success);

/* Raft */
sds InterpreteAE(raft_server_t *raft, bool send, raft_msg_id_t req_msg_id, sds joined_req_ids, RedisModuleString **rmargv, char **cargv, int argc, const size_t *argvlen, size_t *cmdlen);

sds InterpreteAER(
  raft_server_t *raft,
  bool send, 
  raft_msg_id_t req_msg_id,
  raft_appendentries_resp_t *resp, 
  raft_node_id_t dst_id,
  raft_node_id_t src_id,
  RedisModuleString **argv, char **cargv, int argc);

sds InterpreteRV(raft_server_t *raft, bool send, raft_msg_id_t req_msg_id, RedisModuleString **argv, char **cargv, int argc);
char* InterpreteRVFormat(raft_server_t *raft, raft_node_t *raft_node, bool send, raft_msg_id_t req_msg_id, raft_requestvote_req_t* msg);

sds InterpreteRVR(
  raft_server_t *raft,
  bool send, 
  raft_msg_id_t req_msg_id, 
  raft_requestvote_resp_t *resp, 
  raft_node_id_t dst_id,
  raft_node_id_t src_id,
  RedisModuleString **argv, char **cargv, int argc);

#endif