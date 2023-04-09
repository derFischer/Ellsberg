#include "interpreter.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Return the number of digits of 'v' when converted to string in radix 10.
 * Implementation borrowed from link in redis/src/util.c:string2ll(). */
static uint32_t countDigits(uint64_t v) {
  uint32_t result = 1;
  for (;;) {
    if (v < 10) return result;
    if (v < 100) return result + 1;
    if (v < 1000) return result + 2;
    if (v < 10000) return result + 3;
    v /= 10000U;
    result += 4;
  }
}

/* Helper that calculates the bulk length given a certain string length. */
static size_t bulklen(size_t len) {
    return 1+countDigits(len)+2+len+2;
}

sds DupAndReplaceSplitter(sds cmd, size_t total_len) {
  sds dcmd = sdsempty();
  dcmd = sdsdup(cmd);

  if (total_len > 0) {
    for (size_t i = 0; i < total_len; i++) {
      if (dcmd[i] == '\0') {
        dcmd[i] = '_';
      }
    }
  }

  int count = 0;
  sds* split_dcmd = sdssplitlen(dcmd, sdslen(dcmd), "\r\n", 2, &count);
  sds joined = sdsjoinsds(split_dcmd, count, "|", 1);

  sdsfree(dcmd);
  sdsfreesplitres(split_dcmd, count);
  
  return joined;
}

void TranslateRMArgvOrUseCArgv(RedisModuleString **rmargv, char **cargv, int argc, const char **argv, int from) {
  if (argc == 0) {
    return;
  }
  if (cargv == NULL) {
    for (int i = 0; i < argc; i++) {
      argv[from + i] = RedisModule_StringPtrLen(rmargv[i], NULL);
    }
  } else {
    assert(cargv != NULL);
    for (int i = 0; i < argc; i++) {
      argv[from + i] = cargv[i];
    }
  }
}

sds InterpreteCmdRaft(raft_server_t *raft, bool send, size_t req_id, RedisModuleString **rmargv, char **cargv, int argc) {
  sds cmd;
  int more_argc = 3;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  int argc_tosend = argc + more_argc;
  argv_tosend[0] = send ? "1" : "0"; 
  // msg_type: Client Request
  argv_tosend[1] = "1";
  // TODO: msg_id
  sds req_id_sds = sdsfromlonglong(req_id);
  argv_tosend[2] = req_id_sds;
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
  
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaft: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}

sds InterpreteCmdRaftReply(raft_server_t *raft, bool send, size_t req_id, RaftRedisCommand *reqcmd, const char* reply_proto, size_t reply_proto_len, bool success) {
  sds cmd = sdsempty();
  size_t argc_tosend = 3;
  const char** argv_tosend = (const char**)calloc(argc_tosend, sizeof(char*));
  argv_tosend[0] = send ? "1" : "0"; 
  // msg_type: Client Response
  if (success) {
    argv_tosend[1] = "2";
  } else {
    argv_tosend[1] = "3";
  }
  sds req_id_sds = sdsfromlonglong(req_id);
  argv_tosend[2] = req_id_sds;

  char *used_reply_proto = (char*)reply_proto;
  if (reply_proto[0] == '*') {
    // reply of set is like this.
    size_t reply_array_size;
    int n = sscanf(reply_proto, "*%ld\r\n", &reply_array_size);
    argc_tosend += reply_array_size + reqcmd->argc;
    sds reqcmd_sds = sdsempty();
    const char** cmd_argv = (const char**)calloc(reqcmd->argc, sizeof(char*));
    TranslateRMArgvOrUseCArgv(reqcmd->argv, NULL, reqcmd->argc, cmd_argv, /*from=*/0);
    redisFormatSdsCommandArgv(&reqcmd_sds, reqcmd->argc, cmd_argv, NULL);
    sdsrange(reqcmd_sds, 4, -1);
    used_reply_proto += n;
    printf("used_reply_proto: %s\n", used_reply_proto); 
    cmd = sdscatprintf(
      cmd, 
      "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "%s" "%s", 
      argc_tosend, 1L, argv_tosend[0], 1L, argv_tosend[1], strlen(argv_tosend[2]), argv_tosend[2], used_reply_proto, reqcmd_sds);
    printf("I forgot when this happens, so exit here. (InterpreteCmdRaftReply)\n");
    exit(1);
  } else if (reply_proto[0] == '$') {
    // Get will reply as bulk stirng, like "$5\r\nhello\r\n"
    argc_tosend += 1 + reqcmd->argc;
    sds reqcmd_sds = sdsempty();
    const char** cmd_argv = (const char**)calloc(reqcmd->argc, sizeof(char*));
    TranslateRMArgvOrUseCArgv(reqcmd->argv, NULL, reqcmd->argc, cmd_argv, /*from=*/0);
    redisFormatSdsCommandArgv(&reqcmd_sds, reqcmd->argc, cmd_argv, NULL);
    sdsrange(reqcmd_sds, 4, -1);
    cmd = sdscatprintf(
      cmd, 
      "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "%s" "%s", 
      argc_tosend, 1L, argv_tosend[0], 1L, argv_tosend[1], strlen(argv_tosend[2]), argv_tosend[2], used_reply_proto, reqcmd_sds);
  } else if (reply_proto[0] == '+') {
    // Set will reply as "+OK\r\n"
    argc_tosend += 1 + reqcmd->argc;
    sds reply_sds = sdsempty();
    reply_sds = sdscpy(reply_sds, used_reply_proto);
    sdsrange(reply_sds, 0, -3);
    sds reqcmd_sds = sdsempty();
    const char** cmd_argv = (const char**)calloc(reqcmd->argc, sizeof(char*));
    TranslateRMArgvOrUseCArgv(reqcmd->argv, NULL, reqcmd->argc, cmd_argv, /*from=*/0);
    redisFormatSdsCommandArgv(&reqcmd_sds, reqcmd->argc, cmd_argv, NULL);
    sdsrange(reqcmd_sds, 4, -1);
    cmd = sdscatprintf(
      cmd, 
      "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s" "%s", 
      argc_tosend, 1L, argv_tosend[0], 1L, argv_tosend[1], strlen(argv_tosend[2]), argv_tosend[2], reply_proto_len-3, used_reply_proto+1, reqcmd_sds);
  } else {
    assert(false);
  }
  
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaftReply: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree(req_id_sds);
  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}

sds InterpreteRaftNode(raft_server_t *raft, bool send, bool add, size_t req_id, RedisModuleString **rmargv, char **cargv, int argc) {
  sds cmd;
  int more_argc = 3;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  int argc_tosend = argc + more_argc;
  argv_tosend[0] = send ? "1" : "0"; 
  // msg_type: Client Request
  argv_tosend[1] = "1";
  // TODO: msg_id
  sds req_id_sds = sdsfromlonglong(req_id);
  argv_tosend[2] = req_id_sds;
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
  
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] RaftNode(%s): < %s >\n", send ? "send" : "recv", add ? "ADDVOTING" : "REMOVE", replaced);

  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}

sds InterpreteRaftNodeReply(raft_server_t *raft, bool send, bool add, size_t req_id, raft_node_id_t node_id, const char* dbid, bool success) {
  sds cmd = sdsempty();
  const char* send_str = send ? "1" : "0"; 
  char* msg_type_str = "2";
  if (!success) {
    msg_type_str = "3";
  }
  sds req_id_sds = sdsfromlonglong(req_id);
  sds node_id_str = sdsfromlonglong(node_id);

  // TODO: we don't care reply now, add reply type if necessary.
  if (dbid != NULL) {
    cmd = sdscatprintf(
      cmd, 
      "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s", 
      5L, 1L, send_str, 1L, msg_type_str, strlen(req_id_sds), req_id_sds, strlen(node_id_str), node_id_str, strlen(dbid), dbid);
  } else {
    cmd = sdscatprintf(
      cmd, 
      "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s", 
      4L, 1L, send_str, 1L, msg_type_str, strlen(req_id_sds), req_id_sds, strlen(node_id_str), node_id_str);
  }

  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] RaftNodeReply(%s): < %s >\n", send ? "send" : "recv", add ? "ADD" : "REMOVE", replaced);

  sdsfree(req_id_sds);
  sdsfree(node_id_str);
  sdsfree(replaced);

  return cmd;
}

sds InterpreteAE(raft_server_t *raft, bool send, raft_msg_id_t req_msg_id, sds joined_req_ids, RedisModuleString **rmargv, char **cargv, int argc, const size_t *argvlen, size_t *cmdlen) {
  sds cmd;
  int more_argc = 4;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  size_t* argvlen_tosend = (size_t*)calloc(more_argc+argc, sizeof(size_t));
  int argc_tosend = argc + more_argc;
  argv_tosend[0] = send ? "1" : "0"; 
  // msg_type: RaftRequest
  argv_tosend[1] = "4";
  // msg_id
  sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
  argv_tosend[2] = req_msg_id_sds;
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, 3);
  argv_tosend[more_argc+argc-1] = joined_req_ids;

  // calculate `argvlen_tosend` and `total_len`
  argvlen_tosend[0] = 1;
  argvlen_tosend[1] = 1;
  argvlen_tosend[2] = strlen(req_msg_id_sds);
  size_t total_len = 1+countDigits(argc)+2 + bulklen(1) + bulklen(1) + bulklen(argvlen_tosend[2]);
  for (int j = 0; j < argc; j++) {
    argvlen_tosend[3+j] = argvlen[j];
    total_len += bulklen(argvlen_tosend[3+j]);
  }
  argvlen_tosend[more_argc+argc-1] = strlen(joined_req_ids);
  total_len += bulklen(argvlen_tosend[more_argc+argc-1]);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, argvlen_tosend);
  
  sds replaced = DupAndReplaceSplitter(cmd, total_len);
  printf("[%s] cmdRaftAppendEntries: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree(replaced);
  free(argv_tosend);

  *cmdlen = total_len;

  return cmd;
}

// TODO: add argvlen
sds InterpreteAER(
    raft_server_t *raft,
    bool send, 
    raft_msg_id_t req_msg_id,
    raft_appendentries_resp_t *resp,
    raft_node_id_t dst_id,
    raft_node_id_t src_id,
    RedisModuleString **rmargv, char **cargv, int argc)
{
  sds cmd;
  int more_argc = 7;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  int argc_tosend = argc + more_argc;
  // send=false
  argv_tosend[0] = send ? "1" : "0";  
  // msg_type: RaftResponse
  argv_tosend[1] = "5";
  // msg_id
  sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
  argv_tosend[2] = req_msg_id_sds;
  // resp: RAFT.AER
  argv_tosend[3] = "RAFT.AER";
  // resp: dst_id
  argv_tosend[4] = sdsfromlonglong(dst_id);
  // resp: src_id
  argv_tosend[5] = sdsfromlonglong(src_id);
  // resp: message content
  argv_tosend[6] = sdscatprintf(sdsempty(), "%ld:%d:%ld:%ld", resp->term, resp->success, resp->current_idx, resp->msg_id);
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
  
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaftAppendEntriesResponse: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree((sds)argv_tosend[2]);
  sdsfree((sds)argv_tosend[4]);
  sdsfree((sds)argv_tosend[5]);
  sdsfree((sds)argv_tosend[6]);
  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}

sds InterpreteRV(raft_server_t *raft, bool send, raft_msg_id_t req_msg_id, RedisModuleString **rmargv, char **cargv, int argc)
{
  sds cmd;
  int more_argc = 3;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  int argc_tosend = argc + more_argc;
  argv_tosend[0] = send ? "1" : "0"; 
  // msg_type: RaftRequest
  argv_tosend[1] = "4";
  // TODO: msg_id
  sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
  argv_tosend[2] = req_msg_id_sds;
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
  
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaftRequestVote: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}

char* InterpreteRVFormat(raft_server_t *raft, raft_node_t *raft_node, bool send, raft_msg_id_t req_msg_id, raft_requestvote_req_t* msg)
{
  char *ccmd;
  redisFormatCommand(&ccmd, "%d %d %d RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld",
                     send,
                     3, // msg_type: RaftRequest
                     req_msg_id, 
                     raft_node_get_id(raft_node),
                     raft_get_nodeid(raft),
                     msg->prevote,
                     msg->term,
                     msg->candidate_id,
                     msg->last_log_idx,
                     msg->last_log_term);

  sds cmd = sdsempty();
  cmd = sdscpy(cmd, ccmd);
  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaftRequestVote: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree(cmd);
  sdsfree(replaced);

  return ccmd;
}

sds InterpreteRVR(
    raft_server_t *raft,
    bool send, 
    raft_msg_id_t req_msg_id, 
    raft_requestvote_resp_t *resp, 
    raft_node_id_t dst_id,
    raft_node_id_t src_id,
    RedisModuleString **rmargv, char **cargv, int argc)
{
  sds cmd;
  int more_argc = 7;
  const char** argv_tosend = (const char**)calloc(more_argc+argc, sizeof(char*));
  int argc_tosend = argc + more_argc;
  // send=false
  argv_tosend[0] = send ? "1" : "0"; ; 
  // msg_type: RaftResponse
  argv_tosend[1] = "5";
  // TODO: msg_id
  sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
  argv_tosend[2] = req_msg_id_sds;
  // resp: RAFT.AER
  argv_tosend[3] = "RAFT.REQUESTVOTEREPLY";
  // resp: dst_id
  argv_tosend[4] = sdsfromlonglong(dst_id);
  // resp: src_id
  argv_tosend[5] = sdsfromlonglong(src_id);
  // resp: message content, TODO: msg_id
  argv_tosend[6] = sdscatprintf(sdsempty(), "%d:%ld:%ld:%d", resp->prevote, resp->request_term, resp->term, resp->vote_granted);
  // others
  TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

  redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);

  sds replaced = DupAndReplaceSplitter(cmd, 0);
  printf("[%s] cmdRaftRequestVoteResponse: < %s >\n", send ? "send" : "recv", replaced);

  sdsfree((sds)argv_tosend[2]);
  sdsfree((sds)argv_tosend[4]);
  sdsfree((sds)argv_tosend[5]);
  sdsfree((sds)argv_tosend[6]);
  sdsfree(replaced);
  free(argv_tosend);

  return cmd;
}
