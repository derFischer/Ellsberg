#include "interpreter.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

bool is_set_or_get(RedisModuleString **rmargv, char **cargv, int argc, int index) {
    if (index >= argc) return false;
    const char* op = cargv == NULL ? RedisModule_StringPtrLen(rmargv[index], NULL) : cargv[index];
    return strcmp(op, "set") == 0 || strcmp(op, "get") == 0;
}

sds
InterpreteCmdRaft(raft_server_t *raft, bool send, size_t req_id, RedisModuleString **rmargv, char **cargv, int argc) {
    sds cmd;
    int more_argc = 3;
    if (is_set_or_get(rmargv, cargv, argc, 1)) {
        argc = 2;
    }
    const char **argv_tosend = (const char **) calloc(more_argc + argc, sizeof(char *));
    int argc_tosend = more_argc + argc;
    argv_tosend[0] = send ? "1" : "0";
    // msg_type: Client Request
    argv_tosend[1] = "1";
    sds req_id_sds = sdsfromlonglong(req_id);
    argv_tosend[2] = req_id_sds;
    // others
    TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);
    redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
    sdsfree(req_id_sds);
    free(argv_tosend);

    return cmd;
}

sds
InterpreteCmdRaftReply(raft_server_t *raft, bool send, size_t req_id, RaftRedisCommand *reqcmd, const char *reply_proto,
                       size_t reply_proto_len, bool success) {
    sds cmd = sdsempty();
    size_t argc_tosend = 3;
    const char **argv_tosend = (const char **) calloc(argc_tosend, sizeof(char *));
    argv_tosend[0] = send ? "1" : "0";
    // msg_type: Client Response
    argv_tosend[1] = success ? "2" : "3";
    sds req_id_sds = sdsfromlonglong(req_id);
    argv_tosend[2] = req_id_sds;

    sds reqcmd_sds = sdsempty();
    int used_argc = is_set_or_get(reqcmd->argv, NULL, reqcmd->argc, 0) ? 1 : reqcmd->argc;
    const char **cmd_argv = (const char **) calloc(used_argc, sizeof(char *));
    TranslateRMArgvOrUseCArgv(reqcmd->argv, NULL, used_argc, cmd_argv, /*from=*/0);
    redisFormatSdsCommandArgv(&reqcmd_sds, used_argc, cmd_argv, NULL);
    char *reqcmd_str_start = strstr(reqcmd_sds, "\r\n") + 2;

    /* The record carries the request command (only its name for set/get),
     * not the reply payload, so it has the same form for every reply type. */
    argc_tosend += used_argc;
    cmd = sdscatprintf(
            cmd,
            "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "%s",
            argc_tosend, 1L, argv_tosend[0], 1L, argv_tosend[1], strlen(argv_tosend[2]), argv_tosend[2],
            reqcmd_str_start);

    sdsfree(reqcmd_sds);
    free(cmd_argv);
    sdsfree(req_id_sds);
    free(argv_tosend);

    return cmd;
}

sds
InterpreteRaftNode(raft_server_t *raft, bool send, bool add, size_t req_id, RedisModuleString **rmargv, char **cargv,
                   int argc) {
    sds cmd;
    int more_argc = 3;
    const char **argv_tosend = (const char **) calloc(more_argc + argc, sizeof(char *));
    int argc_tosend = argc + more_argc;
    argv_tosend[0] = send ? "1" : "0";
    // msg_type: Client Request
    argv_tosend[1] = "1";
    sds req_id_sds = sdsfromlonglong(req_id);
    argv_tosend[2] = req_id_sds;
    // others
    TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);
    redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);
    sdsfree(req_id_sds);
    free(argv_tosend);

    return cmd;
}

sds InterpreteRaftNodeReply(raft_server_t *raft, bool send, bool add, size_t req_id, raft_node_id_t node_id,
                            const char *dbid, bool success) {
    sds cmd = sdsempty();
    const char *send_str = send ? "1" : "0";
    char *msg_type_str = "2";
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
                5L, 1L, send_str, 1L, msg_type_str, strlen(req_id_sds), req_id_sds, strlen(node_id_str), node_id_str,
                strlen(dbid), dbid);
    } else {
        cmd = sdscatprintf(
                cmd,
                "*%ld\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s\r\n" "$%ld\r\n%s",
                4L, 1L, send_str, 1L, msg_type_str, strlen(req_id_sds), req_id_sds, strlen(node_id_str), node_id_str);
    }
    sdsfree(req_id_sds);
    sdsfree(node_id_str);

    return cmd;
}

sds
InterpreteAE(raft_server_t *raft, bool send, raft_msg_id_t req_msg_id, sds joined_req_ids, RedisModuleString **rmargv,
             char **cargv, int argc, const size_t *argvlen, size_t *cmdlen) {
    sds cmd;
    int more_argc = 4;
    const char **argv_tosend = (const char **) calloc(more_argc + argc, sizeof(char *));
    size_t *argvlen_tosend = (size_t *) calloc(more_argc + argc, sizeof(size_t));
    int argc_tosend = argc + more_argc;
    argv_tosend[0] = send ? "1" : "0";
    // msg_type: RaftRequest
    argv_tosend[1] = "4";
    // msg_id
    sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
    argv_tosend[2] = req_msg_id_sds;
    // others
    TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, 3);
    argv_tosend[more_argc + argc - 1] = joined_req_ids;

    argvlen_tosend[0] = 1;
    argvlen_tosend[1] = 1;
    argvlen_tosend[2] = strlen(req_msg_id_sds);
    for (int j = 0; j < argc; j++) {
        argvlen_tosend[3 + j] = argvlen[j];
    }
    argvlen_tosend[more_argc + argc - 1] = strlen(joined_req_ids);

    redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, argvlen_tosend);

    sdsfree(req_msg_id_sds);
    free(argvlen_tosend);
    free(argv_tosend);
    *cmdlen = sdslen(cmd);

    return cmd;
}

sds InterpreteAER(
        raft_server_t *raft,
        bool send,
        raft_msg_id_t req_msg_id,
        raft_appendentries_resp_t *resp,
        raft_node_id_t dst_id,
        raft_node_id_t src_id,
        RedisModuleString **rmargv, char **cargv, int argc) {
    sds cmd;
    int more_argc = 7;
    const char **argv_tosend = (const char **) calloc(more_argc + argc, sizeof(char *));
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
    argv_tosend[6] = sdscatprintf(sdsempty(), "%ld:%d:%ld:%ld", resp->term, resp->success, resp->current_idx,
                                  resp->msg_id);
    // others
    TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

    redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);

    sdsfree((sds) argv_tosend[2]);
    sdsfree((sds) argv_tosend[4]);
    sdsfree((sds) argv_tosend[5]);
    sdsfree((sds) argv_tosend[6]);
    free(argv_tosend);

    return cmd;
}

sds InterpreteRVFormat(raft_server_t *raft, raft_node_t *raft_node, bool send, raft_msg_id_t req_msg_id,
                       raft_requestvote_req_t *msg) {
    char *ccmd;
    redisFormatCommand(&ccmd, "%d %d %d RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld",
                       send,
                       4, // msg_type: RaftRequest
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
    free(ccmd);

    return cmd;
}

sds InterpreteRVR(
        raft_server_t *raft,
        bool send,
        raft_msg_id_t req_msg_id,
        raft_requestvote_resp_t *resp,
        raft_node_id_t dst_id,
        raft_node_id_t src_id,
        RedisModuleString **rmargv, char **cargv, int argc) {
    sds cmd;
    int more_argc = 7;
    const char **argv_tosend = (const char **) calloc(more_argc + argc, sizeof(char *));
    int argc_tosend = argc + more_argc;
    // send=false
    argv_tosend[0] = send ? "1" : "0";
    // msg_type: RaftResponse
    argv_tosend[1] = "5";
    // TODO: msg_id
    sds req_msg_id_sds = sdsfromlonglong(req_msg_id);
    argv_tosend[2] = req_msg_id_sds;
    // resp: RAFT.REQUESTVOTEREPLY
    argv_tosend[3] = "RAFT.REQUESTVOTEREPLY";
    // resp: dst_id
    argv_tosend[4] = sdsfromlonglong(dst_id);
    // resp: src_id
    argv_tosend[5] = sdsfromlonglong(src_id);
    // resp: message content, TODO: msg_id
    argv_tosend[6] = sdscatprintf(sdsempty(), "%d:%ld:%ld:%d", resp->prevote, resp->request_term, resp->term,
                                  resp->vote_granted);
    // others
    TranslateRMArgvOrUseCArgv(rmargv, cargv, argc, argv_tosend, more_argc);

    redisFormatSdsCommandArgv(&cmd, argc_tosend, argv_tosend, NULL);

    sdsfree((sds) argv_tosend[2]);
    sdsfree((sds) argv_tosend[4]);
    sdsfree((sds) argv_tosend[5]);
    sdsfree((sds) argv_tosend[6]);
    free(argv_tosend);

    return cmd;
}
