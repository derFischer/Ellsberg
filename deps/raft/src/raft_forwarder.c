#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "raft_forwarder.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define LOG_ROOT "/tmp/log"
#define ROTATE_INTERVAL_SEC 1

static void fail(const char *what)
{
    fprintf(stderr, "forwarder: %s (errno=%d: %s)\n", what, errno, strerror(errno));
    exit(1);
}

static void format_now(char *buf, size_t size)
{
    time_t now = time(NULL);
    struct tm now_tm;
    localtime_r(&now, &now_tm);
    snprintf(buf, size, "%04d%02d%02d-%02d%02d%02d", now_tm.tm_year + 1900, now_tm.tm_mon + 1,
             now_tm.tm_mday, now_tm.tm_hour, now_tm.tm_min, now_tm.tm_sec);
}

/* Opens messages.<IID>.<time>.data and the matching .meta file. */
static void open_log_files(forwarder_t *fwdr)
{
    char format_time[32];
    format_now(format_time, sizeof(format_time));

    char path[256];
    snprintf(path, sizeof(path), "%s/messages.%s.%s.data", fwdr->log_dir, fwdr->iid, format_time);
    fwdr->file = fopen(path, "w");
    if (fwdr->file == NULL) {
        fail("cannot open data file");
    }
    snprintf(path, sizeof(path), "%s/messages.%s.%s.meta", fwdr->log_dir, fwdr->iid, format_time);
    fwdr->meta_file = fopen(path, "w");
    if (fwdr->meta_file == NULL) {
        fail("cannot open meta file");
    }
}

/* Closes the current data file and writes its meta file: the cumulative
 * message count when the file was opened and when it was closed.
 * Called with the mutex held. */
static void close_log_files(forwarder_t *fwdr)
{
    fclose(fwdr->file);
    fprintf(fwdr->meta_file, "%ld\n%ld\n", fwdr->prev_num_messages, fwdr->num_messages);
    fclose(fwdr->meta_file);
    fwdr->prev_num_messages = fwdr->num_messages;
}

/* FILE mode: start a new data/meta file pair every ROTATE_INTERVAL_SEC. */
static void *rotate_thread_main(void *arg)
{
    forwarder_t *fwdr = arg;
    while (1) {
        sleep(ROTATE_INTERVAL_SEC);
        pthread_mutex_lock(&fwdr->mutex);
        if (fwdr->closing) {
            pthread_mutex_unlock(&fwdr->mutex);
            break;
        }
        close_log_files(fwdr);
        open_log_files(fwdr);
        pthread_mutex_unlock(&fwdr->mutex);
    }
    return NULL;
}

static void make_dir(const char *path)
{
    if (mkdir(path, 0755) != 0 && errno != EEXIST) {
        fail("cannot create log directory");
    }
}

static void init_file_mode(forwarder_t *fwdr)
{
    char format_time[32];
    format_now(format_time, sizeof(format_time));
    make_dir(LOG_ROOT);
    snprintf(fwdr->log_dir, sizeof(fwdr->log_dir), "%s/log%s", LOG_ROOT, format_time);
    make_dir(fwdr->log_dir);

    open_log_files(fwdr);
    if (pthread_create(&fwdr->rotate_thread, NULL, rotate_thread_main, fwdr) != 0) {
        fail("cannot start log rotation thread");
    }
}

static void init_socket_mode(forwarder_t *fwdr)
{
    if (fwdr->CONNTYPE != SIMCONNTYPE_UNIX) {
        fprintf(stderr, "forwarder: unsupported SIMCONNTYPE (only UNIX is supported)\n");
        exit(1);
    }

    struct sockaddr_un client_addr = {.sun_family = AF_UNIX};
    struct sockaddr_un server_addr = {.sun_family = AF_UNIX};
    snprintf(client_addr.sun_path, sizeof(client_addr.sun_path), "/tmp/client.%s.sock", fwdr->iid);
    snprintf(server_addr.sun_path, sizeof(server_addr.sun_path), "/tmp/server.%s.sock", fwdr->iid);

    fwdr->conn = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fwdr->conn == -1) {
        fail("cannot create socket");
    }
    unlink(client_addr.sun_path);
    if (bind(fwdr->conn, (struct sockaddr *) &client_addr, sizeof(client_addr)) == -1) {
        fail("cannot bind socket");
    }
    if (connect(fwdr->conn, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1) {
        fail("cannot connect to simulator");
    }
    printf("Connected to simulator at %s\n", server_addr.sun_path);
}

forwarder_t *forwarder_new(void)
{
    const char *mode = getenv("SIMMODE");
    if (mode == NULL || strcmp(mode, "NONE") == 0) {
        return NULL;
    }

    forwarder_t *fwdr = calloc(1, sizeof(forwarder_t));
    if (fwdr == NULL) {
        fail("out of memory");
    }
    fwdr->conn = -1;

    if (strcmp(mode, "ASYNC") == 0) {
        fwdr->MODE = SIMMODE_ASYNC;
    } else if (strcmp(mode, "SYNC") == 0) {
        fwdr->MODE = SIMMODE_SYNC;
    } else if (strcmp(mode, "FILE") == 0) {
        fwdr->MODE = SIMMODE_FILE;
    } else {
        fprintf(stderr, "forwarder: unknown SIMMODE '%s'\n", mode);
        exit(1);
    }

    const char *conntype = getenv("SIMCONNTYPE");
    if (conntype == NULL || strcmp(conntype, "UNIX") == 0) {
        fwdr->CONNTYPE = SIMCONNTYPE_UNIX;
    } else if (strcmp(conntype, "TCP") == 0) {
        fwdr->CONNTYPE = SIMCONNTYPE_TCP;
    } else if (strcmp(conntype, "PIPE") == 0) {
        fwdr->CONNTYPE = SIMCONNTYPE_PIPE;
    } else {
        fprintf(stderr, "forwarder: unknown SIMCONNTYPE '%s'\n", conntype);
        exit(1);
    }

    const char *iid = getenv("IID");
    if (iid == NULL || iid[0] == '\0') {
        fprintf(stderr, "forwarder: IID must be set when SIMMODE is %s\n", mode);
        exit(1);
    }
    snprintf(fwdr->iid, sizeof(fwdr->iid), "%s", iid);

    printf("RPCConnection Setup: MODE=%d, CONNTYPE=%d\n", fwdr->MODE, fwdr->CONNTYPE);
    pthread_mutex_init(&fwdr->mutex, NULL);

    if (fwdr->MODE == SIMMODE_FILE) {
        init_file_mode(fwdr);
    } else {
        init_socket_mode(fwdr);
    }
    return fwdr;
}

static void send_all(int fd, const void *buf, size_t len)
{
    size_t pos = 0;
    while (pos < len) {
        ssize_t rc = send(fd, (const char *) buf + pos, len - pos, 0);
        if (rc == -1) {
            if (errno == EINTR) {
                continue;
            }
            fail("send to simulator failed");
        }
        pos += (size_t) rc;
    }
}

static void write_all(FILE *file, const void *buf, size_t len)
{
    if (fwrite(buf, 1, len, file) != len) {
        fail("write to log file failed");
    }
}

/* Each record is an 8-byte length followed by the message. */
void forwarder_send(forwarder_t *fwdr, const void *buf, size_t len)
{
    uint64_t len64 = len;

    pthread_mutex_lock(&fwdr->mutex);
    if (fwdr->MODE == SIMMODE_FILE) {
        write_all(fwdr->file, &len64, sizeof(len64));
        write_all(fwdr->file, buf, len);
        fwdr->num_messages++;
    } else {
        send_all(fwdr->conn, &len64, sizeof(len64));
        send_all(fwdr->conn, buf, len);
    }
    pthread_mutex_unlock(&fwdr->mutex);
}

void forwarder_close(forwarder_t *fwdr)
{
    if (fwdr == NULL) {
        return;
    }

    if (fwdr->MODE == SIMMODE_FILE) {
        pthread_mutex_lock(&fwdr->mutex);
        fwdr->closing = 1;
        close_log_files(fwdr);
        pthread_mutex_unlock(&fwdr->mutex);
        pthread_join(fwdr->rotate_thread, NULL);
    } else if (fwdr->conn != -1) {
        close(fwdr->conn);
    }

    pthread_mutex_destroy(&fwdr->mutex);
    free(fwdr);
}
