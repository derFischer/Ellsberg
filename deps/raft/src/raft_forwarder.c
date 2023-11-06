#include "raft_forwarder.h"

#define SERVER_PATH "/tmp/server.sock"
#define CLIENT_PATH "/tmp/client.sock"

void* file_reset_thread(void* arg) {
  forwarder_t *fwdr = (forwarder_t*)arg;
  long interval = 30;
  long prev_num_messages = 0;
  while (1) {
    // new file every 1 minutes
    sleep(interval);
    pthread_mutex_lock(fwdr->mutex);
    fflush(fwdr->file);
    fclose(fwdr->file);
    fprintf(fwdr->meta_file, "%ld\n%ld\n", prev_num_messages, fwdr->num_messages);
    fclose(fwdr->meta_file);
    prev_num_messages = fwdr->num_messages;

    time_t now = time(NULL);
    struct tm *now_tm = localtime(&now);
    char format_time[64];
    sprintf(format_time, "%04d%02d%02d-%02d%02d%02d", now_tm->tm_year+1900, now_tm->tm_mon+1, now_tm->tm_mday, now_tm->tm_hour, now_tm->tm_min, now_tm->tm_sec);
    char log_path[256];
    sprintf(log_path, "%s/messages.%s.%s.data", fwdr->log_dir, getenv("IID"), format_time);
    fwdr->file = fopen(log_path, "w");
    if (fwdr->file == NULL) {
      printf("ERROR: Cannot open file %s for writing\n", log_path);
      exit(1);
    }
    char meta_path[256];
    sprintf(meta_path, "%s/messages.%s.%s.meta", fwdr->log_dir, getenv("IID"), format_time);
    fwdr->meta_file = fopen(meta_path, "w");
    if (fwdr->file == NULL) {
        printf("ERROR: Cannot open file %s for writing\n", log_path);
        exit(1);
    }
    pthread_mutex_unlock(fwdr->mutex);
    interval = 60;
  }
}

forwarder_t *forwarder_new(void) {
  forwarder_t *fwdr = malloc(sizeof(forwarder_t));
  fwdr->num_messages = 0;
  
  // parse the environment variables
  char* SIMMODE_STR = getenv("SIMMODE");
  if (SIMMODE_STR == NULL) {
    fwdr->MODE = SIMMODE_NONE;
  } else if (strcmp(SIMMODE_STR, "NONE") == 0) {
    goto free_and_return_null;
  } else if (strcmp(SIMMODE_STR, "ASYNC") == 0) {
    fwdr->MODE = SIMMODE_ASYNC;
  } else if (strcmp(SIMMODE_STR, "SYNC") == 0){
    fwdr->MODE = SIMMODE_SYNC;
  } else if (strcmp(SIMMODE_STR, "FILE") == 0) {
    fwdr->MODE = SIMMODE_FILE;
  }

  char* SIMCONNTYPE_STR = getenv("SIMCONNTYPE");
  if (SIMCONNTYPE_STR == NULL) {
    fwdr->CONNTYPE = SIMCONNTYPE_UNIX;
  } else if (strcmp(SIMCONNTYPE_STR, "TCP") == 0) {
    fwdr->CONNTYPE = SIMCONNTYPE_TCP;
  } else if (strcmp(SIMCONNTYPE_STR, "UNIX") == 0) {
    fwdr->CONNTYPE = SIMCONNTYPE_UNIX;
  } else if (strcmp(SIMCONNTYPE_STR, "PIPE") == 0) {
    fwdr->CONNTYPE = SIMCONNTYPE_PIPE;
  }

  printf("RPCConnection Setup: MODE=%d, CONNTYPE=%d\n", fwdr->MODE, fwdr->CONNTYPE);

  fwdr->mutex = malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(fwdr->mutex, NULL);

  char *iid = getenv("IID");
  if (fwdr->MODE == SIMMODE_FILE) {
    time_t now = time(NULL);
		struct tm *now_tm = localtime(&now);
    char format_time[64];
		sprintf(format_time, "%04d%02d%02d-%02d%02d%02d", now_tm->tm_year+1900, now_tm->tm_mon+1, now_tm->tm_mday, now_tm->tm_hour, now_tm->tm_min, now_tm->tm_sec);
    sprintf(fwdr->log_dir, "/tmp/log/log%s", format_time);
    int rc = mkdir(fwdr->log_dir, 0755);
    if (rc != 0) {
      printf("ERROR[%d]: Cannot create directory %s\n", errno, fwdr->log_dir);
      exit(1);
    }

    char log_path[256];
    sprintf(log_path, "%s/messages.%s.%s.data", fwdr->log_dir, iid, format_time);
    fwdr->file = fopen(log_path, "w");
    if (fwdr->file == NULL) {
      printf("ERROR: Cannot open file %s\n", log_path);
      exit(1);
    }
    char meta_path[256];
    sprintf(meta_path, "%s/messages.%s.%s.meta", fwdr->log_dir, getenv("IID"), format_time);
    fwdr->meta_file = fopen(meta_path, "w");
    if (fwdr->file == NULL) {
        printf("ERROR: Cannot open file %s for writing\n", log_path);
        exit(1);
    }
    
    // create a thread to reset the file every interval
    pthread_t thread;
    pthread_create(&thread, NULL, file_reset_thread, fwdr);
  } else {
    if (fwdr->CONNTYPE == SIMCONNTYPE_UNIX) {
      char server_sock_path[128] = "/tmp/server.sock.";
      char client_sock_path[128] = "/tmp/client.sock.";
      sprintf(server_sock_path, "/tmp/server.%s.sock", iid);
      sprintf(client_sock_path, "/tmp/client.%s.sock", iid);
      int rc, len;
      struct sockaddr_un server_sockaddr; 
      struct sockaddr_un client_sockaddr; 
      memset(&server_sockaddr, 0, sizeof(struct sockaddr_un));
      memset(&client_sockaddr, 0, sizeof(struct sockaddr_un));

      /* Create a UNIX domain stream socket */
      fwdr->out_conn = socket(AF_UNIX, SOCK_STREAM, 0);
      if (fwdr->out_conn == -1) {
        printf("SOCKET ERROR = %d\n", errno);
        exit(1);
        goto free_and_return_null;
      }

      /* Bind */
      client_sockaddr.sun_family = AF_UNIX;   
      strcpy(client_sockaddr.sun_path, client_sock_path); 
      len = sizeof(client_sockaddr);
      
      unlink(client_sock_path);
      rc = bind(fwdr->out_conn, (struct sockaddr *) &client_sockaddr, len);
      if (rc == -1){
        printf("BIND ERROR: %d\n", errno);
        close(fwdr->out_conn);
        exit(1);
        goto free_and_return_null;
      }

      /* Connect */
      server_sockaddr.sun_family = AF_UNIX;
      strcpy(server_sockaddr.sun_path, server_sock_path);
      rc = connect(fwdr->out_conn, (struct sockaddr *) &server_sockaddr, len);
      if(rc == -1){
        printf("CONNECT ERROR = %d\n", errno);
        close(fwdr->out_conn);
        exit(1);
        goto free_and_return_null;
      }

      fwdr->in_conn = fwdr->out_conn;

      printf("Connected to simulatior!\n");
    } else {
      printf("ERROR: Unsupported connection type: %s\n", getenv("SIMCONNTYPE"));
      exit(1);
      goto free_and_return_null;
    }
  }

  return fwdr;

free_and_return_null:
  free(fwdr);
  fwdr = NULL;
  return fwdr;
}

void send_until_all(int fd, const void *buf, size_t len) {
  ssize_t n = len;
  ssize_t pos = 0;
  while (1) {
    ssize_t rc = send(fd, buf+pos, n-pos, 0);
    if (rc == -1) {
      printf("send ERROR = %d\n", errno);
      close(fd);
      exit(1);
    }
    pos += rc;
    if (pos == n) {
      break;
    }
  }
}

void fwrite_until_all(FILE* fd, const void *buf, size_t len) {
  size_t n = len;
    size_t pos = 0;
  while (1) {
    size_t rc = fwrite(buf+pos, 1, n-pos, fd);
    if (rc == -1) {
      printf("fwrite ERROR = %d\n", errno);
      fclose(fd);
      exit(1);
    }
    pos += rc;
    if (pos == n) {
      break;
    }
  }
}

void forwarder_send(forwarder_t *fwdr, const void *buf, size_t len) {
  if (fwdr->MODE == SIMMODE_FILE) {
    pthread_mutex_lock(fwdr->mutex);
    fwrite_until_all(fwdr->file, (void*)(&len), 8);
    fwrite_until_all(fwdr->file, buf, len);
    fwdr->num_messages++;
    pthread_mutex_unlock(fwdr->mutex);
    return;
  } else {
    pthread_mutex_lock(fwdr->mutex);
    send_until_all(fwdr->out_conn, (void*)(&len), 8);
    send_until_all(fwdr->out_conn, buf, len);
    pthread_mutex_unlock(fwdr->mutex);
  }
}

void forwarder_close(forwarder_t *fwdr) {
  close(fwdr->out_conn);
  if (fwdr->in_conn != fwdr->out_conn) {
    close(fwdr->in_conn);
  }
  if (fwdr->mutex != NULL) {
    pthread_mutex_destroy(fwdr->mutex);
    free(fwdr->mutex);
  }
}
