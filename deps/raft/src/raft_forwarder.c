#include "raft_forwarder.h"

#define SERVER_PATH "/tmp/server.sock"
#define CLIENT_PATH "/tmp/client.sock"

void* file_reset_thread(void* arg) {
  forwarder_t *fwdr = (forwarder_t*)arg;
  while (1) {
    // new file every 1 minutes
    sleep(60);
    pthread_mutex_lock(fwdr->mutex);
    fflush(fwdr->file);
    fclose(fwdr->file);
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
    pthread_mutex_unlock(fwdr->mutex);
  }
}

forwarder_t *forwarder_new() {
  forwarder_t *fwdr = malloc(sizeof(forwarder_t));
  
  // parse the environment variables
  char* SIMMODE_STR = getenv("SIMMODE");
  if (SIMMODE_STR == NULL) {
    fwdr->MODE = SIMMODE_ASYNC;
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

  char *iid = getenv("IID");
  if (fwdr->MODE == SIMMODE_FILE) {
    fwdr->mutex = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(fwdr->mutex, NULL);
    time_t now = time(NULL);
		struct tm *now_tm = localtime(&now);
    char format_time[64];
		sprintf(format_time, "%04d%02d%02d-%02d%02d%02d", now_tm->tm_year+1900, now_tm->tm_mon+1, now_tm->tm_mday, now_tm->tm_hour, now_tm->tm_min, now_tm->tm_sec);
    sprintf(fwdr->log_dir, "/mydata/log%s", format_time);
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

void forwarder_send(forwarder_t *fwdr, const void *buf, size_t len) { 
  if (fwdr->MODE == SIMMODE_FILE) {
    pthread_mutex_lock(fwdr->mutex);
    fwrite((void*)(&len), 1, 8, fwdr->file);
    fwrite(buf, 1, len, fwdr->file);
    pthread_mutex_unlock(fwdr->mutex);
    return;
  } else {
    int rc, pos = 0;
    size_t n = 0;
    pos = 0;
    n = 8;
    while (1) {
      int rc = send(fwdr->out_conn, (void*)(&len)+pos, n-pos, 0);
      if (rc == -1) {
        printf("SEND ERROR 1 = %d\n", errno);
        close(fwdr->out_conn);
        exit(1);
      }
      pos += rc;
      if (pos == n) {
        break;
      }
    }
    pos = 0;
    n = len;
    while (1) {
      int rc = send(fwdr->out_conn, buf+pos, n-pos, 0);
      if (rc == -1) {
        printf("SEND ERROR 1 = %d\n", errno);
        close(fwdr->out_conn);
        exit(1);
      }
      pos += rc;
      if (pos == n) {
        break;
      }
    }
  }
}

void forwarder_recv(forwarder_t *fwdr, void *buf, size_t len) {
  printf("haven't check this function yet, maybe buggy, don't call!!\n");
  exit(1);
  memset(buf, 0, len);
  int rc = recv(fwdr->in_conn, buf, len, 0);
  if (rc == -1) {
    printf("RECV ERROR = %d\n", errno);
    close(fwdr->in_conn);
    exit(1);
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
