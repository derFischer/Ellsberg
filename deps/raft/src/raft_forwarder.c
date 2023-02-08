#include "raft_forwarder.h"

#define SERVER_PATH "/tmp/server.sock"
#define CLIENT_PATH "/tmp/client.sock"
#define DATA "Hello from client"

forwarder_t *forwarder_new(char *server_path, char *client_path) {
  forwarder_t *fwdr = malloc(sizeof(forwarder_t));
  int rc, len;
  struct sockaddr_un server_sockaddr; 
  struct sockaddr_un client_sockaddr; 
  memset(&server_sockaddr, 0, sizeof(struct sockaddr_un));
  memset(&client_sockaddr, 0, sizeof(struct sockaddr_un));

  /* Create a UNIX domain stream socket */
  fwdr->sock = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fwdr->sock == -1) {
    printf("SOCKET ERROR = %d\n", errno);
    exit(1);
    return NULL;
  }

  /* Bind */
  client_sockaddr.sun_family = AF_UNIX;   
  strcpy(client_sockaddr.sun_path, client_path); 
  len = sizeof(client_sockaddr);
  
  unlink(client_path);
  rc = bind(fwdr->sock, (struct sockaddr *) &client_sockaddr, len);
  if (rc == -1){
    printf("BIND ERROR: %d\n", errno);
    close(fwdr->sock);
    exit(1);
    return NULL;
  }

  /* Connect */
  server_sockaddr.sun_family = AF_UNIX;
  strcpy(server_sockaddr.sun_path, server_path);
  rc = connect(fwdr->sock, (struct sockaddr *) &server_sockaddr, len);
  if(rc == -1){
    printf("CONNECT ERROR = %d\n", errno);
    close(fwdr->sock);
    exit(1);
    return NULL;
  }

  printf("Connected to simulatior!\n");
  return fwdr;
}

void forwarder_send(forwarder_t *fwdr, const void *buf, int len) { 
  int rc = send(fwdr->sock, buf, len, 0);
  if (rc == -1) {
    printf("SEND ERROR = %d\n", errno);
    close(fwdr->sock);
    exit(1);
  }
}

void forwarder_recv(forwarder_t *fwdr, void *buf, int len) {
  memset(buf, 0, len);
  int rc = recv(fwdr->sock, buf, len, 0);
  if (rc == -1) {
    printf("RECV ERROR = %d\n", errno);
    close(fwdr->sock);
    exit(1);
  }
}

void forwarder_close(forwarder_t *fwdr) {
  close(fwdr->sock);
}
