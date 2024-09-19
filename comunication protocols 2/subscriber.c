#include <math.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#define BUFFER_LENGTH 1024

#define ID_LENGTH 11

#define TCP_MESSAGE_LENGTH 1600

typedef struct {
  char message[TCP_MESSAGE_LENGTH];
} TcpPacket;


void checkCondition(int condition, char* message) {
    if (!condition) {
        perror(message);
        exit(EXIT_FAILURE);
    }
}


int main(int argc, char** argv) {

    // disable buffering
    setvbuf(stdout, NULL, _IONBF, BUFSIZ);


    if (argc != 4) {
        fprintf(stderr, "The clients expects 3 parameters: clientId, serverAddress, serverPort.\n");
        exit(EXIT_FAILURE);
    }


    if (strlen(argv[1]) >= ID_LENGTH) {
        fprintf(stderr, "The ID should be at most 10 characters long.\n");
        exit(EXIT_FAILURE);
    }

    char clientId[ID_LENGTH];
    strcpy(clientId, argv[1]);

    // get the server address from the command-line 
    struct in_addr serverIp;
    int callReturnValue = inet_aton(argv[2], &serverIp);
    checkCondition(callReturnValue != 0, "inet_aton");

    // get the server port from the command-line 
    int serverPort = atoi(argv[3]);
    if (serverPort < 1024 || serverPort > 65535) {
        fprintf(stderr, "The port is invalid.\n");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = serverIp.s_addr;
    serverAddress.sin_port = htons(serverPort);

    // create the tcp socket
    int tcpSocketFd = socket(AF_INET, SOCK_STREAM, 0);
    checkCondition(tcpSocketFd >= 0, "socket");

    // disable Nagle's algorithm
    int flag = 1;
    callReturnValue = setsockopt(tcpSocketFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
    checkCondition(callReturnValue >= 0, "setsockopt");

    TcpPacket* tcpPacket = malloc(sizeof(TcpPacket));
    checkCondition(tcpPacket != NULL, "malloc");

    fd_set readFdSet;

    FD_ZERO(&readFdSet);

    // add the stdin, udp and tcp ports to the fd set
    FD_SET(STDIN_FILENO, &readFdSet);
    FD_SET(tcpSocketFd, &readFdSet);

    int maxFd = tcpSocketFd + 1;

    // connect to the server
    callReturnValue = connect(tcpSocketFd, (struct sockaddr*) &serverAddress, sizeof(struct sockaddr));
    checkCondition(callReturnValue >= 0, "connect");

    memset(tcpPacket, 0, sizeof(TcpPacket));

    // copy the client id into the tcp packet
    memcpy(tcpPacket->message, clientId, strlen(clientId));

    callReturnValue = send(tcpSocketFd, tcpPacket, sizeof(TcpPacket), 0);
    checkCondition(callReturnValue >= 0, "send");

    char buffer[BUFFER_LENGTH];

    int running = 1;

    // the client runs until it receives "exit" from the keyboard
    while (running) {
        fd_set currentReadFdSet = readFdSet;

        callReturnValue = select(maxFd, &currentReadFdSet, NULL, NULL, NULL);
        checkCondition(callReturnValue >= 0, "select");

        // check which fd is set
        for (int fd = 0; fd < maxFd; ++fd) {

            if (FD_ISSET(fd, &currentReadFdSet)) {

                // if the stdin fd is set, then something has been written at the keyboard
                if (fd == STDIN_FILENO) {
                    // clear the buffer and read the content into it
                    memset(buffer, 0, BUFFER_LENGTH);
                    fgets(buffer, BUFFER_LENGTH, stdin);

                     buffer[strlen(buffer) - 1] = 0;

                    // if it's "exit" stop the while loop
                    if (strcmp(buffer, "exit") == 0) {
                        running = 0;
                        break;
                    }

                    // if it's not exit, then we need to send the message to the server
                    strcpy(tcpPacket->message, buffer);

                    char command[256];


                    char* token = strtok(buffer, " ");
                    if (token != NULL) {
                        strcpy(command, token);
                    }

                    // send the message to the server
                    callReturnValue = send(tcpSocketFd, tcpPacket, sizeof(TcpPacket), 0);
                    checkCondition(callReturnValue >= 0, "send");

                    // print the corresponding message
                    if (strcmp(command, "subscribe") == 0) {
                        printf("Subscribed to topic.\n");        
                    } else if (strcmp(command, "unsubscribe") == 0) {
                        printf("Unsubscribed from topic.\n");
                    }

                }

                // if we received data from the server
                else if (fd == tcpSocketFd) {
                    // clear the tcp packet
                    memset(tcpPacket, 0, sizeof(TcpPacket));

                    // read the data from the server
                    callReturnValue = recv(tcpSocketFd, tcpPacket, sizeof(TcpPacket), 0);
                    checkCondition(callReturnValue >= 0, "recv");
                    
                    // if the server closed the socket, exit the loop
                    // (either the server closed, or there already is a client with the same id)
                    if (callReturnValue == 0) {
                        running = 0;
                        break;
                    }

                    // else, if there is data in the tcp packet, it comes
                    // from a udp client on a topic that we are subscribed to, 
                    // so we print it
                    printf("%s", tcpPacket->message);
                }

            }

        }
    }

    // free the memory used by the tcp packet
    free(tcpPacket);

    // close the tcp socket
    close(tcpSocketFd);

    return 0;
}
