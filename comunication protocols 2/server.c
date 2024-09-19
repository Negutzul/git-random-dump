#include <math.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#define max(a, b) (((a) > (b)) ? (a) : (b))

#define BUFFER_LENGTH 1024

#define SIGN_STRING_LENGTH 3
#define FLOAT_OFFSET 5

#define TOPIC_LENGTH 50
#define CONTENT_LENGTH 1501

#define TCP_MESSAGE_LENGTH 1600

#define INT_TYPE 0
#define SHORT_REAL_TYPE 1
#define FLOAT_TYPE 2
#define STRING_TYPE 3

typedef struct {
  char topic[TOPIC_LENGTH];
  uint8_t dataType;
  char content[CONTENT_LENGTH];
} UdpPacket;

typedef struct {
  char message[TCP_MESSAGE_LENGTH];
} TcpPacket;

// used for a linked list of topics
struct TopicNode {
    char topic[TOPIC_LENGTH];
    struct TopicNode* next;
};

// tcp client structure, holding its id, socketFd and list of topics
typedef struct {
    char id[11];
    int socketFd;
    struct TopicNode* topics;
} TcpClient;

// used for a linked list of clients
struct ClientNode {
    TcpClient* client;
    struct ClientNode* next;
};

// check a condition and exit the program if it is invalid
void checkCondition(int condition, char* message) {
    if (!condition) {
        perror(message);
        exit(EXIT_FAILURE);
    }
}

// entry point in the application
int main(int argc, char** argv) {

    // disable buffering
    setvbuf(stdout, NULL, _IONBF, BUFSIZ);

    // check that the port is provided as a command-line argument
    if (argc != 2) {
        fprintf(stderr, "The server port is a mandatory parameter.\n");
        exit(EXIT_FAILURE);
    }

    // get the port from the command-line arguments and validate it
    int port = atoi(argv[1]);
    if (port < 1024 || port > 65535) {
        fprintf(stderr, "The port is invalid.\n");
        exit(EXIT_FAILURE);
    }

    // create the udp socket
    int udpSocketFd = socket(AF_INET, SOCK_DGRAM, 0);
    checkCondition(udpSocketFd >= 0, "socket");

    // create the tcp socket
    int tcpSocketFd = socket(AF_INET, SOCK_STREAM, 0);
    checkCondition(tcpSocketFd >= 0, "socket");

    // disable Nagle's algorithm
    int flag = 1;
    int callReturnValue = setsockopt(tcpSocketFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
    checkCondition(callReturnValue >= 0, "setsockopt");

    // create the sockaddr object with the address and port of the server
    struct sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
    serverAddress.sin_port = htons(port);    

    // bind the udp socket
    callReturnValue = bind(udpSocketFd, (struct sockaddr*) &serverAddress, sizeof(struct sockaddr));
    checkCondition(callReturnValue >= 0, "udp bind");

    // bind the tcp socket
    callReturnValue = bind(tcpSocketFd, (struct sockaddr*) &serverAddress, sizeof(struct sockaddr));
    checkCondition(callReturnValue >= 0, "tcp bind");

    // listen to the tcp socket
    callReturnValue = listen(tcpSocketFd, 128);
    checkCondition(callReturnValue >= 0, "listen");

    fd_set readFdSet;

    FD_ZERO(&readFdSet);

    // add the stdin, udp and tcp ports to the fd set
    FD_SET(STDIN_FILENO, &readFdSet);
    FD_SET(udpSocketFd, &readFdSet);
    FD_SET(tcpSocketFd, &readFdSet);

    int maxFd = tcpSocketFd + 1;

    // buffer used to copy the contents from stdin
    char buffer[BUFFER_LENGTH];

    // allocate memory for the udp packet
    UdpPacket* udpPacket = malloc(sizeof(UdpPacket));
    checkCondition(udpPacket != NULL, "malloc");

    // allocate memory for the tcp packet
    TcpPacket* tcpPacket = malloc(sizeof(TcpPacket));
    checkCondition(tcpPacket != NULL, "malloc");

    // linked list of clients
    struct ClientNode* clients = NULL;

    int running = 1;

    // the server runs until it receives "exit" from the keyboard
    while (running) {
        fd_set currentReadFdSet = readFdSet;

        // perform the select call
        callReturnValue = select(maxFd, &currentReadFdSet, NULL, NULL, NULL);
        checkCondition(callReturnValue >= 0, "select");

        // check which fd is set
        for (int fd = 0; fd < maxFd; ++fd) {
            if (FD_ISSET(fd, &currentReadFdSet)) {

                // if the stdin fd is set, then something has been written from the keyboard
                if (fd == STDIN_FILENO) {
                    // clear the buffer and read the contents from stdin into it
                    memset(buffer, 0, BUFFER_LENGTH);
                    fgets(buffer, BUFFER_LENGTH, stdin);
                    // remove the last character (usually a newline)
                    buffer[strlen(buffer) - 1] = 0;
                    // if it's "exit", then stop the while loop and close all sockets
                    if (strcmp(buffer, "exit") == 0) {
                        running = 0;
                        for (int socketFd = udpSocketFd; socketFd < maxFd; ++socketFd) {
                            close(socketFd);
                        }
                        break;
                    }
                }

                // if the udp socket fd is set, then we need to read a udp packet and process it
                else if (fd == udpSocketFd) {
                    // clear the udp packet
                    memset(udpPacket, 0, sizeof(UdpPacket));

                    // initialize the client address object
                    struct sockaddr_in clientAddress;
                    socklen_t clientAddressLength = sizeof(struct sockaddr_in);

                    // read the udp packet
                    callReturnValue = recvfrom(udpSocketFd, udpPacket, sizeof(UdpPacket), 0, (struct sockaddr*) &clientAddress, &clientAddressLength);
                    checkCondition(callReturnValue >= 0, "recvFrom");

                    // get the client ip and port
                    char* clientIP = inet_ntoa(clientAddress.sin_addr);
                    uint16_t clientPort = ntohs(clientAddress.sin_port);

                    // get the message from the tcp packet to update it based on the data type from the udp packet
                    char* tcpMessage = tcpPacket->message;

                    // check type from the udp packet
                    if (udpPacket->dataType == INT_TYPE) {

                        // first byte is the sign byte
                        char sign[SIGN_STRING_LENGTH] = " ";
                        if (*((uint8_t*) udpPacket->content)) {
                            sign[1] = '-';
                        }

                        // the next 4 bytes contain a uint32_t in network byte order
                        char* nextBytes = udpPacket->content + 1;
                        uint32_t number = ntohl(*((uint32_t*) nextBytes));

                        // write the corresponding message to the tcp packet
                        sprintf(tcpMessage, "%s:%d - %s - INT -%s%d\n", clientIP, clientPort, udpPacket->topic, sign, number);

                    } else if (udpPacket->dataType == SHORT_REAL_TYPE) {

                        // 2 bytes containing the modulo of a number multiplied with 100
                        uint16_t shortReal = ntohs(*((uint16_t*) udpPacket->content));
                        float number = shortReal * pow(10, -2);

                        // write the corresponding message to the tcp packet
                        sprintf(tcpMessage, "%s:%d - %s - SHORT_REAL - %g\n", clientIP, clientPort, udpPacket->topic, number);

                    } else if (udpPacket->dataType == FLOAT_TYPE) {

                        // first byte is the sign byte
                        char sign[SIGN_STRING_LENGTH] = " ";
                        if (*((uint8_t*) udpPacket->content)) {
                            sign[1] = '-';
                        }

                        // the next 4 bytes contain a uint32_t in network byte order
                        char *nextBytes = udpPacket->content + 1;
                        uint32_t integer = ntohl(*((uint32_t*) nextBytes));

                        // the next byte contains the negative power of 10
                        nextBytes = udpPacket->content + FLOAT_OFFSET;
                        uint8_t decimals = *((uint8_t*) nextBytes);
                        float number = integer * pow(10, -decimals);

                        // write the corresponding message to the tcp packet
                        sprintf(tcpMessage, "%s:%d - %s - FLOAT -%s%f\n", clientIP, clientPort, udpPacket->topic, sign, number);

                    } else if (udpPacket->dataType == STRING_TYPE) {

                        // write the corresponding message to the tcp packet
                        sprintf(tcpMessage, "%s:%d - %s - STRING - %s\n", clientIP, clientPort, udpPacket->topic, udpPacket->content);

                    }

                    // now we need to send this message to all the clients
                    // which are subscribed to the topic of the udp packet

                    // go through each of the connected clients
                    struct ClientNode* currentClientNode = clients;
                    while (currentClientNode != NULL) {
                        TcpClient* client = currentClientNode->client;

                        // and to those who are subscribed to the topic from
                        // the udp packet, send a tcp packet with the message
                        struct TopicNode* topics = client->topics;                        
                        while (topics != NULL) {
                            if (strcmp(udpPacket->topic, topics->topic) == 0) {
                                callReturnValue = send(client->socketFd, tcpPacket, sizeof(TcpPacket), 0);
                                checkCondition(callReturnValue >= 0, "send");
                                break;
                            }
                            topics = topics->next;                   
                        }

                        currentClientNode = currentClientNode->next;
                    }

                }

                // if the tcp socket fd is set
                else if (fd == tcpSocketFd) {
                    // initialize the client address object
                    struct sockaddr_in clientAddress;
                    socklen_t clientAddressLength = sizeof(struct sockaddr_in);

                    // accept the connection from the client
                    int newClientFd = accept(fd, (struct sockaddr*) &clientAddress, &clientAddressLength);
                    checkCondition(callReturnValue >= 0, "accept");

                    // disable Nagle's algorithm
                    flag = 1;
                    callReturnValue = setsockopt(newClientFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
                    checkCondition(callReturnValue >= 0, "setsockopt");

                    // read the tcp packet, which should contain the id of the client
                    memset(tcpPacket, 0, sizeof(TcpPacket));
                    callReturnValue = recv(newClientFd, tcpPacket, sizeof(TcpPacket), 0);
                    checkCondition(callReturnValue >= 0, "recv");

                    // check if the id of the client already exists
                    int alreadyConnected = 0;

                    struct ClientNode* currentClientNode = clients;
                    while (currentClientNode != NULL) {
                        if (strcmp(currentClientNode->client->id, tcpPacket->message) == 0) {
                            alreadyConnected = 1;
                            break;
                        }
                        currentClientNode = currentClientNode->next;
                    }

                    // if a client is already connected with the same id, close its tcp connection
                    if (alreadyConnected) {
                        close(newClientFd);
                        printf("Client %s already connected.\n", tcpPacket->message);
                    } 
                    
                    // otherwise, add the new client to the list
                    else {
                        // update the fd set and max fd
                        FD_SET(newClientFd, &readFdSet);
                        maxFd = max(maxFd, newClientFd + 1);

                        // add the client to the list of clients:

                        // create a tcp client
                        TcpClient* newClient = (TcpClient*) malloc(sizeof(TcpClient));
                        checkCondition(newClient != NULL, "malloc");

                        // set its values (id, fd, topics)
                        strcpy(newClient->id, tcpPacket->message);
                        newClient->socketFd = newClientFd;
                        newClient->topics = NULL;

                        // create a client node
                        struct ClientNode* clientNode = (struct ClientNode*) malloc(sizeof(struct ClientNode));
                        checkCondition(clientNode != NULL, "malloc");

                        // add it as the head of the linked list
                        clientNode->client = newClient;
                        clientNode->next = clients;
                        clients = clientNode;

                        // print the message that the client has connected
                        printf("New client %s connected from %s:%d.\n", tcpPacket->message, inet_ntoa(clientAddress.sin_addr), ntohs(clientAddress.sin_port));
                    }

                }

                // if any other fd is set (we received data from a tcp client)
                else {
                    // clear the contents of the tcp packet
                    memset(tcpPacket, 0, sizeof(TcpPacket));

                    // read the data from the client
                    callReturnValue = recv(fd, tcpPacket, sizeof(TcpPacket), 0);
                    checkCondition(callReturnValue >= 0, "recv");

                    // if the client closed the tcp connection
                    if (callReturnValue == 0) {

                        struct ClientNode* currentClientNode = clients;
                        while (currentClientNode != NULL) {
                            TcpClient* client = currentClientNode->client;
                            if (client->socketFd == fd) {
                                close(fd);
                                FD_CLR(fd, &readFdSet);
                                printf("Client %s disconnected.\n", client->id);
                                break;
                            }

                            currentClientNode = currentClientNode->next;
                        }

                        // remove the client from the list of clients (free all the memory used by the node - for client and topics)
                        struct ClientNode* head = clients;

                        if (head != NULL && head->client->socketFd == fd) {
                            clients = head->next;

                            struct TopicNode* currentTopicNode = head->client->topics;
                            while (currentTopicNode != NULL) {
                                struct TopicNode* nextTopicNode = currentTopicNode->next;
                                free(currentTopicNode);
                                currentTopicNode = nextTopicNode;
                            }

                            free(head->client);
                            free(head);
                        } else {
                            struct ClientNode* prev = head;
                            while (head != NULL && head->client->socketFd != fd) {
                                prev = head;
                                head = head->next;
                            }

                            if (head != NULL) {
                                prev->next = head->next;

                                struct TopicNode* currentTopicNode = head->client->topics;
                                while (currentTopicNode != NULL) {
                                    struct TopicNode* nextTopicNode = currentTopicNode->next;
                                    free(currentTopicNode);
                                    currentTopicNode = nextTopicNode;
                                }

                                free(head->client);
                                free(head);
                            }
                        }

                    }
                    
                    // otherwise, it has sent some data
                    else {
                        // remove the last character (usually a newline)
                        tcpPacket->message[strlen(tcpPacket->message) - 1] = 0;

                        char command[256];
                        char topic[TOPIC_LENGTH];
                        // int sf;

                        // split the tokens from the tcp message
                        // into command, topic, sf
                        int counter = 0;
                        char* token = strtok(tcpPacket->message, " ");
                        while (token != NULL) {
                            if (counter == 0) {
                                strcpy(command, token);
                            } else if (counter == 1) {
                                strcpy(topic, token);
                            } else if (counter == 2) {
                                // sf = atoi(token);
                            }

                            token = strtok(NULL, " ");
                            ++counter;
                        }

                        if (strcmp(command, "subscribe") == 0) {
                            // add topic to client
                            struct ClientNode* currentClientNode = clients;
                            while (currentClientNode != NULL) {
                                // find the client with the current fd
                                TcpClient* client = currentClientNode->client;
                                if (client->socketFd == fd) {
                                    // create a new topic node
                                    struct TopicNode* topicNode = (struct TopicNode*) malloc(sizeof(struct TopicNode));
                                    checkCondition(topicNode != NULL, "malloc");

                                    // set its values and make it the head of the
                                    // list of topic nodes of the client
                                    strcpy(topicNode->topic, topic);
                                    topicNode->next = client->topics;
                                    client->topics = topicNode;

                                    break;
                                }

                                currentClientNode = currentClientNode->next;
                            }
                        } else if (strcmp(command, "unsubscribe") == 0) {
                            // remove topic from client
                            struct ClientNode* currentClientNode = clients;
                            while (currentClientNode != NULL) {
                                // find the client with the current fd
                                TcpClient* client = currentClientNode->client;
                                if (client->socketFd == fd) {
                                    // remove the topic from the client topic list
                                    struct TopicNode* head = client->topics;

                                    if (head != NULL && strcmp(head->topic, topic) == 0) {
                                        client->topics = head->next;
                                        free(head);
                                    } else {
                                        struct TopicNode* prev = head;
                                        while (head != NULL && strcmp(head->topic, topic) != 0) {
                                            prev = head;
                                            head = head->next;
                                        }

                                        if (head != NULL) {
                                            prev->next = head->next;
                                            free(head);
                                        }
                                    }

                                    break;
                                }

                                currentClientNode = currentClientNode->next;
                            }
                        }
                    }

                }
            }
        }

    }

    // free the memory allocated for the udp and tcp packets
    free(udpPacket);
    free(tcpPacket);

    // free the memory allocated for the connected clients
    struct ClientNode* currentClientNode = clients;
    while (currentClientNode != NULL) {

        struct TopicNode* currentTopicNode = currentClientNode->client->topics;
        while (currentTopicNode != NULL) {
            struct TopicNode* nextTopicNode = currentTopicNode->next;
            free(currentTopicNode);
            currentTopicNode = nextTopicNode;
        }

        free(currentClientNode->client);

        struct ClientNode* nextClientNode = currentClientNode->next;
        free(currentClientNode);
        currentClientNode = nextClientNode;
    }

    return 0;

}