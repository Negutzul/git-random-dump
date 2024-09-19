#include <stdio.h>      /* printf, sprintf */
#include <stdlib.h>     /* exit, atoi, malloc, free */
#include <unistd.h>     /* read, write, close */
#include <string.h>     /* memcpy, memset */
#include <sys/socket.h> /* socket, connect */
#include <netinet/in.h> /* struct sockaddr_in, struct sockaddr */
#include <netdb.h>      /* struct hostent, gethostbyname */
#include <arpa/inet.h>
#include "helpers.h"
#include "requests.h"
#include "parson.h"


int main(int argc, char *argv[])
{   
	char *message = (char*) malloc(sizeof(char)*100);
    int sockfd;

	char str[100];
	char username[100];
	char password[100];
	char logged_username[100];
	char logged_password[100];
	char *token_jwt = (char*)malloc(sizeof(char) * 50);;
	char *cookie=NULL;
	int logged=0;
	int acces=0;
    while(1){
		printf( "Enter requests :");
		fgets(str, 100, stdin);

		printf( "\nYou entered: ");
		printf("%s\n",str );

		if(strcmp(str,"register\n")==0){
			printf( "username :");
			fgets(username, 100, stdin);
			printf( "password :");
			fgets(password, 100, stdin);
			username[strlen(username)-1]='\0';
			password[strlen(password)-1]='\0';

			JSON_Value *root_value = json_value_init_object();
    		JSON_Object *root_object = json_value_get_object(root_value);
    		
    		char *serialized_string = NULL;
		    
		    json_object_set_string(root_object, "username", username);
		    json_object_set_string(root_object, "password", password);
		    
		    serialized_string = json_serialize_to_string_pretty(root_value);

		    sockfd = open_connection("34.118.48.238", 8080, AF_INET, SOCK_STREAM, 0);
		    
			char** body_data = (char**)malloc(sizeof(char*) * 2);
			body_data[0] = (char*)malloc(sizeof(char) * 50);
			strcpy(body_data[0],serialized_string);
			message = compute_post_request("34.118.48.238", "/api/v1/tema/auth/register", "application/json", body_data, 1, NULL, 0);
			send_to_server(sockfd,message);
    		char *response=receive_from_server(sockfd);

    		
    		if(strstr(response,"error\":\"The username student is taken!")==NULL)
    			printf("The connection was successful\n");
    		else
    			printf("error:The username student is taken!\n");
		    close(sockfd);
		    json_free_serialized_string(serialized_string);
		    json_value_free(root_value);
		}
		else
		if(strcmp(str,"login\n")==0)
		{
			printf( "username :");
			fgets(username, 100, stdin);
			printf( "password :");
			fgets(password, 100, stdin);
			username[strlen(username)-1]='\0';
			password[strlen(password)-1]='\0';

			JSON_Value *root_value = json_value_init_object();
    		JSON_Object *root_object = json_value_get_object(root_value);
    		
    		char *serialized_string = NULL;
		    
		    json_object_set_string(root_object, "username", username);
		    json_object_set_string(root_object, "password", password);
		    
		    serialized_string = json_serialize_to_string_pretty(root_value);
		    sockfd = open_connection("34.118.48.238", 8080, AF_INET, SOCK_STREAM, 0);
		    
			char** body_data = (char**)malloc(sizeof(char*) * 2);
			body_data[0] = (char*)malloc(sizeof(char) * 500);
			strcpy(body_data[0],serialized_string);
			message = compute_post_request("34.118.48.238", "/api/v1/tema/auth/login", "application/json", body_data, 1, NULL, 0);
			//"error":"No account with this username!"
			send_to_server(sockfd,message);
    		char *response=receive_from_server(sockfd);
			cookie = strstr(response,"connect.sid=");

    		if(strstr(response,"error\":\"No account with this username!")==NULL) {
    			printf("login successful\n");
	    		const char s[2] = ";";
				char *token;
				/* get the first token */
				token = strtok(cookie, s);
				cookie=token;
			    strcpy(logged_username,username);
			    strcpy(logged_password,password);
			    logged=1;
			    token_jwt[0]='\0';
			    acces=0;
			}
    		else
    			printf("error:No account with this username!\n");
    		

		    close(sockfd);
		    json_free_serialized_string(serialized_string);
		    json_value_free(root_value);
			
			
		}
		else if(strcmp(str,"enter_library\n")==0)
		{
		    sockfd = open_connection("34.118.48.238", 8080, AF_INET, SOCK_STREAM, 0);
			if(logged==1){
				char** respect_format = (char**)malloc(sizeof(char*)*4);
				respect_format[0]=(char*)malloc(sizeof(char) * 50);
				respect_format[1]=(char*)malloc(sizeof(char) * 50);
				respect_format[2]=(char*)malloc(sizeof(char) * 50);

				strcpy(respect_format[0],cookie);
	 	   		strcpy(respect_format[1], "Path=/");
	    		strcpy(respect_format[2], "HttpOnly");
				message = compute_get_request("34.118.48.238", "/api/v1/tema/library/access",NULL, respect_format, 3,NULL);
				
				send_to_server(sockfd,message);
				char *response=receive_from_server(sockfd);
				if(strstr(response,"401 Unauthorized")==NULL)
				{
					printf("Acces is granted to the library\n");				
		    		const char z[2] = "\"";
					/* get the first token */
					char *token = strtok(response, z);
					for (int i = 0; i < 5; ++i)
						token = strtok(NULL, z);
					
					strcpy(token_jwt,token);
					acces=1;
			    }
			    else
			    	printf("Acces is not granted to the library\n");
			    close(sockfd);
			}
			else
				printf("No acount was logged in\n");

		}
		else if(strcmp(str,"get_books\n")==0)
		{
		    sockfd = open_connection("34.118.48.238", 8080, AF_INET, SOCK_STREAM, 0);
			if(logged==1){
				if(acces==1){
					char** respect_format = (char**)malloc(sizeof(char*)*4);
					respect_format[0]=(char*)malloc(sizeof(char) * 50);
					respect_format[1]=(char*)malloc(sizeof(char) * 50);
					respect_format[2]=(char*)malloc(sizeof(char) * 50);
					strcpy(respect_format[0],cookie);
			   		strcpy(respect_format[1], "Path=/");
					strcpy(respect_format[2], "HttpOnly");
					
					char** respect_format1 = (char**)malloc(sizeof(char*)*2);
					respect_format[0]=(char*)malloc(sizeof(char) * 50);
					strcpy(respect_format1[0],token_jwt);
					message = compute_get_request("34.118.48.238", "/api/v1/tema/library/books",NULL,respect_format ,3,respect_format1);

					printf("Am primit mesajul \n\n%s\n", message);
					send_to_server(sockfd,message);

					char *response=receive_from_server(sockfd);

					printf("Am primit raspunsu \n\n%s\n", response);

				    close(sockfd);
				}
				else
				printf("Check if you have acces\n");
			}
			else
				printf("No acount was logged in\n");

		}


    }



    return 0;
}