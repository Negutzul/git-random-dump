#include <string.h>
#include <stdio.h>

int main () {
	char str[80] = "asadasdasdasds";
	const char s[2] = " ";
	char *token;
	/* get the first token */
	char *topic;
	char *topic_msg;
	topic = strtok(str, s);
	printf( " %s\n", topic);
	topic_msg = strtok(NULL, "\0");
	printf( " %s\n", topic_msg);

	return(0);
}
