#include <unistd.h>  
#include <stdio.h>  
int main()  
{  
        int i;  
	char buf[100];
        snprintf(buf, 100, "echo yangyazhou-test-Committing-suicide-by-killing-the-process-group::execute32-pid:%d,pgid:%d >> /yyz2.log", 
		getpid(), getpgid(0));
        printf("%s\r\n",buf);
        printf("\t pid \tppid \t pgid\n");  
        printf("parent:\t%d\t%d\t%d\n",getpid(),getppid(),getpgid(0));  
        for(i=0; i<2; i++)  
        {  
                if(fork()==0)  
                {  
                        printf("child:\t%d\t%d\t%d\n",getpid(),getppid(),getpgid(0));  
                }  
        }  
        sleep(500);  
        return 0;  
  
}  

