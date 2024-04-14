#include <pthread.h>
#include <mysql/mysql.h>
#include <stdio.h>
#include <fstream>
#include <vector>
#include <string>

#define NUM_THREADS 1000

struct thread_data {
    long thread_id;
    std::string query;
};

void *run_query(void *threadarg) {
    if (threadarg == NULL) {
        fprintf(stderr, "threadarg is NULL\n");
        return 0;
    }

    struct thread_data *my_data;
    my_data = (struct thread_data *) threadarg;
    long tid = my_data->thread_id;
    std::string query = my_data->query;

    MYSQL *con = mysql_init(NULL);

    if (con == NULL) {
        fprintf(stderr, "Failed to initialize MYSQL object\n");
        return 0;
    }

    if (mysql_real_connect(con, "localhost", "root", "", 
          "employees", 0, NULL, 0) == NULL) {
        fprintf(stderr, "%s\n", mysql_error(con));
        mysql_close(con);
        return 0;
    }

    if (mysql_query(con, query.c_str())) {
        fprintf(stderr, "%s\n", mysql_error(con));
        mysql_close(con);
        return 0;
    }

    mysql_close(con);
    pthread_exit(NULL);
} // This closing brace was missing

int main () {
    pthread_t threads[NUM_THREADS];
    struct thread_data thread_data_array[NUM_THREADS];
    int rc;
    long t;

    // Read queries from file
    std::ifstream file("queries.txt");
    std::vector<std::string> queries;
    std::string line;
    while (std::getline(file, line)) {
        queries.push_back(line);
    }
    file.close();

    for(t=0; t<NUM_THREADS; t++){
        printf("In main: creating thread %ld\n", t);
        thread_data_array[t].thread_id = t;
        thread_data_array[t].query = queries[t];
        rc = pthread_create(&threads[t], NULL, run_query, (void *)&thread_data_array[t]);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }

    for(t=0; t<NUM_THREADS; t++){
        pthread_join(threads[t], NULL);
    }

    pthread_exit(NULL);
}