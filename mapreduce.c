#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include "mapreduce.h"
#include "hashmap.h"
#include <pthread.h>

#include <stdlib.h>
#include <stdbool.h>

pthread_mutex_t lock;

//contains key value
struct kv
{
    char *key;
    char *value;
};

//data for each partition
struct pData
{
    struct kv *elements;
    size_t num_elements;
    size_t size;
};

int keys = -1;
int size = 1000; //changed to num_reducers in MR_Run
struct pData *data; //allocated in MR_Run
int *valids; //Keeps track of partitions that have been initialized, allocated in MR_Run

Partitioner part;

//puts kv in intermediate data structure
void MR_Emit(char *key, char *value)
{

    //gets the partition number
    long int p = part(key, size);

    //if partition has been initialized
    if (valids[p] == 1)
    {

        pthread_mutex_lock(&lock);

        //set the next available slot in partition to key and value, increment num elements
        data[p].elements[data[p].num_elements].key = strdup(key);
        data[p].elements[data[p].num_elements++].value = strdup(value);

        //double size of element array if it is full
        if (data[p].num_elements == data[p].size)
        {

            data[p].size *= 2;
            struct kv *doubled = realloc(data[p].elements, data[p].size * sizeof(struct kv));
            if (doubled)
            {
                data[p].elements = doubled;
            }
            else
            {
                // realloc failed
            }
        }
        pthread_mutex_unlock(&lock);
    }
    else
    {

        pthread_mutex_lock(&lock);
        
        //initialize a partition
        data[p].num_elements = 1;
        data[p].size = 10;
        data[p].elements = malloc(10 * sizeof(struct kv));
        data[p].elements[0].key = strdup(key);
        data[p].elements[0].value = strdup(value);

        valids[p] = 1;

        pthread_mutex_unlock(&lock);
    }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

//args for reduce function
struct rargs
{
    char *key;
    Getter get_func;
    int partition_number;
    Reducer reduce;
};

//args for map function
struct margs
{
    Mapper map;
    char **file_name;
    int fn; //number of files for the mapper thread to read
};

void Run_Map(void *m)
{
    struct margs *mm = (struct margs *)m;
    for (int i = 0; i < (*mm).fn; i++)
    {
        (*mm).map((*mm).file_name[i]);
    }
    free(m);
}

int *next;
int *done; // not done to start

void Run_Reduce(void *r)
{

    struct rargs *rr = (struct rargs *)r;
    int p = rr->partition_number;

    //loops while there is there are still more kvs left to reduce in the given partition
    while (next[p] != -1)
    {
        rr->key = data[p].elements[next[p]].key;
        rr->reduce(rr->key, rr->get_func, p);
    }
    free(r);
}


char *get_next(char *key, int partition_number)
{

    int n = next[partition_number];

    //if the next index to check is not filled
    if (n == data[partition_number].num_elements)
    {
        next[partition_number] = -1;
        return NULL;
    }

    //if the next index has the key that is looked for
    if (strcmp(data[partition_number].elements[n].key, key) == 0)
    {
        return data[partition_number].elements[next[partition_number]++].value;
    }
    else
    {
        return NULL;
    }
    return NULL;
};

int cmpfunc(const void *p, const void *q)
{
    char *l = ((struct kv *)p)->key;
    char *r = ((struct kv *)q)->key;
    return strcmp(l, r);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Partitioner partition)
{

    data = malloc(num_reducers * sizeof(struct pData));
    size = num_reducers;
    next = malloc(num_reducers * sizeof(int));
    part = partition;

    //set all next indexes to 0, the start of each partition
    for (int i = 0; i < num_reducers; i++)
    {
        next[i] = 0;
    }

    valids = malloc(size * sizeof(int));

    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
    }

    //set every index to 0, meaning uninitiallized
    for (int i = 0; i < size; i++)
    {
        valids[i] = 0;
    }

    pthread_t *mthread = malloc(sizeof(pthread_t) * num_mappers);
    pthread_t *rthread = malloc(sizeof(pthread_t) * num_reducers);
    struct margs *m;

    int filesPer[num_mappers]; //array containing the number of files each mapper thread should read

    //if there are the amount of files is >= number of mapper threads, just given a file to every mapper until there are no more files
    if ((argc - 1) <= num_mappers)
    {

        for (int i = 0; i < num_mappers; i++)
        {
            if ((argc - 2) >= i)
            {
                filesPer[i] = 1;
            }
            else
            {
                filesPer[i] = 0;
            }
        }
    }
    //if there are more files than mappers, divide the files up as equally as possible
    else 
    {
        for (int i = 0; i < num_mappers; i++)
        {
            int count = ((argc - 1) / num_mappers);
            int leftover = ((argc - 1) % num_mappers);

            if (i >= num_mappers - leftover)
            {
                count++;
            }

            filesPer[i] = count;
        }
    }

    int nextFileIndex = 1; //the index of args that that has the next unused file

    for (int i = 0; i < num_mappers; i++)
    {

        m = malloc(sizeof(struct margs));
        (*m).map = map;
        (*m).file_name = malloc(filesPer[i] * sizeof(char *));

        //give the mapper thread the right amount of files to read
        for (int j = 0; j < filesPer[i]; j++)
        {
            (*m).file_name[j] = argv[nextFileIndex++];
        }

        (*m).fn = filesPer[i];

        pthread_create(&mthread[i], NULL, (void *)Run_Map, (void *)m);
    }

    // wait for mapping to conclude
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_join(mthread[i], NULL);
    }

    for (int i = 0; i < num_reducers; i++)
    {

        //if the partition for the thread is uninitialized, create the thread, but mark next[i] to -1 to make sure no work is done
        if (data[i].size == 0)
        {

            struct rargs *r;
            r = malloc(sizeof(struct rargs));

            r->get_func = get_next;
            r->key = "none";
            r->partition_number = i;
            r->reduce = reduce;

            next[i] = -1;

            pthread_create(&rthread[i], NULL, (void *)Run_Reduce, r); // wrapper function that loops
            continue;
        }

        struct rargs *r;
        r = malloc(sizeof(struct rargs));

        qsort(data[i].elements, data[i].num_elements, sizeof(struct kv), cmpfunc);

        r->get_func = get_next;
        r->key = data[i].elements[next[i]].key;
        r->partition_number = i;
        r->reduce = reduce;

        pthread_create(&rthread[i], NULL, (void *)Run_Reduce, r); // wrapper function that loops
    }

    // wait for reducing to conclude
    for (int i = 0; i < num_reducers; i++)
    {
        pthread_join(rthread[i], NULL);
    }

    //free initialized element arrays
    for (int i = 0; i < num_reducers; i++) {
        if (data[i].size > 0) {
            free(data[i].elements);
        }
    }

    free(mthread);
    free(rthread);
    free(data);
    free(next);
    free(valids);
}
