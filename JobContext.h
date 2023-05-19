//
// Created by alexk on 14/05/2023.
//

#ifndef _JOBCONTEXT_H_
#define _JOBCONTEXT_H_
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <map>

#include "Barrier.h"
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "ThreadContext.h"



/*
 * Inside MapReduceFramework.cpp you are encouraged to define JobContext – a srtuct
which includes all the parameters which are relevant to the job (e.g., the threads, state,
mutexes…). The pointer to this struct can be casted to JobHandle. You are encouraged to
use C++ static casting.
 */

struct JobContext {

    JobContext (const MapReduceClient &client, const InputVec &inputVec,
                OutputVec &outputVec, int numOfThreads);
    ~JobContext ();

    void startThreads ();
    void shuffleStage();
    void JoinAllThreads ();
    void InsertVector(const IntermediateVec &vec );

    int numOfThreads;
    const MapReduceClient &client;
    const InputVec &input_vec;
    OutputVec &output_vec;
    pthread_t *threads;
    JobState job_state;
    std::atomic<uint64_t> *atomic_counter;

    Barrier barrier;
    std::map<K2*, IntermediateVec> shuffle_map;
    std::vector<ThreadContext* > threadContexts;
    std::vector<IntermediateVec> shuffle_vec;

    pthread_mutex_t  reduce_mutex;
    pthread_mutex_t  emit3_mutex;
    pthread_mutex_t  map_mutex;
    pthread_mutex_t job_state_mutex;

    bool alreadyWait;

    void incCounter () const;
    void setStage (int stage);

};

#endif //_JOBCONTEXT_H_
