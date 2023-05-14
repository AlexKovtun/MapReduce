//
// Created by alexk on 14/05/2023.
//

#ifndef _JOBCONTEXT_H_
#define _JOBCONTEXT_H_

#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <pthread.h>

/*
 * Inside MapReduceFramework.cpp you are encouraged to define JobContext – a srtuct
which includes all the parameters which are relevant to the job (e.g., the threads, state,
mutexes…). The pointer to this struct can be casted to JobHandle. You are encouraged to
use C++ static casting.
 */

class JobContext {
 public:
    JobContext(const MapReduceClient& client, const InputVec& inputVec,
               OutputVec& outputVec,int numOfThreads);

  void initThreads();

 private:
  int numOfThreads;
  const MapReduceClient &client;
  const InputVec &input_vec;
  OutputVec &output_vec;
  pthread_t *threads;
  JobState job_state;
};

#endif //_JOBCONTEXT_H_
