//
// Created by alexk on 14/05/2023.
//

#include "JobContext.h"
JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec,
                        OutputVec &outputVec, int numOfThreads) :
    client (client), input_vec (inputVec),
    numOfThreads (numOfThreads), output_vec (outputVec)
{
  //threads init somehow?
  threads = new pthread_t[numOfThreads];//TODO: consider not allocating
  initThreads ();
  job_state.stage = UNDEFINED_STAGE;
  job_state.percentage = 0;
}
void JobContext::initThreads () const
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      //todo: create the threads,start them , init the relvant data, and method
      //pthread_create (threads + i, NULL, foo, contexts + i);
    }
}

/*
 * for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {&atomic_counter, &bad_counter};
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, foo, contexts + i);
    }
 */


