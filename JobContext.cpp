//
// Created by alexk on 14/05/2023.
//

#include "JobContext.h"
#include "ThreadContext.h"


/** ########################## START thread function #########################
 * This Function will run whole logic consists of 4 stages:
 *  1.map
 *  2.sort
 *  3.shuffle
 *  4.reduce
 *  **/

void* MapReduceLogic(void *threadContext){
  auto thread_context = (ThreadContext*) threadContext;
  //params->id
  //TODO : is Critical?
  auto vec = thread_context->job_context->input_vec;
  int old_value = *thread_context->job_context->next_to_process;
  (*thread_context->job_context->next_to_process)++;
  if(old_value < vec.size()){
      auto next_to_process = vec[old_value];
      thread_context->job_context->client.map (next_to_process.first,next_to_process.second,threadContext);
  }

  //sort()
  //shuffle()
  //reduce()
  return nullptr;
}

/** ########################## END thread function ##########################
 */


JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec,
                        OutputVec &outputVec, int numOfThreads) :
    numOfThreads (numOfThreads), client (client),
    input_vec (inputVec), output_vec (outputVec)
{
  //threads init somehow?
  threads = new pthread_t[numOfThreads];//TODO: consider not allocating
  job_state.stage = UNDEFINED_STAGE; //TODO:  threads coming initialized to this val?
  job_state.percentage = 0;
  next_to_process  = new std::atomic<int>(0);
}

void JobContext::startThreads ()
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      void *thread_context = new ThreadContext (i, this);
      // thread_context-> the data that each thread will have(passed by param)
      // basically it's the parameters the MapReduceLogic will get
      // MapReduceLogic-> will be the method that the thread will run
      if(pthread_create (threads + i, NULL,
                      &MapReduceLogic , thread_context)!= 0){
        //TODO: printf error
      }
    }
  job_state.stage = MAP_STAGE;
}

JobContext::~JobContext ()
{
  //TODO: delete Threads
  delete next_to_process;
}


/*
 * for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {&atomic_counter, &bad_counter};
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, foo, contexts + i);
    }
 */


