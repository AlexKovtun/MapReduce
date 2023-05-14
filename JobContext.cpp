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

void* MapReduceLogic(void *arg){

  //map()
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

}

void JobContext::initThreads ()
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      void *thread_context = new ThreadContext(i);
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


/*
 * for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {&atomic_counter, &bad_counter};
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, foo, contexts + i);
    }
 */


