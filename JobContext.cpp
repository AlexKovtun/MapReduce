//
// Created by alexk on 14/05/2023.
//

#include "JobContext.h"
#include "ThreadContext.h"

#define SHUFFLE_THREAD 0
/** ########################## START thread function #########################
 * This Function will run whole logic consists of 4 stages:
 *  1.map
 *  2.sort
 *  3.shuffle
 *  4.reduce
 *  **/

void *MapReduceLogic (void *threadContext)
{
  auto thread_context = (ThreadContext *) threadContext;
  //params->id
  //TODO : is Critical?
  auto job_vec = thread_context->job_context->input_vec;
  int old_value = *thread_context->job_context->next_to_process;
  (*thread_context->job_context->next_to_process)++;
  if (old_value < job_vec.size ())
    {
      auto next_to_process = job_vec[old_value];
      thread_context->job_context->client.map (next_to_process.first,
                                               next_to_process.second,
                                               threadContext);
    }

  auto thread_vec = thread_context->vec;
  std::sort (thread_vec.begin (), thread_vec.end ());
  thread_context->job_context->barrier.barrier ();
  if (thread_context->id == SHUFFLE_THREAD)
    {
      thread_context->job_context->shuffle ();
    }
  thread_context->job_context->barrier.barrier ();
  //reduce();
  return nullptr;
}

/** ########################## END thread function ##########################
 */


JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec,
                        OutputVec &outputVec, int numOfThreads) :
    numOfThreads (numOfThreads), client (client),
    input_vec (inputVec), output_vec (outputVec),
    barrier (numOfThreads)
{
  //threads init somehow?
  threads = new pthread_t[numOfThreads];//TODO: consider not allocating
  job_state.stage = UNDEFINED_STAGE; //TODO:  threads coming initialized to this val?
  job_state.percentage = 0;
  next_to_process = new std::atomic<int> (0);
}

void JobContext::startThreads ()
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      //auto thread_context = new ThreadContext(i, this);
      threadContexts.push_back (new ThreadContext (i, this));

      // thread_context-> the data that each thread will have(passed by param)
      // basically it's the parameters the MapReduceLogic will get
      // MapReduceLogic-> will be the method that the thread will run
      if (pthread_create (threads + i, NULL,
                          &MapReduceLogic, threadContexts[i]) != 0)
        {
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

void JobContext::shuffle ()
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      InsertVector (threadContexts[i]->vec);
    }

  for (auto it = shuffle_map.begin (); it != shuffle_map.end (); ++it)
    {
      shuffle_vec.push_back (it->second);
    }
}

void JobContext::InsertVector (const IntermediateVec &vec)
{
  for (const auto &elem: vec)
    {
      if (shuffle_map.find (elem.first) == shuffle_map.end ())
        {
          //TODO: mutual resource same pointer/shallow copy
          IntermediateVec tmp = {{elem.first, elem.second}};
          shuffle_map[elem.first] = tmp;
        }
      else
        {
          shuffle_map[elem.first].push_back
              (IntermediatePair (elem.first, elem.second));
        }
    }
}





