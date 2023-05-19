//
// Created by alexk on 14/05/2023.
//

#include "JobContext.h"
#include "ThreadContext.h"

#define SHUFFLE_THREAD 0
#define OUT_OF_PERCENTAGE 100
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
  //TODO: is Critical?

  /** MAP STAGE **/
  auto job_vec = thread_context->job_context->input_vec;
  while(*thread_context->job_context->next_to_process < job_vec.size ())
    {
      pthread_mutex_lock (map_mutex);
      if(*thread_context->job_context->next_to_process >= job_vec.size()){
          pthread_mutex_unlock (map_mutex);
          break;
      }

      uint64_t old_value =  *thread_context->job_context->next_to_process; //wrap into fucntion because we will get wierd number to process
      *thread_context->job_context->next_to_process;
      (*thread_context->job_context->next_to_process)++;

//  //TODO: is Critical?
//  thread_context->job_context->job_state.percentage =
//      (float) old_value / (float) job_vec.size () * OUT_OF_PERCENTAGE;

      //thread_context->job_context->job_state.stage = MAP_STAGE; //TODO: shouldn't be here according to race condition
      if (old_value < job_vec.size ())
        {
          auto next_to_process = job_vec[old_value];
          thread_context->job_context->client.map (next_to_process.first,
                                                   next_to_process.second,
                                                   threadContext);
        }
    }
  /** END MAP STAGE **/

  /** SORT STAGE **/
  auto thread_vec = thread_context->vec;
  std::sort (thread_vec.begin (), thread_vec.end ());
  /** END SORT STAGE **/


  /** SHUFFLE STAGE **/
  thread_context->job_context->barrier.barrier ();
  thread_context->job_context->job_state.stage = SHUFFLE_STAGE;
  thread_context->job_context->job_state.percentage = 0;
  if (thread_context->id == SHUFFLE_THREAD)
    {
      thread_context->job_context->shuffle ();
    }
  thread_context->job_context->barrier.barrier ();

  /** END SHUFFLE STAGE **/


  /** REDUCE STAGE **/
  pthread_mutex_lock (&thread_context->job_context->reduce_mutex);
  thread_context->job_context->job_state.stage = REDUCE_STAGE;
  auto vec = thread_context->job_context->shuffle_vec.back ();
  if (vec.empty ())
    {
      pthread_mutex_unlock (&thread_context->job_context->reduce_mutex);
      return nullptr;
    }
  thread_context->job_context->shuffle_vec.pop_back ();
  pthread_mutex_unlock (&thread_context->job_context->reduce_mutex);
  thread_context->job_context->client.reduce (&vec, thread_context);
  return nullptr;
  /** END REDUCE STAGE **/
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
  count_reduced = new std::atomic<int> (0);
  reduce_mutex = PTHREAD_MUTEX_INITIALIZER;
  emit3_mutex = PTHREAD_MUTEX_INITIALIZER;
  alreadyWait = false;
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
  pthread_mutex_destroy (&reduce_mutex);
  delete next_to_process;

}

void JobContext::shuffle ()
{
  int counter = 0;
  total_size = 0;
  for (int i = 0; i < numOfThreads; ++i)
    {
      total_size += (int)threadContexts[i]->vec.size();
    }
  for (int i = 0; i < numOfThreads; ++i)
    {
      InsertVector (threadContexts[i]->vec, &counter);
    }

  for (auto &it: shuffle_map)
    {
      shuffle_vec.push_back (it.second);
    }
}

void JobContext::InsertVector (const IntermediateVec &vec, int *counter)
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
      ++(*counter);
      job_state.percentage = (float) *counter / (float) total_size;
    }
}

void JobContext::JoinAllThreads ()
{
  //TODO: is mutex needed here?
  if (!alreadyWait)
    {
      alreadyWait = true;
      for (int i = 0; i < numOfThreads; ++i)
        {

          pthread_join (threads[i], nullptr);
        }
    }
}



