//
// Created by alexk on 14/05/2023.
//

#include "JobContext.h"
#include "ThreadContext.h"

#define SHUFFLE_THREAD 0
#define TOTAL_PAIRS 31
#define STAGE 62
#define LEFT_MOST_31 0x7FFFFFFF

#define FAILED_CREATE_THREAD " failed create thread"

bool compareKey (IntermediatePair p1, IntermediatePair p2)
{
  return *p1.first < *p2.first;
}

/** ########################## START thread function #########################
 * This Function will run whole logic consists of 4 stages:
 *  1.map
 *  2.sort
 *  3.shuffleStage
 *  4.reduce
 *  **/

void *MapReduceLogic (void *threadContext)
{
  auto thread_context = (ThreadContext *) threadContext;

  /** MAP STAGE **/
  auto job_vec = thread_context->job_context->input_vec;
  while (thread_context->job_context->getCounter () < job_vec.size ())
    {
      pthread_mutex_lock (&thread_context->job_context->map_mutex);
      uint64_t old_value = thread_context->job_context->getCounter ();
      if (old_value >= job_vec.size ())
        {
          pthread_mutex_unlock (&thread_context->job_context->map_mutex);
          break;
        }

      auto next_to_process = job_vec[old_value];
      ++(*thread_context->job_context->atomic_counter);
      pthread_mutex_unlock (&thread_context->job_context->map_mutex);
      thread_context->job_context->client.map (next_to_process.first,
                                               next_to_process.second,
                                               threadContext);

      //(*thread_context->job_context->atomic_counter) += (uint64_t) 1 << 31;
    }
  /** END MAP STAGE **/

  /** SORT STAGE **/
  auto thread_vec = thread_context->vec;
  std::sort (thread_vec.begin (), thread_vec.end (), compareKey);
  /** END SORT STAGE **/

  /** SHUFFLE STAGE **/

  thread_context->job_context->barrier->barrier ();
  if (thread_context->id == SHUFFLE_THREAD)
    {
      thread_context->job_context->shuffleStage ();
    }
  thread_context->job_context->barrier->barrier ();

  /** END SHUFFLE STAGE **/


  /** REDUCE STAGE **/
  while (!thread_context->job_context->shuffle_vec.empty ())
    {
      pthread_mutex_lock (&thread_context->job_context->reduce_mutex);
      if (thread_context->job_context->shuffle_vec.empty ())
        {
          pthread_mutex_unlock (&thread_context->job_context->reduce_mutex);
          return nullptr;
        }
      auto vec = thread_context->job_context->shuffle_vec.back ();
      if (vec.empty ())
        {
          pthread_mutex_unlock (&thread_context->job_context->reduce_mutex);
          return nullptr;
        }
      thread_context->job_context->shuffle_vec.pop_back ();
      pthread_mutex_unlock (&thread_context->job_context->reduce_mutex);
      thread_context->job_context->client.reduce (&vec, thread_context);
      *thread_context->job_context->atomic_counter += vec.size ();
    }
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
    barrier (new Barrier (numOfThreads))
{
  //threads init somehow?
  threads = new pthread_t[numOfThreads];//TODO: consider not allocating
  job_state.stage = UNDEFINED_STAGE; //TODO:  threads coming initialized to this val?
  job_state.percentage = 0.0;
  atomic_counter = new std::atomic<uint64_t> (0);
  reduce_mutex = PTHREAD_MUTEX_INITIALIZER;
  emit3_mutex = PTHREAD_MUTEX_INITIALIZER;
  map_mutex = PTHREAD_MUTEX_INITIALIZER;
  job_state_mutex = PTHREAD_MUTEX_INITIALIZER;
  already_wait_mutex = PTHREAD_MUTEX_INITIALIZER;
  alreadyWait = false;
}

void JobContext::startThreads ()
{
  for (int i = 0; i < numOfThreads; ++i)
    {
      threadContexts.push_back (new ThreadContext (i, this));

      // thread_context-> the data that each thread will have(passed by param)
      // basically it's the parameters the MapReduceLogic will get
      // MapReduceLogic-> will be the method that the thread will run
      if (pthread_create (threads + i, NULL,
                          &MapReduceLogic, threadContexts[i]) != 0)
        {
          printf ("system error: %s\n", FAILED_CREATE_THREAD);
          exit (1);
        }
    }
}

JobContext::~JobContext ()
{
  delete[]threads;
  delete barrier;
  pthread_mutex_destroy (&reduce_mutex);
  pthread_mutex_destroy (&emit3_mutex);
  pthread_mutex_destroy (&map_mutex);
  pthread_mutex_destroy (&already_wait_mutex);
  pthread_mutex_destroy (&job_state_mutex);
  delete atomic_counter;
  for (auto &threadContext: threadContexts)
    {
      delete threadContext;
    }
  threadContexts.clear ();
}

void JobContext::shuffleStage ()
{
  uint64_t total_size = 0;
  for (int i = 0; i < numOfThreads; ++i)
    {
      total_size += (int) threadContexts[i]->vec.size ();
    }
//  *atomic_counter&= 0x3FFFFFFFFFFFFFFF;
//  *atomic_counter|= 2ULL << STAGE;
  *atomic_counter = total_size << TOTAL_PAIRS | (uint64_t) SHUFFLE_STAGE << STAGE;
  job_state.stage = SHUFFLE_STAGE;
  for (int i = 0; i < numOfThreads; ++i)
    {
      InsertVector (threadContexts[i]->vec);
    }

  for (auto &it: shuffle_map)
    {
      shuffle_vec.push_back (it.second);
    }
  *atomic_counter = total_size << TOTAL_PAIRS | (uint64_t) REDUCE_STAGE << 62;
  job_state.stage = REDUCE_STAGE;
  //std::cout<<*atomic_counter<<std::endl;
}

void JobContext::InsertVector (const IntermediateVec &vec)
{
  for (const auto &elem: vec)
    {
      bool isInVec = false;
      for (auto &pair: shuffle_map)
        {
          if (isKeysEqual (pair.second[0], elem))
            { //TODO: needed check if vec is empty?
              pair.second.push_back (elem);
              isInVec = true;
              break;
            }
        }
      if (!isInVec)
        {
          shuffle_map[elem.first].emplace_back (elem.first, elem.second);
        }
      ++(*atomic_counter);
    }
}

void JobContext::JoinAllThreads ()
{

}

void JobContext::incCounter () const
{
  ++(*atomic_counter);
}

void JobContext::setStage (int stage)
{
  //need to reset the flag
  (*atomic_counter) &= 0x3FFFFFFFFFFFFFFF;
  if (stage == MAP_STAGE)
    {
      (*atomic_counter) |= 1ULL << STAGE;
      //std::cout<<*atomic_counter<<std::endl;
      this->job_state.stage = MAP_STAGE;
    }
  else if (stage == SHUFFLE_STAGE)
    {
      (*atomic_counter) |= 2ULL << STAGE;
      //std::cout<<*atomic_counter<<std::endl;
      this->job_state.stage = SHUFFLE_STAGE;
    }
  else if (stage == REDUCE_STAGE)
    {
      (*atomic_counter) |= 3ULL << STAGE;
      //std::cout<<*atomic_counter<<std::endl;
      this->job_state.stage = REDUCE_STAGE;
    }
}

uint64_t JobContext::getCounter () const
{
  return *atomic_counter & (uint64_t) LEFT_MOST_31;
}

bool JobContext::isKeysEqual (IntermediatePair p1, IntermediatePair p2)
{
  return !(*p1.first < *p2.first || *p2.first < *p1.first);
}



