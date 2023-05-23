//
// Created by alexk on 14/05/2023.
//
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "ThreadContext.h"

#define STAGE 62
#define TOTAL_PAIRS 31
#define RIGHT_MOST_31 0x7FFFFFFF
#define FAILED 1

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{

  auto *jb = new JobContext (client, inputVec, outputVec,
                             multiThreadLevel);
  *jb->atomic_counter = (uint64_t) 1 << STAGE;
  jb->job_state.stage = MAP_STAGE;
  jb->startThreads ();
  return static_cast<JobHandle>(jb);//TODO: is this the way static cast done?
}

void waitForJob (JobHandle job)
{
  auto job_context = static_cast<JobContext *> (job);
  pthread_mutex_lock (&job_context->already_wait_mutex);
  if (job_context->alreadyWait)
    {
      printf ("ERROR: Calling twice\n");
      pthread_mutex_unlock (&job_context->already_wait_mutex);
      return;
    }
  else
    {
      job_context->alreadyWait = true;
    }
  pthread_mutex_unlock (&job_context->already_wait_mutex);

  for (int i = 0; i < job_context->numOfThreads; ++i)
    {
      if (pthread_join (job_context->threads[i], nullptr) != 0)
        {
          printf ("system error: couldn't join thread\n");
          exit (FAILED);
        }
    }
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto thread_context = (ThreadContext *) context;
  thread_context->vec.push_back (IntermediatePair (key, value));
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto thread_context = (ThreadContext *) context;
  pthread_mutex_lock (&thread_context->job_context->emit3_mutex);
  thread_context->job_context->output_vec.push_back (OutputPair (key, value));
  pthread_mutex_unlock (&thread_context->job_context->emit3_mutex);
}

void closeJobHandle (JobHandle job)
{
  auto job_context = (JobContext *) job;
  if (!job_context->alreadyWait)
    {
      waitForJob (job);
    }
  delete job_context;
}

void getJobState (JobHandle job, JobState *state)
{
  auto job_context = static_cast<JobContext *> (job);
  pthread_mutex_lock (&job_context->job_state_mutex);
  uint64_t total, currentlyProcessed;
  uint64_t currentVal = *job_context->atomic_counter;
  if (currentVal >> STAGE == MAP_STAGE)
    {
      total = job_context->input_vec.size ();
    }
  else
    {
      total = ((currentVal >> TOTAL_PAIRS) & (RIGHT_MOST_31));
    }
  currentlyProcessed = currentVal & (RIGHT_MOST_31);
  if (total == 0)
    {
      printf ("Error : Division by 0\n");
      pthread_mutex_unlock (&job_context->job_state_mutex);
      return;
    }
  auto per = ((float) currentlyProcessed / (float) total) * 100;
  auto stage = (stage_t) (currentVal >> STAGE);
  job_context->job_state.percentage = per;
  job_context->job_state.stage = stage;
  state->percentage = per;
  state->stage = stage;
  pthread_mutex_unlock (&job_context->job_state_mutex);
}



