//
// Created by alexk on 14/05/2023.
//
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "ThreadContext.h"

#define STAGE 62
#define TOTAL_PAIRS 31
#define RIGHT_MOST_31 0x7FFFFFFF
JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{

  auto *jb = new JobContext (client, inputVec, outputVec,
                             multiThreadLevel);
  jb->setStage (MAP_STAGE);
  jb->job_state.stage = MAP_STAGE;//TODO: should it be here?
  jb->startThreads ();
  return static_cast<JobHandle>(jb);//TODO: is this the way static cast done?
}

void waitForJob (JobHandle job)
{
  auto job_context = static_cast<JobContext *> (job);
  job_context->JoinAllThreads ();
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
  waitForJob (job);
  auto job_context = (JobContext *) job;
  delete job_context;
}

void getJobState (JobHandle job, JobState *state)
{
  auto job_context = static_cast<JobContext *> (job);
  pthread_mutex_lock (&job_context->job_state_mutex);

  state->stage = job_context->job_state.stage;
  state->percentage = job_context->job_state.percentage;
  uint64_t total = 0, already_processed = 0;
  if (*job_context->atomic_counter >> STAGE == MAP_STAGE)
    {
      total = job_context->input_vec.size ();
    }
  else
    {
      total = (*job_context->atomic_counter >> TOTAL_PAIRS) & RIGHT_MOST_31;
    }
  already_processed = *job_context->atomic_counter & RIGHT_MOST_31;
  //TODO: check division by zero
  state->percentage = ((float) already_processed / (float) total) * 100;
  state->stage = (stage_t) ((*job_context->atomic_counter )>> STAGE);
  pthread_mutex_unlock (&job_context->job_state_mutex);
}