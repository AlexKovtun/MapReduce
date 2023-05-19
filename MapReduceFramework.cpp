//
// Created by alexk on 14/05/2023.
//
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "ThreadContext.h"

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{

  auto *jb = new JobContext (client, inputVec, outputVec,
                             multiThreadLevel);
  jb->startThreads ();
  return static_cast<JobHandle>(jb);//TODO: is this the way static cast done?
}

void waitForJob (JobHandle job)
{
  auto job_context = static_cast<JobContext *> (job);
  job_context->JoinAllThreads();
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
  thread_context->job_context->count_reduced++;
  pthread_mutex_unlock (&thread_context->job_context->emit3_mutex);
}

void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  auto job_context = (JobContext *) job;
  delete job_context;
}

void getJobState(JobHandle job, JobState* state){
  auto job_context = static_cast<JobContext *> (job);
  state->stage =  job_context->job_state.stage;
  state->percentage = job_context->job_state.percentage;
}