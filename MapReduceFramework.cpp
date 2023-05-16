//
// Created by alexk on 14/05/2023.
//
#include "MapReduceFramework.h"
#include "JobContext.h"
#include "ThreadContext.h"

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

  auto *jb = new JobContext(client, inputVec, outputVec,
                            multiThreadLevel);
  jb->startThreads ();
  return static_cast<JobHandle>(jb);//TODO: is this the way static cast done?
}


void emit2 (K2* key, V2* value, void* context){
  auto thread_context = (ThreadContext*) context;
  thread_context->vec.push_back (IntermediatePair(key,value));
}


void emit3 (K3* key, V3* value, void* context){}