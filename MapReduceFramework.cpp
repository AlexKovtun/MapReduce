//
// Created by alexk on 14/05/2023.
//
#include "MapReduceFramework.h"
#include "JobContext.h"

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

  auto *jb = new JobContext(client, inputVec, outputVec,
                            multiThreadLevel);
  return static_cast<JobHandle>(jb);//TODO: is this the way static cast done?
}
