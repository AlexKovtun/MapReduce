//
// Created by alexk on 14/05/2023.
//

#ifndef _THREADCONTEXT_H_
#define _THREADCONTEXT_H_
#include <vector>
#include "MapReduceClient.h"
#include "JobContext.h"

struct ThreadContext {
    explicit ThreadContext (int id, JobContext *p_context)
        : id (id), job_context(p_context)
    {}

    int id;
    IntermediateVec vec;
    JobContext *job_context;//TODO check reference or not
};

#endif //_THREADCONTEXT_H_
