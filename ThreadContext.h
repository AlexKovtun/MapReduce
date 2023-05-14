//
// Created by alexk on 14/05/2023.
//

#ifndef _THREADCONTEXT_H_
#define _THREADCONTEXT_H_

struct ThreadContext {
  explicit ThreadContext(int id): id(id){}
  int id;
};

#endif //_THREADCONTEXT_H_
