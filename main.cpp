#include <iostream>
#include "MapReduceFramework.h"
#include <cstdio>
#include <array>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <random>
#include <memory>



static const int REPEATS = 10000;
static const int DEADLOCK_REPEATS = 1000000;

static const int RANDOM_REPEATS = 2000;

pthread_mutex_t k2ResourcesMutex = PTHREAD_MUTEX_INITIALIZER;

class VString : public V1 {
public:
    VString(std::string content) : content(content) {}

    std::string content;
};

class KChar : public K2, public K3 {
public:
    KChar(char c) : c(c) {}

    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    char c;
};

class VCount : public V2, public V3 {
public:
    VCount(unsigned int count) : count(count) {}

    unsigned int count;
};


class CounterClient : public MapReduceClient {
public:
    mutable std::vector<std::unique_ptr<KChar>> resourcesK2;
    mutable std::vector<std::unique_ptr<VCount>> resourcesV2;

    InputVec inputVec;
    OutputVec outputVec;

    CounterClient() : resourcesK2(), resourcesV2(), inputVec(), outputVec()
    {}

    ~CounterClient()
    {
        for (auto& kvp: inputVec)
        {
            delete kvp.first;
            delete kvp.second;
        }
        for (auto& kvp: outputVec)
        {
            delete kvp.first;
            delete kvp.second;
        }
    }

    void map(const K1 *key, const V1 *value, void *context) const override {
        (void)key;
        std::array<unsigned int, 256> counts;
        counts.fill(0);
        for (const char &c : static_cast<const VString *>(value)->content) {
            counts[(unsigned char) c]++;
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar *k2 = new KChar(i);
            VCount *v2 = new VCount(counts[i]);
            pthread_mutex_lock(&k2ResourcesMutex);
            resourcesK2.emplace_back(k2);
            resourcesV2.emplace_back(v2);
            pthread_mutex_unlock(&k2ResourcesMutex);
            emit2(k2, v2, context);
        }
    }

    void reduce(const IntermediateVec* pairs, void* context) const override {
        if(pairs->empty()) {
            return;
        }

        const char c = static_cast<const KChar *>(pairs->back().first)->c;
        unsigned int count = 0;
        for (auto pair: *pairs) {
            count += static_cast<const VCount *>(pair.second)->count;
        }
        KChar *k3 = new KChar(c);
        VCount *v3 = new VCount(count);
        emit3(k3, v3, context);
    }
};


int main(int argc, char** argv)
{
  for (int i = 0; i < REPEATS; ++i)
    {
      std::cout << "repetition #" << i << std::endl;
      CounterClient client;
      auto s1 = new VString ("This string is full of characters");
      auto s2 = new VString ("Multithreading is awesome");
      auto s3 = new VString ("conditions are race bad");
      client.inputVec.push_back ({nullptr, s1});
      client.inputVec.push_back ({nullptr, s2});
      client.inputVec.push_back ({nullptr, s3});
      JobState state;
      JobState last_state = {UNDEFINED_STAGE, 0};
      JobHandle job = startMapReduceJob (client, client.inputVec, client.outputVec, 6);
      getJobState (job, &state);

      while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
        {
          if (last_state.stage != state.stage
              || last_state.percentage != state.percentage)
            {
              printf ("last stage %d, %d%% \n", last_state.stage, last_state.percentage);
              printf ("stage %d, %f%% \n", state.stage, state.percentage);
              if (state.percentage > 100 || state.percentage < 0)
                {
                  std::cout << "Invalid percentage(not in 0-100): "
                            << state.percentage
                            << ", encountered during stage " << state.stage
                            << ")";
                  exit (1);
                }
              if (last_state.stage == state.stage
                  && state.percentage < last_state.percentage)
                {
                  std::cout
                      << "Bad percentage(smaller than previous percentage at same stage): "
                      << state.percentage << "(" << last_state.percentage
                      << ")" << ", encountered during stage " << state.stage
                      << ")";
                  exit (1);
                }
              if (last_state.stage > state.stage)
                {
                  std::cout << "Bad stage " << state.stage
                            << " - smaller than previous stage, encountered with percentage "
                            << state.percentage;
                  exit (1);
                }
            }
          last_state = state;
          getJobState (job, &state);
        }
      printf ("Done!\n");

      closeJobHandle (job);

    }
  return 0;
}

