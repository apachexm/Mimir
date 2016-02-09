#ifndef STAT_H
#define STAT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <string>

namespace MAPREDUCE_NS {

class Stat{
public:
  Stat(int nmax=1024);
  ~Stat();

  int  init_counter(const char *, int verb=0);
  void inc_counter(int, int inc=1);
  void print_counters(int verb=0, FILE *out=stdout);

  int  init_timer(const char *, int verb=0);
  void inc_timer(int, double inc=0.0);
  void print_timers(int verb=0, FILE *out=stdout);

  void print(int verb=0, FILE *out=stdout);

  void clear();

public:
  int nmax;

  uint64_t *counters;
  int ncounter;
  int *counter_verb;
  std::vector<std::string> counter_str;

  double *timers;
  int ntimer;
  int *timer_verb;
  std::vector<std::string> timer_str;
};
}

#if GATHER_STAT
extern Stat st;
#endif

#endif
