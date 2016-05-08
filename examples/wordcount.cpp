#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>
#include <omp.h>

#include "mapreduce.h"
#include "config.h"

using namespace MAPREDUCE_NS;

#include "stat.h"

void map(MapReduce *mr, char *word, void *ptr);
void countword(MapReduce *, char *, int,  MultiValueIterator *, void*);

#define USE_LOCAL_DISK  0
void output(const char *filename, const char *outdir, \
  const char *prefix, MapReduce *mr);

int me, nprocs;
int commmode=0;
int inputsize=512;
int blocksize=512;
int gbufsize=8192;
int lbufsize=16;

uint64_t nword, nunique;
double t1, t2, t3;

int main(int argc, char *argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
  if (provided < MPI_THREAD_FUNNELED){
    fprintf(stderr, "MPI don't support multithread!");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if(argc <= 3){
    if(me == 0) printf("Syntax: wordcount filepath\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  const char *prefix = argv[2];
  const char *outdir = argv[3];

  char *filedir=argv[1];

  // copy files
#if USE_LOCAL_DISK
  char dir[100];
  sprintf(dir, "/tmp/mtmr_mpi.%d", me);

  char cmd[1024+1];
  sprintf(cmd, "mkdir %s", dir);
  system(cmd);
  sprintf(cmd, "cp -r %s %s", filedir, dir);
  system(cmd);

  filedir=dir;
#endif

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

#if 1
  mr->set_threadbufsize(lbufsize);
  mr->set_sendbufsize(gbufsize);
  mr->set_blocksize(blocksize);
  mr->set_inputsize(inputsize);
  mr->set_maxmem(32);
  mr->set_commmode(commmode);
#endif

  mr->set_outofcore(0);

  MPI_Barrier(MPI_COMM_WORLD);

  t1 = MPI_Wtime();

  char whitespace[20] = " \n";
  nword = mr->map(filedir, 1, 1, whitespace, map, NULL);

  t2 = MPI_Wtime();

  nunique = mr->reduce(countword, 1, NULL);

  t3 = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  output("mtmr.wc", outdir, prefix, mr);
 
  delete mr;

  // clear files
#if USE_LOCAL_DISK 
  sprintf(cmd, "rm -rf %s", dir);
  system(cmd);
#endif

  MPI_Finalize();
}

void map(MapReduce *mr, char *word, void *ptr){
  int len=strlen(word)+1;
  char one[10]={"1"};

  if(len <= 8192)
    mr->add(word,len,one,2);
}

void countword(MapReduce *mr, char *key, int keysize,  MultiValueIterator *iter, void* ptr){
  uint64_t count=0;
  
  for(iter->Begin(); !iter->Done(); iter->Next()){
    count+=atoi(iter->getValue());
  }
  
  char count_str[100];
  sprintf(count_str, "%lu", count);
  mr->add(key, keysize, count_str, strlen(count_str)+1);
}

void output(const char *filename, const char *outdir, const char *prefix, MapReduce *mr){
  char tmp[1000];
  
  sprintf(tmp, "%s/%s.%s.%dk.%dk.%dm.%d.P.%d.%d.csv", outdir, filename, prefix, lbufsize, gbufsize, blocksize, commmode, nprocs, me);
  FILE *fp = fopen(tmp, "a+");
  fprintf(fp, "%ld,%ld,%g,%g,%g\n", nword, nunique, t3-t1, t2-t1, t3-t2);
  fclose(fp);

  sprintf(tmp, "%s/%s.%s.%dk.%dk.%dm.%d.T.%d.%d.csv", outdir, filename, prefix, lbufsize, gbufsize, blocksize, commmode, nprocs, me); 
  fp = fopen(tmp, "a+");
  mr->show_stat(0, fp);
  fclose(fp);
}
