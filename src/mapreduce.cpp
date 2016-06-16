#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <sys/wait.h>
#include <math.h>
#include <dirent.h>
#include <sys/stat.h>

#include <mpi.h>
#include <omp.h>

#include <iostream>
#include <string>
#include <list>
#include <vector>

#include "mapreduce.h"
#include "dataobject.h"
#include "alltoall.h"
#include "ptop.h"
#include "spool.h"
#include "log.h"
#include "config.h"
#include "const.h"
#include "hash.h"

#include "stat.h"

using namespace MAPREDUCE_NS;


/**
   Create MapReduce Object
 
   @param[in]     _caller MPI Communicator
   @return return MapReduce Object.
*/
MapReduce::MapReduce(MPI_Comm _caller)
{
  comm = _caller;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid=omp_get_thread_num();
  tnum = omp_get_num_threads();
#pragma omp master
{
  TRACKER_START(tnum);
  PROFILER_START(tnum);
  thread_info=new thread_private_info[tnum];
}
#pragma omp barrier
  TRACKER_TIMER_INIT(tid);
}
#else
  tnum=1;
  TRACKER_START(tnum);
  PROFILER_START(tnum);
  thread_info=new thread_private_info[tnum];
  TRACKER_TIMER_INIT(0);
#endif
 
  _get_default_values(); 
  _bind_threads();

  data = NULL;
  c = NULL;

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: create. (thread number=%d)\n", me, nprocs, tnum);
}

/**
   Copy MapReduce Object
 
   @param[in]     _mr original MapReduce Object
   @return return new MapReduce Object.
*/
MapReduce::MapReduce(const MapReduce &_mr){
  // copy stats
  local_kvs_count=_mr.local_kvs_count;
  global_kvs_count=_mr.global_kvs_count;
  // copy configures
  kvtype=_mr.kvtype;
  ksize=_mr.ksize;
  vsize=_mr.vsize;
  nmaxblock=_mr.nmaxblock;
  maxmemsize=_mr.maxmemsize;
  outofcore=_mr.outofcore;
  commmode=_mr.commmode;
  lbufsize=_mr.lbufsize;
  gbufsize=_mr.gbufsize;
  blocksize=_mr.blocksize;
  tmpfpath=_mr.tmpfpath;
  myhash=_mr.myhash;
  nbucket=_mr.nbucket;
  nset=_mr.nset;
  // copy internal states
  comm=_mr.comm;
  me=_mr.me;
  nprocs=_mr.nprocs;
  tnum=_mr.tnum;
  mode=_mr.mode;
  ukeyoffset=_mr.ukeyoffset;
  // copy data
  data=_mr.data;
  DataObject::addRef(data);

  c=NULL;
  ifiles.clear();

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid=omp_get_thread_num();
  tnum = omp_get_num_threads();
#pragma omp master
{
  TRACKER_START(tnum);
  PROFILER_START(tnum);
  thread_info=new thread_private_info[tnum];
}
  thread_info[tid].block=-1;
  thread_info[tid].nitem=0;
  TRACKER_TIMER_INIT(tid);
}
#else
  tnum=1;
  TRACKER_START(tnum);
  PROFILER_START(tnum);
  thread_info=new thread_private_info[tnum];
  thread_info[0].block=-1;
  thread_info[0].nitem=0;
  TRACKER_TIMER_INIT(0);
#endif

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: copy\n", me, nprocs);
}

/**
   Destory MapReduce Object
*/
MapReduce::~MapReduce()
{
  DataObject::subRef(data);
  
  if(c) delete c;
  delete [] thread_info;

  TRACKER_END;
  PROFILER_END;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: destroy.\n", me, nprocs);
}

/**
   Map function without input
 
   @param[in]  _mymap user-defined map function
   @param[in]  _ptr   user-defined pointer
   @param[in]  _comm  communication or not
   @return output <key,value> count
*/
uint64_t MapReduce::init_key_value(UserInitKV _myinit, \
  void *_ptr, int _comm){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (no input)\n", me, nprocs);

  // create data object
  DataObject::subRef(data);
  //if(data) delete data;
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
  kv->setKVsize(ksize,vsize);

  if(_comm){
    c=Communicator::Create(comm, tnum, commmode);
    c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
    c->init(kv);
    mode = MapMode;
  }else{
    mode = MapLocalMode;
  }

  // begin traversal
#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid = omp_get_thread_num();
  
  _tinit(tid);
  
  if(tid == 0)
    _myinit(this, _ptr);
  
  if(_comm) c->twait(tid);
}
#else
  _tinit(0);
  _myinit(this, _ptr);
#endif
  // wait all processes done
  if(_comm){
    c->wait();
    delete c;
    c = NULL;
  }

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (no input)\n", me, nprocs);

  return _get_kv_count();
}

/**
   Map function without input
 
   @param[in]  _filename file name or file path
   @param[in]  _ptr   user-defined pointer
   @param[in]  _comm  communication or not
   @return output <key,value> count
*/
uint64_t MapReduce::map_text_file(char *_filename, int _shared, 
                        int _recur, char *_whitespace, 
                        UserMapFile _mymap, 
                        void *_ptr, int _comm){
  if(strlen(_whitespace) == 0){
    LOG_ERROR("%s", "Error: the white space should not be empty string!\n");
  }
#ifdef USE_MT_IO
  return _map_multithread_io(_filename, _shared, 
    _recur, _whitespace, _mymap, _ptr, _comm);
#else
  return _map_master_io(_filename, _shared, 
    _recur, _whitespace, _mymap, _ptr, _comm);
#endif
}

/**
   Map function KV input
 
   @param[in]  _mr MapReduce Object pointer
   @param[in]  _mymap user-defined map function
   @param[in]  _ptr   user-defined pointer
   @param[in]  _comm  communication or not
   @return output <key,value> count
*/

uint64_t MapReduce::map_key_value(MapReduce *_mr, 
    UserMapKV _mymap, 
    void *_ptr, int _comm){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (KV as input)\n", me, nprocs);

  DataObject::addRef(_mr->data);
  DataObject::subRef(data);

  // save original data object
  DataObject *data = _mr->data;

  // create new data object
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  kv->setKVsize(ksize, vsize);
  this->data = kv;
  DataObject::addRef(this->data);

  // create communicator
  //c = new Alltoall(comm, tnum);
  //Communicator *c=NULL;
  if(_comm){
    c=Communicator::Create(comm, tnum, commmode);
    c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
    c->init(kv);
    mode = MapMode;
  }else{
    mode = MapLocalMode;
  }

  KeyValue *inputkv = (KeyValue*)data;

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid = omp_get_thread_num();
#else
  int tid = 0;
#endif
  _tinit(tid);  

  char *key, *value;
  int keybytes, valuebytes;

  int i;
#ifdef MTMR_MULTITHREAD 
#pragma omp for nowait
#endif
  for(i = 0; i < inputkv->nblock; i++){
    int offset = 0;

    inputkv->acquire_block(i);

    offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
  
    while(offset != -1){

      _mymap(this, key, keybytes, value, valuebytes, _ptr);

      offset = inputkv->getNextKV(i, offset, &key, keybytes, &value, valuebytes);
    }
   
    inputkv->release_block(i);
  }

#ifdef MTMR_MULTITHREAD
  if(_comm) c->twait(tid);
}
#endif

  if(_comm){
    c->wait();
    delete c;
    c = NULL;
  }

  DataObject::subRef(data);

  mode= NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (KV as input)\n", me, nprocs);

  return _get_kv_count();
}

/**
   Map function without input
 
   @param[in]  _myreduce user-defined reduce function
   @param[in]  _compress if apply compress
   @param[in]  _ptr user-defined pointer
   @return output <key,value> count
*/
uint64_t MapReduce::reduce(
  void (*_myreduce)(MapReduce *, char *, int, 
  MultiValueIterator *, void*), 
  int _compress, void* _ptr){

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_INPUT_KV, \
    (data->blocksize)*(data->nblock));

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce start.\n", me, nprocs);
  
  KeyValue *kv = (KeyValue*)data;
  int kvtype=kv->getKVtype();

  // create new data object
  KeyValue *outkv = new KeyValue(kvtype, 
                      blocksize, 
                      nmaxblock, 
                      maxmemsize, 
                      outofcore, 
                      tmpfpath);
  outkv->setKVsize(ksize, vsize);
  data=outkv;

  mode = ReduceMode;

  // Reduce without compress
  if(!_compress){
    local_kvs_count = _convert_small(kv, _myreduce, _ptr);
    DataObject::subRef(kv);
  // Reduce with compress
  }else{
    _convert_compress(kv, _myreduce, _ptr);
    DataObject::subRef(kv);

    PROFILER_RECORD_COUNT(0, COUNTER_CPS_OUTPUT_KV, \
      (outkv->blocksize)*(outkv->nblock));

    KeyValue *tmpkv=outkv;

    outkv = new KeyValue(kvtype, 
                  blocksize, 
                  nmaxblock, 
                  maxmemsize, 
                  outofcore, 
                  tmpfpath);
    outkv->setKVsize(ksize, vsize);
    data=outkv;
    local_kvs_count = _convert_small(tmpkv, _myreduce, _ptr);
    DataObject::subRef(tmpkv);
  }

  DataObject::addRef(data);
  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: reduce end.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_RDC_OUTPUT_KV, \
    (data->blocksize)*(data->nblock));

  return _get_kv_count(); 
}

/**
   Scan <key,value>
 
   @param[in]  _myscan user-defined scan function
   @param[in]  _ptr user-defined pointer
   @return nothing
*/
void MapReduce::scan(
  void (_myscan)(char *, int, char *, int ,void *), 
  void * _ptr){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan begin\n", me, nprocs);

  KeyValue *kv = (KeyValue*)data;

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel for
#endif
  for(int i = 0; i < kv->nblock; i++){

     char *key, *value;
     int keybytes, valuebytes, kvsize;

     kv->acquire_block(i);
     char *kvbuf=kv->getblockbuffer(i);
     int datasize=kv->getdatasize(i);
     
     int offset=0;
     while(offset < datasize){
       GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

       _myscan(key, keybytes, value, valuebytes, _ptr);
       
       offset += kvsize;
     }
     kv->release_block(i);
  }

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: scan end.\n", me, nprocs);
}

/**
   Add <eky,value>
 
   @param[in]  _key key pointer
   @param[in]  _keybytes key size
   @param[in]  _value value pointer
   @param[in] _valubytes value size
   @return nothing
*/
void MapReduce::add_key_value(char *key, int keybytes, char *value, int valuebytes){
#ifdef MTMR_MULTITHREAD 
  int tid = omp_get_thread_num();
#else
  int tid = 0;
#endif
  // invoked in map function
  if(mode == MapMode){
    // get target process
    int target = 0;
    if(myhash != NULL){
      target=myhash(key, keybytes);
    }else{
      uint32_t hid = 0;
      hid = hashlittle(key, keybytes, nprocs);
      target = hid % (uint32_t)nprocs;
    }

    //printf("%d send KV to %d key=%s\n", me, target, key); fflush(stdout);
    // send KV    
    c->sendKV(tid, target, key, keybytes, value, valuebytes);

    thread_info[tid].nitem++;
 
    return;
   }else if(mode == MapLocalMode || mode == ReduceMode){

    // add KV into data object 
    KeyValue *kv = (KeyValue*)data;

    if(thread_info[tid].block == -1){
      thread_info[tid].block = kv->add_block();
    }

    kv->acquire_block(thread_info[tid].block);

    while(kv->addKV(thread_info[tid].block, key, keybytes, value, valuebytes) == -1){
      kv->release_block(thread_info[tid].block);
      thread_info[tid].block = kv->add_block();
      kv->acquire_block(thread_info[tid].block);
    }

    kv->release_block(thread_info[tid].block);
    thread_info[tid].nitem++;
  }
  return;
}



uint64_t MapReduce::_map_master_io(char *filepath, int sharedflag, int recurse, 
    char *whitespace, void (*mymap) (MapReduce *, char *, void *), void *ptr, int _comm){

  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (main thread read file)\n", me, nprocs);

  // delete data
  DataObject::subRef(data);

  // distribute files
  ifiles.clear();
  _disinputfiles(filepath, sharedflag, recurse);

  TRACKER_RECORD_EVENT(0, EVENT_MAP_DIS_FILES);

  // create dataobject
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data=kv;
  DataObject::addRef(data);
  kv->setKVsize(ksize,vsize);

  // create communicator
  if(_comm){
    c=Communicator::Create(comm, tnum, commmode);
    c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
    c->init(kv);
    mode = MapMode;
  }else{
    mode = MapLocalMode;
  }

  // create input file buffer
  uint64_t input_buffer_size=inputsize;
#ifdef USE_MPI_IO
  char **input_file_buffers = new char*[INPUT_BUF_COUNT];
  for(int i=0; i<INPUT_BUF_COUNT; i++){
    input_file_buffers[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, input_buffer_size+MAX_STR_SIZE+1);
  }
#else
  char *text = new char[input_buffer_size+MAX_STR_SIZE+1];
#endif

  int64_t *tstart=new int64_t[tnum];

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid = omp_get_thread_num();
  _tinit(tid);
}
#else
  _tinit(0);
#endif

  int fcount = ifiles.size();
  for(int i = 0; i < fcount; i++){
    int64_t input_char_size=0;

    // Open file
#ifdef USE_MPI_IO
    int ibuf=0;
    MPI_Request reqs[INPUT_BUF_COUNT];
    for(int j=0; j<INPUT_BUF_COUNT; j++) reqs[j]=MPI_REQUEST_NULL;

    MPI_File fp;
    int err=MPI_File_open(MPI_COMM_SELF, ifiles[i].c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &fp);

#ifdef USE_MPI_ASYN_IO
    MPI_File_iread_at(fp, 0, input_file_buffers[ibuf], input_buffer_size, MPI_BYTE, &reqs[ibuf]);
#endif

#else
    FILE *fp = fopen(ifiles[i].c_str(), "r");
#endif

    //TRACKER_RECORD_EVENT(0, EVENT_PFS_OPEN);

#ifdef USE_MPI_IO
    MPI_Offset fsize;
    MPI_File_get_size(fp, &fsize);
#else
    struct stat stbuf;
    stat(ifiles[i].c_str(), &stbuf);
    int64_t fsize = stbuf.st_size;
#endif

    // Process file
    int64_t foff = 0, boff = 0;
    int64_t readsize=0;

    do{
#ifdef USE_MPI_IO

#ifdef USE_MPI_ASYN_IO
      char *text=input_file_buffers[ibuf];
      MPI_Status status;
      MPI_Wait(&reqs[ibuf], &status);
      int count;
      MPI_Get_count(&status, MPI_BYTE, &count); 
      readsize = (int64_t)count;
      //fprintf(stdout, "%d[%d] readsize=%ld\n", me, nprocs, readsize); fflush(stdout);
      //printf("")
#else
      char *text=input_file_buffers[ibuf];
      // set file pointer
      MPI_Status status;
      MPI_File_read_at(fp, foff, text+boff, input_buffer_size, MPI_BYTE, &status);
      int count;
      MPI_Get_count(&status, MPI_BYTE, &count);
      readsize = (int64_t)count;
#endif
   
#else
      fseek(fp, foff, SEEK_SET);   
      readsize = fread(text+boff, 1, input_buffer_size, fp);
#endif
      // read a block
      text[boff+readsize] = '\0';
      input_char_size = boff+readsize;

      LOG_PRINT(DBG_IO, "%d[%d] read file %s, %ld->%ld\n", me, nprocs, ifiles[i].c_str(), foff, foff+readsize);

      // Divide input buffer
      int64_t divisor = input_char_size / tnum;
      int64_t remain  = input_char_size % tnum;

      tstart[0]=0;
      for(int j=0; j<tnum; j++){
        int64_t tend=0;   
        if(j<tnum-1){
          tend = tstart[j] + divisor;
          if(j < remain) tend += 1;
          int64_t text_index=tend;
          do{
            if(strchr(whitespace, text[text_index]) != NULL || text[text_index]=='\0') break;
            text_index++;
          }while(1);
          tend=text_index;
          text[tend]='\0';
          if(tend+1>input_char_size)
            tstart[j+1]=input_char_size;
          else
            tstart[j+1]=tend+1;
        }else{
          tend=input_char_size;
          boff=0;
          if(readsize >= input_buffer_size && foff+readsize<fsize){
           while(strchr(whitespace, text[input_char_size-boff-1])==NULL) boff++;
            tend-=(boff+1);
            text[tend]='\0';
          }
        }
      }

      if(boff > MAX_STR_SIZE) LOG_ERROR("%s", "Error: string length is large than max size!\n");

#ifdef USE_MPI_ASYN_IO
      ibuf=(ibuf+1)%INPUT_BUF_COUNT;
      if(foff+readsize<fsize){
        MPI_Offset offset=foff+readsize;
        //printf("offset=%lld, length=%d\n", offset, input_buffer_size); fflush(stdout);
        MPI_File_iread_at(fp, offset, input_file_buffers[ibuf]+boff, input_buffer_size, MPI_BYTE, &reqs[ibuf]);
      }
#endif

      // Process input buffer
#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
      int tid = omp_get_thread_num();
#else
      int tid = 0;
#endif

      char *saveptr = NULL;
      char *word = strtok_r(text+tstart[tid], whitespace, &saveptr);
      while(word){
        mymap(this, word, ptr);
        word = strtok_r(NULL,whitespace,&saveptr);
      }

#ifdef MTMR_MULTITHREAD 
      if(_comm) c->tpoll(tid);
}
#endif

      // Prepare for next buffer
      foff += readsize;

#ifdef USE_MPI_IO
#ifdef USE_MPI_ASYN_IO
      for(int j =0; j < boff; j++) input_file_buffers[ibuf][j]=text[input_char_size-boff+j];
#else
      for(int j =0; j < boff; j++) text[j] = text[input_char_size-boff+j];
#endif

#else
      for(int j =0; j < boff; j++) text[j] = text[input_char_size-boff+j];
#endif

   }while(foff<fsize);

    // Close file
#ifdef USE_MPI_IO
    MPI_File_close(&fp);
#else   
    fclose(fp);
#endif

    LOG_PRINT(DBG_IO, "%d[%d] close file %s\n", me, nprocs, ifiles[i].c_str());
  }

  // Wait thread end
#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid = omp_get_thread_num();

  //printf("thread=%d begin wait\n", tid); fflush(stdout);
  if(_comm) c->twait(tid);
}
#endif

  // Free buffers
  delete [] tstart;

#ifdef USE_MPI_IO 
  for(int i=0; i<INPUT_BUF_COUNT; i++) mem_aligned_free(input_file_buffers[i]);
  delete [] input_file_buffers;
#else
  delete []  text;
#endif

  // delete communicator
 if(_comm){
   c->wait();
   delete c;
   c = NULL;
 }

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (main thread read file)\n", me, nprocs);

  return _get_kv_count();
}

uint64_t MapReduce::_map_multithread_io(char *filepath, int sharedflag, int recurse, 
    char *whitespace, void (*mymap) (MapReduce *, char *, void *), void *ptr, int _comm){

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (File name to mymap)\n", me, nprocs);
  TRACKER_RECORD_EVENT(0, EVENT_MR_GENERAL);
  // Distribute files
  ifiles.clear();
  _disinputfiles(filepath, sharedflag, recurse);

  LOG_PRINT(DBG_GEN, "%d[%d] Distribute files end\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_MAP_DIS_FILES);

  DataObject::subRef(data);

  // Create data
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);
  data = kv;
  DataObject::addRef(data);

  kv->setKVsize(ksize, vsize);

  // Create communicator
  //Communicator *c=NULL;
  if(_comm){
    c=Communicator::Create(comm, tnum, commmode);
    c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
    c->init(data);
    mode = MapMode;
  }else{
    mode = MapLocalMode;
  }

  int fp=0;
  int fcount = ifiles.size();

  TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel shared(fp)
{
  int tid = omp_get_thread_num();
#else
  int tid = 0;
#endif

  _tinit(tid);
 
  TRACKER_RECORD_EVENT(tid, EVENT_OMP_IDLE);

  // create input file buffer
  uint64_t input_buffer_size=inputsize;
  int64_t input_char_size=0;
  char *text = new char[input_buffer_size+MAX_STR_SIZE+1];

  PROFILER_RECORD_COUNT(tid, COUNTER_MAP_INPUT_SIZE, input_buffer_size);

  // Process files
  int i;
  while((i=__sync_fetch_and_add(&fp,1))<fcount){

    TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);

    FILE *fp = fopen(ifiles[i].c_str(), "r");

    TRACKER_RECORD_EVENT(tid, EVENT_PFS_OPEN);
    PROFILER_RECORD_COUNT(tid, COUNTER_MAP_FILE_COUNT, 1);
    // Process File
    //int64_t fsize = stbuf.st_size;
    int64_t foff = 0, boff = 0;
    int64_t readsize=0;

    do{
      TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
      fseek(fp, foff, SEEK_SET);   
      TRACKER_RECORD_EVENT(tid, EVENT_PFS_SEEK);
      readsize = fread(text+boff, 1, input_buffer_size, fp);
      TRACKER_RECORD_EVENT(tid, EVENT_PFS_READ);
      PROFILER_RECORD_COUNT(tid, COUNTER_MAP_FILE_SIZE, readsize);
      // read a block
      text[boff+readsize] = '\0';

      input_char_size=boff+readsize;
      uint64_t tend=input_char_size;
      boff=0;
      if(readsize >= input_buffer_size){
        while(strchr(whitespace, text[input_char_size-boff-1])==NULL) boff++;
        tend-=(boff+1);
        text[tend]='\0';
      }

      if(boff > MAX_STR_SIZE) LOG_ERROR("%s", "Error: string length is large than max size!\n");

      char *saveptr = NULL;
      char *word = strtok_r(text, whitespace, &saveptr);
      while(word){

        mymap(this, word, ptr);

        word = strtok_r(NULL,whitespace,&saveptr);
      }

      foff += readsize;

      for(int j =0; j < boff; j++) text[j] = text[input_char_size-boff+j];
    }while(readsize >= input_buffer_size);

    TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
    fclose(fp);
    TRACKER_RECORD_EVENT(tid, EVENT_PFS_CLOSE);
  }

  delete []  text;

#ifdef MTMR_MULTITHREAD 
  if(_comm) c->twait(tid);
#endif

  PROFILER_RECORD_COUNT(tid, COUNTER_MAP_KV_COUNT, thread_info[tid].nitem);
  TRACKER_RECORD_EVENT(tid, EVENT_MAP_COMPUTING);
#ifdef MTMR_MULTITHREAD 
}
#endif

  // delete communicator
  if(_comm){
    c->wait();
    delete c;
    c = NULL; 
  }

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (File name to mymap)\n", me, nprocs);
  
  TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
  PROFILER_RECORD_COUNT(0, COUNTER_MAP_OUTPUT_KV, \
    (data->blocksize)*(data->nblock));

  return _get_kv_count();
}


#if 0
/*
 * map: (files as input, user-defined map reads files)
 * argument:
 *  filepath:   input file path
 *  sharedflag: 0 for local file system, 1 for global file system
 *  recurse:    1 for resucse
 *  mymap:      user-defined map
 *  ptr:        user-defined pointer
 * return:
 *  global KV count
 */
uint64_t MapReduce::map(char *filepath, int sharedflag, int recurse, 
  void (*mymap) (MapReduce *, const char *, void *), void *ptr, int _comm){
  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map start. (File name to mymap)\n", me, nprocs);
  // create new data object
  //if(data) delete data;
  DataObject::subRef(data);

  ifiles.clear();
  KeyValue *kv = new KeyValue(kvtype, 
                              blocksize, 
                              nmaxblock, 
                              maxmemsize, 
                              outofcore, 
                              tmpfpath);

  // distribute input fil list
  _disinputfiles(filepath, sharedflag, recurse);

  data = kv;
  DataObject::addRef(data);

  kv->setKVsize(ksize, vsize);

  //printf("_comm=%d\n", _comm); fflush(stdout);
 
  // create communicator
  //c = new Alltoall(comm, tnum);
  if(_comm){
    //if(commmode==0)
    //  c = new Alltoall(comm, tnum);
    //else if(commmode==1)
    //  c = new Ptop(comm, tnum);
    c=Communicator::Create(comm, tnum, commmode);
    c->setup(lbufsize, gbufsize, kvtype, ksize, vsize);
    c->init(data);

    mode = MapMode;
  }else{
    mode = MapLocalMode;
  }

#if GATHER_STAT
  double t1 = MPI_Wtime();
#endif

  int fp=0;
  int fcount = ifiles.size();

#pragma omp parallel shared(fp) shared(fcount)
{
  int tid = omp_get_thread_num();
  _tinit(tid);

#if GATHER_STAT
  double t1 = omp_get_wtime();
#endif

  int i;
  while((i=__sync_fetch_and_add(&fp,1))<fcount){
#if GATHER_STAT
    st.inc_counter(tid, COUNTER_FILE_COUNT, 1);
#endif  
    mymap(this,ifiles[i].c_str(), ptr);
  }

#if GATHER_STAT
  double t2 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_USER, t2-t1);
#endif

  if(_comm) c->twait(tid);

#if GATHER_STAT
  double t3 = omp_get_wtime();
  st.inc_timer(tid, TIMER_MAP_TWAIT, t3-t2);
#endif
}
  
#if GATHER_STAT
  double t2= MPI_Wtime();
  st.inc_timer(0, TIMER_MAP_PARALLEL, t2-t1);
#endif

  //printf("here!\n"); fflush(stdout);

  if(_comm){
    c->wait();
    delete c;
    c = NULL; 
  }
  //local_kvs_count=c->get_recv_KVs();

#if GATHER_STAT
  double t3= MPI_Wtime();
  st.inc_timer(0, TIMER_MAP_WAIT, t3-t2);
#endif

  mode = NoneMode;

  LOG_PRINT(DBG_GEN, "%d[%d] MapReduce: map end. (File name to mymap)\n", me, nprocs);

  return _get_kv_count();
}
#endif


/***** internal functions ******/

// find the key in the unique list
MapReduce::Unique* MapReduce::_findukey(Unique **unique_list, int ibucket, char *key, int keybytes, Unique *&uprev){
  Unique *uptr = unique_list[ibucket];
  
  if(!uptr){
    uprev = NULL;
    return NULL;
  }

  char *keyunique;
  while(uptr){
    keyunique = ((char*)uptr)+ukeyoffset;
    if(keybytes==uptr->keybytes && memcmp(key,keyunique,keybytes)==0)
      return uptr;
    uprev = uptr;
    uptr = uptr->next;
  }

  return NULL;
}

void MapReduce::_unique2set(UniqueInfo *u){

  Set *set=(Set*)u->set_pool->add_block();

  int nunique=0;

  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){
      Unique *ukey = (Unique*)(ubuf);

      if((ubuf_end-ubuf < sizeof(Unique)) || 
        (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      Set *pset=&set[u->nset%nset];
      pset->myid=u->nset++;
      pset->nvalue=ukey->nvalue;
      pset->mvbytes=ukey->mvbytes;
      pset->pid=0;
      pset->next=NULL;

      ukey->firstset=pset;
      ukey->lastset=pset;
      
      if(u->nset%nset==0)
        set = (Set*)u->set_pool->add_block();

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);

    }// end while
  }

end:
  return;
}

int  MapReduce::_kv2unique(int tid, KeyValue *kv, UniqueInfo *u, DataObject *mv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *ptr,
  int shared){

  //DEFINE_KV_VARS;
  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;
  
  int isfirst=1, pid=0;
  int last_blockid=0, last_offset=0, last_set=0;

  int kmvbytes=0, mvbytes=0;

  char *ubuf=u->unique_pool->add_block();
  char *ubuf_end=ubuf+u->unique_pool->blocksize;

  Set *sets=NULL, *pset = NULL;

  // scan all KVs
  for(int i=0; i<kv->nblock; i++){

    kv->acquire_block(i);

    kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf+kv->getdatasize(i);
    int kvbuf_off=0;

    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid%tnum != (uint32_t)tid) {
        kvbuf_off += kvsize;
        continue;
      }

      // Find the key
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      int key_hit=1;
      if(!ukey) key_hit=0;

      int mv_inc=valuebytes;
      if(kv->kvtype==GeneralKV)  mv_inc+=oneintlen;

      // The First Block
      if(isfirst){
        //printf("kmvbytes=%d, mv_inc=%d\n", kmvbytes, mv_inc);
        kmvbytes+=mv_inc;
        if(!key_hit) kmvbytes+=(keybytes+3*sizeof(int));

        // We need the intermediate convert
        if(kmvbytes>mv->blocksize){
          //printf("nunique=%d\n", u->nunique); fflush(stdout);
          _unique2set(u);
          //printf("nset=%d\n", u->nset); fflush(stdout);
          sets=(Set*)u->set_pool->blocks[u->nset/nset];
          isfirst=0;
        }
      }
    
      // Add a new partition 
      if(mvbytes+mv_inc>mv->blocksize){
        Partition p;
        p.start_blockid=last_blockid;
        p.start_offset=last_offset;
        p.end_blockid=i;
        p.end_offset=kvbuf_off;
        p.start_set=last_set;
        p.end_set=u->nset;

        LOG_PRINT(DBG_CVT, "%d[%d] T%d Partition %d\n", me, nprocs, tid, pid);

        _unique2mv(tid, kv, &p, u, mv);

        last_blockid=p.end_blockid;
        last_offset=p.end_offset;
        last_set=p.end_set;
        pid++;
        mvbytes=0;
      }
      mvbytes += mv_inc;

      if(ukey){
        ukey->nvalue++;
        ukey->mvbytes += valuebytes;
      // add unique key
      }else{

        if(ubuf_end-ubuf<ukeyoffset+keybytes){
          //printf("add a new unique buffer! ubuf_end-ubuf=%ld\n", ubuf_end-ubuf); fflush(stdout);
          memset(ubuf, 0, ubuf_end-ubuf);
          ubuf=u->unique_pool->add_block();
          ubuf_end=ubuf+u->unique_pool->blocksize;
        }


        ukey=(Unique*)(ubuf);
        ubuf += ukeyoffset;

        // add to the list
        ukey->next = NULL;
        if(pre == NULL)
          u->ubucket[ibucket] = ukey;
        else
          pre->next = ukey;

        // copy key
        ukey->key = ubuf;
        memcpy(ubuf, key, keybytes);
        ubuf += keybytes;
        //ubuf = ROUNDUP(ubuf, ualignm);
 
        ukey->keybytes=keybytes;
        ukey->nvalue=1;
        ukey->mvbytes=valuebytes;
        ukey->flag=0;
        ukey->firstset=ukey->lastset=NULL;

        //printf("add key=%s\n", ukey->key); fflush(stdout);

        u->nunique++;
      }// end else if

      if(!isfirst) {
        // add one new set
        if((key_hit && ukey->lastset->pid != pid) || (!key_hit)){
          // add a block
          pset=&sets[u->nset%nset];

          pset->myid=u->nset;
          pset->nvalue=0;
          pset->mvbytes=0;
          pset->pid=pid;

          pset->next=NULL;
          if(ukey->lastset != NULL)
            ukey->lastset->next=pset;
          ukey->lastset=pset;
          if(ukey->firstset==NULL)
            ukey->firstset=pset;

          u->nset++;
          if(u->nset%nset==0){
            sets=(Set*)u->set_pool->add_block();
          }
        }else{
          pset=ukey->lastset;
        }

        // add key information into block
        pset->nvalue++;
        pset->mvbytes+=valuebytes;
      }

      kvbuf_off += kvsize;

    }// end while

    kv->release_block(i);
  }// end For

  if(!isfirst && kv->nblock>0){
    Partition p;
    p.start_blockid=last_blockid;
    p.start_offset=last_offset;
    p.end_blockid=kv->nblock-1;
    p.end_offset=kv->getdatasize(kv->nblock-1);
    p.start_set=last_set;
    p.end_set=u->nset;

    _unique2mv(tid, kv, &p, u, mv);
  }

  return isfirst;
}

void MapReduce::_unique2kmv(int tid, KeyValue *kv, UniqueInfo *u,DataObject *mv,  
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void *ptr, int shared){
 
  //DEFINE_KV_VARS; 
  char *key, *value;
  int keybytes, valuebytes, kvsize;
 
  char *kvbuf;

  int mv_block_id=mv->add_block();
  mv->acquire_block(mv_block_id);
  char *mv_buf=mv->getblockbuffer(mv_block_id);
  int mv_off=0;

  int nunique=0;

  // Set the offset
  Spool *unique_pool=u->unique_pool;
  for(int i=0; i<unique_pool->nblock; i++){
    char *ubuf=unique_pool->blocks[i];
    char *ubuf_end=ubuf+unique_pool->blocksize;
    while(ubuf < ubuf_end){

      Unique *ukey = (Unique*)(ubuf);
      if((ubuf_end-ubuf < sizeof(Unique)) || (ukey->key==NULL))
        break;

      nunique++;
      if(nunique>u->nunique) goto end;

      *(int*)(mv_buf+mv_off)=ukey->keybytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->mvbytes;
      mv_off+=sizeof(int);
      *(int*)(mv_buf+mv_off)=ukey->nvalue;
      mv_off+=sizeof(int);
     
      if(kv->kvtype==GeneralKV){
        ukey->soffset=(int*)(mv_buf+mv_off);
        mv_off+=ukey->nvalue*sizeof(int);
      }

      ubuf+=ukeyoffset;
      memcpy(mv_buf+mv_off, ubuf, ukey->keybytes);
      mv_off+=ukey->keybytes;

      ukey->voffset=mv_buf+mv_off;
      mv_off+=ukey->mvbytes;

      ubuf+=ukey->keybytes;
      //ubuf=ROUNDUP(ubuf, ualignm);
           
      ukey->nvalue=0;
      ukey->mvbytes=0;
    }
  }

end:

#if SAFE_CHECK
  if(mv_off > (blocksize)){
    LOG_ERROR("KMV size %d is larger than a single block size %ld!\n", mv_off, blocksize);
  }
#endif

  mv->setblockdatasize(mv_block_id, mv_off);

  // gain KVS
  for(int i=0; i<kv->nblock; i++){
    kv->acquire_block(i);
    char *kvbuf=kv->getblockbuffer(i);
    int datasize=kv->getdatasize(i);
    char *kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      //printf("key=%s, value=%s\n", key, value); fflush(stdout);
        
      uint32_t hid = hashlittle(key, keybytes, 0);
      if(shared && (uint32_t)hid % tnum != (uint32_t)tid) continue;

      //printf("tid=%d, key=%s, value=%s\n", tid, key, value);

      // Find the key
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);
      
      if(kv->kvtype==GeneralKV){
        ukey->soffset[ukey->nvalue]=valuebytes;
      }

      memcpy(ukey->voffset+ukey->mvbytes, value, valuebytes);
      
      ukey->mvbytes+=valuebytes;
      ukey->nvalue++;
    }
    kv->release_block(i);
  }

  //printf("here!\n"); fflush(stdout);

  char *values;
  int nvalue, mvbytes, kmvsize, *valuesizes;

  int datasize=mv->getdatasize(mv_block_id);
  int offset=0;

  //printf("offset=%d, datasize=%d\n", offset, datasize);

  while(offset < datasize){

    //printf("offset=%d, datasize=%d\n", offset, datasize); fflush(stdout);
    
    GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);

    //printf("key=%s, nvalue=%d\n", key, nvalue); fflush(stdout);
 
    MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
    myreduce(this, key, keybytes, iter, ptr);
    delete iter;


    offset += kmvsize;
  }

  mv->release_block(mv_block_id);
}

void MapReduce::_unique2mv(int tid, KeyValue *kv, Partition *p, UniqueInfo *u, DataObject *mv, int shared){
  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;

  //DEFINE_KV_VARS;

  //printf("unique2mv: add_block, [%d,%d]->[%d,%d]  mv=%d\n", p->start_blockid, p->start_offset, p->end_blockid, p->end_offset, mv->nblock);

  int mv_blockid=mv->add_block();
  //printf("mvblockid=%d\n", mv_blockid);
  mv->acquire_block(mv_blockid);

  char *mvbuf = mv->getblockbuffer(mv_blockid);
  int mvbuf_off=0;

  for(int i=p->start_set; i<p->end_set; i++){
    Set *pset=(Set*)u->set_pool->blocks[i/nset]+i%nset;
    
    if(kv->kvtype==GeneralKV){
      pset->soffset=(int*)(mvbuf+mvbuf_off);
      pset->s_off=mvbuf_off;
      mvbuf_off += pset->nvalue*sizeof(int);
    }
    pset->voffset=mvbuf+mvbuf_off;
    pset->v_off=mvbuf_off;
    mvbuf_off += pset->mvbytes;

    pset->nvalue=0;
    pset->mvbytes=0;
  }

#if SAFE_CHECK
  if(mvbuf_off > mv->blocksize){
    LOG_ERROR("The offset %d of MV is larger than blocksize %ld!\n", mvbuf_off, mv->blocksize);
  }
#endif

  mv->setblockdatasize(mv_blockid, mvbuf_off);

  for(int i=p->start_blockid; i<=p->end_blockid; i++){
    kv->acquire_block(i);
    char *kvbuf=kv->getblockbuffer(i);
    char *kvbuf_end=kvbuf;
    if(i<p->end_blockid)
      kvbuf_end+=kv->getdatasize(i);
    else
      kvbuf_end+=p->end_offset;
    if(i==p->start_blockid) kvbuf += p->start_offset;

    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize, kv);

      //printf("second: key=%s, value=%s\n", key, value);

      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;

      if(shared && (uint32_t)hid%tnum != (uint32_t)tid) continue;

      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);

      Set *pset=ukey->lastset;

      //if(!pset || pset->pid != mv_blockid) 
      //  LOG_ERROR("Cannot find one set for key %s!\n", ukey->key);

      if(kv->kvtype==GeneralKV){
        pset->soffset[pset->nvalue]=valuebytes;
      }
      memcpy(pset->voffset+pset->mvbytes, value, valuebytes);
      pset->mvbytes+=valuebytes;
      pset->nvalue++;
    }// end while(kvbuf<kvbuf_end)

    kv->release_block(i);
  }

  mv->release_block(mv_blockid);
}

void MapReduce::_mv2kmv(DataObject *mv,UniqueInfo *u, int kvtype, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr){

  int nunique=0;
  char *ubuf, *kmvbuf=NULL;
  int uoff=0, kmvoff=0;
  char *ubuf_end;
  
  for(int i=0; i < u->unique_pool->nblock; i++){
    ubuf = u->unique_pool->blocks[i];
    ubuf_end=ubuf+u->unique_pool->blocksize;

    while(ubuf<ubuf_end){
      Unique *ukey = (Unique*)ubuf;    

      if(ubuf_end-ubuf<ukeyoffset || ukey->key==NULL)
        break;

      nunique++;
      if(nunique > u->nunique) goto end;

      MultiValueIterator *iter = new MultiValueIterator(ukey, mv, kvtype);     

      myreduce(this, ukey->key, ukey->keybytes, iter, ptr);
      
      delete iter;

      ubuf += ukeyoffset;
      ubuf += ukey->keybytes;
      //ubuf = ROUNDUP(ubuf, ualignm);
    }
  }// End for

end:
  ;
  //kmv->setblockdatasize(kmv_blockid, kmvoff);
  //kmv->release_block(kmv_blockid);
}

uint64_t MapReduce::_convert_small(KeyValue *kv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr){

  LOG_PRINT(DBG_CVT, "%d[%d] Convert(small) start.\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);

  uint64_t tmax_mem_bytes=0;

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel reduction(+:tmax_mem_bytes) 
{
  int tid = omp_get_thread_num();
#else
  int tid = 0;
#endif
  _tinit(tid);

  TRACKER_RECORD_EVENT(tid, EVENT_OMP_IDLE);
 
  // initialize the unique info
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*sizeof(Unique));
  u->set_pool=new Spool(nset*sizeof(Set));
  u->nunique=0;
  u->nset=0;

  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  DataObject *mv = NULL;
  mv = new DataObject(ByteType, 
             blocksize, 
             nmaxblock, 
             maxmemsize, 
             outofcore, 
             tmpfpath.c_str(),
             0);

  int isfirst = _kv2unique(tid, kv, u, mv, myreduce, ptr, 1);

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_BUCKET_SIZE, nbucket*sizeof(void*));
  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_UNIQUE_SIZE, \
    (u->unique_pool->blocksize)*(u->unique_pool->nblock));
  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_SET_SIZE, \
    (u->set_pool->blocksize)*(u->set_pool->nblock));

  LOG_PRINT(DBG_CVT, "%d KV2Unique end:first=%d\n", tid, isfirst);

  if(isfirst){
    _unique2kmv(tid, kv, u, mv, myreduce, ptr);
  }else{
    _mv2kmv(mv, u, kv->kvtype, myreduce, ptr);
  }

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_KMV_SIZE, \
    (mv->blocksize)*(mv->nblock));

  delete mv;

  //printf("T%d: %ld\n", tid, u->nunique);

  thread_info[tid].nitem = u->nunique;

  delete [] u->ubucket;
  delete u->unique_pool;
  delete u->set_pool;
  delete u;

  PROFILER_RECORD_COUNT(tid, COUNTER_CVT_NUNIQUE, thread_info[tid].nitem);
  TRACKER_RECORD_EVENT(tid, EVENT_RDC_COMPUTING);
#ifdef MTMR_MULTITHREAD 
}
#endif

  uint64_t nunique=0;
  for(int i=0; i<tnum; i++)
    nunique += thread_info[i].nitem;

  //printf("nunique=%d, tnum=%d\n", nunique, tnum);

  LOG_PRINT(DBG_CVT, "%d[%d] Convert(small) end.\n", me, nprocs);

  return nunique;
}

uint64_t MapReduce::_convert_compress(KeyValue *kv, 
  void (*myreduce)(MapReduce *, char *, int,  MultiValueIterator *iter, void*), void* ptr){

  LOG_PRINT(DBG_CVT, "%d[%d] MapReduce: compress begin\n", me, nprocs);

  TRACKER_RECORD_EVENT(0, EVENT_RDC_COMPUTING);

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel 
{
  int tid = omp_get_thread_num();
#else
  int tid = 0;
#endif
  _tinit(tid);

  TRACKER_RECORD_EVENT(tid, EVENT_OMP_IDLE);

  // initialize the unique info
  UniqueInfo *u=new UniqueInfo();
  u->ubucket = new Unique*[nbucket];
  u->unique_pool=new Spool(nbucket*sizeof(Unique));
  u->nunique=0;
  memset(u->ubucket, 0, nbucket*sizeof(Unique*));

  char *kmv_buf=(char*)mem_aligned_malloc(MEMPAGE_SIZE, blocksize);
  int kmv_off=0; 

  PROFILER_RECORD_COUNT(tid,COUNTER_CPS_BUCKET_SIZE,nbucket*sizeof(void*));
  PROFILER_RECORD_COUNT(tid,COUNTER_CPS_KMV_SIZE,blocksize);

  char *key, *value;
  int keybytes, valuebytes, kvsize;
  char *kvbuf;
  int unique_pool_max_block=0;

#ifdef MTMR_MULTITHREAD 
#pragma omp for
#endif
  for(int i=0; i<kv->nblock; i++){ 
    u->unique_pool->clear();
    u->nunique=0;
    kmv_off=0;
    memset(u->ubucket, 0, nbucket*sizeof(Unique*));

    // build unique structure
    kv->acquire_block(i);

    char *ubuf=u->unique_pool->add_block();
    char *ubuf_end=ubuf+u->unique_pool->blocksize;

    kvbuf=kv->getblockbuffer(i);
    int datasize=kv->getdatasize(i);
    char *kvbuf_end=kvbuf+datasize;
    //int blocksize=
    int kvbuf_off=0;

    //printf("block %d start\n", i); fflush(stdout);

    int kv_count=0;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      kv_count++;

      //printf("key=%s, value=%s\n", key, value); 
    
      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;
     
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);
      if(ukey){
        ukey->nvalue++;
        ukey->mvbytes += valuebytes;
      }else{
        if(ubuf_end-ubuf<ukeyoffset+keybytes){
          memset(ubuf, 0, ubuf_end-ubuf);
          ubuf=u->unique_pool->add_block();
          ubuf_end=ubuf+u->unique_pool->blocksize;
        }

        ukey=(Unique*)(ubuf);
        ubuf += ukeyoffset;

        // add to the list
        ukey->next = NULL;
        if(pre == NULL)
          u->ubucket[ibucket] = ukey;
        else
          pre->next = ukey;

        // copy key
        ukey->key = ubuf;
        memcpy(ubuf, key, keybytes);
        ubuf += keybytes;
 
        ukey->keybytes=keybytes;
        ukey->nvalue=1;
        ukey->mvbytes=valuebytes;

        u->nunique++;
      }// end else if
    }

    int nunique=0;
    Spool *unique_pool=u->unique_pool;
    for(int j=0; j<unique_pool->nblock; j++){
      char *ubuf=unique_pool->blocks[j];
      char *ubuf_end=ubuf+unique_pool->blocksize;
      
      while(ubuf < ubuf_end){
        Unique *ukey = (Unique*)(ubuf);
        if((ubuf_end-ubuf < sizeof(Unique)) || (ukey->key==NULL))
          break;

        nunique++;
        if(nunique>u->nunique) goto out;

        *(int*)(kmv_buf+kmv_off)=ukey->keybytes;
        kmv_off+=sizeof(int);
        *(int*)(kmv_buf+kmv_off)=ukey->mvbytes;
        kmv_off+=sizeof(int);
        *(int*)(kmv_buf+kmv_off)=ukey->nvalue;
        kmv_off+=sizeof(int);
     
        if(kv->kvtype==GeneralKV){
          ukey->soffset=(int*)(kmv_buf+kmv_off);
          kmv_off+=ukey->nvalue*sizeof(int);
        }

        ubuf+=ukeyoffset;
        memcpy(kmv_buf+kmv_off, ubuf, ukey->keybytes);
        kmv_off+=ukey->keybytes;

        ukey->voffset=kmv_buf+kmv_off;
        kmv_off+=ukey->mvbytes;

        ubuf+=ukey->keybytes;
          
        ukey->nvalue=0;
        ukey->mvbytes=0;
      }
    }

out:

    kvbuf=kv->getblockbuffer(i);
    kvbuf_end=kvbuf+datasize;
    while(kvbuf<kvbuf_end){
      GET_KV_VARS(kv->kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,kv);

      // Find the key
      uint32_t hid = hashlittle(key, keybytes, 0);
      int ibucket = hid % nbucket;
      Unique *ukey, *pre;
      ukey = _findukey(u->ubucket, ibucket, key, keybytes, pre);
      
      if(kv->kvtype==GeneralKV){
        ukey->soffset[ukey->nvalue]=valuebytes;
        ukey->nvalue++;
      }

      memcpy(ukey->voffset+ukey->mvbytes, value, valuebytes);
      ukey->mvbytes+=valuebytes;
    }

    kv->release_block(i);

    if(u->unique_pool->nblock>unique_pool_max_block)
      unique_pool_max_block=u->unique_pool->nblock;

    char *values;
    int nvalue, mvbytes, kmvsize, *valuesizes;

    KeyValue *kv = (KeyValue*)data;
    //kv->print();

    char *mv_buf=kmv_buf;
    datasize=kmv_off;
    int offset=0;

    while(offset < datasize){   
      GET_KMV_VARS(kv->kvtype, mv_buf, key, keybytes, nvalue, values, valuesizes, mvbytes, kmvsize, kv);
 
      MultiValueIterator *iter = new MultiValueIterator(nvalue,valuesizes,values,kv->kvtype,kv->vsize);
      myreduce(this, key, keybytes, iter, ptr);
      delete iter;

      offset += kmvsize;
    }

  }// end for

  mem_aligned_free(kmv_buf);

  PROFILER_RECORD_COUNT(tid, COUNTER_CPS_UNIQUE_SIZE, \
    unique_pool_max_block*(u->unique_pool->blocksize));

  delete [] u->ubucket;
  delete u->unique_pool;
  //delete u->set_pool;
  delete u;

  TRACKER_RECORD_EVENT(tid, EVENT_CPS_COMPUTING);
#ifdef MTMR_MULTITHREAD 
}
#endif

  LOG_PRINT(DBG_CVT, "%d[%d] MapReduce: compress end\n", me, nprocs);

  return 0;
}

void MapReduce::print_stat(FILE *out){
//#if GATHER_STAT  
  //st.print(verb, out);
//#endif
 
  fprintf(out, "rank:%d", me); 
  fprintf(out, ",size:%d", nprocs);
  fprintf(out, ",thread:%d", tnum);
#ifdef ENABLE_PROFILER
  fprintf(out, ",profiler:enable");
#endif
#ifdef ENABLE_TRACKER
  fprintf(out, ",tracker:enable");
#endif 
  fprintf(out, "\n");
  for(int i=0;i<tnum; i++){
#ifdef ENABLE_PROFILER
    fprintf(out, "action:profiler_start");
    std::map<std::string,double>::iterator timer_iter;
    for(timer_iter=profiler_event_timer[i].begin(); timer_iter!=profiler_event_timer[i].end(); timer_iter++){
      fprintf(out, ",%s:%g",timer_iter->first.c_str(), timer_iter->second);
    }
    std::map<std::string,uint64_t>::iterator counter_iter;
    for(counter_iter=profiler_event_counter[i].begin(); counter_iter!=profiler_event_counter[i].end(); counter_iter++){
      fprintf(out, ",%s:%ld", counter_iter->first.c_str(), counter_iter->second);
    }
    fprintf(out, ",action:profiler_end\n");
#endif
#ifdef ENABLE_TRACKER
    fprintf(out, "action:tracker_start");
    std::vector<std::pair<std::string,double> >::iterator event_iter;
    for(event_iter=tracker_event_timer[i].begin(); event_iter!=tracker_event_timer[i].end(); event_iter++){
      fprintf(out, ",%s:%g", event_iter->first.c_str(), event_iter->second);
    }
    fprintf(out, ",action:tracker_end\n");
#endif
  }

  //PROFILER_PRINT(out, tnum);
  //TRACKER_PRINT(out, tnum);
}

//void MapReduce::init_stat(){
//#if GATHER_STAT
  //st.clear();
//#endif
//}

/*
 * Output data in this object
 *  type: 0 for string, 1 for int, 2 for int64_t
 *  fp:     file pointer
 *  format: hasn't been used
 */
void MapReduce::output(int type, FILE* fp, int format){
  if(data){
    data->print(type, fp, format);
  }else{
    LOG_ERROR("%s","Error to output empty data object\n");
  }
}

// private function
/*****************************************************************************/

// process init
void MapReduce::_get_default_values(){
  bind_thread=0;
  procs_per_node=0;
  thrs_per_proc=0;  
  show_binding=0;

  char *env = getenv(ENV_BIND_THREADS);
  if(env){
    bind_thread=atoi(env);
    if(bind_thread == 1){
      env = getenv(ENV_PROCS_PER_NODE);
      if(env){
        procs_per_node=atoi(env);
      }
      env = getenv(ENV_THRS_PER_PROC);
      if(env){
        thrs_per_proc=atoi(env);
      }
      if(procs_per_node <=0 || thrs_per_proc <=0 )
        bind_thread = 0;
    }else bind_thread = 0;
  }
  env = getenv(ENV_SHOW_BINGDING);
  if(env){
    show_binding = atoi(env);
    if(show_binding != 1) show_binding=0;
  }

  inputsize = INPUT_SIZE;
  blocksize = BLOCK_SIZE;
  nmaxblock = MAX_BLOCKS;
  maxmemsize = MAXMEM_SIZE;
  lbufsize = LOCAL_BUF_SIZE;
  gbufsize = GLOBAL_BUF_SIZE;

  kvtype = KV_TYPE;

  outofcore = OUT_OF_CORE; 
  tmpfpath = std::string(TMP_PATH);

  commmode=0;
  
  myhash = NULL;

  nbucket=pow(2, BUCKET_SIZE);
  nset = nbucket;

  ukeyoffset = sizeof(Unique);
}

void MapReduce::_bind_threads(){

#ifdef MTMR_MULTITHREAD 
#pragma omp parallel
{
  int tid = omp_get_thread_num();

  cpu_set_t mask;

  if(bind_thread){
    CPU_ZERO(&mask);

    int lrank=me%procs_per_node;
    int coreid=lrank*thrs_per_proc+tid;

    CPU_SET(coreid, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
  }

  if(show_binding){
    CPU_ZERO(&mask);

    sched_getaffinity(0, sizeof(mask), &mask);
    for(int i=0; i<PCS_PER_NODE*THS_PER_PROC*2; i++){
      if(CPU_ISSET(i, &mask)){
        printf("P%d[T%d] bind to cpu%d\n", me, tid, i);fflush(stdout);
      }
    }
  }
}

  if(show_binding){
    printf("Process count=%d, thread count=%d\n", nprocs, tnum);   
  }

#endif

}

// thread init
void MapReduce::_tinit(int tid){
  thread_info[tid].block=-1;
  thread_info[tid].nitem=0;
  //blocks[tid] = -1;
  //nitems[tid] = 0;
}

int64_t MapReduce::_stringtoint(const char *_str){
  std::string str=_str;
  int64_t num=0;
  if(str[str.size()-1]=='k'||str[str.size()-1]=='K'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='k')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='K')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024; 
  }else if(str[str.size()-1]=='m'||str[str.size()-1]=='M'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='m')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='M')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024*1024; 
  }else if(str[str.size()-1]=='g'||str[str.size()-1]=='G'||\
    (str[str.size()-1]=='b'&&str[str.size()-2]=='g')||\
    (str[str.size()-1]=='B'&&str[str.size()-2]=='G')){
    if(str[str.size()-1]=='b'||str[str.size()-1]=='B'){
      str=str.substr(0, str.size()-2);
    }else{
      str=str.substr(0, str.size()-1);
    }
    num=atoi(str.c_str());
    num*=1024*1024*1024; 
  }else{
    LOG_ERROR("Error: set buffer size %s error! \
      The buffer size should end with k,K,kb,KB,m,M,mb,MB,g,G,gb,GB", _str);
  }
  if(num==0){
    LOG_ERROR("Error: buffer size %s should not be zero!", _str);
  }

  return num;
}


// distribute input file list
void MapReduce::_disinputfiles(const char *filepath, int sharedflag, int recurse){
  _getinputfiles(filepath, sharedflag, recurse);

  if(sharedflag){
    int fcount = ifiles.size();
    int div = fcount / nprocs;
    int rem = fcount % nprocs;
    int *send_count = new int[nprocs];
    int total_count = 0;

    if(me == 0){
      int j = 0, end=0;
      for(int i = 0; i < nprocs; i++){
        send_count[i] = 0;
        end += div;
        if(i < rem) end++;
        while(j < end){
          send_count[i] += strlen(ifiles[j].c_str())+1;
          j++;
        }
        total_count += send_count[i];
      }
    }

    int recv_count;
    MPI_Scatter(send_count, 1, MPI_INT, &recv_count, 1, MPI_INT, 0, comm);

    int *send_displs = new int[nprocs];
    if(me == 0){
      send_displs[0] = 0;
      for(int i = 1; i < nprocs; i++){   
        send_displs[i] = send_displs[i-1]+send_count[i-1];
      }
    }

    char *send_buf = new char[total_count];
    char *recv_buf = new char[recv_count];

    if(me == 0){
      int offset = 0;
      for(int i = 0; i < fcount; i++){
          memcpy(send_buf+offset, ifiles[i].c_str(), strlen(ifiles[i].c_str())+1);
          offset += strlen(ifiles[i].c_str())+1;
        }
    }

    MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE, recv_buf, recv_count, MPI_BYTE, 0, comm);

    ifiles.clear();
    int off=0;
    while(off < recv_count){
      char *str = recv_buf+off;
      ifiles.push_back(std::string(str));
      off += strlen(str)+1;
    }

    delete [] send_count;
    delete [] send_displs;
    delete [] send_buf;
    delete [] recv_buf;
  }
}

// get input file list
void MapReduce::_getinputfiles(const char *filepath, int sharedflag, int recurse){
  // if shared, only process 0 read file names
  if(!sharedflag || (sharedflag && me == 0)){
    
    struct stat inpath_stat;
    int err = stat(filepath, &inpath_stat);
    if(err) LOG_ERROR("Error in get input files, err=%d\n", err);
    
    // regular file
    if(S_ISREG(inpath_stat.st_mode)){
      ifiles.push_back(std::string(filepath));
    // dir
    }else if(S_ISDIR(inpath_stat.st_mode)){
      
      struct dirent *ep;
      DIR *dp = opendir(filepath);
      if(!dp) LOG_ERROR("%s", "Error in get input files\n");
      
      while(ep = readdir(dp)){
        
        if(ep->d_name[0] == '.') continue;
       
        char newstr[MAXLINE]; 
        sprintf(newstr, "%s/%s", filepath, ep->d_name);
        err = stat(newstr, &inpath_stat);
        if(err) LOG_ERROR("Error in get input files, err=%d\n", err);
        
        // regular file
        if(S_ISREG(inpath_stat.st_mode)){
          ifiles.push_back(std::string(newstr));
        // dir
        }else if(S_ISDIR(inpath_stat.st_mode) && recurse){
          _getinputfiles(newstr, sharedflag, recurse);
        }
      }
    }
  }  
}
