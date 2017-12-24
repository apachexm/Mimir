/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MTMR_CONFIG_H
#define MTMR_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

//#define MPI_FETCH_AND_OP

// Buffers
extern int64_t COMM_BUF_SIZE;
extern int64_t DATA_PAGE_SIZE;
extern int64_t INPUT_BUF_SIZE;
extern int BUCKET_COUNT;
extern int MAX_RECORD_SIZE;

// Settings
extern int SHUFFLE_TYPE;
extern int MIN_SBUF_COUNT;
extern int MAX_SBUF_COUNT;
extern int READ_TYPE;
extern int WRITE_TYPE;
extern int DIRECT_READ;
extern int DIRECT_WRITE;

// Features
extern int STREAM_IO;
extern int WORK_STEAL;
extern int MAKE_PROGRESS;
extern int BALANCE_LOAD;
extern int BIN_COUNT;
extern double BALANCE_FACTOR;
extern int BALANCE_FREQ;
extern int USE_MCDRAM;
extern int LIMIT_POWER;
extern double LIMIT_SCALE;

// Profile & Debug
extern int OUTPUT_STAT;
extern int OUTPUT_TRACE;
extern const char *STAT_FILE;
extern int DBG_LEVEL;

#endif
