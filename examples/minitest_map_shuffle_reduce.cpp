/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>

#include "mimir.h"

using namespace MIMIR_NS;

int rank, size;

void map (Readable<uint64_t, void> *input,
          Writable<uint64_t, void> *output, void *ptr);
void reduce (Readable<uint64_t, void> *input,
             Writable<uint64_t, void> *output, void *ptr);

uint64_t nunique = 0, nwords = 0;

int main (int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0)
            fprintf(stdout, "Usage: %s nunique nwords\n", argv[0]);
        return 0;
    }

    nunique = strtoull(argv[1], NULL, 0);
    nwords = strtoull(argv[2], NULL, 0);

    MimirContext<uint64_t, void>* ctx
        = new MimirContext<uint64_t, void>();
    ctx->map(map);
    ctx->map(reduce);
    delete ctx;

    MPI_Finalize();
}

void map (Readable<uint64_t, void> *input,
          Writable<uint64_t, void> *output, void *ptr)
{
    uint64_t off = 0;
    uint64_t cnt = 0;

    cnt = nwords / size;
    off = cnt * rank;
    if (rank < (int)(nwords % size)) {
        cnt += 1;
        off += rank;
    }

    for (uint64_t i = 0; i < cnt; i ++) {
        uint64_t tmp = (i + off) % nunique;
        output->write(&tmp, NULL);
    }
}

void reduce (Readable<uint64_t, void> *input,
             Writable<uint64_t, void> *output, void *ptr) 
{
    uint64_t key = 0;
    input->read(&key, NULL);
    output->write(&key, NULL);
}
