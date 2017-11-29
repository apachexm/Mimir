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

void map (Readable<char*,void> *input,
          Writable<char*,void> *output, void *ptr);

int main (int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 3) {
        if (rank == 0)
            fprintf(stdout, "Usage: %s output input ...\n", argv[0]);
        return 0;
    }

    std::string output = argv[1];
    std::vector<std::string> input;
    for (int i = 2; i < argc; i++) {
        input.push_back(argv[i]);
    }
    MimirContext<char*, void>* ctx 
        = new MimirContext<char*, void>(input, output,
                                        "text", "text");
    ctx->map(map, NULL, true, true);
    delete ctx;

    MPI_Finalize();
}

void map (Readable<char*,void> *input, Writable<char*,void> *output, void *ptr)
{
    char *line = NULL;
    while (input->read(&line, NULL) == true) {
        output->write(&line, NULL);
    }
}
