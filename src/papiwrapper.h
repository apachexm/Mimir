/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#ifndef MIMIR_PAPI_WRAPPER_H
#define MIMIR_PAPI_WRAPPER_H

#include <mpi.h>
#include "ac_config.h"

#if HAVE_LIBPAPI

void papi_init(MPI_Comm);
void papi_uinit();

void papi_start();
void papi_stop();

void papi_event_init();
void papi_event_uinit();

void papi_print(); 

void papi_powercap(double scale);
int64_t papi_powercap_energy();

#endif

#endif
