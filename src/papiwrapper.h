/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#ifndef MIMIR_PAPI_WRAPPER_H
#define MIMIR_PAPI_WRAPPER_H

#include "ac_config.h"

#if HAVE_LIBPAPI

void papi_init();
void papi_uinit();

void papi_powercap_init();
void papi_powercap_uinit();
void papi_powercap(double scale);
void papi_powercap_record();
 
#endif

#endif
