/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#include "log.h"
#include "stat.h"
#include "globals.h"
#include "config.h"
#include "papiwrapper.h"

#if HAVE_LIBPAPI

int EventSet = PAPI_NULL;
long long oldvalues[MAX_PAPI_EVENTS];
int limit_map[MAX_PAPI_EVENTS];
int num_events = 0, num_limits = 0;
long long event_values[MAX_PAPI_EVENTS];
char event_names[MAX_PAPI_EVENTS][PAPI_MAX_STR_LEN];

const int eventlist[] = {
    //PAPI_TOT_CYC,
    //PAPI_TOT_INS,
    //PAPI_L1_DCM,
    //PAPI_L1_ICM,
    //PAPI_L1_TCM,
    //PAPI_L1_LDM,
    //PAPI_L1_DCA,
    //PAPI_L1_ICH,
    //PAPI_L1_ICA,
    //PAPI_L2_TCM,
    //PAPI_L2_LDM,
    //PAPI_L2_TCH,
    //PAPI_L2_TCA,
    //PAPI_RES_STL,
    0
};

const char* nativeeventlist[] = {
    NULL
}; 

MPI_Comm papi_shared_comm;
int papi_shared_rank, papi_shared_size;

void papi_init(MPI_Comm shared_comm)
{
    papi_shared_comm = shared_comm;
    MPI_Comm_rank(papi_shared_comm, &papi_shared_rank);
    MPI_Comm_size(papi_shared_comm, &papi_shared_size);

    // Init PAPI
    int retval = PAPI_library_init( PAPI_VER_CURRENT );
    if (retval != PAPI_VER_CURRENT) {
        LOG_ERROR("PAPI_library_init failed! ret=%d\n", retval);
    }
}

void papi_uinit()
{
}

void papi_event_init() {
    int i, cid, powercap_cid = -1, numcmp, r, code, retval;
    const PAPI_component_info_t *cmpinfo = NULL;

    // Add power events
    retval = PAPI_create_eventset(&EventSet);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_create_eventset error: ret=%d\n", retval);
    }

    // Add general events
    //for (i = 0; eventlist[i] != 0; i++) { 
    //    retval = PAPI_event_code_to_name(eventlist[i], event_names[num_events]);
    //    if (retval != PAPI_OK) {
    //        LOG_ERROR("Error from PAPI_event_code_to_name!\n");
    //    }
    //    retval = PAPI_add_event(EventSet, eventlist[i]);
    //    if (retval != PAPI_OK) continue;
    //    num_events ++;
    //}

    // Add native events
    //for (i = 0; nativeeventlist[i] != NULL; i++) {
    //    int EventCode;
    //    retval = PAPI_event_name_to_code(nativeeventlist[i], &EventCode);
    //    if (retval != PAPI_OK) {
    //        LOG_ERROR("Error from PAPI_event_name_to_code!\n");
    //    }
    //    retval = PAPI_add_event(EventSet, EventCode);
    //    if (retval != PAPI_OK) continue;
    //    strcpy(event_names[num_events], nativeeventlist[i]);
    //    num_events ++;
    //}


    numcmp = PAPI_num_components();
    for (cid = 0; cid < numcmp; cid ++) {
	    if ((cmpinfo = PAPI_get_component_info(cid)) == NULL) {
		    LOG_ERROR("PAPI_get_component_info failed\n");
	    }
	    if ( strstr(cmpinfo->name, "powercap")) {
		    powercap_cid = cid;
		    if (cmpinfo->disabled) {
			    LOG_ERROR("powercap component disabled: %s\n", cmpinfo->disabled_reason);
		    }
		    break;
	    }
    }
    if (cid == numcmp) {
	    LOG_ERROR("No powercap component found numcmp=%d\n", numcmp);
    }

    code = PAPI_NATIVE_MASK;
    r = PAPI_enum_cmp_event(&code, PAPI_ENUM_FIRST, powercap_cid);
    while (r == PAPI_OK) {
	    retval = PAPI_event_code_to_name(code, event_names[num_events]);
	    if (retval != PAPI_OK) {
		    LOG_ERROR("Error from PAPI_event_code_to_name! retval=%d\n", retval);
	    }
	    if (!strstr(event_names[num_events], "ENERGY_UJ") 
                && !strstr(event_names[num_events], "POWER_LIMIT")) {
 	        r = PAPI_enum_cmp_event(&code, PAPI_ENUM_EVENTS, powercap_cid);
                continue;
            }
	    retval = PAPI_add_event(EventSet, code);
	    if (retval != PAPI_OK) break;
	    if (!(strstr(event_names[num_events], "SUBZONE"))
			    && (strstr(event_names[num_events], "POWER_LIMIT"))) {
		    limit_map[num_limits] = num_events;
		    num_limits ++;
	    }
	    num_events ++;
	    r = PAPI_enum_cmp_event(&code, PAPI_ENUM_EVENTS, powercap_cid);
    }
}


void papi_event_uinit() {

    int retval = PAPI_cleanup_eventset(EventSet);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_cleanup_eventset error!\n");
    
    retval = PAPI_destroy_eventset(&EventSet);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_destroy_eventset error!\n"); 
}

void papi_start() {
    int retval = PAPI_start( EventSet );
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_start error!ret=%d\n", retval); 
    }
    retval = PAPI_read( EventSet, oldvalues);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_read error!ret=%d\n", retval);
    }
    if (LIMIT_POWER) {
        MPI_Barrier(papi_shared_comm);
        if (papi_shared_rank == 0) papi_powercap(LIMIT_SCALE);
        MPI_Barrier(papi_shared_comm);
    }
}

void papi_stop() {
    int retval = PAPI_read(EventSet, event_values);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_read error!\n");
    }
    // Recover Power Limit
    if (LIMIT_POWER) {
        for (int i = 0; i < num_events; i++) {
            if (!(strstr(event_names[i], "SUBZONE"))
                && strstr(event_names[i], "POWER_LIMIT_A_UW")) {
                PROFILER_RECORD_COUNT(COUNTER_POWER_LIMIT, event_values[i], OPMAX);
            }
        }
        MPI_Barrier(papi_shared_comm);
        if (papi_shared_rank == 0) papi_powercap(1.0);
        MPI_Barrier(papi_shared_comm);
    }
    retval = PAPI_stop(EventSet, event_values);
    if (retval != PAPI_OK) LOG_ERROR("PAPI_stop error!\n");
    for (int i = 0; i < num_events; i++) {
        if (!(strstr(event_names[i], "SUBZONE")) 
            && strstr(event_names[i], "ENERGY_UJ")) {
            PROFILER_RECORD_COUNT(COUNTER_PACKAGE_ENERGY, event_values[i], OPMAX);
	}
	if (strstr(event_names[i], "SUBZONE") 
            && strstr(event_names[i], "ENERGY_UJ")) {
            PROFILER_RECORD_COUNT(COUNTER_DRAM_ENERGY, event_values[i], OPMAX);
	}
    }
}

void papi_print() {
    int retval = PAPI_read(EventSet, event_values);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_read error!\n");
    }
    for (int i = 0; i < num_events; i++) {
        fprintf(stdout, "EVENT: %s\tVALUE: %.02lf\n",
            event_names[i], (double)event_values[i]);
    }
}

void papi_powercap(double scale) {    
    for (int i = 0; i < num_events; i++) {
        event_values[i] = oldvalues[i];
    }
    for (int i = 0; i < num_limits; i++) {
        event_values[limit_map[i]] = (long long)(event_values[limit_map[i]] * scale);
        //printf("Set power limit=%lf!\n", event_values[limit_map[i]] / 1e6);
    }
    int retval = PAPI_write(EventSet, event_values);
    if (retval != PAPI_OK) {
        LOG_ERROR("PAPI_write error!\n");
    }
}

int64_t papi_powercap_energy() {
    int retval = PAPI_read(EventSet, event_values);
    if (retval != PAPI_OK) {
	    LOG_ERROR("PAPI_read error!\n");
    }
    for (int i = 0; i < num_events; i++) {
	    if (!(strstr(event_names[i], "SUBZONE"))
			    && strstr(event_names[i], "ENERGY_UJ")) {
		    return event_values[i];
	    }
    }
    return 0;
}

#endif

