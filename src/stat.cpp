/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "stat.h"

double init_wtime = 0;

Profiler_info profiler_info;
double *profiler_timer = NULL;
uint64_t *profiler_counter = NULL;

char timestr[1024];

const char *timer_str[TIMER_NUM] = {
    "total_time",
    "map_time",
    "reduce_time",
    "output_time",
    "pfs_input_time",
    "pfs_output_time",
    "mpi_a2a_time",
    "mpi_a2av_time",
    "mpi_rdc_time",
    "mpi_block_time",
    "mpi_barrier_time",
    "mpi_allgather_time",
    "mpi_allgatherv_time",
    "mem_allocate_time",
    "lb_check_time",
    "lb_rp_time",
    "lb_migrate_time",
    "lb_split_time"
};

const char *counter_str[COUNTER_NUM] = {
    "comm_bufs",
    "shuffle_times",
    "send_bytes",
    "recv_bytes",
    "file_count",
    "file_size",
    "max_file",
    "output_size",
    "send_tail",
    "recv_tail",
    "balance_times",
    "split_keys",
    "redirect_bins",
    "max_bin_size",
    "migrate_kvs",
    "max_kvs",
    "max_kmvs",
    "max_kv_pages",
    "max_kmv_pages",
    "hash_bucket",
    "peakmem_use",
    "power_limit",
    "package_energy",
    "dram_energy",
    "map_package_energy",
    "reduce_package_energy",
    "output_package_energy",
};

Tracker_info tracker_info;
std::vector <std::pair<std::string, double>> *tracker_event = NULL;

#if HAVE_LIBPAPI
std::vector <int64_t> *tracker_power = NULL;
#endif
