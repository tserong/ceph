// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"
#include "common/perf_counters.h"

extern PerfCounters *perfcounter;
extern PerfCounters *perfcounter_ops;
extern PerfCounters *perfcounter_ops_svc_time_hist;
extern PerfCounters *perfcounter_ops_svc_time_sum;
extern PerfCounters *perfcounter_prom_time_hist;
extern PerfCounters *perfcounter_prom_time_sum;
extern PerfHistogramCommon::axis_config_d perfcounter_op_hist_x_axis_config;
extern PerfHistogramCommon::axis_config_d perfcounter_op_hist_y_axis_config;


extern int rgw_perf_start(CephContext *cct);
extern void rgw_perf_stop(CephContext *cct);

enum {
  l_rgw_first = 15000,
  l_rgw_req,
  l_rgw_failed_req,

  l_rgw_get,
  l_rgw_get_b,
  l_rgw_get_lat,

  l_rgw_put,
  l_rgw_put_b,
  l_rgw_put_lat,

  l_rgw_qlen,
  l_rgw_qactive,

  l_rgw_cache_hit,
  l_rgw_cache_miss,

  l_rgw_keystone_token_cache_hit,
  l_rgw_keystone_token_cache_miss,

  l_rgw_gc_retire,

  l_rgw_lc_expire_current,
  l_rgw_lc_expire_noncurrent,
  l_rgw_lc_expire_dm,
  l_rgw_lc_transition_current,
  l_rgw_lc_transition_noncurrent,
  l_rgw_lc_abort_mpu,

  l_rgw_pubsub_event_triggered,
  l_rgw_pubsub_event_lost,
  l_rgw_pubsub_store_ok,
  l_rgw_pubsub_store_fail,
  l_rgw_pubsub_events,
  l_rgw_pubsub_push_ok,
  l_rgw_pubsub_push_failed,
  l_rgw_pubsub_push_pending,
  l_rgw_pubsub_missing_conf,

  l_rgw_lua_current_vms,
  l_rgw_lua_script_ok,
  l_rgw_lua_script_fail,

  l_rgw_sfs_sqlite_retry_total,
  l_rgw_sfs_sqlite_retry_retried_count,
  l_rgw_sfs_sqlite_retry_failed_count,

  l_rgw_sfs_gc_count,
  l_rgw_sfs_gc_processing_time,
  l_rgw_sfs_gc_process_exit,
  l_rgw_sfs_gc_pending_objects_data_elapsed,
  l_rgw_sfs_gc_pending_multiparts_data_elapsed,
  l_rgw_sfs_gc_deleted_buckets_elapsed,
  l_rgw_sfs_gc_deleted_objects_elapsed,
  l_rgw_sfs_gc_done_aborted_multiparts_elapsed,
  l_rgw_sfs_gc_abort_bucket_multiparts_elapsed,

  l_rgw_last,
};

enum {
  l_rgw_prom_first = 25000,
  l_rgw_prom_sfs_sqlite_profile,
  l_rgw_prom_last,
};

enum class sfs_gc_process_exit_state : int {
  delete_pending_objects_data = 1,
  delete_pending_multiparts_data,
  process_deleted_buckets,
  process_deleted_objects,
  process_done_and_aborted_multiparts,
  finished,
};

std::ostream& operator<<(std::ostream& os, sfs_gc_process_exit_state state);
