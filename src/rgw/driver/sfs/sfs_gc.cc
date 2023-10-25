// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "sfs_gc.h"

#include <common/perf_counters.h>
#include <driver/sfs/sqlite/buckets/multipart_definitions.h>

#include <filesystem>

#include "common/Clock.h"
#include "driver/sfs/types.h"
#include "multipart_types.h"
#include "rgw/driver/sfs/sfs_log.h"
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/rgw_perf_counters.h"
#include "rgw_obj_types.h"
#include "sqlite/buckets/bucket_definitions.h"
#include "sqlite/sqlite_versioned_objects.h"
#include "sqlite/versioned_object/versioned_object_definitions.h"

namespace rgw::sal::sfs {

SFSGC::SFSGC(CephContext* _cctx, SFStore* _store) : cct(_cctx), store(_store) {
  worker = std::make_unique<GCWorker>(this, cct, this);
}

SFSGC::~SFSGC() {
  down_flag = true;
  if (worker->is_started()) {
    worker->stop();
    worker->join();
  }
}

int SFSGC::process() {
  // This is the method that does the garbage collection.
  initial_process_time = ceph_clock_now();
  perfcounter->inc(l_rgw_sfs_gc_count);

  // start by deleting possible pending objects data in the filesystem
  // this could be stopped in a previous execution due to max exec time elapsed
  auto time_to_process_more = delete_pending_objects_data();
  if (!time_to_process_more) {
    perfcounter->set(
        l_rgw_sfs_gc_process_exit,
        static_cast<uint64_t>(
            sfs_gc_process_exit_state::delete_pending_objects_data
        )
    );
    return 0;
  }
  // now delete possible pending multiparts data
  time_to_process_more = delete_pending_multiparts_data();
  if (!time_to_process_more) {
    perfcounter->set(
        l_rgw_sfs_gc_process_exit,
        static_cast<uint64_t>(
            sfs_gc_process_exit_state::delete_pending_multiparts_data
        )
    );
    return 0;
  }
  // process deleted buckets
  time_to_process_more = process_deleted_buckets();
  if (!time_to_process_more) {
    perfcounter->set(
        l_rgw_sfs_gc_process_exit,
        static_cast<uint64_t>(sfs_gc_process_exit_state::process_deleted_buckets
        )
    );
    return 0;
  }
  // process deleted objects
  time_to_process_more = process_deleted_objects();
  if (!time_to_process_more) {
    perfcounter->set(
        l_rgw_sfs_gc_process_exit,
        static_cast<uint64_t>(sfs_gc_process_exit_state::process_deleted_objects
        )
    );
    return 0;
  }

  // process done or aborted multiparts
  time_to_process_more = process_done_and_aborted_multiparts();
  perfcounter->set(
      l_rgw_sfs_gc_process_exit,
      static_cast<uint64_t>(sfs_gc_process_exit_state::finished)
  );
  return 0;
}

bool SFSGC::going_down() {
  return down_flag;
}

/*
 * The constructor must have finished before the worker thread can be created,
 * because otherwise the logging will dereference an invalid pointer, since the
 * SFSGC instance is a prefix provider for the logging in the worker thread
 */
void SFSGC::initialize() {
  max_process_time = cct->_conf.get_val<std::chrono::milliseconds>(
      "rgw_sfs_gc_max_process_time"
  );

  max_objects_to_delete_per_iteration =
      cct->_conf.get_val<uint64_t>("rgw_sfs_gc_max_objects_per_iteration");

  worker->create("rgw_gc");
  down_flag = false;
}

bool SFSGC::suspended() {
  return suspend_flag;
}

void SFSGC::suspend() {
  suspend_flag = true;
}

void SFSGC::resume() {
  suspend_flag = false;
}

std::ostream& SFSGC::gen_prefix(std::ostream& out) const {
  return out << "garbage collection: ";
}

bool SFSGC::process_deleted_buckets() {
  common::PerfGuard elapsed(perfcounter, l_rgw_sfs_gc_deleted_buckets_elapsed);
  // permanently delete removed buckets and their objects and versions
  sqlite::SQLiteBuckets db_buckets(store->db_conn);
  auto deleted_buckets = db_buckets.get_deleted_buckets_ids();
  lsfs_debug(this) << "deleted buckets found = " << deleted_buckets.size()
                   << dendl;
  bool time_to_delete_more = true;
  for (auto const& bucket_id : deleted_buckets) {
    time_to_delete_more = abort_bucket_multiparts(bucket_id);
    if (!time_to_delete_more) {
      return false;
    }
    bool all_parts_deleted = false;
    while (time_to_delete_more && !all_parts_deleted) {
      time_to_delete_more =
          delete_bucket_multiparts(bucket_id, all_parts_deleted);
    }
    if (!time_to_delete_more) {
      return false;
    }
    // we delete buckets in batches, so we might need to call delete bucket
    // more than once until the bucket itself is finally deleted
    // we do this while the time to delete more stuff is not elapsed
    bool bucket_fully_deleted = false;
    while (time_to_delete_more && !bucket_fully_deleted) {
      time_to_delete_more = delete_bucket(bucket_id, bucket_fully_deleted);
    }
    if (!time_to_delete_more) {
      return false;
    }
  }
  return true;
}

bool SFSGC::process_deleted_objects() {
  common::PerfGuard elapsed(perfcounter, l_rgw_sfs_gc_deleted_objects_elapsed);
  bool more_objects = true;
  bool time_to_process_more = true;
  while (time_to_process_more && more_objects) {
    // process deleted objects now in batches
    time_to_process_more = process_deleted_objects_batch(more_objects);
  }
  return time_to_process_more;
}

bool SFSGC::process_deleted_objects_batch(bool& more_objects) {
  more_objects = true;
  sqlite::SQLiteVersionedObjects db_versions(store->db_conn);
  pending_objects_to_delete = db_versions.remove_deleted_versions_transact(
      max_objects_to_delete_per_iteration
  );
  if (pending_objects_to_delete.has_value() &&
      (*pending_objects_to_delete).empty()) {
    more_objects = false;
  }
  return delete_pending_objects_data();
}

bool SFSGC::process_done_and_aborted_multiparts() {
  common::PerfGuard elapsed(
      perfcounter, l_rgw_sfs_gc_done_aborted_multiparts_elapsed
  );
  bool all_parts_deleted = false;
  bool time_to_process_more = true;
  while (time_to_process_more && !all_parts_deleted) {
    // process deleted objects now in batches
    time_to_process_more =
        process_done_and_aborted_multiparts_batch(all_parts_deleted);
  }
  return time_to_process_more;
}

bool SFSGC::process_done_and_aborted_multiparts_batch(bool& all_parts_deleted) {
  all_parts_deleted = false;
  sqlite::SQLiteMultipart db_multipart(store->db_conn);
  pending_multiparts_to_delete =
      db_multipart.remove_done_or_aborted_multiparts_transact(
          max_objects_to_delete_per_iteration
      );
  if (pending_multiparts_to_delete.has_value() &&
      (*pending_multiparts_to_delete).empty()) {
    all_parts_deleted = true;
  }
  return delete_pending_multiparts_data();
}

bool SFSGC::delete_pending_objects_data() {
  common::PerfGuard elapsed(
      perfcounter, l_rgw_sfs_gc_pending_objects_data_elapsed
  );
  // delete objects in a loop and check if the max process time has reached
  // for every object.
  if (pending_objects_to_delete.has_value()) {
    for (auto it = (*pending_objects_to_delete).begin();
         it != (*pending_objects_to_delete).end();) {
      Object::delete_version_data(
          store, sqlite::get_uuid((*it)), sqlite::get_version_id((*it))
      );
      it = (*pending_objects_to_delete).erase(it);
      if (process_time_elapsed()) {
        lsfs_debug(this) << "Exit due to max process time reached." << dendl;
        return false;  // had no time to delete everything
      }
    }
  }
  return true;  // all objects were successfully deleted
}

bool SFSGC::delete_pending_multiparts_data() {
  common::PerfGuard elapsed(
      perfcounter, l_rgw_sfs_gc_pending_multiparts_data_elapsed
  );
  if (pending_multiparts_to_delete.has_value()) {
    // delete multiparts in a loop and check if the max process time has reached
    // for every part.
    for (auto it = (*pending_multiparts_to_delete).begin();
         it != (*pending_multiparts_to_delete).end();) {
      MultipartPartPath pp(
          sqlite::get_path_uuid((*it)), sqlite::get_part_id((*it))
      );
      auto p = store->get_data_path() / pp.to_path();
      if (std::filesystem::exists(p)) {
        std::filesystem::remove(p);
      }
      it = (*pending_multiparts_to_delete).erase(it);
      // check that we didn't exceed the max before keep going
      if (process_time_elapsed()) {
        lsfs_debug(this) << "Exit due to max process time reached." << dendl;
        return false;  // had no time to delete everything
      }
    }
  }
  return true;  // all objects were successfully deleted
}

bool SFSGC::abort_bucket_multiparts(const std::string& bucket_id) {
  common::PerfGuard elapsed(
      perfcounter, l_rgw_sfs_gc_abort_bucket_multiparts_elapsed
  );
  sqlite::SQLiteMultipart db_mp(store->db_conn);
  int ret = db_mp.abort_multiparts_by_bucket_id(bucket_id);
  ceph_assert(ret >= 0);

  // check that we didn't exceed the max before keep going
  if (process_time_elapsed()) {
    lsfs_debug(this) << "Exit due to max process time reached." << dendl;
    return false;  // had no time to delete everything
  }
  return true;
}

bool SFSGC::delete_bucket_multiparts(
    const std::string& bucket_id, bool& all_parts_deleted
) {
  sqlite::SQLiteMultipart db_mp(store->db_conn);
  pending_multiparts_to_delete = db_mp.remove_multiparts_by_bucket_id_transact(
      bucket_id, max_objects_to_delete_per_iteration
  );
  all_parts_deleted = pending_multiparts_to_delete.has_value() &&
                      (*pending_multiparts_to_delete).empty();
  return delete_pending_multiparts_data();
}

bool SFSGC::delete_bucket(const std::string& bucket_id, bool& bucket_deleted) {
  sqlite::SQLiteBuckets db_buckets(store->db_conn);
  // deletes the db bucket (and all it's objects and versions) first in a
  // transaction.
  // The call return the objects (and versions) that need to be deleted from
  // the filesystem
  pending_objects_to_delete = db_buckets.delete_bucket_transact(
      bucket_id, max_objects_to_delete_per_iteration, bucket_deleted
  );
  return delete_pending_objects_data();
}

bool SFSGC::process_time_elapsed() const {
  auto now = ceph_clock_now();
  return (now.to_msec() - initial_process_time.to_msec()) >
         static_cast<uint64_t>(max_process_time.count());
}

SFSGC::GCWorker::GCWorker(
    const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc
)
    : dpp(_dpp), cct(_cct), gc(_gc) {}

void* SFSGC::GCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    lsfs_startup(dpp) << "start" << dendl;

    if (!gc->suspended()) {
      common::PerfGuard elapsed(perfcounter, l_rgw_sfs_gc_processing_time);
      int r = gc->process();
      if (r < 0) {
        lsfs_err(dpp) << "ERROR: garbage collection process() returned error r="
                      << r << dendl;
      }
      lsfs_shutdown(dpp) << "stop" << dendl;
    }

    if (gc->going_down()) break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_gc_processor_period;
    secs -= end.sec();
    if (secs <= 0) {
      // in case the GC iteration took more time than the period
      secs = cct->_conf->rgw_gc_processor_period;
      ;
    }

    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!gc->going_down());

  return nullptr;
}

void SFSGC::GCWorker::stop() {
  std::lock_guard l{lock};
  cond.notify_all();
}

}  //  namespace rgw::sal::sfs
