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
#pragma once

#include <memory>

#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

#define sfs_dout_subsys ceph_subsys_rgw_sfs

namespace rgw::sal::sfs {

class SFSGC : public DoutPrefixProvider {
  CephContext* cct = nullptr;
  SFStore* store = nullptr;
  std::atomic<bool> down_flag = {true};
  std::atomic<bool> suspend_flag = {false};
  std::chrono::milliseconds max_process_time;
  utime_t initial_process_time;
  uint64_t max_objects_to_delete_per_iteration;

  class GCWorker : public Thread {
    const DoutPrefixProvider* dpp = nullptr;
    CephContext* cct = nullptr;
    SFSGC* gc = nullptr;
    ceph::mutex lock = ceph::make_mutex("GCWorker");
    ceph::condition_variable cond;

    std::string get_cls_name() const { return "GCWorker"; }

   public:
    GCWorker(const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc);

    void* entry() override;
    void stop();
  };

  std::unique_ptr<GCWorker> worker = nullptr;

 public:
  SFSGC(CephContext*, SFStore*);
  ~SFSGC();

  int process();

  bool going_down();
  void initialize();
  bool suspended();
  void suspend();
  void resume();

  CephContext* get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const override { return sfs_dout_subsys; }

  std::ostream& gen_prefix(std::ostream& out) const override;

  std::string get_cls_name() const { return "SFSGC"; }

 private:
  // Return false if it was forced to exit because max process time was met
  // which means there are still objects to be deleted
  bool process_deleted_buckets();
  bool process_deleted_objects();
  bool delete_pending_objects_data();
  bool delete_pending_multiparts_data();
  bool process_done_and_aborted_multiparts();
  bool abort_bucket_multiparts(const std::string& bucket_id);
  bool delete_bucket_multiparts(
      const std::string& bucket_id, bool& all_parts_deleted
  );
  bool process_deleted_objects_batch(bool& more_objects);
  bool process_done_and_aborted_multiparts_batch(bool& all_parts_deleted);
  bool delete_bucket(const std::string& bucket_id, bool& bucket_deleted);
  bool process_time_elapsed() const;

  std::optional<sqlite::DBDeletedObjectItems> pending_objects_to_delete;
  std::optional<sqlite::DBDeletedMultipartItems> pending_multiparts_to_delete;
};

}  //  namespace rgw::sal::sfs
