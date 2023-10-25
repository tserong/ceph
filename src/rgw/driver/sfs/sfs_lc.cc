// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#include "sfs_lc.h"

#include <vector>

#include "sqlite/sqlite_lifecycle.h"

namespace rgw::sal::sfs {

SFSLifecycle::SFSLifecycle(SFStore* _st) : store(_st) {}

int SFSLifecycle::get_entry(
    const std::string& oid, const std::string& marker,
    std::unique_ptr<LCEntry>* entry
) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  auto db_entry = sqlite_lc.get_entry(oid, marker);
  if (!db_entry) {
    return -ENOENT;
  }
  *entry = std::make_unique<rgw::sal::StoreLifecycle::StoreLCEntry>(
      db_entry->bucket_name, db_entry->start_time, db_entry->status
  );
  return 0;
}

int SFSLifecycle::get_next_entry(
    const std::string& oid, const std::string& marker,
    std::unique_ptr<LCEntry>* entry
) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  auto db_entry = sqlite_lc.get_next_entry(oid, marker);
  if (!db_entry) {
    std::string empty;
    *entry =
        std::make_unique<rgw::sal::StoreLifecycle::StoreLCEntry>(empty, 0, 0);
  } else {
    *entry = std::make_unique<rgw::sal::StoreLifecycle::StoreLCEntry>(
        db_entry->bucket_name, db_entry->start_time, db_entry->status
    );
  }
  return 0;
}

int SFSLifecycle::set_entry(const std::string& oid, LCEntry& entry) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  rgw::sal::sfs::sqlite::DBOPLCEntry db_entry{
      oid, entry.get_bucket(), entry.get_start_time(), entry.get_status()};
  sqlite_lc.store_entry(db_entry);
  return 0;
}

int SFSLifecycle::list_entries(
    const std::string& oid, const std::string& marker, uint32_t max_entries,
    std::vector<std::unique_ptr<LCEntry>>& entries
) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  auto db_entries = sqlite_lc.list_entries(oid, marker, max_entries);
  for (auto& db_entry : db_entries) {
    entries.push_back(std::make_unique<rgw::sal::StoreLifecycle::StoreLCEntry>(
        db_entry.bucket_name, db_entry.start_time, db_entry.status
    ));
  }
  return 0;
}

int SFSLifecycle::rm_entry(const std::string& oid, LCEntry& entry) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  sqlite_lc.remove_entry(oid, entry.get_bucket());
  return 0;
}

int SFSLifecycle::get_head(
    const std::string& oid, std::unique_ptr<LCHead>* head
) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  auto db_head = sqlite_lc.get_head(oid);
  *head = std::make_unique<rgw::sal::StoreLifecycle::StoreLCHead>(
      db_head.start_date, 0, db_head.marker
  );
  return 0;
}

int SFSLifecycle::put_head(const std::string& oid, LCHead& head) {
  rgw::sal::sfs::sqlite::SQLiteLifecycle sqlite_lc(store->db_conn);
  rgw::sal::sfs::sqlite::DBOPLCHead db_head{
      oid, head.get_marker(), head.get_start_date()};
  sqlite_lc.store_head(db_head);
  return 0;
}

std::unique_ptr<LCSerializer> SFSLifecycle::get_serializer(
    const std::string& lock_name, const std::string& oid,
    const std::string& cookie
) {
  // creates or returns the mutex in the map for the given oid
  std::timed_mutex& mutex = mutex_map[oid];
  return std::make_unique<LCSFSSerializer>(
      mutex, store, oid, lock_name, cookie
  );
}

}  // namespace rgw::sal::sfs
