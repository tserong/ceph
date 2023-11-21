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
#include "dbconn.h"

#include <ceph_assert.h>
#include <sqlite3.h>

#include <filesystem>
#include <system_error>

#include "common/dout.h"
#include "rgw/driver/sfs/sfs_log.h"

#define dout_subsys ceph_subsys_rgw_sfs

namespace fs = std::filesystem;
namespace orm = sqlite_orm;

namespace rgw::sal::sfs::sqlite {

static std::string get_temporary_db_path(CephContext* ctt) {
  auto rgw_sfs_path = ctt->_conf.get_val<std::string>("rgw_sfs_data_path");
  auto tmp_db_name = std::string(DB_FILENAME) + "_tmp";
  auto db_path = std::filesystem::path(rgw_sfs_path) / std::string(tmp_db_name);
  return db_path.string();
}

static void sqlite_error_callback(void* ctx, int error_code, const char* msg) {
  const auto cct = static_cast<CephContext*>(ctx);
  lderr(cct) << "[SQLITE] (" << error_code << ") " << msg << dendl;
}

static int sqlite_wal_hook_callback(
    void* ctx, sqlite3* db, const char* zDb, int frames
) {
  const auto cct = static_cast<CephContext*>(ctx);
  if (frames <=
      cct->_conf.get_val<int64_t>("rgw_sfs_wal_checkpoint_passive_frames")) {
    // Don't checkpoint unless WAL > rgw_sfs_wal_checkpoint_passive_frames
    // (1000, or ~4MB by default)
    return SQLITE_OK;
  }
  int total_frames = 0;
  int checkpointed_frames = 0;
  int mode = SQLITE_CHECKPOINT_PASSIVE;
  if (frames >
      cct->_conf.get_val<int64_t>("rgw_sfs_wal_checkpoint_truncate_frames")) {
    // Trunate if WAL > rgw_sfs_wal_checkpoint_truncate_frames
    // (4000, or ~16MB by default)
    mode = SQLITE_CHECKPOINT_TRUNCATE;
  }
  int rc = sqlite3_wal_checkpoint_v2(
      db, zDb, mode, &total_frames, &checkpointed_frames
  );
  ldout(cct, SFS_LOG_DEBUG)
      << "[SQLITE] WAL checkpoint ("
      << (mode == SQLITE_CHECKPOINT_PASSIVE ? "passive" : "truncate")
      << ") returned " << rc << " (" << sqlite3_errstr(rc)
      << "), total_frames=" << total_frames
      << ", checkpointed_frames=" << checkpointed_frames << dendl;
  return SQLITE_OK;
}

static int sqlite_profile_callback(
    unsigned int reason, void* ctx, void* vstatement, void* runtime_ptr
) {
  const auto cct = static_cast<CephContext*>(ctx);
  const static std::chrono::milliseconds slowlog_time =
      cct->_conf.get_val<std::chrono::milliseconds>(
          "rgw_sfs_sqlite_profile_slowlog_time"
      );

  if (reason == SQLITE_TRACE_PROFILE) {
    const uint64_t runtime_ns = *static_cast<uint64_t*>(runtime_ptr);
    const int64_t runtime_ms = runtime_ns / 1000 / 1000;
    sqlite3_stmt* statement = static_cast<sqlite3_stmt*>(vstatement);
    const std::unique_ptr<char, void (*)(char*)> sql(
        sqlite3_expanded_sql(statement),
        [](char* p) { sqlite3_free(static_cast<void*>(p)); }
    );
    const sqlite3* db = sqlite3_db_handle(statement);
    const char* sql_str = sql ? sql.get() : sqlite3_sql(statement);

    if (runtime_ms > slowlog_time.count()) {
      lsubdout(cct, rgw_sfs, SFS_LOG_INFO)
          << fmt::format(
                 "[SQLITE SLOW QUERY] {} {:L}ms {}", fmt::ptr(db), runtime_ms,
                 sql_str
             )
          << dendl;
    }
    lsubdout(cct, rgw_sfs, SFS_LOG_TRACE)
        << fmt::format(
               "[SQLITE PROFILE] {} {:L}ms {}", fmt::ptr(db), runtime_ms,
               sql_str
           )
        << dendl;
    perfcounter_prom_time_hist->hinc(
        l_rgw_prom_sfs_sqlite_profile, runtime_ns, 1
    );
    perfcounter_prom_time_sum->tinc(
        l_rgw_prom_sfs_sqlite_profile, timespan(runtime_ns)
    );
  }

  return 0;
}

DBConn::DBConn(CephContext* _cct)
    : main_thread(std::this_thread::get_id()),
      storage_pool_mutex(),
      cct(_cct),
      profile_enabled(_cct->_conf.get_val<bool>("rgw_sfs_sqlite_profile")) {
  maybe_rename_database_file();
  sqlite3_config(SQLITE_CONFIG_LOG, &sqlite_error_callback, cct);
  // get_storage() relies on there already being an entry in the pool
  // for the main thread (i.e. the thread that created the DBConn).
  storage_pool.emplace(main_thread, _make_storage(getDBPath(cct)));
  StorageRef storage = get_storage();
  storage->on_open = [this](sqlite3* db) {
    // This is safe because we're either in the main thread, or inside
    // storage->on_open() called from get_storage(), which has the exclusive
    // mutex.
    sqlite_conns.emplace_back(db);

    sqlite3_extended_result_codes(db, 1);
    sqlite3_busy_timeout(db, 10000);
    sqlite3_exec(
        db,
        fmt::format(
            "PRAGMA journal_mode=WAL;"
            "PRAGMA synchronous=normal;"
            "PRAGMA temp_store = memory;"
            "PRAGMA case_sensitive_like=ON;"
            "PRAGMA mmap_size = 30000000000;"
            "PRAGMA journal_size_limit = {};",
            cct->_conf.get_val<int64_t>("rgw_sfs_wal_size_limit")
        )
            .c_str(),
        0, 0, 0
    );
    if (!cct->_conf.get_val<bool>("rgw_sfs_wal_checkpoint_use_sqlite_default"
        )) {
      sqlite3_wal_hook(db, sqlite_wal_hook_callback, this->cct);
    }
    if (this->profile_enabled) {
      sqlite3_trace_v2(
          db, SQLITE_TRACE_PROFILE, &sqlite_profile_callback, this->cct
      );
    }
  };
  storage->open_forever();
  storage->busy_timeout(5000);
  maybe_upgrade_metadata();
  check_metadata_is_compatible();
  storage->sync_schema();
}

StorageRef DBConn::get_storage() {
  std::thread::id this_thread = std::this_thread::get_id();
  try {
    std::shared_lock lock(storage_pool_mutex);
    return &storage_pool.at(this_thread);
  } catch (const std::out_of_range& ex) {
    std::unique_lock lock(storage_pool_mutex);
    auto [it, _] =
        storage_pool.emplace(this_thread, storage_pool.at(main_thread));
    StorageRef storage = &(*it).second;
    // A copy of the main thread's Storage object won't have an open DB
    // connection yet, so we'd better make it have one (otherwise we're
    // back to a gadzillion sqlite3_open()/sqlite3_close() calls again)
    storage->open_forever();
    storage->busy_timeout(5000);
    lsubdout(cct, rgw, 10) << "[SQLITE CONNECTION NEW] Added Storage "
                           << storage << " to pool for thread " << std::hex
                           << this_thread << std::dec << dendl;
    return storage;
  }
}

void DBConn::check_metadata_is_compatible() const {
  bool sync_error = false;
  std::string result_message;
  std::string temporary_db_path(get_temporary_db_path(cct));
  // create a copy of the actual metadata
  sqlite3* temporary_db;
  int rc = sqlite3_open(temporary_db_path.c_str(), &temporary_db);
  if (rc == SQLITE_OK) {
    sqlite3_backup* backup =
        sqlite3_backup_init(temporary_db, "main", first_sqlite_conn(), "main");
    if (backup) {
      sqlite3_backup_step(backup, -1);
      sqlite3_backup_finish(backup);
    }
    rc = sqlite3_errcode(temporary_db);
    sqlite3_close(temporary_db);
  }
  if (rc == SQLITE_OK) {
    // try to sync the storage based on the temporary db
    // in case something goes wrong show possible errors and return
    auto test_storage = _make_storage(temporary_db_path);
    test_storage.open_forever();
    test_storage.busy_timeout(5000);
    try {
      auto sync_res = test_storage.sync_schema();
      std::vector<std::string> non_compatible_tables;
      for (auto const& [table_name, sync_result] : sync_res) {
        if (sync_result == orm::sync_schema_result::dropped_and_recreated) {
          // this result is aggressive as it drops the table and
          // recreates it.
          // Data loss is expected and we should warn the user and
          // stop the final sync in the real database.
          result_message +=
              "Table: [" + table_name + "] is no longer compatible. ";
          non_compatible_tables.push_back(table_name);
        }
      }
      if (non_compatible_tables.size() > 0) {
        sync_error = true;
        result_message = "Tables: [ ";
        for (auto const& table : non_compatible_tables) {
          result_message += table + " ";
        }
        result_message += "] are no longer compatible.";
      }
    } catch (std::exception& e) {
      // check for any other errors (foreign keys constrains, etc...)
      result_message =
          "Metadata database might be corrupted or is no longer compatible";
      sync_error = true;
    }
  } else {
    sync_error = true;
    result_message = sqlite3_errstr(rc);
  }
  // remove the temporary db
  fs::remove(temporary_db_path);

  // if there was a sync issue, throw an exception
  if (sync_error) {
    throw sqlite_sync_exception(
        "ERROR ACCESSING SFS METADATA. " + result_message
    );
  }
}

static int get_version(CephContext* cct, StorageRef storage) {
  try {
    return storage->pragma.user_version();
  } catch (const std::system_error& e) {
    lsubdout(cct, rgw_sfs, SFS_LOG_ERROR)
        << "error opening db: " << e.code().message() << " ("
        << e.code().value() << "), " << e.what() << dendl;
    throw e;
  }
}

static int upgrade_metadata_from_v1(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'bucket_id' TEXT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'state' INTEGER NOT NULL ,"
          "'state_change_time' INTEGER NOT NULL,"
          "'object_name' TEXT NOT NULL,"
          "'object_uuid' TEXT NOT NULL,"
          "'meta_str' TEXT NOT NULL,"
          "'owner_id' TEXT NOT NULL,"
          "'owner_display_name' TEXT NOT NULL,"
          "'mtime' INTEGER NOT NULL,"
          "'attrs' BLOB NOT NULL,"
          "'placement_name' TEXT NOT NULL,"
          "'placement_storage_class' TEXT NOT NULL,"
          "UNIQUE(upload_id),"
          "UNIQUE(bucket_id, upload_id),"
          "UNIQUE(object_uuid),"
          "FOREIGN KEY('bucket_id') REFERENCES '{}' ('bucket_id')"
          ")",
          MULTIPARTS_TABLE, BUCKETS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_TABLE, sqlite3_errmsg(db)
      );
    }
    return -1;
  }

  rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'part_num' INTEGER NOT NULL,"
          "'len' INTEGER NOT NULL,"
          "'etag' TEXT,"
          "'mtime' INTEGER,"
          "UNIQUE(upload_id, part_num),"
          "FOREIGN KEY('upload_id') REFERENCES '{}'('upload_id')"
          ")",
          MULTIPARTS_PARTS_TABLE, MULTIPARTS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_PARTS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }
  return 0;
}

static int upgrade_metadata_from_v2(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "ALTER TABLE {} RENAME COLUMN len TO size", MULTIPARTS_PARTS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error updating '{}' table: {}", MULTIPARTS_PARTS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }
  return 0;
}

static int upgrade_metadata_from_v4(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "ALTER TABLE {} ADD COLUMN mtime INTEGER NOT NULL DEFAULT 0",
          BUCKETS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );

  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating column 'mtime' in table '{}': {}", BUCKETS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }

  return 0;
}

static void upgrade_metadata(
    CephContext* cct, StorageRef storage, sqlite3* db
) {
  while (true) {
    int cur_version = get_version(cct, storage);
    ceph_assert(cur_version <= SFS_METADATA_VERSION);
    ceph_assert(cur_version >= SFS_METADATA_MIN_VERSION);
    if (cur_version == SFS_METADATA_VERSION) {
      break;
    }

    std::string errmsg;
    int rc = 0;
    if (cur_version == 1) {
      rc = upgrade_metadata_from_v1(db, &errmsg);
    } else if (cur_version == 2) {
      rc = upgrade_metadata_from_v2(db, &errmsg);
    } else if (cur_version == 4) {
      rc = upgrade_metadata_from_v4(db, &errmsg);
    }

    if (rc < 0) {
      auto err = fmt::format(
          "Error upgrading from version {}: {}", cur_version, errmsg
      );
      lsubdout(cct, rgw_sfs, SFS_LOG_ERROR) << err << dendl;
      throw sqlite_sync_exception(err);
    }

    lsubdout(cct, rgw_sfs, SFS_LOG_INFO)
        << fmt::format(
               "upgraded metadata from version {} to version {}", cur_version,
               cur_version + 1
           )
        << dendl;
    storage->pragma.user_version(cur_version + 1);
  }
}

void DBConn::maybe_upgrade_metadata() {
  StorageRef storage = get_storage();
  int db_version = get_version(cct, storage);
  lsubdout(cct, rgw_sfs, SFS_LOG_INFO)
      << "db user version: " << db_version << dendl;

  if (db_version == 0) {
    // must have just been created, set version!
    storage->pragma.user_version(SFS_METADATA_VERSION);
  } else if (db_version < SFS_METADATA_VERSION && db_version >= SFS_METADATA_MIN_VERSION) {
    // perform schema update
    upgrade_metadata(cct, storage, first_sqlite_conn());
  } else if (db_version < SFS_METADATA_MIN_VERSION) {
    throw sqlite_sync_exception(
        "Existing metadata too far behind! Unable to upgrade schema!"
    );
  } else if (db_version > SFS_METADATA_VERSION) {
    // we won't be able to read a database in the future.
    throw sqlite_sync_exception(
        "Existing metadata too far ahead! Please upgrade!"
    );
  }
}

void DBConn::maybe_rename_database_file() const {
  if (!std::filesystem::exists(getLegacyDBPath(cct))) {
    return;
  }
  if (std::filesystem::exists(getDBPath(cct))) {
    return;
  }

  lsubdout(cct, rgw_sfs, SFS_LOG_STARTUP)
      << fmt::format(
             "Migrating legacy database file {} -> {}", getLegacyDBPath(cct),
             getDBPath(cct)
         )
      << dendl;

  dbapi::sqlite::database src_db(getLegacyDBPath(cct));
  dbapi::sqlite::database dst_db(getDBPath(cct));
  auto state =
      std::unique_ptr<sqlite3_backup, decltype(&sqlite3_backup_finish)>(
          sqlite3_backup_init(
              dst_db.connection().get(), "main", src_db.connection().get(),
              "main"
          ),
          sqlite3_backup_finish
      );

  if (!state) {
    lsubdout(cct, rgw_sfs, SFS_LOG_ERROR)
        << fmt::format(
               "Error opening legacy database file {} {}. Please migrate "
               "s3gw.db to sfs.db manually",
               getLegacyDBPath(cct), sqlite3_errmsg(dst_db.connection().get())
           )
        << dendl;
    ceph_abort_msg("sfs database file migration failed");
  }

  int rc = sqlite3_backup_step(state.get(), -1);
  if (rc != SQLITE_DONE) {
    lsubdout(cct, rgw_sfs, SFS_LOG_ERROR)
        << fmt::format(
               "Error migrating legacy database file {} {}. Please migrate "
               "s3gw.db to sfs.db manually",
               getLegacyDBPath(cct), sqlite3_errmsg(dst_db.connection().get())
           )
        << dendl;
    ceph_abort_msg("sfs database file migration failed");
  }

  std::error_code ec;  // ignore errors
  fs::remove(getLegacyDBPath(cct), ec);
  fs::remove(getLegacyDBPath(cct) + "-wal", ec);
  fs::remove(getLegacyDBPath(cct) + "-shm", ec);

  lsubdout(cct, rgw_sfs, SFS_LOG_STARTUP)
      << fmt::format(
             "Done migrating legacy database. Continuing startup with {}",
             getDBPath(cct)
         )
      << dendl;
}
}  // namespace rgw::sal::sfs::sqlite
