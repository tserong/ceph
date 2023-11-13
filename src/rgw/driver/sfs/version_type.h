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
#ifndef RGW_SFS_VERSION_TYPE_H
#define RGW_SFS_VERSION_TYPE_H

#include "sqlite/dbapi.h"

namespace rgw::sal::sfs {

enum class VersionType {
  REGULAR = 0,
  DELETE_MARKER,
  LAST_VALUE = DELETE_MARKER
};

template <>
struct dbapi::sqlite::has_sqlite_type<VersionType, SQLITE_INTEGER, void>
    : ::std::true_type {};

inline int bind_col_in_db(
    sqlite3_stmt* stmt, int inx, const rgw::sal::sfs::VersionType& val
) {
  return sqlite3_bind_int(stmt, inx, static_cast<int>(val));
}
inline void store_result_in_db(
    sqlite3_context* db, const rgw::sal::sfs::VersionType& val
) {
  sqlite3_result_int(db, static_cast<int>(val));
}
inline rgw::sal::sfs::VersionType
get_col_from_db(sqlite3_stmt* stmt, int inx, dbapi::sqlite::result_type<rgw::sal::sfs::VersionType>) {
  if (sqlite3_column_type(stmt, inx) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return static_cast<rgw::sal::sfs::VersionType>(sqlite3_column_int(stmt, inx));
}

inline rgw::sal::sfs::VersionType
get_val_from_db(sqlite3_value* value, dbapi::sqlite::result_type<rgw::sal::sfs::VersionType>) {
  if (sqlite3_value_type(value) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return static_cast<rgw::sal::sfs::VersionType>(sqlite3_value_int(value));
}

}  // namespace rgw::sal::sfs

#endif  // RGW_SFS_VERSION_TYPE_H
