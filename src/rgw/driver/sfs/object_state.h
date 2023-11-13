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
#ifndef RGW_SFS_OBJECT_STATE_H
#define RGW_SFS_OBJECT_STATE_H

#include <iostream>

#include "include/ceph_assert.h"
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif
#include "sqlite/dbapi.h"

namespace rgw::sal::sfs {

enum class ObjectState { OPEN = 0, COMMITTED, DELETED, LAST_VALUE = DELETED };

inline std::string str_object_state(ObjectState state) {
  std::string result;
  switch (state) {
    case rgw::sal::sfs::ObjectState::OPEN:
      result.append("O");
      break;
    case rgw::sal::sfs::ObjectState::COMMITTED:
      result.append("C");
      break;
    case rgw::sal::sfs::ObjectState::DELETED:
      result.append("D");
      break;
    default:
      result.append("?");
      break;
  }
  result.append("(");
  result.append(std::to_string(
      static_cast<std::underlying_type<rgw::sal::sfs::ObjectState>::type>(state)
  ));
  result.append(")");
  return result;
}

template <>
struct dbapi::sqlite::has_sqlite_type<ObjectState, SQLITE_INTEGER, void>
    : ::std::true_type {};

inline int bind_col_in_db(
    sqlite3_stmt* stmt, int inx, const rgw::sal::sfs::ObjectState& val
) {
  return sqlite3_bind_int(stmt, inx, static_cast<int>(val));
}
inline void store_result_in_db(
    sqlite3_context* db, const rgw::sal::sfs::ObjectState& val
) {
  sqlite3_result_int(db, static_cast<int>(val));
}
inline rgw::sal::sfs::ObjectState
get_col_from_db(sqlite3_stmt* stmt, int inx, dbapi::sqlite::result_type<rgw::sal::sfs::ObjectState>) {
  if (sqlite3_column_type(stmt, inx) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return static_cast<rgw::sal::sfs::ObjectState>(sqlite3_column_int(stmt, inx));
}

inline rgw::sal::sfs::ObjectState
get_val_from_db(sqlite3_value* value, dbapi::sqlite::result_type<rgw::sal::sfs::ObjectState>) {
  if (sqlite3_value_type(value) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return static_cast<rgw::sal::sfs::ObjectState>(sqlite3_value_int(value));
}

}  // namespace rgw::sal::sfs

inline std::ostream& operator<<(
    std::ostream& out, rgw::sal::sfs::ObjectState o
) {
  out << rgw::sal::sfs::str_object_state(o);
  return out;
}

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<rgw::sal::sfs::ObjectState> : fmt::ostream_formatter {};
#endif

#endif  // RGW_SFS_OBJECT_STATE_H
