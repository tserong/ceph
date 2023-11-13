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
#pragma once

#include "common/ceph_time.h"
#include "include/ceph_assert.h"
#include "rgw/driver/sfs/sqlite/dbapi.h"
#include "rgw/driver/sfs/sqlite/sqlite_orm.h"

/// ceph::real_time is represented as a uint64 (unsigned).
/// SQLite works with int64 (signed) values, which means we lose 1 bit of
/// resolution.
/// This means max possible time to be stored is 2262-04-11 23:47:16.854775807
/// timestamps are stored with the same resolution as
/// ceph::real_cock::time_point (nanoseconds)
namespace rgw::sal::sfs::sqlite {

static int64_t time_point_to_int64(const ceph::real_time& t) {
  uint64_t nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch())
          .count();
  // we check that the value is not greater than int64 max
  if (nanos > std::numeric_limits<int64_t>::max()) {
    std::stringstream oss;
    oss << "Error converting ceph::real_time to int64. "
           "Nanoseconds value: "
        << nanos << " is out of range";
    throw std::system_error(ERANGE, std::system_category(), oss.str());
  }
  // we can safely static_cast to int64_t now
  return static_cast<int64_t>(nanos);
}

static ceph::real_time time_point_from_int64(int64_t value) {
  if (value < 0) {
    // to ensure that we stick to the int64 positive range.
    std::stringstream oss;
    oss << "Error converting int64 nanoseconds value to "
           "ceph::real_cock::time_point. Value: "
        << value << " is out of range";
    throw std::system_error(ERANGE, std::system_category(), oss.str());
  }
  uint64_t uint64_nanos = static_cast<uint64_t>(value);
  return ceph::real_time(std::chrono::nanoseconds(uint64_nanos));
}

}  // namespace rgw::sal::sfs::sqlite

namespace sqlite_orm {

template <>
struct type_printer<ceph::real_time> : public integer_printer {};

template <>
struct statement_binder<ceph::real_time> {
  int bind(sqlite3_stmt* stmt, int index, const ceph::real_time& value) const {
    return statement_binder<uint64_t>().bind(
        stmt, index, rgw::sal::sfs::sqlite::time_point_to_int64(value)
    );
  }
};

template <>
struct field_printer<ceph::real_time> {
  std::string operator()(const ceph::real_time& t) const {
    auto int_value = rgw::sal::sfs::sqlite::time_point_to_int64(t);
    return std::to_string(int_value);
  }
};

template <>
struct row_extractor<ceph::real_time> {
  ceph::real_time extract(int64_t row_value) const {
    return rgw::sal::sfs::sqlite::time_point_from_int64(row_value);
  }

  ceph::real_time extract(sqlite3_stmt* stmt, int columnIndex) const {
    auto int_value = sqlite3_column_int64(stmt, columnIndex);
    return this->extract(int_value);
  }
};

}  // namespace sqlite_orm

namespace rgw::sal::sfs::dbapi::sqlite {

template <>
struct has_sqlite_type<ceph::real_time, SQLITE_INTEGER, void>
    : ::std::true_type {};

inline int bind_col_in_db(
    sqlite3_stmt* stmt, int inx, const ceph::real_time& val
) {
  return sqlite3_bind_int64(
      stmt, inx, rgw::sal::sfs::sqlite::time_point_to_int64(val)
  );
}
inline void store_result_in_db(
    sqlite3_context* db, const ceph::real_time& val
) {
  sqlite3_result_int64(db, rgw::sal::sfs::sqlite::time_point_to_int64(val));
}
inline ceph::real_time
get_col_from_db(sqlite3_stmt* stmt, int inx, result_type<ceph::real_time>) {
  if (sqlite3_column_type(stmt, inx) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return rgw::sal::sfs::sqlite::time_point_from_int64(
      sqlite3_column_int64(stmt, inx)
  );
}

inline ceph::real_time
get_val_from_db(sqlite3_value* value, result_type<ceph::real_time>) {
  if (sqlite3_value_type(value) == SQLITE_NULL) {
    ceph_abort_msg("cannot make enum value from NULL");
  }
  return rgw::sal::sfs::sqlite::time_point_from_int64(sqlite3_value_int64(value)
  );
}
}  // namespace rgw::sal::sfs::dbapi::sqlite
