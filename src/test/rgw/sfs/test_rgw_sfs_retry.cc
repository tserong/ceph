// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/core.h>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "driver/sfs/sqlite/dbapi.h"
#include "driver/sfs/sqlite/retry.h"
#include "driver/sfs/sqlite/sqlite_orm.h"
#include "gtest/gtest.h"
#include "rgw/rgw_sal_sfs.h"
#include "rgw_common.h"

using namespace rgw::sal::sfs::sqlite;

class TestSFSRetrySQLite : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;

  TestSFSRetrySQLite() : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)) {
    cct->_log->start();
    rgw_perf_start(cct.get());
  }
  void SetUp() override {}

  void TearDown() override {}
};

// TODO(https://github.com/aquarist-labs/s3gw/issues/788) Remove
// TestSFSRetrySQLiteOrm tests

class TestSFSRetrySQLiteOrm : public TestSFSRetrySQLite {};
class TestSFSRetrySQLiteOrmDeathTest : public TestSFSRetrySQLiteOrm {};

TEST_F(TestSFSRetrySQLiteOrm, retry_non_crit_till_failure) {
  auto exception =
      std::system_error{SQLITE_BUSY, sqlite_orm::get_sqlite_error_category()};
  RetrySQLiteBusy<int> uut([&]() {
    throw exception;
    return 0;
  });
  EXPECT_EQ(uut.run(), std::nullopt);
  EXPECT_FALSE(uut.successful());
  EXPECT_EQ(uut.failed_error(), exception.code().value());
  EXPECT_GT(uut.retries(), 0);
}

TEST_F(TestSFSRetrySQLiteOrmDeathTest, crit_aborts) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto exception = std::system_error{
      SQLITE_CORRUPT, sqlite_orm::get_sqlite_error_category()
  };
  RetrySQLiteBusy<int> uut([&]() {
    throw exception;
    return 0;
  });
  ASSERT_DEATH(uut.run(), "Critical SQLite error");
}

TEST_F(TestSFSRetrySQLiteOrm, simple_return_succeeds_immediately) {
  RetrySQLiteBusy<int> uut([&]() { return 42; });
  EXPECT_EQ(uut.run(), 42);
  EXPECT_TRUE(uut.successful());
  EXPECT_EQ(uut.retries(), 0);
}

TEST_F(TestSFSRetrySQLiteOrm, retry_second_time_success) {
  auto exception =
      std::system_error{SQLITE_BUSY, sqlite_orm::get_sqlite_error_category()};
  bool first = true;
  RetrySQLiteBusy<int> uut([&]() {
    if (first) {
      first = false;
      throw exception;
    } else {
      return 23;
    }
  });
  EXPECT_EQ(uut.run(), 23);
  EXPECT_TRUE(uut.successful());
  EXPECT_NE(uut.failed_error(), exception.code().value());
  EXPECT_EQ(uut.retries(), 1);
}

class TestSFSRetrySQLiteDeathTest : public TestSFSRetrySQLite {};

TEST_F(TestSFSRetrySQLite, retry_non_crit_till_failure) {
  auto exception = rgw::sal::sfs::dbapi::sqlite::errors::busy(SQLITE_BUSY, "");
  RetrySQLiteBusy<int> uut([&]() {
    throw exception;
    return 0;
  });
  EXPECT_EQ(uut.run(), std::nullopt);
  EXPECT_FALSE(uut.successful());
  EXPECT_EQ(uut.failed_error(), exception.get_code());
  EXPECT_GT(uut.retries(), 0);
}

TEST_F(TestSFSRetrySQLite, retry_non_crit_extended_till_failure) {
  auto exception = rgw::sal::sfs::dbapi::sqlite::errors::busy_snapshot(
      SQLITE_BUSY_SNAPSHOT, ""
  );
  RetrySQLiteBusy<int> uut([&]() {
    throw exception;
    return 0;
  });
  EXPECT_EQ(uut.run(), std::nullopt);
  EXPECT_FALSE(uut.successful());
  EXPECT_EQ(uut.failed_error(), exception.get_code());
  EXPECT_GT(uut.retries(), 0);
}

TEST_F(TestSFSRetrySQLiteDeathTest, crit_aborts) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  auto exception = std::system_error{
      SQLITE_CORRUPT, sqlite_orm::get_sqlite_error_category()
  };
  RetrySQLiteBusy<int> uut([&]() {
    rgw::sal::sfs::dbapi::sqlite::errors::throw_sqlite_error(SQLITE_CORRUPT);
    return 0;
  });
  ASSERT_DEATH(uut.run(), "Critical SQLite error");
}

TEST_F(TestSFSRetrySQLite, simple_return_succeeds_immediately) {
  RetrySQLiteBusy<int> uut([&]() { return 42; });
  EXPECT_EQ(uut.run(), 42);
  EXPECT_TRUE(uut.successful());
  EXPECT_EQ(uut.retries(), 0);
}

TEST_F(TestSFSRetrySQLite, retry_second_time_success) {
  auto exception = rgw::sal::sfs::dbapi::sqlite::sqlite_exception(
      SQLITE_BUSY, "", "non critical error"
  );
  bool first = true;
  RetrySQLiteBusy<int> uut([&]() {
    if (first) {
      first = false;
      throw exception;
    } else {
      return 23;
    }
  });
  EXPECT_EQ(uut.run(), 23);
  EXPECT_TRUE(uut.successful());
  EXPECT_NE(uut.failed_error(), exception.get_code());
  EXPECT_EQ(uut.retries(), 1);
}
