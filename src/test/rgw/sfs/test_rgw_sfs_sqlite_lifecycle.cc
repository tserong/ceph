// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <random>

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_lifecycle.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";
const static std::string LC_SHARD = "lc.0";

class TestSFSSQLiteLifecycle : public ::testing::Test {
 protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string& base_dir) const {
    return fs::path(base_dir) / DB_FILENAME;
  }

  fs::path getDBFullPath() const { return getDBFullPath(getTestDir()); }
};

std::string getLCBucketName(const rgw::sal::sfs::sqlite::DBOPBucketInfo& bucket
) {
  return ":" + bucket.binfo.bucket.name + ":" + bucket.binfo.bucket.marker;
}

TEST_F(TestSFSSQLiteLifecycle, GetHeadFirstTime) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  // first time we call get_head there is no head and we create an empty one
  auto lc_head = db_lc->get_head(LC_SHARD);
  ASSERT_EQ(lc_head.lc_index, LC_SHARD);
  ASSERT_EQ(lc_head.marker, "");
  ASSERT_EQ(lc_head.start_date, 0);
}

TEST_F(TestSFSSQLiteLifecycle, StoreHead) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  DBOPLCHead db_head{LC_SHARD, "bucket_name_marker", 12345};
  db_lc->store_head(db_head);

  auto lc_head = db_lc->get_head(LC_SHARD);
  ASSERT_EQ(lc_head.lc_index, LC_SHARD);
  ASSERT_EQ(lc_head.marker, "bucket_name_marker");
  ASSERT_EQ(lc_head.start_date, 12345);
}

TEST_F(TestSFSSQLiteLifecycle, StoreDeleteHead) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  DBOPLCHead db_head{LC_SHARD, "bucket_name_marker", 12345};
  db_lc->store_head(db_head);

  auto lc_head = db_lc->get_head(LC_SHARD);
  ASSERT_EQ(lc_head.lc_index, LC_SHARD);
  ASSERT_EQ(lc_head.marker, "bucket_name_marker");
  ASSERT_EQ(lc_head.start_date, 12345);

  db_lc->remove_head(LC_SHARD);
  // now head is not stored so we create an empty one when calling get
  lc_head = db_lc->get_head(LC_SHARD);
  ASSERT_EQ(lc_head.lc_index, LC_SHARD);
  ASSERT_EQ(lc_head.marker, "");
  ASSERT_EQ(lc_head.start_date, 0);
}

TEST_F(TestSFSSQLiteLifecycle, StoreGetEntry) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  // first look for a non existing entry
  auto db_entry_not_found = db_lc->get_entry(LC_SHARD, "bucket_name_marker");
  ASSERT_FALSE(db_entry_not_found.has_value());

  // store entry
  DBOPLCEntry db_entry{LC_SHARD, "bucket_name_marker", 12345, 123};
  db_lc->store_entry(db_entry);

  auto db_entry_found = db_lc->get_entry(LC_SHARD, "bucket_name_marker");
  ASSERT_TRUE(db_entry_found.has_value());
  ASSERT_EQ(db_entry_found->lc_index, LC_SHARD);
  ASSERT_EQ(db_entry_found->bucket_name, "bucket_name_marker");
  ASSERT_EQ(db_entry_found->start_time, 12345);
  ASSERT_EQ(db_entry_found->status, 123);
}

TEST_F(TestSFSSQLiteLifecycle, GetNextEntry) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  // returns nothing when there are no entries
  auto no_entries = db_lc->get_next_entry(LC_SHARD, "");
  ASSERT_FALSE(no_entries.has_value());

  // store a few entries
  DBOPLCEntry db_entry_1{LC_SHARD, "bucket_1", 1111, 1};
  DBOPLCEntry db_entry_2{LC_SHARD, "bucket_2", 2222, 2};
  DBOPLCEntry db_entry_3{LC_SHARD, "bucket_3", 3333, 3};
  DBOPLCEntry db_entry_4{LC_SHARD, "bucket_4", 4444, 4};
  DBOPLCEntry db_entry_5{LC_SHARD, "a_bucket_5", 5555, 5};
  db_lc->store_entry(db_entry_1);
  db_lc->store_entry(db_entry_2);
  db_lc->store_entry(db_entry_3);
  db_lc->store_entry(db_entry_4);
  db_lc->store_entry(db_entry_5);

  // get next entry with marker = ""
  auto marker_empty = db_lc->get_next_entry(LC_SHARD, "");
  ASSERT_TRUE(marker_empty.has_value());
  ASSERT_EQ(marker_empty->lc_index, LC_SHARD);
  ASSERT_EQ(marker_empty->bucket_name, "a_bucket_5");
  ASSERT_EQ(marker_empty->start_time, 5555);
  ASSERT_EQ(marker_empty->status, 5);

  // get next entry with marker = "a_bucket_5"
  auto a_bucket_5 = db_lc->get_next_entry(LC_SHARD, "a_bucket_5");
  ASSERT_TRUE(a_bucket_5.has_value());
  ASSERT_EQ(a_bucket_5->lc_index, LC_SHARD);
  ASSERT_EQ(a_bucket_5->bucket_name, "bucket_1");
  ASSERT_EQ(a_bucket_5->start_time, 1111);
  ASSERT_EQ(a_bucket_5->status, 1);

  // get next entry with marker = "bucket_1"
  auto bucket_1 = db_lc->get_next_entry(LC_SHARD, "bucket_1");
  ASSERT_TRUE(bucket_1.has_value());
  ASSERT_EQ(bucket_1->lc_index, LC_SHARD);
  ASSERT_EQ(bucket_1->bucket_name, "bucket_2");
  ASSERT_EQ(bucket_1->start_time, 2222);
  ASSERT_EQ(bucket_1->status, 2);

  // get next entry with marker = "bucket_2"
  auto bucket_2 = db_lc->get_next_entry(LC_SHARD, "bucket_2");
  ASSERT_TRUE(bucket_2.has_value());
  ASSERT_EQ(bucket_2->lc_index, LC_SHARD);
  ASSERT_EQ(bucket_2->bucket_name, "bucket_3");
  ASSERT_EQ(bucket_2->start_time, 3333);
  ASSERT_EQ(bucket_2->status, 3);

  // get next entry with marker = "bucket_3"
  auto bucket_3 = db_lc->get_next_entry(LC_SHARD, "bucket_3");
  ASSERT_TRUE(bucket_3.has_value());
  ASSERT_EQ(bucket_3->lc_index, LC_SHARD);
  ASSERT_EQ(bucket_3->bucket_name, "bucket_4");
  ASSERT_EQ(bucket_3->start_time, 4444);
  ASSERT_EQ(bucket_3->status, 4);

  // get next entry with marker = "bucket_4"
  auto bucket_4 = db_lc->get_next_entry(LC_SHARD, "bucket_4");
  ASSERT_FALSE(bucket_4.has_value());
}

TEST_F(TestSFSSQLiteLifecycle, ListEntries) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_lc = std::make_shared<SQLiteLifecycle>(conn);

  // store a few entries
  DBOPLCEntry db_entry_1{LC_SHARD, "bucket_1", 1111, 1};
  DBOPLCEntry db_entry_2{LC_SHARD, "bucket_2", 2222, 2};
  DBOPLCEntry db_entry_3{LC_SHARD, "bucket_3", 3333, 3};
  DBOPLCEntry db_entry_4{LC_SHARD, "bucket_4", 4444, 4};
  DBOPLCEntry db_entry_5{LC_SHARD, "a_bucket_5", 5555, 5};
  db_lc->store_entry(db_entry_1);
  db_lc->store_entry(db_entry_2);
  db_lc->store_entry(db_entry_3);
  db_lc->store_entry(db_entry_4);
  db_lc->store_entry(db_entry_5);

  // list entryes with marker = "" and 1 entry
  auto entries = db_lc->list_entries(LC_SHARD, "", 1);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(entries[0].lc_index, LC_SHARD);
  ASSERT_EQ(entries[0].bucket_name, "a_bucket_5");
  ASSERT_EQ(entries[0].start_time, 5555);
  ASSERT_EQ(entries[0].status, 5);

  entries = db_lc->list_entries(LC_SHARD, "", 2);
  ASSERT_EQ(entries.size(), 2);
  ASSERT_EQ(entries[0].lc_index, LC_SHARD);
  ASSERT_EQ(entries[0].bucket_name, "a_bucket_5");
  ASSERT_EQ(entries[0].start_time, 5555);
  ASSERT_EQ(entries[0].status, 5);

  ASSERT_EQ(entries[1].lc_index, LC_SHARD);
  ASSERT_EQ(entries[1].bucket_name, "bucket_1");
  ASSERT_EQ(entries[1].start_time, 1111);
  ASSERT_EQ(entries[1].status, 1);

  entries = db_lc->list_entries(LC_SHARD, "", 10);
  ASSERT_EQ(entries.size(), 5);
  ASSERT_EQ(entries[0].lc_index, LC_SHARD);
  ASSERT_EQ(entries[0].bucket_name, "a_bucket_5");
  ASSERT_EQ(entries[0].start_time, 5555);
  ASSERT_EQ(entries[0].status, 5);

  ASSERT_EQ(entries[1].lc_index, LC_SHARD);
  ASSERT_EQ(entries[1].bucket_name, "bucket_1");
  ASSERT_EQ(entries[1].start_time, 1111);
  ASSERT_EQ(entries[1].status, 1);

  ASSERT_EQ(entries[2].lc_index, LC_SHARD);
  ASSERT_EQ(entries[2].bucket_name, "bucket_2");
  ASSERT_EQ(entries[2].start_time, 2222);
  ASSERT_EQ(entries[2].status, 2);

  ASSERT_EQ(entries[3].lc_index, LC_SHARD);
  ASSERT_EQ(entries[3].bucket_name, "bucket_3");
  ASSERT_EQ(entries[3].start_time, 3333);
  ASSERT_EQ(entries[3].status, 3);

  ASSERT_EQ(entries[4].lc_index, LC_SHARD);
  ASSERT_EQ(entries[4].bucket_name, "bucket_4");
  ASSERT_EQ(entries[4].start_time, 4444);
  ASSERT_EQ(entries[4].status, 4);

  // list not from the beginning
  entries = db_lc->list_entries(LC_SHARD, "bucket_3", 10);
  ASSERT_EQ(entries.size(), 1);
  ASSERT_EQ(entries[0].lc_index, LC_SHARD);
  ASSERT_EQ(entries[0].bucket_name, "bucket_4");
  ASSERT_EQ(entries[0].start_time, 4444);
  ASSERT_EQ(entries[0].status, 4);
}
