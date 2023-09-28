// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs;

namespace fs = std::filesystem;

const static std::string TEST_DIR = "rgw_sfs_tests";
const std::uintmax_t SIZE_1MB = 1024 * 1024;

// See https://stackoverflow.com/questions/16190078/how-to-atomically-update-a-maximum-value
template <typename T>
void update_maximum(std::atomic<T>& maximum_value, T const& value) noexcept {
  T prev_value = maximum_value;
  while (prev_value < value &&
         !maximum_value.compare_exchange_weak(prev_value, value)) {
  }
}

class TestSFSWALCheckpoint : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path test_dir;
  std::unique_ptr<rgw::sal::SFStore> store;
  BucketRef bucket;

  TestSFSWALCheckpoint()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        test_dir(fs::temp_directory_path() / TEST_DIR) {
    fs::create_directory(test_dir);
    cct->_conf.set_val("rgw_sfs_data_path", test_dir);
    cct->_log->start();
  }

  ~TestSFSWALCheckpoint() override {
    store.reset();
    fs::remove_all(test_dir);
  }

  // Ordinarily this would just go in the TestSFSWALCheckpoint constructor.
  // Unfortunately our tests need to tweak config settings that must be
  // done _before_ the SFStore is created so that they're in place when
  // DBConn's storage->on_open handler is invoked, so each test has to
  // call this function explicitly.
  void init_store() {
    store.reset(new rgw::sal::SFStore(cct.get(), test_dir));

    sqlite::SQLiteUsers users(store->db_conn);
    sqlite::DBOPUserInfo user;
    user.uinfo.user_id.id = "testuser";
    user.uinfo.display_name = "display_name";
    users.store_user(user);

    sqlite::SQLiteBuckets db_buckets(store->db_conn);
    sqlite::DBOPBucketInfo db_binfo;
    db_binfo.binfo.bucket = rgw_bucket("", "testbucket", "1234");
    db_binfo.binfo.owner = rgw_user("testuser");
    db_binfo.binfo.creation_time = ceph::real_clock::now();
    db_binfo.binfo.placement_rule = rgw_placement_rule();
    db_binfo.binfo.zonegroup = "";
    db_binfo.deleted = false;
    db_buckets.store_bucket(db_binfo);
    RGWUserInfo bucket_owner;

    bucket = std::make_shared<Bucket>(
        cct.get(), store.get(), db_binfo.binfo, bucket_owner, db_binfo.battrs
    );
  }

  // This will spawn num_threads threads, each creating num_objects objects,
  // and will record and return the maximum size the WAL reaches while this
  // is ongoing.
  std::uintmax_t multithread_object_create(
      size_t num_threads, size_t num_objects
  ) {
    std::atomic<std::uintmax_t> max_wal_size{0};
    fs::path wal(test_dir / "s3gw.db-wal");

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
      std::thread t([&, i]() {
        for (size_t j = 0; j < num_objects; ++j) {
          ObjectRef obj;
          while (!obj) {
            obj = bucket->create_version(rgw_obj_key(
                "object-" + std::to_string(i) + "-" + std::to_string(j)
            ));
          }
          obj->metadata_finish(store.get(), false);
          update_maximum(max_wal_size, fs::file_size(wal));
        }
      });
      threads.push_back(std::move(t));
    }
    for (size_t i = 0; i < num_threads; ++i) {
      threads[i].join();
    }
    return max_wal_size;
  }
};

// This test checks whether SQLite's default checkpoint
// mechanism suffers from the WAL growth problem.
TEST_F(TestSFSWALCheckpoint, test_default_wal_checkpoint) {
  cct->_conf.set_val("rgw_sfs_wal_checkpoint_use_sqlite_default", "true");
  cct->_conf.set_val("rgw_sfs_wal_size_limit", "-1");
  init_store();

  // Prior to the change to use only a single DB connection,
  // 10 concurrent writer threads would easily push us past
  // 500MB.  Since using only a single connection the problem
  // goes away and we should never go over about 4MB (we're
  // allowing up to 5MB here for a bit of wiggle room).
  std::uintmax_t max_wal_size = multithread_object_create(10, 1000);
  EXPECT_LE(max_wal_size, SIZE_1MB * 5);

  // The fact that we have no size limit set means the WAL
  // won't be truncated even when the last writer completes,
  // so the file size now should be the same as the maximum
  // size it reached during the above write operation.
  EXPECT_EQ(fs::file_size(test_dir / "s3gw.db-wal"), max_wal_size);
}

// This test proves that our SFS checkpoint mechanism works
// the same as the default checkpoint mechanism, but that the
// WAL is truncated to 4MB by default.
TEST_F(TestSFSWALCheckpoint, test_sfs_wal_checkpoint) {
  init_store();

  // As with the default checkpoint mechanism, this can go
  // over 4MB, but shouldn't go over by much (hence the 5MB
  // limit)
  std::uintmax_t max_wal_size = multithread_object_create(10, 1000);
  EXPECT_LT(max_wal_size, SIZE_1MB * 5);

  // Once the writes are all done, the WAL should be finally
  // truncated to 4MB or less
  EXPECT_LE(fs::file_size(test_dir / "s3gw.db-wal"), SIZE_1MB * 4);
}

// This test proves that our SFS checkpoint mechanism can let
// the WAL get bigger than 4MB if configured to do so, but that
// it's still ultimately truncated eventually.
TEST_F(TestSFSWALCheckpoint, test_sfs_wal_checkpoint_16mb) {
  cct->_conf.set_val("rgw_sfs_wal_checkpoint_passive_frames", "4000");
  init_store();

  // Given we're checkpointing at 4000 frames (16MB), the WAL will
  // surely grow to at least somewhere near that size before being
  // checkpointed.
  std::uintmax_t max_wal_size = multithread_object_create(10, 1000);
  EXPECT_GE(max_wal_size, SIZE_1MB * 12);

  // Once the writes are all done, the WAL should be finally
  // either truncated to to 4MB, or be less than 16MB (in case it
  // truncated part way through the write operation, but didn't get
  // up to the 16MB limit again yet before we finished).
  EXPECT_LT(fs::file_size(test_dir / "s3gw.db-wal"), SIZE_1MB * 16);
}
