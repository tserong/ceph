// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;

const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSConnectionPool : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct;
  const fs::path test_dir;
  std::unique_ptr<rgw::sal::SFStore> store;

  TestSFSConnectionPool()
      : cct(new CephContext(CEPH_ENTITY_TYPE_ANY)),
        test_dir(fs::temp_directory_path() / TEST_DIR) {
    fs::create_directory(test_dir);
    cct->_conf.set_val("rgw_sfs_data_path", test_dir);
    cct->_log->start();
    store.reset(new rgw::sal::SFStore(cct.get(), test_dir));
  }

  ~TestSFSConnectionPool() override {
    store.reset();
    fs::remove_all(test_dir);
  }
};

TEST_F(TestSFSConnectionPool, verify_one_connection_per_thread) {
  DBConnRef conn = store->db_conn;

  // At this point there should be only one connection in the pool.
  // Alas, DBConn::storage_pool is private, but DBConn::all_sqlite_conns
  // is a reasonably proxy for this.
  EXPECT_EQ(conn->all_sqlite_conns().size(), 1);

  std::set<StorageRef> storages;
  storages.emplace(conn->get_storage());

  // Having now called get_storage from the main thread, we should
  // still have only one connection.
  EXPECT_EQ(conn->all_sqlite_conns().size(), 1);

  std::shared_mutex mutex;
  std::vector<std::thread> threads;
  const size_t num_threads = 10;
  for (size_t i = 0; i < num_threads; ++i) {
    std::thread t([&]() {
      // Multiple calls to get_storage() in a new thread should return
      // the same pointer...
      StorageRef s1 = conn->get_storage();
      StorageRef s2 = conn->get_storage();
      EXPECT_EQ(s1, s2);

      // ...and that pointer shouldn't be in use by any other thread.
      std::unique_lock lock(mutex);
      EXPECT_EQ(storages.find(s1), storages.end());

      // Now we have to save it so this check works for the other threads.
      storages.emplace(s1);
    });
    threads.push_back(std::move(t));
  }
  for (size_t i = 0; i < num_threads; ++i) {
    threads[i].join();
  }

  // Now there should be the original connection, plus ten more
  EXPECT_EQ(conn->all_sqlite_conns().size(), 1 + num_threads);
}
