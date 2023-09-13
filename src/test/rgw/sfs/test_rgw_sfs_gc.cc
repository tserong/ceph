// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "driver/sfs/object_state.h"
#include "driver/sfs/sqlite/buckets/multipart_definitions.h"
#include "driver/sfs/sqlite/sqlite_multipart.h"
#include "driver/sfs/version_type.h"
#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/sfs_gc.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_conversions.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/driver/sfs/uuid_path.h"
#include "rgw/rgw_perf_counters.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;
using namespace std::this_thread;
using namespace std::chrono_literals;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";
const static std::string TEST_USERNAME = "test_user";

class TestSFSGC : public ::testing::Test {
 protected:
  const std::unique_ptr<CephContext> cct =
      std::unique_ptr<CephContext>(new CephContext(CEPH_ENTITY_TYPE_ANY));

  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
    cct->_conf.set_val("rgw_sfs_data_path", getTestDir());
    cct->_log->start();
    rgw_perf_start(cct.get());
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
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) / db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const { return getDBFullPath(getTestDir()); }

  std::size_t getStoreDataFileCount() {
    using std::filesystem::recursive_directory_iterator;
    return std::count_if(
        recursive_directory_iterator(getTestDir()),
        recursive_directory_iterator{},
        [](const std::filesystem::path& path) {
          return (
              std::filesystem::is_regular_file(path) &&
              !path.filename().string().starts_with("s3gw.db")
          );
        }
    );
  }

  std::size_t databaseFileExists() {
    return std::filesystem::exists(getDBFullPath());
  }

  void createTestUser(DBConnRef conn) {
    SQLiteUsers users(conn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = TEST_USERNAME;
    users.store_user(user);
  }

  void storeRandomFile(const std::filesystem::path& file_path) {
    std::filesystem::create_directories(file_path.parent_path());
    auto mode = std::ofstream::binary | std::ofstream::out | std::ofstream::app;
    std::ofstream ofs(file_path, mode);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(1, 4096);
    auto file_size = dist(gen);
    while (file_size) {
      ofs << dist(gen);
      --file_size;
    }
    ofs.flush();
    ofs.close();
  }

  void storeRandomPart(const uuid_d& uuid, int id) {
    rgw::sal::sfs::MultipartPartPath pp(uuid, id);
    auto part_path = getTestDir() / pp.to_path();
    storeRandomFile(part_path);
  }

  void storeRandomObjectVersion(
      const std::shared_ptr<rgw::sal::sfs::Object>& object
  ) {
    std::filesystem::path object_path =
        getTestDir() / object->get_storage_path();
    storeRandomFile(object_path);
  }

  void createTestBucket(const std::string& bucket_id, DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.name = bucket_id + "_name";
    bucket.binfo.bucket.bucket_id = bucket_id;
    bucket.binfo.owner.id = TEST_USERNAME;
    bucket.deleted = false;
    db_buckets.store_bucket(bucket);
  }

  bool bucketExists(const std::string& bucket_id, DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    auto bucket = db_buckets.get_bucket(bucket_id);
    return bucket.has_value();
  }

  rgw::sal::sfs::sqlite::DBMultipart createMultipartWithParts(
      const std::string& bucket_id, const std::string& upload_id,
      rgw::sal::sfs::MultipartState state, uint num_parts, DBConnRef conn
  ) {
    SQLiteMultipart db_multiparts(conn);
    rgw::sal::sfs::sqlite::DBMultipart mp;
    mp.bucket_id = bucket_id;
    mp.upload_id = upload_id;
    mp.state = state;
    mp.state_change_time = ceph::real_clock::now();
    mp.object_name = upload_id;
    uuid_d uuid;
    uuid.generate_random();
    mp.path_uuid = uuid;
    db_multiparts.insert(mp);
    for (uint i = 1; i <= num_parts; ++i) {
      createMultipartPart(upload_id, uuid, i, conn);
    }
    return mp;
  }

  rgw::sal::sfs::sqlite::DBMultipart createMultipart(
      const std::string& bucket_id, const std::string& upload_id,
      rgw::sal::sfs::MultipartState state, DBConnRef conn
  ) {
    SQLiteMultipart db_multiparts(conn);
    rgw::sal::sfs::sqlite::DBMultipart mp;
    mp.bucket_id = bucket_id;
    mp.upload_id = upload_id;
    mp.state = state;
    mp.state_change_time = ceph::real_clock::now();
    mp.object_name = upload_id;
    uuid_d uuid;
    uuid.generate_random();
    mp.path_uuid = uuid;
    db_multiparts.insert(mp);
    return mp;
  }

  rgw::sal::sfs::sqlite::DBMultipartPart createMultipartPart(
      const std::string& upload_id, const uuid_d& uuid, int part_num,
      DBConnRef conn
  ) {
    SQLiteMultipart db_multiparts(conn);
    rgw::sal::sfs::sqlite::DBMultipartPart mp;
    auto& storage = conn->get_storage();
    mp.upload_id = upload_id;
    mp.part_num = part_num;
    mp.size = 123;
    const int id = storage.insert(mp);
    storeRandomPart(uuid, id);
    return mp;
  }

  std::shared_ptr<rgw::sal::sfs::Object> createTestObject(
      const std::string& bucket_id, const std::string& name, DBConnRef conn
  ) {
    auto object = std::shared_ptr<rgw::sal::sfs::Object>(
        rgw::sal::sfs::Object::create_for_testing(name)
    );
    SQLiteObjects db_objects(conn);
    DBObject db_object;
    db_object.uuid = object->path.get_uuid();
    db_object.name = name;
    db_object.bucket_id = bucket_id;
    db_objects.store_object(db_object);
    return object;
  }

  void createTestObjectVersion(
      std::shared_ptr<rgw::sal::sfs::Object>& object, uint version,
      DBConnRef conn
  ) {
    object->version_id = version;
    storeRandomObjectVersion(object);
    SQLiteVersionedObjects db_versioned_objects(conn);
    DBVersionedObject db_version;
    db_version.id = version;
    db_version.object_id = object->path.get_uuid();
    db_version.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
    db_version.version_id = std::to_string(version);
    db_versioned_objects.insert_versioned_object(db_version);
  }

  void deleteMarkTestObject(
      std::shared_ptr<rgw::sal::sfs::Object>& object, DBConnRef conn
  ) {
    // delete mark the object
    SQLiteVersionedObjects db_versioned_objects(conn);
    auto last_version =
        db_versioned_objects.get_last_versioned_object(object->path.get_uuid());
    ASSERT_TRUE(last_version.has_value());
    last_version->version_type = rgw::sal::sfs::VersionType::DELETE_MARKER;
    last_version->version_id.append("delete_marker");
    last_version->version_id.append(std::to_string(last_version->id));
    db_versioned_objects.insert_versioned_object(*last_version);
  }

  void deleteTestObjectVersion(uint version_id, DBConnRef conn) {
    // delete mark the object
    SQLiteVersionedObjects db_versioned_objects(conn);
    auto version = db_versioned_objects.get_versioned_object(version_id);
    ASSERT_TRUE(version.has_value());
    version->object_state = rgw::sal::sfs::ObjectState::DELETED;
    db_versioned_objects.store_versioned_object(*version);
  }

  void deleteTestBucket(const std::string& bucket_id, DBConnRef conn) {
    SQLiteBuckets db_buckets(conn);
    auto bucket = db_buckets.get_bucket(bucket_id);
    ASSERT_TRUE(bucket.has_value());
    bucket->deleted = true;
    db_buckets.store_bucket(*bucket);
  }

  size_t getNumberObjectsForBucket(
      const std::string& bucket_id, DBConnRef conn
  ) {
    SQLiteObjects db_objs(conn);
    auto objects = db_objs.get_objects(bucket_id);
    return objects.size();
  }
};

TEST_F(TestSFSGC, TestDeletedBuckets) {
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // nothing should be removed
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());
  SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versions = db_versioned_objs.get_versioned_object_ids();
  EXPECT_EQ(versions.size(), 5);

  // delete bucket 2
  deleteTestBucket("test_bucket_2", store->db_conn);
  // nothing should be removed permanently yet
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  gc->process();

  // only objects for bucket 1 should be available
  EXPECT_EQ(getStoreDataFileCount(), 3);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_TRUE(bucketExists("test_bucket_1", store->db_conn));

  // delete bucket 1 now
  deleteTestBucket("test_bucket_1", store->db_conn);
  gc->process();

  // only the db file should be present
  EXPECT_EQ(getStoreDataFileCount(), 0);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_1", store->db_conn));
}

TEST_F(TestSFSGC, TestDeletedBucketsWithMultiparts) {
  uint MAX_OBJECTS_ITERATION = 1;
  cct->_conf.set_val(
      "rgw_sfs_gc_max_objects_per_iteration",
      std::to_string(MAX_OBJECTS_ITERATION)
  );
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->initialize();
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // now create multiparts with a few parts
  // we don't set the state to DONE nor ABORTED so GC only deletes them
  // when deleting the bucket containing them
  auto multipart1 = createMultipartWithParts(
      "test_bucket_1", "multipart1", rgw::sal::sfs::MultipartState::COMPLETE, 4,
      store->db_conn
  );

  auto multipart2 = createMultipartWithParts(
      "test_bucket_2", "multipart2", rgw::sal::sfs::MultipartState::COMPLETE, 2,
      store->db_conn
  );

  // we should have 11 files (5 version + 6 parts)
  EXPECT_EQ(getStoreDataFileCount(), 11);
  SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versions = db_versioned_objs.get_versioned_object_ids();
  EXPECT_EQ(versions.size(), 5);

  // delete bucket 2
  deleteTestBucket("test_bucket_2", store->db_conn);
  // nothing should be removed yet
  EXPECT_EQ(getStoreDataFileCount(), 11);

  gc->process();

  // only objects and parts for bucket 1 should be available
  EXPECT_EQ(getStoreDataFileCount(), 7);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(1, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_TRUE(bucketExists("test_bucket_1", store->db_conn));

  // delete bucket 1 now
  deleteTestBucket("test_bucket_1", store->db_conn);
  gc->process();

  // only the db file should be present
  EXPECT_EQ(getStoreDataFileCount(), 0);
  EXPECT_TRUE(databaseFileExists());
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_2", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_2", store->db_conn));
  EXPECT_EQ(0, getNumberObjectsForBucket("test_bucket_1", store->db_conn));
  EXPECT_FALSE(bucketExists("test_bucket_1", store->db_conn));
}

TEST_F(TestSFSGC, TestDeletedObjects) {
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 1 bucket
  createTestBucket("test_bucket_1", store->db_conn);

  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_1", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  gc->process();
  // we should still 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // add a delete marker on object1
  deleteMarkTestObject(object1, store->db_conn);

  gc->process();
  // we should still 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // delete first version of object1
  deleteTestObjectVersion(1, store->db_conn);

  // before GC runs we should have the same files
  EXPECT_EQ(getStoreDataFileCount(), 5);
  gc->process();
  // before GC runs we should have 1 file less
  EXPECT_EQ(getStoreDataFileCount(), 4);

  // delete everything now (all versions in object 1 and object 2)
  deleteTestObjectVersion(2, store->db_conn);
  deleteTestObjectVersion(3, store->db_conn);
  deleteTestObjectVersion(4, store->db_conn);
  deleteTestObjectVersion(5, store->db_conn);

  // check we have the same number of files before GC hits
  EXPECT_EQ(getStoreDataFileCount(), 4);
  gc->process();
  // all should be gone now
  EXPECT_EQ(getStoreDataFileCount(), 0);
}

TEST_F(TestSFSGC, TestDeletedObjectsAndDeletedBuckets) {
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->initialize();
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_1", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  auto object3 = createTestObject("test_bucket_2", "obj_3", store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);
  createTestObjectVersion(object3, version_id++, store->db_conn);

  // we should have 7 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 7);

  gc->process();
  // we should still 7 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 7);

  // add a delete marker on object1
  deleteMarkTestObject(object1, store->db_conn);

  gc->process();
  // we should still 7 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 7);
  EXPECT_TRUE(databaseFileExists());

  // delete first version of object1
  deleteTestObjectVersion(1, store->db_conn);

  // before GC runs we should have the same files
  EXPECT_EQ(getStoreDataFileCount(), 7);
  gc->process();
  // before GC runs we should have 1 file less
  EXPECT_EQ(getStoreDataFileCount(), 6);

  // delete everything now (all versions in object 1 and object 2)
  deleteTestObjectVersion(2, store->db_conn);
  deleteTestObjectVersion(3, store->db_conn);
  deleteTestObjectVersion(4, store->db_conn);
  deleteTestObjectVersion(5, store->db_conn);

  // add a delete marker on object3
  // when deleting the bucket, it will test the case of deleting delete markers
  // from the filesystem
  deleteMarkTestObject(object3, store->db_conn);

  // also delete bucket_2 and bucket_1
  deleteTestBucket("test_bucket_2", store->db_conn);
  deleteTestBucket("test_bucket_1", store->db_conn);
  // check we have the same number of files before GC hits
  EXPECT_EQ(getStoreDataFileCount(), 6);
  gc->process();
  // all should be gone
  EXPECT_EQ(getStoreDataFileCount(), 0);
}

TEST_F(TestSFSGC, TestDoneAndAbortedMultiparts) {
  uint MAX_OBJECTS_ITERATION = 1;
  cct->_conf.set_val(
      "rgw_sfs_gc_max_objects_per_iteration",
      std::to_string(MAX_OBJECTS_ITERATION)
  );
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->initialize();
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // now create multiparts with a few parts in states that are not done
  // nor aborted
  auto multipart1 = createMultipartWithParts(
      "test_bucket_1", "multipart1", rgw::sal::sfs::MultipartState::INPROGRESS,
      10, store->db_conn
  );

  auto multipart2 = createMultipartWithParts(
      "test_bucket_2", "multipart2", rgw::sal::sfs::MultipartState::COMPLETE, 5,
      store->db_conn
  );

  auto multipart3 = createMultipartWithParts(
      "test_bucket_1", "multipart3", rgw::sal::sfs::MultipartState::AGGREGATING,
      20, store->db_conn
  );

  // now add 2 done multiparts
  auto multipart4 = createMultipartWithParts(
      "test_bucket_1", "multipart4", rgw::sal::sfs::MultipartState::DONE, 10,
      store->db_conn
  );
  auto multipart5 = createMultipartWithParts(
      "test_bucket_1", "multipart5", rgw::sal::sfs::MultipartState::DONE, 5,
      store->db_conn
  );

  // add also 1 multipart aborted
  auto multipart6 = createMultipartWithParts(
      "test_bucket_1", "multipart6", rgw::sal::sfs::MultipartState::ABORTED, 5,
      store->db_conn
  );

  // we should have 60 files (5 version + 55 parts)
  EXPECT_EQ(getStoreDataFileCount(), 60);
  SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versions = db_versioned_objs.get_versioned_object_ids();
  EXPECT_EQ(versions.size(), 5);

  gc->process();
  // parts for multiparts DONE and ABORTED should be gone now.
  EXPECT_EQ(getStoreDataFileCount(), 40);

  // set multipart3 to DONE (it was previously AGGREGATTING)
  rgw::sal::sfs::sqlite::SQLiteMultipart db_multipart(store->db_conn);
  EXPECT_TRUE(db_multipart.mark_done("multipart3"));

  gc->process();
  // multipart had 20 parts so we should have 20 files now
  EXPECT_EQ(getStoreDataFileCount(), 20);

  // check that the multiparts deleted don't exist in the db
  EXPECT_FALSE(db_multipart.get_multipart("multipart3").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart4").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart5").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart6").has_value());

  // check that the multiparts not deleted remain in thd db
  EXPECT_TRUE(db_multipart.get_multipart("multipart1").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart2").has_value());
}

TEST_F(TestSFSGC, TestMultipartsWithZeroParts) {
  uint MAX_OBJECTS_ITERATION = 1;
  cct->_conf.set_val(
      "rgw_sfs_gc_max_objects_per_iteration",
      std::to_string(MAX_OBJECTS_ITERATION)
  );
  auto store = new rgw::sal::SFStore(cct.get(), getTestDir());
  auto gc = store->gc;
  gc->initialize();
  gc->suspend();  // start suspended so we have control over processing

  NoDoutPrefix ndp(cct.get(), 1);
  RGWEnv env;
  env.init(cct.get());

  // create the test user
  createTestUser(store->db_conn);

  // create 2 buckets
  createTestBucket("test_bucket_1", store->db_conn);
  createTestBucket("test_bucket_2", store->db_conn);

  // create a few objects in bucket_1 with a few versions
  uint version_id = 1;
  auto object1 = createTestObject("test_bucket_1", "obj_1", store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);
  createTestObjectVersion(object1, version_id++, store->db_conn);

  auto object2 = createTestObject("test_bucket_2", "obj_2", store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);
  createTestObjectVersion(object2, version_id++, store->db_conn);

  // we should have 5 version files plus the sqlite db
  EXPECT_EQ(getStoreDataFileCount(), 5);
  EXPECT_TRUE(databaseFileExists());

  // now create multiparts with all states
  auto multipart1 = createMultipartWithParts(
      "test_bucket_1", "multipart1", rgw::sal::sfs::MultipartState::INPROGRESS,
      0, store->db_conn
  );

  auto multipart2 = createMultipartWithParts(
      "test_bucket_2", "multipart2", rgw::sal::sfs::MultipartState::COMPLETE, 0,
      store->db_conn
  );

  auto multipart3 = createMultipartWithParts(
      "test_bucket_1", "multipart3", rgw::sal::sfs::MultipartState::AGGREGATING,
      0, store->db_conn
  );

  auto multipart4 = createMultipartWithParts(
      "test_bucket_1", "multipart4", rgw::sal::sfs::MultipartState::DONE, 0,
      store->db_conn
  );

  auto multipart5 = createMultipartWithParts(
      "test_bucket_1", "multipart5", rgw::sal::sfs::MultipartState::ABORTED, 0,
      store->db_conn
  );

  // we should still have 5 files.. (there are no parts)
  EXPECT_EQ(getStoreDataFileCount(), 5);
  SQLiteVersionedObjects db_versioned_objs(store->db_conn);
  auto versions = db_versioned_objs.get_versioned_object_ids();
  EXPECT_EQ(versions.size(), 5);

  // verify that all multiparts are present
  // (including the done and aborted)
  rgw::sal::sfs::sqlite::SQLiteMultipart db_multipart(store->db_conn);
  EXPECT_TRUE(db_multipart.get_multipart("multipart1").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart2").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart3").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart4").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart5").has_value());
  gc->process();
  // we should still have 5 files
  EXPECT_EQ(getStoreDataFileCount(), 5);
  // verify that the done and aborted multiparts are gone
  // the rest stay
  EXPECT_TRUE(db_multipart.get_multipart("multipart1").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart2").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart3").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart4").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart5").has_value());

  // delete bucket 1 now
  deleteTestBucket("test_bucket_1", store->db_conn);
  gc->process();
  // check that all multiparts of bucket 1 should be gone
  EXPECT_FALSE(db_multipart.get_multipart("multipart1").has_value());
  EXPECT_TRUE(db_multipart.get_multipart("multipart2").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart3").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart4").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart5").has_value());

  // delete bucket 2 now
  deleteTestBucket("test_bucket_2", store->db_conn);
  gc->process();
  // all multiparts should be gone
  EXPECT_FALSE(db_multipart.get_multipart("multipart1").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart2").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart3").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart4").has_value());
  EXPECT_FALSE(db_multipart.get_multipart("multipart5").has_value());

  // objects and version should be gone too
  EXPECT_EQ(getStoreDataFileCount(), 0);
}
