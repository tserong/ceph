// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/ceph_context.h"
#include "rgw/driver/sfs/sqlite/dbconn.h"
#include "rgw/driver/sfs/sqlite/sqlite_buckets.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"
#include "rgw/driver/sfs/sqlite/sqlite_users.h"
#include "rgw/rgw_sal_sfs.h"

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

class TestSFSSQLiteObjects : public ::testing::Test {
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

  void createUser(const std::string& username, DBConnRef conn) {
    SQLiteUsers users(conn);
    DBOPUserInfo user;
    user.uinfo.user_id.id = username;
    users.store_user(user);
  }

  void createBucket(
      const std::string& username, const std::string& bucketname, DBConnRef conn
  ) {
    createUser(username, conn);
    SQLiteBuckets buckets(conn);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.bucket_id = bucketname;
    bucket.binfo.bucket.name = bucketname;
    bucket.binfo.owner.id = username;
    buckets.store_bucket(bucket);
  }
};

void compareObjects(const DBObject& origin, const DBObject& dest) {
  ASSERT_EQ(origin.uuid, dest.uuid);
  ASSERT_EQ(origin.bucket_id, dest.bucket_id);
  ASSERT_EQ(origin.name, dest.name);
}

DBObject createTestObject(
    const std::string& suffix, CephContext* context,
    const std::string& username = "usertest"
) {
  DBObject object;
  object.uuid.generate_random();
  object.bucket_id = "test_bucket";
  object.name = "test" + suffix;
  return object;
}

DBVersionedObject createTestVersionedObject(
    uint id, const std::string& object_id, const std::string& suffix
) {
  DBVersionedObject test_versioned_object;
  test_versioned_object.id = id;
  uuid_d uuid;
  uuid.parse(object_id.c_str());
  test_versioned_object.object_id = uuid;
  test_versioned_object.checksum = "test_checksum_" + suffix;
  // test_versioned_object.size = rand();
  test_versioned_object.size = 1999;
  test_versioned_object.create_time = ceph::real_clock::now();
  test_versioned_object.delete_time = ceph::real_clock::now();
  test_versioned_object.commit_time = ceph::real_clock::now();
  test_versioned_object.mtime = ceph::real_clock::now();
  test_versioned_object.object_state = rgw::sal::sfs::ObjectState::COMMITTED;
  test_versioned_object.version_id = "test_version_id_" + suffix;
  test_versioned_object.etag = "test_etag_" + suffix;
  test_versioned_object.version_type = rgw::sal::sfs::VersionType::REGULAR;

  //set attrs with default ACL
  {
    RGWAccessControlPolicy aclp;
    rgw_user aclu("usertest");
    aclp.get_acl().create_default(aclu, "usertest");
    aclp.get_owner().set_name("usertest");
    aclp.get_owner().set_id(aclu);
    bufferlist acl_bl;
    aclp.encode(acl_bl);
    rgw::sal::Attrs attrs;
    attrs[RGW_ATTR_ACL] = acl_bl;
    test_versioned_object.attrs = attrs;
  }

  return test_versioned_object;
}

TEST_F(TestSFSSQLiteObjects, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());
  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto object = createTestObject("1", ceph_context.get());

  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteObjects, remove_object) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto obj1 = createTestObject("1", ceph_context.get());
  db_objects->store_object(obj1);
  auto obj2 = createTestObject("2", ceph_context.get());
  db_objects->store_object(obj2);
  auto obj3 = createTestObject("3", ceph_context.get());
  db_objects->store_object(obj3);

  db_objects->remove_object(obj2.uuid);
  auto ret_object = db_objects->get_object(obj2.uuid);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteObjects, remove_objectThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  auto obj1 = createTestObject("1", ceph_context.get());
  db_objects->store_object(obj1);
  auto obj2 = createTestObject("2", ceph_context.get());
  db_objects->store_object(obj2);
  auto obj3 = createTestObject("3", ceph_context.get());
  db_objects->store_object(obj3);

  uuid_d non_existing_uuid;
  db_objects->remove_object(non_existing_uuid);
  auto objects = db_objects->get_objects("test_bucket");
  EXPECT_EQ(objects.size(), 3);
}

TEST_F(TestSFSSQLiteObjects, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);
  auto object = createTestObject("1", ceph_context.get());
  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);

  object.name = "ObjectRenamed";
  db_objects->store_object(object);
  ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(ret_object->name, "ObjectRenamed");
  compareObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteObjects, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);
  auto object = createTestObject("1", ceph_context.get());
  db_objects->store_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);

  // create a new instance, bucket should exist
  auto db_objects_2 = std::make_shared<SQLiteObjects>(conn);
  ret_object = db_objects->get_object(object.uuid);
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteObjects, CreateObjectForNonExistingBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create the bucket, we need it because BucketName is a foreign key of Bucket::BucketID
  createBucket("usertest", "test_bucket", conn);

  SQLiteObjects db_objects(conn);
  auto storage = conn->get_storage();

  DBObject db_object;

  uuid_d uuid_obj;
  uuid_obj.parse("254ddc1a-06a6-11ed-b939-0242ac120002");
  db_object.uuid = uuid_obj;
  db_object.name = "test";

  EXPECT_THROW(
      {
        try {
          storage->replace(db_object);
          ;
        } catch (const std::system_error& e) {
          EXPECT_STREQ(
              "FOREIGN KEY constraint failed: constraint failed", e.what()
          );
          throw;
        }
      },
      std::system_error
  );
}

TEST_F(TestSFSSQLiteObjects, GetObjectByBucketAndObjectName) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());
  ceph_context->_log->start();

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  DBConnRef conn = std::make_shared<DBConn>(ceph_context.get());

  // Create a few buckets
  createBucket("usertest", "test_bucket", conn);
  createBucket("usertest", "test_bucket_2", conn);
  createBucket("usertest", "test_bucket_3", conn);

  auto db_objects = std::make_shared<SQLiteObjects>(conn);

  // create objects with names "test1", "test2" and "test3"... in bucket "test_bucket"
  auto object_1 = createTestObject(
      "1", ceph_context.get()
  );  // name is "test1", bucket is "test_bucket"
  db_objects->store_object(object_1);
  auto object_2 = createTestObject(
      "2", ceph_context.get()
  );  // name is "test2", bucket is "test_bucket"
  db_objects->store_object(object_2);
  auto object_3 = createTestObject(
      "3", ceph_context.get()
  );  // name is "test3", bucket is "test_bucket"
  db_objects->store_object(object_3);

  // create object "test1" in bucket "test_bucket_2"
  auto object_1_2 = createTestObject(
      "1", ceph_context.get()
  );  // name is "test3", bucket is "test_bucket"
  object_1_2.bucket_id = "test_bucket_2";
  db_objects->store_object(object_1_2);

  // run a few queries
  auto ret_object =
      db_objects->get_object("test_bucket", "this_object_does_not_exist");
  ASSERT_FALSE(ret_object.has_value());

  ret_object = db_objects->get_object("test_bucket", "test1");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_1, *ret_object);

  ret_object = db_objects->get_object("test_bucket", "test2");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_2, *ret_object);

  ret_object = db_objects->get_object("test_bucket", "test3");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_3, *ret_object);

  ret_object = db_objects->get_object("test_bucket_2", "test1");
  ASSERT_TRUE(ret_object.has_value());
  compareObjects(object_1_2, *ret_object);

  ret_object = db_objects->get_object("this_bucket_does_not_exist", "test1");
  ASSERT_FALSE(ret_object.has_value());
}
