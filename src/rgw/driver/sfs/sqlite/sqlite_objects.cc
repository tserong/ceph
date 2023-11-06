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
#include "sqlite_objects.h"

#include "sqlite_versioned_objects.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteObjects::SQLiteObjects(DBConnRef _conn) : conn(_conn) {}

std::vector<DBObject> SQLiteObjects::get_objects(const std::string& bucket_id
) const {
  auto storage = conn->get_storage();
  return storage->get_all<DBObject>(
      where(is_equal(&DBObject::bucket_id, bucket_id))
  );
}

std::optional<DBObject> SQLiteObjects::get_object(const uuid_d& uuid) const {
  auto storage = conn->get_storage();
  auto object = storage->get_pointer<DBObject>(uuid.to_string());
  std::optional<DBObject> ret_value;
  if (object) {
    ret_value = *object;
  }
  return ret_value;
}

std::optional<DBObject> SQLiteObjects::get_object(
    const std::string& bucket_id, const std::string& object_name
) const {
  auto storage = conn->get_storage();
  auto objects = storage->get_all<DBObject>(where(
      is_equal(&DBObject::bucket_id, bucket_id) and
      is_equal(&DBObject::name, object_name)
  ));

  std::optional<DBObject> ret_value;
  // value must be unique
  if (objects.size() == 1) {
    ret_value = objects[0];
  }
  return ret_value;
}

void SQLiteObjects::store_object(const DBObject& object) const {
  auto storage = conn->get_storage();
  storage->replace(object);
}

void SQLiteObjects::remove_object(const uuid_d& uuid) const {
  auto storage = conn->get_storage();
  storage->remove<DBObject>(uuid);
}

}  // namespace rgw::sal::sfs::sqlite
