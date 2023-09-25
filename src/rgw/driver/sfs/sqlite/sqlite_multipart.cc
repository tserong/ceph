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
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"

#include <driver/sfs/sqlite/buckets/multipart_definitions.h>
#include <sqlite_orm/sqlite_orm.h>

#include <iterator>
#include <optional>

#include "retry.h"
#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_definitions.h"
#include "rgw/driver/sfs/sqlite/conversion_utils.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteMultipart::SQLiteMultipart(DBConnRef _conn) : conn(_conn) {}

std::optional<std::vector<DBMultipart>> SQLiteMultipart::list_multiparts(
    const std::string& bucket_name, const std::string& prefix,
    const std::string& marker, const std::string& delim, const int& max_uploads,
    bool* is_truncated
) const {
  auto& storage = conn->get_storage();

  auto bucket_entries = storage.get_all<DBBucket>(
      where(is_equal(&DBBucket::bucket_name, bucket_name))
  );
  if (bucket_entries.size() == 0) {
    return std::nullopt;
  }
  ceph_assert(bucket_entries.size() == 1);
  auto bucket_id = bucket_entries.front().bucket_id;

  return list_multiparts_by_bucket_id(
      bucket_id, prefix, marker, delim, max_uploads, is_truncated, false
  );
}

std::vector<DBMultipart> SQLiteMultipart::list_multiparts_by_bucket_id(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& marker, const std::string& /*delim*/,
    const int& max_uploads, bool* is_truncated, bool get_all
) const {
  auto& storage = conn->get_storage();

  auto start_state = get_all ? MultipartState::NONE : MultipartState::INIT;
  auto end_state =
      get_all ? MultipartState::LAST_VALUE : MultipartState::INPROGRESS;

  auto start_cond = greater_or_equal(&DBMultipart::state, start_state);
  auto end_cond = lesser_or_equal(&DBMultipart::state, end_state);

  auto cond = start_cond and end_cond;

  auto db_entries = storage.get_all<DBMultipart>(
      where(
          is_equal(&DBMultipart::bucket_id, bucket_id) and cond and
          greater_or_equal(&DBMultipart::meta_str, marker) and
          prefix_to_like(&DBMultipart::object_name, prefix)
      ),
      order_by(&DBMultipart::meta_str), limit(max_uploads + 1)
  );

  ceph_assert(max_uploads >= 0);
  if (db_entries.size() > static_cast<size_t>(max_uploads)) {
    ceph_assert(db_entries.size() == static_cast<size_t>(max_uploads + 1));
    db_entries.pop_back();
    if (is_truncated) {
      *is_truncated = true;
    }
  }

  return db_entries;
}

int SQLiteMultipart::abort_multiparts_by_bucket_id(const std::string& bucket_id
) const {
  auto& storage = conn->get_storage();
  uint64_t num_changes = 0;
  storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::ABORTED,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::bucket_id, bucket_id) and
            greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
            lesser_than(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    num_changes = storage.changes();
    return true;
  });

  return num_changes;
}

int SQLiteMultipart::abort_multiparts(const std::string& bucket_name) const {
  auto& storage = conn->get_storage();
  auto bucket_ids_vec = storage.select(
      &DBBucket::bucket_id, where(is_equal(&DBBucket::bucket_name, bucket_name))
  );
  if (bucket_ids_vec.size() == 0) {
    return -ERR_NO_SUCH_BUCKET;
  }
  ceph_assert(bucket_ids_vec.size() == 1);
  auto bucket_id = bucket_ids_vec.front();
  return abort_multiparts_by_bucket_id(bucket_id);
}

std::optional<DBMultipart> SQLiteMultipart::get_multipart(
    const std::string& upload_id
) const {
  if (upload_id.empty()) {
    return std::nullopt;
  }

  auto& storage = conn->get_storage();
  auto entries = storage.get_all<DBMultipart>(
      where(is_equal(&DBMultipart::upload_id, upload_id))
  );
  ceph_assert(entries.size() <= 1);
  std::optional<DBMultipart> mp;
  if (entries.size() == 1) {
    mp = entries[0];
  }
  return mp;
}

std::optional<DBMultipart> SQLiteMultipart::get_multipart(int id) const {
  ceph_assert(id >= 0);
  auto& storage = conn->get_storage();
  auto entries =
      storage.get_all<DBMultipart>(where(is_equal(&DBMultipart::id, id)));
  ceph_assert(entries.size() <= 1);  // primary key
  std::optional<DBMultipart> mp;
  if (entries.size() == 1) {
    mp = entries[0];
  }
  return mp;
}

uint SQLiteMultipart::insert(const DBMultipart& mp) const {
  auto& storage = conn->get_storage();
  return storage.insert(mp);
}

std::vector<DBMultipartPart> SQLiteMultipart::list_parts(
    const std::string& upload_id, int num_parts, int marker, int* next_marker,
    bool* truncated
) const {
  auto& storage = conn->get_storage();
  std::vector<DBMultipartPart> db_entries;
  db_entries = storage.get_all<DBMultipartPart>(
      where(
          is_equal(&DBMultipartPart::upload_id, upload_id) and
          is_not_null(&DBMultipartPart::etag) and
          c(&DBMultipartPart::id) >= marker
      ),
      order_by(&DBMultipartPart::id), limit(num_parts + 1)
  );

  ceph_assert(num_parts >= 0);
  if (db_entries.size() > static_cast<size_t>(num_parts)) {
    ceph_assert(db_entries.size() == static_cast<size_t>(num_parts + 1));
    // we got a next marker on the last row
    if (truncated) {
      *truncated = true;
    }
    auto next = db_entries.back();
    db_entries.pop_back();
    if (next_marker) {
      *next_marker = next.id;
    }
  }

  return db_entries;
}

std::vector<DBMultipartPart> SQLiteMultipart::get_parts(
    const std::string& upload_id
) const {
  auto& storage = conn->get_storage();
  auto db_entries = storage.get_all<DBMultipartPart>(
      where(is_equal(&DBMultipartPart::upload_id, upload_id)),
      order_by(&DBMultipartPart::part_num)
  );
  return db_entries;
}

std::optional<DBMultipartPart> SQLiteMultipart::get_part(
    const std::string& upload_id, uint32_t part_num
) const {
  auto& storage = conn->get_storage();
  auto entries = storage.get_all<DBMultipartPart>(where(
      is_equal(&DBMultipartPart::upload_id, upload_id) and
      is_equal(&DBMultipartPart::part_num, part_num)
  ));
  if (entries.empty()) {
    return std::nullopt;
  }
  ceph_assert(entries.size() == 1);
  return entries.front();
}

std::optional<DBMultipartPart> SQLiteMultipart::create_or_reset_part(
    const std::string& upload_id, uint32_t part_num, std::string* error_str
) const {
  auto& storage = conn->get_storage();
  std::optional<DBMultipartPart> entry = std::nullopt;

  storage.transaction([&]() mutable {
    auto cnt = storage.count<DBMultipart>(where(
        is_equal(&DBMultipart::upload_id, upload_id) and
        (is_equal(&DBMultipart::state, MultipartState::INPROGRESS) or
         is_equal(&DBMultipart::state, MultipartState::INIT))
    ));
    if (cnt != 1) {
      if (error_str) {
        *error_str = "could not find upload";
      }
      return false;
    }

    // set multipart upload as being in progress
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::INPROGRESS,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::INIT)
        )
    );

    // find if there's already a part with said upload_id/part_num combination
    auto parts = storage.get_all<DBMultipartPart>(where(
        is_equal(&DBMultipartPart::upload_id, upload_id) and
        is_equal(&DBMultipartPart::part_num, part_num)
    ));
    DBMultipartPart part;

    if (parts.size() > 0) {
      ceph_assert(parts.size() == 1);
      // reset part entry
      part = parts.front();
      part.size = 0;
      part.etag = std::nullopt;
      part.mtime = std::nullopt;
      try {
        storage.replace(part);
      } catch (const std::system_error& e) {
        if (error_str) {
          *error_str = e.what();
        }
        return false;
      }
    } else {
      part = DBMultipartPart{
          .id = -1 /* ignored by insert */,
          .upload_id = upload_id,
          .part_num = part_num,
          .size = 0,
          .etag = std::nullopt,
          .mtime = std::nullopt,
      };
      try {
        part.id = storage.insert(part);
      } catch (const std::system_error& e) {
        if (error_str) {
          *error_str = e.what();
        }
        return false;
      }
    }

    entry = part;
    return true;
  });

  return entry;
}

bool SQLiteMultipart::finish_part(
    const std::string& upload_id, uint32_t part_num, const std::string& etag,
    uint64_t bytes_written
) const {
  auto& storage = conn->get_storage();
  bool committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipartPart::etag) = etag,
            c(&DBMultipartPart::mtime) = ceph::real_time::clock::now(),
            c(&DBMultipartPart::size) = bytes_written),
        where(
            is_equal(&DBMultipartPart::upload_id, upload_id) and
            is_equal(&DBMultipartPart::part_num, part_num) and
            is_null(&DBMultipartPart::etag)
        )
    );
    if (storage.changes() != 1) {
      return false;
    }
    return true;
  });
  return committed;
}

bool SQLiteMultipart::abort(const std::string& upload_id) const {
  auto& storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::ABORTED,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
            lesser_than(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    auto num_aborted = storage.changes();
    if (num_aborted == 0) {
      return false;
    }
    ceph_assert(num_aborted == 1);
    return true;
  });

  return committed;
}

static int _mark_complete(
    rgw::sal::sfs::sqlite::Storage& storage, const std::string& upload_id
) {
  storage.update_all(
      set(c(&DBMultipart::state) = MultipartState::COMPLETE,
          c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
      where(
          is_equal(&DBMultipart::upload_id, upload_id) and
          greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
          lesser_or_equal(&DBMultipart::state, MultipartState::INPROGRESS)
      )
  );
  return storage.changes();
}

bool SQLiteMultipart::mark_complete(const std::string& upload_id) const {
  auto& storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    auto num_complete = _mark_complete(storage, upload_id);
    if (num_complete == 0) {
      return false;
    }
    ceph_assert(num_complete == 1);
    return true;
  });

  return committed;
}

bool SQLiteMultipart::mark_complete(
    const std::string& upload_id, bool* duplicate
) const {
  ceph_assert(duplicate != nullptr);
  auto& storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    auto entries = storage.get_all<DBMultipart>(
        where(is_equal(&DBMultipart::upload_id, upload_id))
    );
    ceph_assert(entries.size() <= 1);
    if (entries.size() == 0) {
      return false;
    }
    auto entry = entries.front();
    if (entry.state == MultipartState::DONE) {
      *duplicate = true;
      return true;
    }

    auto num_complete = _mark_complete(storage, upload_id);
    if (num_complete == 0) {
      return false;
    }
    ceph_assert(num_complete == 1);
    return true;
  });
  return committed;
}

bool SQLiteMultipart::mark_aggregating(const std::string& upload_id) const {
  auto& storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::AGGREGATING,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    auto num_changed = storage.changes();
    if (num_changed == 0) {
      return false;
    }
    ceph_assert(num_changed == 1);
    return true;
  });

  return committed;
}

bool SQLiteMultipart::mark_done(const std::string& upload_id) const {
  auto& storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::DONE,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::AGGREGATING)
        )
    );
    auto num_changed = storage.changes();
    if (num_changed == 0) {
      return false;
    }
    ceph_assert(num_changed == 1);
    return true;
  });
  return committed;
}

void SQLiteMultipart::remove_parts(const std::string& upload_id) const {
  auto& storage = conn->get_storage();
  storage.remove_all<DBMultipartPart>(
      where(c(&DBMultipartPart::upload_id) = upload_id)
  );
}

void SQLiteMultipart::remove_multiparts_by_bucket_id(
    const std::string& bucket_id
) const {
  auto& storage = conn->get_storage();
  storage.remove_all<DBMultipart>(where(c(&DBMultipart::bucket_id) = bucket_id)
  );
}

std::optional<DBDeletedMultipartItems>
SQLiteMultipart::remove_multiparts_by_bucket_id_transact(
    const std::string& bucket_id, uint max_items
) const {
  DBDeletedMultipartItems ret_parts;
  auto& storage = conn->get_storage();
  RetrySQLite<DBDeletedMultipartItems> retry([&]() {
    auto transaction = storage.transaction_guard();
    // get first the list of parts to be deleted up to max_items
    ret_parts = storage.select(
        columns(
            &DBMultipart::upload_id, &DBMultipart::path_uuid,
            &DBMultipartPart::id
        ),
        inner_join<DBMultipart>(
            on(is_equal(&DBMultipart::upload_id, &DBMultipartPart::upload_id))
        ),
        where(is_equal(&DBMultipart::bucket_id, bucket_id)),
        order_by(&DBMultipartPart::id), limit(max_items)
    );
    // extract the ids from the query.
    // we'll use the ids to delete them with remove_all where in.
    std::vector<int> ids;
    ids.reserve(ret_parts.size());
    std::ranges::transform(
        ret_parts, std::back_inserter(ids),
        [](DBDeletedMultipartItem item) -> int { return get_part_id(item); }
    );
    ceph_assert(ids.size() == ret_parts.size());
    if (ret_parts.size() == 0) {
      // there are no pending parts for this bucket
      // we can safely delete all multiparts in it.
      // This will take care of multiparts that had 0 parts initially
      storage.remove_all<DBMultipart>(
          where(is_equal(&DBMultipart::bucket_id, bucket_id))
      );
      transaction.commit();
      return ret_parts;
    }
    // remove the parts selected
    storage.remove_all<DBMultipartPart>(where(in(&DBMultipartPart::id, ids)));

    // now check if the multipart holding the part is empty
    std::map<std::string, bool> already_checked_mp;
    for (auto const& part : ret_parts) {
      auto upload_id = get_upload_id(part);
      if (already_checked_mp.find(upload_id) == already_checked_mp.end()) {
        already_checked_mp[upload_id] = true;

        auto nb_parts = storage.count(
            &DBMultipartPart::id,
            where(is_equal(&DBMultipartPart::upload_id, upload_id))
        );
        if (nb_parts == 0) {
          // delete this multipart as it has no parts
          storage.remove_all<DBMultipart>(
              where(is_equal(&DBMultipart::upload_id, upload_id))
          );
        }
      }
    }
    transaction.commit();
    return ret_parts;
  });
  return retry.run();
}

std::optional<DBDeletedMultipartItems>
SQLiteMultipart::remove_done_or_aborted_multiparts_transact(uint max_items
) const {
  DBDeletedMultipartItems ret_parts;
  auto& storage = conn->get_storage();
  RetrySQLite<DBDeletedMultipartItems> retry([&]() {
    auto transaction = storage.transaction_guard();
    // get first the list of parts to be deleted up to max_items
    ret_parts = storage.select(
        columns(
            &DBMultipart::upload_id, &DBMultipart::path_uuid,
            &DBMultipartPart::id
        ),
        inner_join<DBMultipart>(
            on(is_equal(&DBMultipart::upload_id, &DBMultipartPart::upload_id))
        ),
        where(
            is_equal(&DBMultipart::state, MultipartState::DONE) or
            is_equal(&DBMultipart::state, MultipartState::ABORTED)
        ),
        order_by(&DBMultipartPart::id), limit(max_items)
    );
    // extract the ids from the query.
    // we'll use the ids to delete them with remove_all where in.
    std::vector<int> ids;
    ids.reserve(ret_parts.size());
    std::ranges::transform(
        ret_parts, std::back_inserter(ids),
        [](DBDeletedMultipartItem item) -> int { return get_part_id(item); }
    );
    ceph_assert(ids.size() == ret_parts.size());
    if (ret_parts.size() == 0) {
      // there are no pending parts for this bucket
      // we can safely delete all multiparts in it.
      // This will take care of multiparts that had 0 parts initially
      storage.remove_all<DBMultipart>(where(
          is_equal(&DBMultipart::state, MultipartState::DONE) or
          is_equal(&DBMultipart::state, MultipartState::ABORTED)
      ));
      transaction.commit();
      return ret_parts;
    }
    // remove the parts selected
    storage.remove_all<DBMultipartPart>(where(in(&DBMultipartPart::id, ids)));

    // now check if the multipart holding the part is empty
    std::map<std::string, bool> already_checked_mp;
    for (auto const& part : ret_parts) {
      auto upload_id = get_upload_id(part);
      if (already_checked_mp.find(upload_id) == already_checked_mp.end()) {
        already_checked_mp[upload_id] = true;

        auto nb_parts = storage.count(
            &DBMultipartPart::id,
            where(is_equal(&DBMultipartPart::upload_id, upload_id))
        );
        if (nb_parts == 0) {
          // delete this multipart as it has no parts
          storage.remove_all<DBMultipart>(
              where(is_equal(&DBMultipart::upload_id, upload_id))
          );
        }
      }
    }
    transaction.commit();
    return ret_parts;
  });
  return retry.run();
}

}  // namespace rgw::sal::sfs::sqlite
