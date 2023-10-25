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

#include <string>

namespace rgw::sal::sfs::sqlite {

std::string prefix_to_escaped_like(const std::string& prefix, char escape) {
  std::string like_expr;
  like_expr.reserve(prefix.length() + 10);
  for (const char c : prefix) {
    switch (c) {
      case '%':
        [[fallthrough]];
      case '_':
        like_expr.push_back(escape);
        [[fallthrough]];
      default:
        like_expr.push_back(c);
    }
  }
  like_expr.push_back('%');
  return like_expr;
}

}  // namespace rgw::sal::sfs::sqlite
