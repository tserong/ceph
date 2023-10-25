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
#pragma once

// SFS dout, shows relevant info for SFS
//
#define lsfs_dout_for(_dpp, _lvl, _whom) \
  ldpp_dout(_dpp, _lvl) << "> " << _whom << "::" << __func__ << " "

#define lsfs_dout(_dpp, _lvl) lsfs_dout_for(_dpp, _lvl, this->get_cls_name())

// In principle, this is what each log level means:
//
//  ERROR: something unrecoverable happened. Must always go to log regardless of
//  what log level the user has set. E.g., database corruption, ceph_abort().
//
//  IMPORTANT: something that we should log even if logging is at zero. Startup
//  messages, or warnings, for instance.
//
//  INFO: we still want the user to know about it, but not important enough to
//  be at level zero.
//
//  VERBOSE: we did a thing and found it weird enough to log, or a recurring
//  action we find interesting to know about is happening. E.g., running GC.
//
//  DEBUG: the vast majority of noise. Important when dealing with weirdness,
//  but should otherwise be hidden from the user.
//
//  TRACE: whatever may affect performance significantly and used only as last
//  resort.
//
//  MEGA_TRACE: TRACE on steroids.
//
#define SFS_LOG_ERROR      -1
#define SFS_LOG_IMPORTANT  0
#define SFS_LOG_INFO       1
#define SFS_LOG_VERBOSE    10
#define SFS_LOG_DEBUG      15
#define SFS_LOG_TRACE      20
#define SFS_LOG_MEGA_TRACE 30

#define SFS_LOG_STARTUP  SFS_LOG_IMPORTANT
#define SFS_LOG_SHUTDOWN SFS_LOG_IMPORTANT
#define SFS_LOG_WARN     SFS_LOG_IMPORTANT

#define lsfs_err(_dpp)            lsfs_dout(_dpp, SFS_LOG_ERROR)
#define lsfs_err_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_ERROR, _whom)

#define lsfs_startup(_dpp) lsfs_dout(_dpp, SFS_LOG_STARTUP)
#define lsfs_startup_for(_dpp, _whom) \
  lsfs_dout_for(_dpp, SFS_LOG_STARTUP, _whom)

#define lsfs_shutdown(_dpp) lsfs_dout(_dpp, SFS_LOG_SHUTDOWN)
#define lsfs_shutdown_for(_dpp, _whom) \
  lsfs_dout_for(_dpp, SFS_LOG_SHUTDOWN, _whom)

#define lsfs_warn(_dpp)            lsfs_dout(_dpp, SFS_LOG_WARN)
#define lsfs_warn_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_WARN, _whom)

#define lsfs_info(_dpp)            lsfs_dout(_dpp, SFS_LOG_INFO)
#define lsfs_info_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_INFO, _whom)

#define lsfs_verb(_dpp)            lsfs_dout(_dpp, SFS_LOG_VERBOSE)
#define lsfs_verb_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_VERBOSE, _whom)

#define lsfs_debug(_dpp)            lsfs_dout(_dpp, SFS_LOG_DEBUG)
#define lsfs_debug_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_DEBUG, _whom)

#define lsfs_trace(_dpp)            lsfs_dout(_dpp, SFS_LOG_TRACE)
#define lsfs_trace_for(_dpp, _whom) lsfs_dout_for(_dpp, SFS_LOG_TRACE, _whom)
