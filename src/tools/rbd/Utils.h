// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_UTILS_H
#define CEPH_RBD_UTILS_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "tools/rbd/ArgumentTypes.h"
#include <string>
#include <boost/program_options.hpp>

namespace rbd {
namespace utils {

namespace detail {

template <typename T, void(T::*MF)(int)>
void aio_completion_callback(librbd::completion_t completion,
                                    void *arg) {
  librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(completion);

  // complete the AIO callback in separate thread context
  T *t = reinterpret_cast<T *>(arg);
  int r = aio_completion->get_return_value();
  aio_completion->release();

  (t->*MF)(r);
}

} // namespace detail

static const std::string RBD_DIFF_BANNER ("rbd diff v1\n");
static const size_t RBD_DEFAULT_SPARSE_SIZE = 4096;

static const std::string RBD_IMAGE_BANNER_V2 ("rbd image v2\n");
static const std::string RBD_IMAGE_DIFFS_BANNER_V2 ("rbd image diffs v2\n");
static const std::string RBD_DIFF_BANNER_V2 ("rbd diff v2\n");

#define RBD_DIFF_FROM_SNAP	'f'
#define RBD_DIFF_TO_SNAP	't'
#define RBD_DIFF_IMAGE_SIZE	's'
#define RBD_DIFF_WRITE		'w'
#define RBD_DIFF_ZERO		'z'
#define RBD_DIFF_END		'e'

#define RBD_EXPORT_IMAGE_ORDER		'O'
#define RBD_EXPORT_IMAGE_FEATURES	'T'
#define RBD_EXPORT_IMAGE_STRIPE_UNIT	'U'
#define RBD_EXPORT_IMAGE_STRIPE_COUNT	'C'
#define RBD_EXPORT_IMAGE_END		'E'

enum SnapshotPresence {
  SNAPSHOT_PRESENCE_NONE,
  SNAPSHOT_PRESENCE_PERMITTED,
  SNAPSHOT_PRESENCE_REQUIRED
};

enum SpecValidation {
  SPEC_VALIDATION_FULL,
  SPEC_VALIDATION_SNAP,
  SPEC_VALIDATION_NONE
};

struct ProgressContext : public librbd::ProgressContext {
  const char *operation;
  bool progress;
  int last_pc;

  ProgressContext(const char *o, bool no_progress)
    : operation(o), progress(!no_progress), last_pc(0) {
  }

  int update_progress(uint64_t offset, uint64_t total) override;
  void finish();
  void fail();
};

template <typename T, void(T::*MF)(int)>
librbd::RBD::AioCompletion *create_aio_completion(T *t) {
  return new librbd::RBD::AioCompletion(
    t, &detail::aio_completion_callback<T, MF>);
}

void aio_context_callback(librbd::completion_t completion, void *arg);

int read_string(int fd, unsigned max, std::string *out);

int extract_spec(const std::string &spec, std::string *pool_name,
                 std::string *image_name, std::string *snap_name,
                 SpecValidation spec_validation);

int extract_group_spec(const std::string &spec,
		       std::string *pool_name,
		       std::string *group_name);

int extract_image_id_spec(const std::string &spec, std::string *pool_name,
                          std::string *image_id);

std::string get_positional_argument(
    const boost::program_options::variables_map &vm, size_t index);

std::string get_default_pool_name();
std::string get_pool_name(const boost::program_options::variables_map &vm,
                          size_t *arg_index);

int get_pool_and_namespace_names(
    const boost::program_options::variables_map &vm,
    bool default_empty_pool_name, bool validate_pool_name,
    std::string* pool_name, std::string* namespace_name, size_t *arg_index);

int get_pool_image_snapshot_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    std::string *pool_name, std::string *image_name, std::string *snap_name,
    SnapshotPresence snapshot_presence, SpecValidation spec_validation,
    bool image_required = true);

int get_pool_snapshot_names(const boost::program_options::variables_map &vm,
                            argument_types::ArgumentModifier mod,
                            size_t *spec_arg_index, std::string *pool_name,
                            std::string *snap_name,
                            SnapshotPresence snapshot_presence,
                            SpecValidation spec_validation);

int get_special_pool_group_names(const boost::program_options::variables_map &vm,
				 size_t *arg_index,
				 std::string *group_pool_name,
				 std::string *group_name);

int get_special_pool_image_names(const boost::program_options::variables_map &vm,
				 size_t *arg_index,
				 std::string *image_pool_name,
				 std::string *image_name);

int get_pool_image_id(const boost::program_options::variables_map &vm,
		      size_t *arg_index, std::string *image_pool_name,
		      std::string *image_id);

int get_pool_group_names(const boost::program_options::variables_map &vm,
			 argument_types::ArgumentModifier mod,
			 size_t *spec_arg_index,
			 std::string *pool_name,
			 std::string *group_name);

int get_pool_journal_names(
    const boost::program_options::variables_map &vm,
    argument_types::ArgumentModifier mod, size_t *spec_arg_index,
    std::string *pool_name, std::string *journal_name);

int validate_snapshot_name(argument_types::ArgumentModifier mod,
                           const std::string &snap_name,
                           SnapshotPresence snapshot_presence,
			   SpecValidation spec_validation);

int get_image_options(const boost::program_options::variables_map &vm,
                      bool get_format, librbd::ImageOptions* opts);

int get_journal_options(const boost::program_options::variables_map &vm,
			librbd::ImageOptions *opts);

int get_image_size(const boost::program_options::variables_map &vm,
                   uint64_t *size);

int get_path(const boost::program_options::variables_map &vm,
             const std::string &positional_path, std::string *path);

int get_formatter(const boost::program_options::variables_map &vm,
                  argument_types::Format::Formatter *formatter);

void init_context();

int init_rados(librados::Rados *rados);

int init(const std::string &pool_name, librados::Rados *rados,
         librados::IoCtx *io_ctx);

int init_io_ctx(librados::Rados &rados, const std::string &pool_name,
                librados::IoCtx *io_ctx);

int open_image(librados::IoCtx &io_ctx, const std::string &image_name,
               bool read_only, librbd::Image *image);

int open_image_by_id(librados::IoCtx &io_ctx, const std::string &image_id,
                     bool read_only, librbd::Image *image);

int init_and_open_image(const std::string &pool_name,
                        const std::string &image_name,
                        const std::string &image_id,
                        const std::string &snap_name, bool read_only,
                        librados::Rados *rados, librados::IoCtx *io_ctx,
                        librbd::Image *image);

int snap_set(librbd::Image &image, const std::string &snap_name);

void calc_sparse_extent(const bufferptr &bp,
                        size_t sparse_size,
			size_t buffer_offset,
                        uint64_t length,
                        size_t *write_length,
			bool *zeroed);

bool check_if_image_spec_present(const boost::program_options::variables_map &vm,
                                 argument_types::ArgumentModifier mod,
                                 size_t spec_arg_index);

std::string image_id(librbd::Image& image);

std::string mirror_image_state(librbd::mirror_image_state_t mirror_image_state);
std::string mirror_image_status_state(librbd::mirror_image_status_state_t state);
std::string mirror_image_status_state(librbd::mirror_image_status_t status);

std::string timestr(time_t t);

// duplicate here to not include librbd_internal lib
uint64_t get_rbd_default_features(CephContext* cct);

} // namespace utils

inline std::ostream& format_u(std::ostream& out, const uint64_t v, const uint64_t n,
      const int index, const uint64_t mult, const char* u)
  {
    char buffer[32];

    if (index == 0) {
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else if ((v % mult) == 0) {
      // If this is an even multiple of the base, always display
      // without any decimal fraction.
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else {
      // We want to choose a precision that reflects the best choice
      // for fitting in 5 characters.  This can get rather tricky when
      // we have numbers that are very close to an order of magnitude.
      // For example, when displaying 10239 (which is really 9.999K),
      // we want only a single place of precision for 10.0K.  We could
      // develop some complex heuristics for this, but it's much
      // easier just to try each combination in turn.
      int i;
      for (i = 2; i >= 0; i--) {
        if (snprintf(buffer, sizeof(buffer), "%.*f%s", i,
          static_cast<double>(v) / mult, u) <= 7)
          break;
      }
    }

    return out << buffer;
  }

/*
 * Use this struct to pretty print values that should be formatted with a
 * decimal unit prefix (the classic SI units). No actual unit will be added.
 */
struct si_u_t {
  uint64_t v;
  explicit si_u_t(uint64_t _v) : v(_v) {};
};

inline std::ostream& operator<<(std::ostream& out, const si_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  uint64_t mult = 1;
  const char* u[] = {"", "k", "M", "G", "T", "P", "E"};

  while (n >= 1000 && index < 7) {
    n /= 1000;
    index++;
    mult *= 1000;
  }

  return format_u(out, b.v, n, index, mult, u[index]);
}

/*
 * Use this struct to pretty print values that should be formatted with a
 * binary unit prefix (IEC units). Since binary unit prefixes are to be used for
 * "multiples of units in data processing, data transmission, and digital
 * information" (so bits and bytes) and so far bits are not printed, the unit
 * "B" for "byte" is added besides the multiplier.
 */
struct byte_u_t {
  uint64_t v;
  explicit byte_u_t(uint64_t _v) : v(_v) {};
};

inline std::ostream& operator<<(std::ostream& out, const byte_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  const char* u[] = {" B", " KiB", " MiB", " GiB", " TiB", " PiB", " EiB"};

  while (n >= 1024 && index < 7) {
    n /= 1024;
    index++;
  }

  return format_u(out, b.v, n, index, 1ULL << (10 * index), u[index]);
}


} // namespace rbd

#endif // CEPH_RBD_UTILS_H
