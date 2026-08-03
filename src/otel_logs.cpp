/**
 * @file src/otel_logs.cpp
 * @brief Boost.Log bridge implementation.
 */

// standard includes
#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>

// lib includes
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/utility/exception_handler.hpp>
#include <boost/make_shared.hpp>

// local includes
#include "otel_logs.h"
#include "otel_exporter.h"
#include "logging.h"

namespace otel::logs {

  namespace {

    namespace bl = boost::log;
    namespace sinks = boost::log::sinks;
    namespace expr = boost::log::expressions;

    std::mutex g_buffer_mutex;
    std::deque<record_t> g_buffer;
    std::size_t g_max_buffered = 2048;
    std::atomic<int> g_min_severity {2};
    std::atomic<std::uint64_t> g_dropped {0};

    /**
     * @brief Guards against a log record produced while building or shipping a
     *        log payload being fed straight back into the buffer.
     */
    thread_local bool tl_capturing = false;

    std::uint64_t now_unix_nano() {
      return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch()
        )
          .count()
      );
    }

    void push_record(record_t record) {
      std::lock_guard lk {g_buffer_mutex};
      while (g_buffer.size() >= g_max_buffered) {
        g_buffer.pop_front();
        g_dropped.fetch_add(1, std::memory_order_relaxed);
      }
      g_buffer.push_back(std::move(record));
    }

    struct otel_sink_backend: public sinks::basic_sink_backend<sinks::concurrent_feeding> {
      void consume(const bl::record_view &rec) {
        // Never capture records emitted from inside the export path, or the
        // exporter's own diagnostics would grow the buffer they are draining.
        if (tl_capturing || exporter::is_exporter_thread()) {
          return;
        }

        int log_severity = 2;
        if (const auto value = rec[severity]; value) {
          log_severity = value.get();
        }
        if (log_severity < g_min_severity.load(std::memory_order_relaxed)) {
          return;
        }

        record_t record;
        record.time_unix_nano = now_unix_nano();
        record.severity = log_severity;
        if (const auto message = rec[expr::smessage]; message) {
          record.body = message.get();
        }

        tl_capturing = true;
        push_record(std::move(record));
        tl_capturing = false;
      }
    };

    using otel_sink_t = sinks::synchronous_sink<otel_sink_backend>;
    boost::shared_ptr<otel_sink_t> g_sink;
    std::mutex g_sink_mutex;

  }  // namespace

  void attach(int min_severity, std::size_t max_buffered) {
    std::lock_guard lk {g_sink_mutex};
    if (g_sink) {
      set_min_severity(min_severity);
      return;
    }

    {
      std::lock_guard buffer_lk {g_buffer_mutex};
      g_max_buffered = max_buffered > 0 ? max_buffered : 2048;
    }
    g_min_severity.store(min_severity, std::memory_order_relaxed);

    g_sink = boost::make_shared<otel_sink_t>();
    g_sink->set_exception_handler(bl::make_exception_suppressor());
    bl::core::get()->add_sink(g_sink);
  }

  void set_min_severity(int min_severity) {
    g_min_severity.store(min_severity, std::memory_order_relaxed);
  }

  void detach() {
    boost::shared_ptr<otel_sink_t> sink;
    {
      std::lock_guard lk {g_sink_mutex};
      sink = g_sink;
      g_sink.reset();
    }

    if (sink) {
      bl::core::get()->remove_sink(sink);
      sink->flush();
    }

    std::lock_guard buffer_lk {g_buffer_mutex};
    g_buffer.clear();
  }

  bool is_attached() {
    std::lock_guard lk {g_sink_mutex};
    return static_cast<bool>(g_sink);
  }

  void emit_event(const std::string &event_name, int severity, const std::string &body, attributes_t attributes) {
    if (!is_attached()) {
      return;
    }

    record_t record;
    record.time_unix_nano = now_unix_nano();
    record.severity = severity;
    record.body = body;
    record.attributes = std::move(attributes);
    record.attributes.emplace_back("event.name", event_name);

    tl_capturing = true;
    push_record(std::move(record));
    tl_capturing = false;
  }

  std::vector<record_t> take_batch(std::size_t max) {
    std::vector<record_t> batch;
    std::lock_guard lk {g_buffer_mutex};
    const std::size_t count = std::min(max, g_buffer.size());
    batch.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      batch.push_back(std::move(g_buffer.front()));
      g_buffer.pop_front();
    }
    return batch;
  }

  std::uint64_t dropped_count() {
    return g_dropped.load(std::memory_order_relaxed);
  }

}  // namespace otel::logs
