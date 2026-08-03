/**
 * @file src/otel_logs.h
 * @brief Boost.Log bridge that buffers records for OTLP log export.
 *
 * Attaches an extra sink to the existing logging core, so every record that
 * already goes to the console and the log file is also captured as a structured
 * OTLP log record. The bridge only buffers; the OTLP payload is built and
 * shipped by @ref otel.
 */
#pragma once

// standard includes
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace otel::logs {

  using attributes_t = std::vector<std::pair<std::string, std::string>>;

  struct record_t {
    std::uint64_t time_unix_nano = 0;
    int severity = 2;  ///< Boost.Log severity (0 verbose .. 5 fatal)
    std::string body;
    attributes_t attributes;
  };

  /**
   * @brief Attach the bridge to the logging core.
   * @param min_severity Lowest Boost.Log severity to capture.
   * @param max_buffered Records held before the oldest ones are discarded.
   */
  void attach(int min_severity, std::size_t max_buffered);

  /** @brief Update the capture threshold without re-attaching. */
  void set_min_severity(int min_severity);

  /** @brief Detach the bridge from the logging core and drop the buffer. */
  void detach();

  /** @brief Whether the bridge is attached. */
  bool is_attached();

  /**
   * @brief Record a structured event that is not part of the Boost.Log stream.
   *
   * Used for stream lifecycle events (session started/ended) so consumers can
   * query them as discrete events rather than parsing log text.
   */
  void emit_event(const std::string &event_name, int severity, const std::string &body, attributes_t attributes);

  /**
   * @brief Move up to @p max buffered records out of the bridge.
   *
   * Returns an empty vector when nothing is pending.
   */
  std::vector<record_t> take_batch(std::size_t max);

  /** @brief Number of records discarded because the buffer was full. */
  std::uint64_t dropped_count();

}  // namespace otel::logs
