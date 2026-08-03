/**
 * @file src/otel_exporter.h
 * @brief OTLP/HTTP transport for the OpenTelemetry export pipeline.
 *
 * Owns a single background thread that POSTs pre-serialized OTLP JSON bodies
 * to an OpenTelemetry collector. Producers (the metric collector and the log
 * bridge) never block on the network: they hand a finished payload to a bounded
 * queue and return immediately.
 */
#pragma once

// standard includes
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace otel::exporter {

  /** @brief Which OTLP signal a queued payload belongs to. */
  enum class signal_e {
    metrics,
    logs
  };

  struct settings_t {
    std::string metrics_url;  ///< Full URL, e.g. http://host:4318/v1/metrics
    std::string logs_url;  ///< Full URL, e.g. http://host:4318/v1/logs
    std::vector<std::pair<std::string, std::string>> headers;  ///< Extra request headers
    int timeout_ms = 10000;
    bool insecure_skip_verify = false;  ///< Skip TLS peer/host verification (self-signed collectors)
    std::size_t max_queued_payloads = 256;  ///< Bounded queue; oldest payloads are dropped first
  };

  struct stats_t {
    std::uint64_t exported = 0;  ///< Payloads accepted by the collector
    std::uint64_t failed = 0;  ///< Payloads given up on after retries
    std::uint64_t dropped = 0;  ///< Payloads discarded because the queue was full
    std::size_t queued = 0;  ///< Payloads currently waiting to be sent
  };

  /**
   * @brief Apply transport settings.
   *
   * Safe to call while the exporter is running; the next request picks up the
   * new values.
   */
  void configure(const settings_t &settings);

  /** @brief Start the sender thread. No-op if already running. */
  void start();

  /**
   * @brief Stop the sender thread.
   * @param flush Attempt one final drain of the queue before returning.
   */
  void stop(bool flush = true);

  /** @brief Whether the sender thread is running. */
  bool is_running();

  /**
   * @brief Queue a serialized OTLP JSON body for delivery.
   * @return false if the payload was dropped (exporter stopped or queue full).
   */
  bool enqueue(signal_e signal, std::string payload);

  /** @brief Snapshot of delivery counters, for diagnostics. */
  stats_t stats();

  /**
   * @brief True while the calling thread is inside the exporter.
   *
   * The log bridge uses this to avoid re-ingesting log records that the
   * exporter itself emits, which would otherwise feed back into the queue.
   */
  bool is_exporter_thread();

}  // namespace otel::exporter
