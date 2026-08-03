/**
 * @file src/otel.h
 * @brief OpenTelemetry export for Vibepollo host and stream telemetry.
 *
 * Exports the counters the app already maintains (host CPU/GPU/RAM/VRAM/network
 * and per-session streaming statistics) as OTLP metrics, and mirrors the boost
 * log stream as OTLP log records. Both signals are shipped over OTLP/HTTP with
 * a JSON body, so no OpenTelemetry SDK dependency is required.
 *
 * The pipeline is entirely independent of the SQLite session history: enabling
 * OTLP export does not require history persistence to be turned on.
 */
#pragma once

// standard includes
#include <string>

// local includes
#include "session_history.h"

namespace otel {

  /**
   * @brief Start the OpenTelemetry pipeline if it is enabled in the config.
   *
   * Attaches the log bridge, starts the exporter thread and the metric
   * collector. Safe to call when export is disabled: it simply does nothing.
   *
   * Call after logging::init so early records reach the collector, and before
   * the streaming subsystems come up.
   */
  void init();

  /** @brief Stop the collector, detach the log bridge and flush pending exports. */
  void shutdown();

  /**
   * @brief Re-read config::otel and apply it without restarting.
   *
   * Handles being switched on or off, endpoint/header changes, and interval
   * changes. Called from the config hot-apply path.
   */
  void reload_settings();

  /** @brief Whether the pipeline is currently running. */
  bool is_running();

  // ── Session lifecycle (called from session_history::begin_session/end_session) ──

  /**
   * @brief Register a stream session so its metrics carry app/client attributes.
   *
   * Called for every session regardless of whether history persistence is
   * enabled, so app-level attribution (which game, which client) is always
   * available on the exported series.
   */
  void on_session_started(const session_history::session_metadata_t &metadata);

  /** @brief Unregister a stream session and emit its end-of-session event. */
  void on_session_ended(const std::string &uuid);

}  // namespace otel
