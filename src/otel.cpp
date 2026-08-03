/**
 * @file src/otel.cpp
 * @brief OpenTelemetry metric collection and OTLP payload construction.
 */

// standard includes
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// lib includes
#include <nlohmann/json.hpp>

// local includes
#include "otel.h"
#include "config.h"
#include "host_stats.h"
#include "logging.h"
#include "otel_exporter.h"
#include "otel_logs.h"
#include "platform/common.h"
#include "process.h"
#include "stream.h"
#include "webrtc_stream.h"

using namespace std::literals;

namespace otel {

  namespace {

    using json = nlohmann::json;

    constexpr std::size_t MAX_LOG_RECORDS_PER_BATCH = 512;
    constexpr std::size_t MAX_BUFFERED_LOG_RECORDS = 4096;

    // ── Runtime state ──────────────────────────────────────────────────

    struct runtime_settings_t {
      bool metrics_enabled = true;
      bool logs_enabled = true;
      std::chrono::milliseconds export_interval {10000};
    };

    /// Per-session rate aggregator. Deliberately independent of the session
    /// history aggregator so OTLP export keeps working with history disabled.
    struct aggregator_t {
      double prev_timestamp = 0;
      std::uint64_t prev_frames_sent = 0;
      std::uint64_t prev_bytes_sent = 0;

      int welford_n = 0;
      double welford_mean = 0;
      double welford_m2 = 0;

      double actual_fps = 0;
      double actual_bitrate_kbps = 0;
      double jitter_ms = 0;

      void update(double ts, std::uint64_t frames, std::uint64_t bytes) {
        if (prev_timestamp > 0) {
          const double dt = ts - prev_timestamp;
          // Counters restart when a session is re-established under the same
          // uuid; treat a decrease as a reset rather than a huge negative rate.
          if (dt > 0.01 && frames >= prev_frames_sent && bytes >= prev_bytes_sent) {
            const auto dframes = static_cast<double>(frames - prev_frames_sent);
            const auto dbytes = static_cast<double>(bytes - prev_bytes_sent);

            actual_fps = dframes / dt;
            actual_bitrate_kbps = (dbytes * 8.0) / (dt * 1000.0);

            if (dframes > 0) {
              const double interval_ms = (dt * 1000.0) / dframes;
              ++welford_n;
              const double delta = interval_ms - welford_mean;
              welford_mean += delta / welford_n;
              welford_m2 += delta * (interval_ms - welford_mean);
              jitter_ms = welford_n > 1 ? std::sqrt(welford_m2 / (welford_n - 1)) : 0;
            }
          }
        }
        prev_timestamp = ts;
        prev_frames_sent = frames;
        prev_bytes_sent = bytes;
      }
    };

    struct tracked_session_t {
      session_history::session_metadata_t metadata;
      std::uint64_t start_time_unix_nano = 0;
      std::chrono::steady_clock::time_point start_steady {};
      aggregator_t aggregator;
    };

    std::mutex g_lifecycle_mutex;
    std::atomic<bool> g_running {false};
    std::thread g_collector_thread;
    std::condition_variable g_collector_cv;
    std::mutex g_collector_mutex;

    std::mutex g_settings_mutex;
    runtime_settings_t g_settings;

    std::mutex g_sessions_mutex;
    std::unordered_map<std::string, tracked_session_t> g_sessions;

    /// Identity of the app the host is currently running.
    struct active_app_t {
      std::string name {"desktop"};
      std::string uuid;
      int app_id = -1;

      bool operator==(const active_app_t &other) const {
        return app_id == other.app_id && name == other.name && uuid == other.uuid;
      }
    };

    /// Cumulative streaming time per app, surviving individual sessions.
    std::mutex g_playtime_mutex;
    std::unordered_map<std::string, double> g_app_playtime_seconds;

    /// Last app observed by the collector, used to detect switches. Collector
    /// thread only, except for the initial value.
    active_app_t g_last_active_app;
    std::chrono::steady_clock::time_point g_playtime_last_accrued {};
    bool g_playtime_started = false;
    bool g_playtime_had_session = false;

    json g_resource;  ///< Written by init() and then only by the collector thread
    bool g_resource_pending_host_info = true;  ///< Collector thread only
    std::uint64_t g_process_start_unix_nano = 0;

    // ── Small helpers ──────────────────────────────────────────────────

    std::uint64_t now_unix_nano() {
      return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch()
        )
          .count()
      );
    }

    double now_unix_seconds() {
      return std::chrono::duration<double>(
               std::chrono::system_clock::now().time_since_epoch()
      )
        .count();
    }

    std::string trim(std::string_view input) {
      const auto begin = input.find_first_not_of(" \t\r\n");
      if (begin == std::string_view::npos) {
        return {};
      }
      const auto end = input.find_last_not_of(" \t\r\n");
      return std::string {input.substr(begin, end - begin + 1)};
    }

    /**
     * @brief Parse a `key=value,key2=value2` list.
     *
     * Used for both `otel_headers` and `otel_resource_attributes`, matching the
     * format of the standard OTEL_EXPORTER_OTLP_HEADERS environment variable.
     */
    std::vector<std::pair<std::string, std::string>> parse_kv_list(const std::string &input) {
      std::vector<std::pair<std::string, std::string>> result;
      std::size_t pos = 0;
      while (pos <= input.size()) {
        const auto comma = input.find(',', pos);
        const auto item = input.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        const auto eq = item.find('=');
        if (eq != std::string::npos) {
          auto key = trim(std::string_view {item}.substr(0, eq));
          auto value = trim(std::string_view {item}.substr(eq + 1));
          if (!key.empty()) {
            result.emplace_back(std::move(key), std::move(value));
          }
        }
        if (comma == std::string::npos) {
          break;
        }
        pos = comma + 1;
      }
      return result;
    }

    std::string env_or(const char *name, const std::string &fallback) {
      if (const char *value = std::getenv(name); value && *value) {
        return value;
      }
      return fallback;
    }

    std::string strip_trailing_slash(std::string url) {
      while (!url.empty() && url.back() == '/') {
        url.pop_back();
      }
      return url;
    }

    /**
     * @brief Resolve the endpoint for one signal.
     *
     * Precedence: explicit per-signal config, per-signal env var, base config
     * endpoint + path, base env var + path.
     */
    std::string resolve_signal_url(
      const std::string &configured_signal_url,
      const char *signal_env,
      const std::string &configured_base,
      const char *base_env,
      const char *path
    ) {
      const auto signal_url = trim(configured_signal_url);
      if (!signal_url.empty()) {
        return signal_url;
      }
      if (const auto from_env = env_or(signal_env, ""); !from_env.empty()) {
        return from_env;
      }

      auto base = trim(configured_base);
      if (base.empty()) {
        base = env_or(base_env, "");
      }
      if (base.empty()) {
        return {};
      }
      return strip_trailing_slash(std::move(base)) + path;
    }

    // ── OTLP JSON building blocks ──────────────────────────────────────
    //
    // OTLP/JSON encodes 64-bit integers as decimal strings, so every timestamp
    // and integer data point below is stringified rather than emitted as a
    // JSON number.

    json attr_string(const std::string &key, const std::string &value) {
      json a;
      a["key"] = key;
      a["value"]["stringValue"] = value;
      return a;
    }

    json attr_int(const std::string &key, std::int64_t value) {
      json a;
      a["key"] = key;
      a["value"]["intValue"] = std::to_string(value);
      return a;
    }

    json attr_bool(const std::string &key, bool value) {
      json a;
      a["key"] = key;
      a["value"]["boolValue"] = value;
      return a;
    }

    /**
     * @brief Accumulates data points into one Metric entry per metric name.
     *
     * Several sessions contribute points to the same series, so points are
     * grouped rather than emitted as repeated single-point Metric entries.
     */
    class metric_set_t {
    public:
      void gauge_double(const char *name, const char *unit, double value, const json &attributes, std::uint64_t ts) {
        auto &points = data_points(name, unit, "gauge");
        json point;
        point["timeUnixNano"] = std::to_string(ts);
        point["asDouble"] = value;
        attach(point, attributes);
        points.push_back(std::move(point));
      }

      void gauge_int(const char *name, const char *unit, std::int64_t value, const json &attributes, std::uint64_t ts) {
        auto &points = data_points(name, unit, "gauge");
        json point;
        point["timeUnixNano"] = std::to_string(ts);
        point["asInt"] = std::to_string(value);
        attach(point, attributes);
        points.push_back(std::move(point));
      }

      void sum_int(
        const char *name,
        const char *unit,
        std::int64_t value,
        const json &attributes,
        std::uint64_t start_ts,
        std::uint64_t ts
      ) {
        auto &points = data_points(name, unit, "sum");
        json point;
        point["startTimeUnixNano"] = std::to_string(start_ts);
        point["timeUnixNano"] = std::to_string(ts);
        point["asInt"] = std::to_string(value);
        attach(point, attributes);
        points.push_back(std::move(point));
      }

      void sum_double(
        const char *name,
        const char *unit,
        double value,
        const json &attributes,
        std::uint64_t start_ts,
        std::uint64_t ts
      ) {
        auto &points = data_points(name, unit, "sum");
        json point;
        point["startTimeUnixNano"] = std::to_string(start_ts);
        point["timeUnixNano"] = std::to_string(ts);
        point["asDouble"] = value;
        attach(point, attributes);
        points.push_back(std::move(point));
      }

      bool empty() const {
        return metrics.empty();
      }

      json release() {
        return std::move(metrics);
      }

    private:
      static void attach(json &point, const json &attributes) {
        if (!attributes.empty()) {
          point["attributes"] = attributes;
        }
      }

      json &data_points(const char *name, const char *unit, const char *kind) {
        if (const auto it = index.find(name); it != index.end()) {
          return metrics[it->second][kind]["dataPoints"];
        }

        json metric;
        metric["name"] = name;
        metric["unit"] = unit;
        metric[kind]["dataPoints"] = json::array();
        if (std::string_view {kind} == "sum") {
          metric[kind]["aggregationTemporality"] = 2;  // AGGREGATION_TEMPORALITY_CUMULATIVE
          metric[kind]["isMonotonic"] = true;
        }

        index.emplace(name, metrics.size());
        metrics.push_back(std::move(metric));
        return metrics.back()[kind]["dataPoints"];
      }

      json metrics = json::array();
      std::unordered_map<std::string, std::size_t> index;
    };

    json build_resource() {
      auto service_name = trim(config::otel.service_name);
      if (service_name.empty()) {
        service_name = env_or("OTEL_SERVICE_NAME", "vibepollo");
      }

      json attributes = json::array();
      attributes.push_back(attr_string("service.name", service_name));
      attributes.push_back(attr_string("service.version", PROJECT_VERSION));
      attributes.push_back(attr_string("service.instance.id", platf::get_host_name()));
      attributes.push_back(attr_string("host.name", platf::get_host_name()));
      attributes.push_back(attr_string("telemetry.sdk.name", "vibepollo-otlp"));
      attributes.push_back(attr_string("telemetry.sdk.language", "cpp"));

      if (const auto namespace_name = trim(config::otel.service_namespace); !namespace_name.empty()) {
        attributes.push_back(attr_string("service.namespace", namespace_name));
      }

#if defined(_WIN32)
      attributes.push_back(attr_string("os.type", "windows"));
#elif defined(__APPLE__)
      attributes.push_back(attr_string("os.type", "darwin"));
#else
      attributes.push_back(attr_string("os.type", "linux"));
#endif

      const auto &host = host_stats::info();
      if (!host.cpu_model.empty()) {
        attributes.push_back(attr_string("host.cpu.model", host.cpu_model));
      }
      if (!host.gpu_model.empty()) {
        attributes.push_back(attr_string("host.gpu.model", host.gpu_model));
      }

      auto extra = trim(config::otel.resource_attributes);
      if (extra.empty()) {
        extra = env_or("OTEL_RESOURCE_ATTRIBUTES", "");
      }
      for (const auto &[key, value] : parse_kv_list(extra)) {
        attributes.push_back(attr_string(key, value));
      }

      json resource;
      resource["attributes"] = std::move(attributes);
      return resource;
    }

    json wrap_metrics(json metrics) {
      json scope_metrics;
      scope_metrics["scope"]["name"] = "vibepollo";
      scope_metrics["scope"]["version"] = PROJECT_VERSION;
      scope_metrics["metrics"] = std::move(metrics);

      json resource_metrics;
      resource_metrics["resource"] = g_resource;
      resource_metrics["scopeMetrics"] = json::array({std::move(scope_metrics)});

      json payload;
      payload["resourceMetrics"] = json::array({std::move(resource_metrics)});
      return payload;
    }

    // ── Metric collection ──────────────────────────────────────────────

    void collect_host_metrics(metric_set_t &metrics, std::uint64_t ts) {
      const auto stats = host_stats::latest();
      const json no_attributes = json::array();

      if (stats.cpu_percent >= 0.f) {
        metrics.gauge_double("vibepollo.host.cpu.utilization", "%", stats.cpu_percent, no_attributes, ts);
      }
      if (stats.cpu_temp_c >= 0.f) {
        metrics.gauge_double("vibepollo.host.cpu.temperature", "Cel", stats.cpu_temp_c, no_attributes, ts);
      }
      if (stats.gpu_percent >= 0.f) {
        metrics.gauge_double("vibepollo.host.gpu.utilization", "%", stats.gpu_percent, no_attributes, ts);
      }
      if (stats.gpu_encoder_percent >= 0.f) {
        metrics.gauge_double("vibepollo.host.gpu.encoder.utilization", "%", stats.gpu_encoder_percent, no_attributes, ts);
      }
      if (stats.gpu_temp_c >= 0.f) {
        metrics.gauge_double("vibepollo.host.gpu.temperature", "Cel", stats.gpu_temp_c, no_attributes, ts);
      }

      if (stats.ram_total_bytes > 0) {
        metrics.gauge_int("vibepollo.host.memory.usage", "By", static_cast<std::int64_t>(stats.ram_used_bytes), no_attributes, ts);
        metrics.gauge_int("vibepollo.host.memory.limit", "By", static_cast<std::int64_t>(stats.ram_total_bytes), no_attributes, ts);
        metrics.gauge_double(
          "vibepollo.host.memory.utilization",
          "%",
          static_cast<double>(stats.ram_used_bytes) * 100.0 / static_cast<double>(stats.ram_total_bytes),
          no_attributes,
          ts
        );
      }

      if (stats.vram_total_bytes > 0) {
        metrics.gauge_int("vibepollo.host.gpu.memory.usage", "By", static_cast<std::int64_t>(stats.vram_used_bytes), no_attributes, ts);
        metrics.gauge_int("vibepollo.host.gpu.memory.limit", "By", static_cast<std::int64_t>(stats.vram_total_bytes), no_attributes, ts);
        metrics.gauge_double(
          "vibepollo.host.gpu.memory.utilization",
          "%",
          static_cast<double>(stats.vram_used_bytes) * 100.0 / static_cast<double>(stats.vram_total_bytes),
          no_attributes,
          ts
        );
      }

      if (stats.net_rx_bps >= 0.0) {
        metrics.gauge_double(
          "vibepollo.host.network.throughput",
          "bit/s",
          stats.net_rx_bps,
          json::array({attr_string("network.io.direction", "receive")}),
          ts
        );
      }
      if (stats.net_tx_bps >= 0.0) {
        metrics.gauge_double(
          "vibepollo.host.network.throughput",
          "bit/s",
          stats.net_tx_bps,
          json::array({attr_string("network.io.direction", "transmit")}),
          ts
        );
      }
    }

    /**
     * @brief Read the app the host is currently running.
     *
     * Deliberately resolved on every collection tick rather than captured once
     * at session start: a client can quit one game and launch another without
     * tearing down the stream, and attributing the whole session to whatever
     * happened to be running first would make per-game totals wrong.
     *
     * Uses only the snapshot-returning accessors — @c current_app_id is an
     * atomic and @c resolve_app copies under the app-list lock. The unlocked
     * @c get_last_run_app_name / @c get_running_app_uuid are avoided on purpose:
     * they read @c proc_t members that the launch path mutates concurrently, and
     * they report the last app run rather than the one running now.
     */
    active_app_t read_active_app() {
      active_app_t app;
      app.app_id = proc::proc.current_app_id();

      if (app.app_id <= 0) {
        return app;  // nothing launched; the client is streaming the desktop
      }

      if (const auto ctx = proc::proc.resolve_app(app.app_id)) {
        app.name = ctx->name.empty() ? "unknown" : ctx->name;
        app.uuid = ctx->uuid;
      } else {
        // Launched from an entry that has since been removed from apps.json.
        app.name = "unknown";
      }

      return app;
    }

    /**
     * @brief Attributes attached to every data point of one stream session.
     *
     * `app.name` is what makes "which game ran, for how long, at what cost"
     * answerable downstream, so it is carried on the metrics as well as on the
     * lifecycle events. It comes from the live lookup rather than the session
     * metadata so a mid-session app switch is reflected immediately.
     */
    json session_attributes(
      const std::string &uuid,
      const tracked_session_t *tracked,
      const char *protocol,
      const active_app_t &app
    ) {
      json attributes = json::array();
      attributes.push_back(attr_string("session.id", uuid));
      attributes.push_back(attr_string("session.protocol", protocol));
      attributes.push_back(attr_string("app.name", app.name));
      if (!app.uuid.empty()) {
        // Stable across renames in the web UI, unlike the display name.
        attributes.push_back(attr_string("app.uuid", app.uuid));
      }

      if (tracked) {
        const auto &meta = tracked->metadata;
        if (!meta.client_name.empty()) {
          attributes.push_back(attr_string("client.name", meta.client_name));
        }
        if (!meta.device_name.empty()) {
          attributes.push_back(attr_string("client.device", meta.device_name));
        }
        if (!meta.codec.empty()) {
          attributes.push_back(attr_string("video.codec", meta.codec));
        }
        if (meta.width > 0 && meta.height > 0) {
          attributes.push_back(attr_int("video.width", meta.width));
          attributes.push_back(attr_int("video.height", meta.height));
        }
        attributes.push_back(attr_bool("video.hdr", meta.hdr));
        if (!meta.stream_gpu_model.empty()) {
          attributes.push_back(attr_string("gpu.model", meta.stream_gpu_model));
        }
      }

      return attributes;
    }

    struct session_counters_t {
      std::string uuid;
      const char *protocol = "rtsp";
      std::string state;  ///< "running", "starting", … ; empty when not reported
      std::uint64_t frames_sent = 0;
      std::uint64_t packets_sent = 0;
      std::uint64_t bytes_sent = 0;
      std::int64_t client_reported_losses = 0;
      std::uint64_t idr_requests = 0;
      std::uint64_t ref_invalidations = 0;
      double encode_latency_ms = 0;
      double uptime_seconds = 0;
      int target_fps = 0;
      int encoder_bitrate_kbps = 0;
      int requested_bitrate_kbps = 0;
    };

    void emit_session_metrics(
      metric_set_t &metrics,
      const session_counters_t &counters,
      const active_app_t &app,
      double ts_seconds,
      std::uint64_t ts
    ) {
      json attributes;
      std::uint64_t start_ts = g_process_start_unix_nano;
      double fps = 0;
      double bitrate_kbps = 0;
      double jitter_ms = 0;

      {
        std::lock_guard lk {g_sessions_mutex};
        auto it = g_sessions.find(counters.uuid);
        tracked_session_t *tracked = it != g_sessions.end() ? &it->second : nullptr;
        attributes = session_attributes(counters.uuid, tracked, counters.protocol, app);

        if (tracked) {
          start_ts = tracked->start_time_unix_nano;
          tracked->aggregator.update(ts_seconds, counters.frames_sent, counters.bytes_sent);
          fps = tracked->aggregator.actual_fps;
          bitrate_kbps = tracked->aggregator.actual_bitrate_kbps;
          jitter_ms = tracked->aggregator.jitter_ms;
        }
      }

      // State is reported as its own info series rather than as an attribute on
      // the throughput counters: a transition would otherwise fork every one of
      // those series and break rate queries across the boundary.
      if (!counters.state.empty()) {
        json state_attributes = attributes;
        state_attributes.push_back(attr_string("session.state", counters.state));
        metrics.gauge_int("vibepollo.session.state", "{info}", 1, state_attributes, ts);
      }

      metrics.gauge_double("vibepollo.session.uptime", "s", counters.uptime_seconds, attributes, ts);
      metrics.gauge_double("vibepollo.session.fps", "{frame}/s", fps, attributes, ts);
      metrics.gauge_double("vibepollo.session.bitrate", "kbit/s", bitrate_kbps, attributes, ts);
      metrics.gauge_double("vibepollo.session.frame_interval.jitter", "ms", jitter_ms, attributes, ts);
      metrics.gauge_double("vibepollo.session.encode.latency", "ms", counters.encode_latency_ms, attributes, ts);

      if (counters.target_fps > 0) {
        metrics.gauge_int("vibepollo.session.fps.target", "{frame}/s", counters.target_fps, attributes, ts);
      }
      if (counters.encoder_bitrate_kbps > 0) {
        metrics.gauge_int("vibepollo.session.bitrate.encoder", "kbit/s", counters.encoder_bitrate_kbps, attributes, ts);
      }
      if (counters.requested_bitrate_kbps > 0) {
        metrics.gauge_int("vibepollo.session.bitrate.requested", "kbit/s", counters.requested_bitrate_kbps, attributes, ts);
      }

      metrics.sum_int("vibepollo.session.frames.sent", "{frame}", static_cast<std::int64_t>(counters.frames_sent), attributes, start_ts, ts);
      metrics.sum_int("vibepollo.session.packets.sent", "{packet}", static_cast<std::int64_t>(counters.packets_sent), attributes, start_ts, ts);
      metrics.sum_int("vibepollo.session.bytes.sent", "By", static_cast<std::int64_t>(counters.bytes_sent), attributes, start_ts, ts);
      metrics.sum_int("vibepollo.session.packets.lost", "{packet}", std::max<std::int64_t>(counters.client_reported_losses, 0), attributes, start_ts, ts);
      metrics.sum_int("vibepollo.session.idr.requests", "{request}", static_cast<std::int64_t>(counters.idr_requests), attributes, start_ts, ts);
      metrics.sum_int("vibepollo.session.reference.invalidations", "{request}", static_cast<std::int64_t>(counters.ref_invalidations), attributes, start_ts, ts);
    }

    /**
     * @brief Credit elapsed wall time to the app that was running during it.
     *
     * Wall time, not summed per session: two clients watching the same game for
     * an hour is one hour of playtime, not two. The interval is credited to the
     * previously observed app rather than the current one, so the seconds before
     * a switch are attributed to the game that was actually running.
     *
     * Collector thread only.
     */
    void accrue_playtime(const active_app_t &current, bool has_active_session) {
      const auto now = std::chrono::steady_clock::now();
      if (!g_playtime_started) {
        g_playtime_started = true;
        g_playtime_last_accrued = now;
        g_last_active_app = current;
        g_playtime_had_session = has_active_session;
        return;
      }

      const double elapsed = std::chrono::duration<double>(now - g_playtime_last_accrued).count();
      g_playtime_last_accrued = now;

      if (g_playtime_had_session && elapsed > 0) {
        std::lock_guard lk {g_playtime_mutex};
        g_app_playtime_seconds[g_last_active_app.name] += elapsed;
      }
      g_playtime_had_session = has_active_session;

      if (!(current == g_last_active_app)) {
        logs::emit_event(
          "vibepollo.app.changed",
          2,
          "Active app changed: " + g_last_active_app.name + " -> " + current.name,
          {
            {"app.name", current.name},
            {"app.uuid", current.uuid},
            {"app.previous_name", g_last_active_app.name},
            {"app.previous_uuid", g_last_active_app.uuid},
            {"session.active", has_active_session ? "true" : "false"},
          }
        );
        g_last_active_app = current;
      }
    }

    void collect_app_metrics(metric_set_t &metrics, const active_app_t &app, std::uint64_t ts) {
      json attributes = json::array({attr_string("app.name", app.name)});
      if (!app.uuid.empty()) {
        attributes.push_back(attr_string("app.uuid", app.uuid));
      }
      metrics.gauge_int("vibepollo.app.active", "{info}", 1, attributes, ts);

      std::unordered_map<std::string, double> snapshot;
      {
        std::lock_guard lk {g_playtime_mutex};
        snapshot = g_app_playtime_seconds;
      }

      for (const auto &[name, seconds] : snapshot) {
        metrics.sum_double(
          "vibepollo.app.playtime",
          "s",
          seconds,
          json::array({attr_string("app.name", name)}),
          g_process_start_unix_nano,
          ts
        );
      }
    }

    std::int64_t collect_session_metrics(metric_set_t &metrics, const active_app_t &app, std::uint64_t ts) {
      const double ts_seconds = now_unix_seconds();
      std::int64_t active = 0;
      std::unordered_map<std::string, std::int64_t> by_state;

      for (const auto &info : stream::get_all_session_info()) {
        ++active;
        ++by_state[info.state];
        emit_session_metrics(
          metrics,
          {
            .uuid = info.uuid,
            .protocol = "rtsp",
            .state = info.state,
            .frames_sent = info.frames_sent,
            .packets_sent = info.packets_sent,
            .bytes_sent = info.bytes_sent,
            .client_reported_losses = info.client_reported_losses,
            .idr_requests = info.idr_requests,
            .ref_invalidations = info.invalidate_ref_count,
            .encode_latency_ms = info.encode_latency_ms,
            .uptime_seconds = info.uptime_seconds,
            .target_fps = info.fps,
            .encoder_bitrate_kbps = info.encoder_bitrate_kbps,
            .requested_bitrate_kbps = info.requested_bitrate_kbps ? info.requested_bitrate_kbps : info.encoder_bitrate_kbps,
          },
          app,
          ts_seconds,
          ts
        );
      }

      for (const auto &ws : webrtc_stream::list_sessions()) {
        ++active;

        // WebRTC has no session state machine; derive an equivalent from the
        // negotiation flags so both protocols report on the same series.
        const char *webrtc_state = ws.has_local_answer ? "running" :
                                   ws.has_remote_offer ? "starting" :
                                                         "negotiating";
        ++by_state[webrtc_state];

        double last_video_age_ms = 0;
        if (ws.last_video_time) {
          last_video_age_ms = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - *ws.last_video_time
          )
                                .count();
        }

        emit_session_metrics(
          metrics,
          {
            .uuid = ws.id,
            .protocol = "webrtc",
            .state = webrtc_state,
            .frames_sent = static_cast<std::uint64_t>(ws.last_video_frame_index > 0 ? ws.last_video_frame_index : 0),
            .packets_sent = ws.video_packets,
            .bytes_sent = ws.video_bytes_total + ws.audio_bytes_total,
            .client_reported_losses = static_cast<std::int64_t>(ws.video_dropped),
            .encode_latency_ms = last_video_age_ms,
            .target_fps = ws.fps.value_or(0),
            .encoder_bitrate_kbps = ws.bitrate_kbps.value_or(0),
            .requested_bitrate_kbps = ws.bitrate_kbps.value_or(0),
          },
          app,
          ts_seconds,
          ts
        );
      }

      metrics.gauge_int("vibepollo.sessions.active", "{session}", active, json::array(), ts);
      for (const auto &[state, count] : by_state) {
        metrics.gauge_int(
          "vibepollo.sessions.by_state",
          "{session}",
          count,
          json::array({attr_string("session.state", state)}),
          ts
        );
      }

      return active;
    }

    void collect_build_info(metric_set_t &metrics, std::uint64_t ts) {
      metrics.gauge_int(
        "vibepollo.build.info",
        "{info}",
        1,
        json::array({
          attr_string("service.version", PROJECT_VERSION),
          attr_string("vcs.revision", PROJECT_VERSION_COMMIT),
        }),
        ts
      );
    }

    void export_metrics() {
      const auto ts = now_unix_nano();

      const auto app = read_active_app();

      metric_set_t metrics;
      collect_build_info(metrics, ts);
      collect_host_metrics(metrics, ts);
      const auto active_sessions = collect_session_metrics(metrics, app, ts);
      accrue_playtime(app, active_sessions > 0);
      collect_app_metrics(metrics, app, ts);

      if (metrics.empty()) {
        return;
      }

      exporter::enqueue(exporter::signal_e::metrics, wrap_metrics(metrics.release()).dump());
    }

    // ── Log export ─────────────────────────────────────────────────────

    /// Maps Boost.Log severities onto the OTLP severity number range.
    std::pair<int, const char *> map_severity(int boost_severity) {
      switch (boost_severity) {
        case 0:
          return {1, "TRACE"};
        case 1:
          return {5, "DEBUG"};
        case 2:
          return {9, "INFO"};
        case 3:
          return {13, "WARN"};
        case 4:
          return {17, "ERROR"};
        case 5:
          return {21, "FATAL"};
        default:
          return {9, "INFO"};
      }
    }

    void export_logs() {
      auto records = logs::take_batch(MAX_LOG_RECORDS_PER_BATCH);
      if (records.empty()) {
        return;
      }

      json log_records = json::array();
      for (const auto &record : records) {
        const auto [severity_number, severity_text] = map_severity(record.severity);

        json entry;
        entry["timeUnixNano"] = std::to_string(record.time_unix_nano);
        entry["observedTimeUnixNano"] = std::to_string(record.time_unix_nano);
        entry["severityNumber"] = severity_number;
        entry["severityText"] = severity_text;
        entry["body"]["stringValue"] = record.body;

        if (!record.attributes.empty()) {
          json attributes = json::array();
          for (const auto &[key, value] : record.attributes) {
            attributes.push_back(attr_string(key, value));
          }
          entry["attributes"] = std::move(attributes);
        }

        log_records.push_back(std::move(entry));
      }

      json scope_logs;
      scope_logs["scope"]["name"] = "vibepollo";
      scope_logs["scope"]["version"] = PROJECT_VERSION;
      scope_logs["logRecords"] = std::move(log_records);

      json resource_logs;
      resource_logs["resource"] = g_resource;
      resource_logs["scopeLogs"] = json::array({std::move(scope_logs)});

      json payload;
      payload["resourceLogs"] = json::array({std::move(resource_logs)});

      exporter::enqueue(exporter::signal_e::logs, payload.dump());
    }

    // ── Collector thread ───────────────────────────────────────────────

    void collector_loop() {
      while (g_running.load(std::memory_order_acquire)) {
        runtime_settings_t settings;
        {
          std::lock_guard lk {g_settings_mutex};
          settings = g_settings;
        }

        // The pipeline starts before the host stats sampler, so the CPU/GPU
        // model attributes are usually empty on the first pass. Pick them up
        // once they become available. Only this thread reads g_resource after
        // startup, so no lock is needed.
        if (g_resource_pending_host_info && !host_stats::info().cpu_model.empty()) {
          g_resource = build_resource();
          g_resource_pending_host_info = false;
        }

        if (settings.metrics_enabled) {
          export_metrics();
        } else {
          // Playtime and app-switch detection must keep running even when the
          // metric signal is switched off, or the counters would jump on resume.
          const auto app = read_active_app();
          std::size_t sessions = 0;
          {
            std::lock_guard lk {g_sessions_mutex};
            sessions = g_sessions.size();
          }
          accrue_playtime(app, sessions > 0);
        }
        if (settings.logs_enabled) {
          export_logs();
        }

        std::unique_lock lk {g_collector_mutex};
        g_collector_cv.wait_for(lk, settings.export_interval, [] {
          return !g_running.load(std::memory_order_acquire);
        });
      }

      // Final drain so records produced during shutdown still leave the host.
      export_logs();
    }

    // ── Configuration ──────────────────────────────────────────────────

    exporter::settings_t build_exporter_settings() {
      exporter::settings_t settings;
      settings.metrics_url = resolve_signal_url(
        config::otel.metrics_endpoint,
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        config::otel.endpoint,
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "/v1/metrics"
      );
      settings.logs_url = resolve_signal_url(
        config::otel.logs_endpoint,
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        config::otel.endpoint,
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "/v1/logs"
      );

      auto headers = trim(config::otel.headers);
      if (headers.empty()) {
        headers = env_or("OTEL_EXPORTER_OTLP_HEADERS", "");
      }
      settings.headers = parse_kv_list(headers);

      settings.timeout_ms = config::otel.timeout_ms;
      settings.insecure_skip_verify = config::otel.insecure_skip_verify;
      return settings;
    }

    runtime_settings_t build_runtime_settings() {
      runtime_settings_t settings;
      settings.metrics_enabled = config::otel.metrics_enabled;
      settings.logs_enabled = config::otel.logs_enabled;
      settings.export_interval = std::chrono::milliseconds(std::max(config::otel.export_interval_ms, 1000));
      return settings;
    }

    void start_locked() {
      const auto exporter_settings = build_exporter_settings();
      if (exporter_settings.metrics_url.empty() && exporter_settings.logs_url.empty()) {
        BOOST_LOG(warning) << "otel: export is enabled but no endpoint is configured; not starting"sv;
        return;
      }

      g_process_start_unix_nano = now_unix_nano();
      g_resource = build_resource();
      g_resource_pending_host_info = host_stats::info().cpu_model.empty();

      // Re-baseline the playtime clock. Without this, a stop/start cycle from
      // hot-apply would credit the entire disabled window to whatever app was
      // running when export was switched off.
      g_playtime_started = false;
      g_playtime_had_session = false;
      g_last_active_app = {};

      {
        std::lock_guard lk {g_settings_mutex};
        g_settings = build_runtime_settings();
      }

      exporter::configure(exporter_settings);
      exporter::start();

      if (config::otel.logs_enabled) {
        logs::attach(config::otel.log_min_level, MAX_BUFFERED_LOG_RECORDS);
      }

      g_running.store(true, std::memory_order_release);
      g_collector_thread = std::thread {collector_loop};

      BOOST_LOG(info) << "otel: exporting to "sv
                      << (exporter_settings.metrics_url.empty() ? "(metrics off)"s : exporter_settings.metrics_url)
                      << " and "sv
                      << (exporter_settings.logs_url.empty() ? "(logs off)"s : exporter_settings.logs_url);
    }

    void stop_locked() {
      if (!g_running.exchange(false, std::memory_order_acq_rel)) {
        return;
      }

      {
        // Take the wait mutex so the collector cannot miss this notification
        // between checking g_running and entering wait_for.
        std::lock_guard collector_lk {g_collector_mutex};
      }
      g_collector_cv.notify_all();
      if (g_collector_thread.joinable()) {
        g_collector_thread.join();
      }

      logs::detach();
      exporter::stop(true);

      const auto stats = exporter::stats();
      BOOST_LOG(info) << "otel: stopped (exported="sv << stats.exported
                      << " failed="sv << stats.failed
                      << " dropped="sv << stats.dropped << ")"sv;
    }

  }  // namespace

  void init() {
    std::lock_guard lk {g_lifecycle_mutex};
    if (!config::otel.enabled || g_running.load(std::memory_order_acquire)) {
      return;
    }
    start_locked();
  }

  void shutdown() {
    std::lock_guard lk {g_lifecycle_mutex};
    stop_locked();
  }

  void reload_settings() {
    std::lock_guard lk {g_lifecycle_mutex};

    if (!config::otel.enabled) {
      stop_locked();
      return;
    }

    if (!g_running.load(std::memory_order_acquire)) {
      start_locked();
      return;
    }

    // Already running: swap in the new endpoints, cadence and log threshold
    // without dropping buffered telemetry.
    exporter::configure(build_exporter_settings());
    {
      std::lock_guard settings_lk {g_settings_mutex};
      g_settings = build_runtime_settings();
    }

    if (config::otel.logs_enabled) {
      logs::attach(config::otel.log_min_level, MAX_BUFFERED_LOG_RECORDS);
      logs::set_min_severity(config::otel.log_min_level);
    } else {
      logs::detach();
    }

    g_collector_cv.notify_all();
  }

  bool is_running() {
    return g_running.load(std::memory_order_acquire);
  }

  void on_session_started(const session_history::session_metadata_t &metadata) {
    if (!g_running.load(std::memory_order_acquire)) {
      return;
    }

    {
      std::lock_guard lk {g_sessions_mutex};
      auto &tracked = g_sessions[metadata.uuid];
      tracked.metadata = metadata;
      tracked.start_time_unix_nano = now_unix_nano();
      tracked.start_steady = std::chrono::steady_clock::now();
      tracked.aggregator = {};
    }

    // The app the client launches with is only a starting point — the live
    // lookup in the collector owns attribution from here on.
    const auto app = read_active_app();

    logs::emit_event(
      "vibepollo.session.started",
      2,
      "Stream session started: " + app.name,
      {
        {"session.id", metadata.uuid},
        {"session.protocol", metadata.protocol},
        {"app.name", app.name},
        {"app.uuid", app.uuid},
        {"client.name", metadata.client_name},
        {"client.device", metadata.device_name},
        {"video.codec", metadata.codec},
        {"video.resolution", std::to_string(metadata.width) + "x" + std::to_string(metadata.height)},
        {"video.target_fps", std::to_string(metadata.target_fps)},
        {"video.hdr", metadata.hdr ? "true" : "false"},
      }
    );
  }

  void on_session_ended(const std::string &uuid) {
    if (!g_running.load(std::memory_order_acquire)) {
      return;
    }

    session_history::session_metadata_t metadata;
    double duration_seconds = 0;
    bool found = false;

    {
      std::lock_guard lk {g_sessions_mutex};
      if (auto it = g_sessions.find(uuid); it != g_sessions.end()) {
        found = true;
        metadata = it->second.metadata;
        duration_seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - it->second.start_steady
        )
                             .count();
        g_sessions.erase(it);
      }
    }

    if (!found) {
      return;
    }

    // Playtime for the tail of this session is settled by the collector on its
    // next tick, which still sees the interval as having had an active session.
    const auto app = read_active_app();

    logs::emit_event(
      "vibepollo.session.ended",
      2,
      "Stream session ended: " + app.name,
      {
        {"session.id", uuid},
        {"session.protocol", metadata.protocol},
        {"app.name", app.name},
        {"app.uuid", app.uuid},
        {"client.name", metadata.client_name},
        {"client.device", metadata.device_name},
        {"session.duration_seconds", std::to_string(duration_seconds)},
      }
    );
  }

}  // namespace otel
