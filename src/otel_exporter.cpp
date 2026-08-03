/**
 * @file src/otel_exporter.cpp
 * @brief OTLP/HTTP transport implementation.
 */

// standard includes
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

// lib includes
#include <curl/curl.h>

// local includes
#include "otel_exporter.h"
#include "httpcommon.h"
#include "logging.h"

using namespace std::literals;

namespace otel::exporter {

  namespace {

    struct payload_t {
      signal_e signal;
      std::string body;
      int attempts = 0;
    };

    std::mutex g_settings_mutex;
    settings_t g_settings;

    std::mutex g_queue_mutex;
    std::condition_variable g_queue_cv;
    std::deque<payload_t> g_queue;

    std::atomic<bool> g_running {false};
    std::thread g_thread;

    std::atomic<std::uint64_t> g_exported {0};
    std::atomic<std::uint64_t> g_failed {0};
    std::atomic<std::uint64_t> g_dropped {0};

    thread_local bool tl_is_exporter_thread = false;

    constexpr int MAX_ATTEMPTS = 3;

    settings_t settings_snapshot() {
      std::lock_guard lk {g_settings_mutex};
      return g_settings;
    }

    std::size_t curl_discard_body(char *, std::size_t size, std::size_t nmemb, void *) {
      return size * nmemb;
    }

    /**
     * @brief POST one payload.
     * @return true on success, false if the request should be retried or dropped.
     */
    bool post_payload(CURL *curl, const payload_t &payload, const settings_t &settings, bool &retryable) {
      retryable = true;

      const std::string &url = payload.signal == signal_e::metrics ? settings.metrics_url : settings.logs_url;
      if (url.empty()) {
        retryable = false;
        return false;
      }

      curl_easy_reset(curl);

      if (url.rfind("https://", 0) == 0) {
        if (settings.insecure_skip_verify) {
          curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
          curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        } else {
          http::configure_curl_tls(curl);
        }
      }

      curl_slist *headers = nullptr;
      headers = curl_slist_append(headers, "Content-Type: application/json");
      for (const auto &[key, value] : settings.headers) {
        const std::string header = key + ": " + value;
        headers = curl_slist_append(headers, header.c_str());
      }

      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.body.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.body.size()));
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
      curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(settings.timeout_ms));
      curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(settings.timeout_ms));
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_discard_body);
      curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

      const CURLcode res = curl_easy_perform(curl);
      long status = 0;
      if (res == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
      }
      curl_slist_free_all(headers);

      if (res != CURLE_OK) {
        BOOST_LOG(debug) << "otel: export to "sv << url << " failed: "sv << curl_easy_strerror(res);
        return false;
      }

      if (status >= 200 && status < 300) {
        return true;
      }

      // 4xx other than 429 means the collector rejected the payload itself.
      // Retrying an identical body would just burn requests.
      if (status >= 400 && status < 500 && status != 429) {
        retryable = false;
        BOOST_LOG(warning) << "otel: collector rejected export with HTTP "sv << status
                           << " ("sv << url << "); payload dropped"sv;
        return false;
      }

      BOOST_LOG(debug) << "otel: export to "sv << url << " returned HTTP "sv << status;
      return false;
    }

    void sender_loop() {
      tl_is_exporter_thread = true;

      CURL *curl = curl_easy_init();
      if (!curl) {
        BOOST_LOG(error) << "otel: failed to initialize curl; export disabled"sv;
        g_running.store(false, std::memory_order_release);
        return;
      }

      while (true) {
        payload_t payload;
        {
          std::unique_lock lk {g_queue_mutex};
          g_queue_cv.wait(lk, [] {
            return !g_queue.empty() || !g_running.load(std::memory_order_acquire);
          });

          if (g_queue.empty()) {
            break;  // stopped and drained
          }

          payload = std::move(g_queue.front());
          g_queue.pop_front();
        }

        const auto settings = settings_snapshot();
        bool retryable = true;
        if (post_payload(curl, payload, settings, retryable)) {
          g_exported.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        ++payload.attempts;
        if (!retryable || payload.attempts >= MAX_ATTEMPTS || !g_running.load(std::memory_order_acquire)) {
          g_failed.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        // Exponential backoff, then put the payload back at the front so
        // ordering within a signal is preserved.
        const auto backoff = std::chrono::milliseconds(500 * (1 << (payload.attempts - 1)));
        {
          std::unique_lock lk {g_queue_mutex};
          g_queue_cv.wait_for(lk, backoff, [] {
            return !g_running.load(std::memory_order_acquire);
          });
          g_queue.push_front(std::move(payload));
        }
      }

      curl_easy_cleanup(curl);
    }

  }  // namespace

  void configure(const settings_t &settings) {
    std::lock_guard lk {g_settings_mutex};
    g_settings = settings;
  }

  void start() {
    bool expected = false;
    if (!g_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }
    g_thread = std::thread {sender_loop};
  }

  void stop(bool flush) {
    if (!g_running.exchange(false, std::memory_order_acq_rel)) {
      return;
    }

    if (!flush) {
      std::lock_guard lk {g_queue_mutex};
      g_queue.clear();
    }

    g_queue_cv.notify_all();
    if (g_thread.joinable()) {
      g_thread.join();
    }

    std::lock_guard lk {g_queue_mutex};
    g_queue.clear();
  }

  bool is_running() {
    return g_running.load(std::memory_order_acquire);
  }

  bool enqueue(signal_e signal, std::string payload) {
    if (!g_running.load(std::memory_order_acquire) || payload.empty()) {
      return false;
    }

    const auto limit = settings_snapshot().max_queued_payloads;

    std::lock_guard lk {g_queue_mutex};
    while (g_queue.size() >= limit) {
      g_queue.pop_front();
      g_dropped.fetch_add(1, std::memory_order_relaxed);
    }
    g_queue.push_back({signal, std::move(payload), 0});
    g_queue_cv.notify_one();
    return true;
  }

  stats_t stats() {
    stats_t out;
    out.exported = g_exported.load(std::memory_order_relaxed);
    out.failed = g_failed.load(std::memory_order_relaxed);
    out.dropped = g_dropped.load(std::memory_order_relaxed);
    {
      std::lock_guard lk {g_queue_mutex};
      out.queued = g_queue.size();
    }
    return out;
  }

  bool is_exporter_thread() {
    return tl_is_exporter_thread;
  }

}  // namespace otel::exporter
