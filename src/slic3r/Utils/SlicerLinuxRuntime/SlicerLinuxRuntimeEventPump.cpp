#include "SlicerLinuxRuntimeEventPump.hpp"
#include "SlicerLinuxRuntimeRpcClient.hpp"
#include "SlicerLinuxRuntimeForwarderState.hpp"

#include <atomic>
#include <boost/log/trivial.hpp>
#include <chrono>
#include <mutex>
#include <thread>

namespace Slic3r::SlicerLinuxRuntime {
namespace {
std::mutex g_pump_mutex;
std::thread g_pump_thread;
std::atomic<bool> g_stop{false};
}

EventPump& EventPump::instance()
{
    static EventPump pump;
    return pump;
}

EventPump::~EventPump()
{
    stop();
}

void EventPump::ensure_started()
{
    std::lock_guard<std::mutex> lock(g_pump_mutex);
    if (m_running)
        return;
    g_stop = false;
    m_running = true;
    g_pump_thread = std::thread([this] { run(); });
}

void EventPump::request_stop()
{
    g_stop.store(true, std::memory_order_release);
}

void EventPump::stop()
{
    std::lock_guard<std::mutex> lock(g_pump_mutex);
    if (!m_running)
        return;
    g_stop = true;
    if (g_pump_thread.joinable())
        g_pump_thread.join();
    m_running = false;
}

void EventPump::run()
{
    using namespace std::chrono_literals;
    std::size_t empty_poll_count = 0;
    BOOST_LOG_TRIVIAL(info) << "[SLRDIAG] event_pump start";

    while (!g_stop.load()) {
        auto& rpc = RpcClient::instance();
        if (!rpc.is_started()) {
            std::this_thread::sleep_for(250ms);
            continue;
        }

        const auto j = rpc.invoke_json("runtime.poll_events", {{"limit", 64}});
        const bool ok = j.value("ok", false);
        const bool has_event_array = j.contains("events") && j["events"].is_array();
        const std::size_t event_count = has_event_array ? j["events"].size() : 0;
        if (!ok || !has_event_array) {
            BOOST_LOG_TRIVIAL(warning)
                << "[SLRDIAG] poll_events invalid_response"
                << " ok=" << ok
                << " has_event_array=" << has_event_array
                << " has_error=" << j.contains("error");
            std::this_thread::sleep_for(80ms);
            continue;
        }
        if (event_count == 0) {
            ++empty_poll_count;
            if (empty_poll_count == 1 || empty_poll_count % 25 == 0)
                BOOST_LOG_TRIVIAL(info) << "[SLRDIAG] poll_events empty count=" << empty_poll_count;
            std::this_thread::sleep_for(80ms);
            continue;
        }

        BOOST_LOG_TRIVIAL(info) << "[SLRDIAG] poll_events count=" << event_count;
        empty_poll_count = 0;

        for (const auto& ev : j["events"]) {
            const auto payload = ev.contains("payload") ? ev["payload"] : nlohmann::json::object();
            const auto name = ev.value("name", std::string());
            if (ev.contains("agent")) {
                const auto agent = ev.value("agent", 0LL);
                BOOST_LOG_TRIVIAL(info)
                    << "[SLRDIAG] poll_events agent_event"
                    << " agent=" << agent
                    << " name=" << name;
                dispatch_agent_event(agent, name, payload);
            } else if (ev.contains("tunnel")) {
                const auto tunnel = ev.value("tunnel", 0LL);
                BOOST_LOG_TRIVIAL(info)
                    << "[SLRDIAG] poll_events tunnel_event"
                    << " tunnel=" << tunnel
                    << " name=" << name;
                dispatch_tunnel_event(tunnel, name, payload);
            } else {
                BOOST_LOG_TRIVIAL(warning)
                    << "[SLRDIAG] poll_events unaddressed_event"
                    << " name=" << name;
            }
        }
    }

    BOOST_LOG_TRIVIAL(info) << "[SLRDIAG] event_pump stop";
}

}
