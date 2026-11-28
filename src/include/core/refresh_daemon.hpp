#ifndef REFRESH_DAEMON_HPP
#define REFRESH_DAEMON_HPP

#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {

// Background thread that periodically refreshes materialized views with a REFRESH EVERY interval.
// There is one daemon per DatabaseInstance, stored as a non-evictable entry of that database's
// ObjectCache. The database owns it, so destroying the database stops and joins its thread.
// The thread holds only a weak reference to the database and locks it for one scheduling cycle
// at a time; it wakes every 30 seconds, checks which views are due, and refreshes them after
// acquiring the shared OpenIVM mutation gate.
class RefreshDaemon : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "openivm_refresh_daemon";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(); // Evicting the daemon would stop scheduled refresh.
	}

	// The daemon of this database, or nullptr if it was never started.
	static shared_ptr<RefreshDaemon> Get(DatabaseInstance &db);
	// Start this database's daemon unless it is running. With restart, stop a running daemon
	// first and start a new thread (resetting backoff). Either way, wake it immediately if requested.
	static void Start(DatabaseInstance &db, bool restart = false, bool wake = false);

	~RefreshDaemon() override;

	// Get the effective interval for a view (may be larger than configured due to backoff).
	int64_t GetEffectiveInterval(const string &view_name) const;

	// Check if a view is currently being refreshed by the daemon.
	bool IsRefreshing(const string &view_name) const;

private:
	// State shared by the database-owned handle and its thread. The thread keeps its own
	// reference, so it never reads freed state even when the handle is destroyed on that thread.
	struct State {
		explicit State(weak_ptr<DatabaseInstance> db_p) : db(std::move(db_p)) {
		}
		weak_ptr<DatabaseInstance> db;
		std::mutex cv_mutex;
		std::condition_variable cv;
		bool shutdown = false;       // guarded by cv_mutex
		bool wake_requested = false; // guarded by cv_mutex

		// Adaptive backoff: runtime-only effective intervals (not persisted)
		mutable std::mutex backoff_mutex;
		std::unordered_map<string, int64_t> effective_intervals;
		bool adaptive_backoff = true; // daemon thread only

		// Track which view the daemon is currently refreshing
		mutable std::mutex refreshing_mutex;
		string currently_refreshing;

		void Signal(bool stop);
		int64_t GetEffectiveInterval(const string &view_name) const;
		bool ShuttingDown();
	};

	void StartThread(DatabaseInstance &db, bool restart, bool wake);
	// Signal the thread and join it (or detach it when called on the thread itself). Holds lifecycle_mutex_.
	void StopThread();
	shared_ptr<State> CurrentState() const;
	static void Run(shared_ptr<State> state);
	static void RunCycle(State &state, DatabaseInstance &db);

	// Serializes start and stop; the daemon thread never takes it, so joining under it cannot deadlock.
	std::mutex lifecycle_mutex_;
	// Guards state_ for status readers, which must not wait behind a join.
	mutable std::mutex state_mutex_;
	shared_ptr<State> state_;
	std::thread thread_;

	static constexpr int64_t WAKE_INTERVAL_SECONDS = 30;
	static constexpr int64_t MAX_BACKOFF_SECONDS = 86400; // 24 hours
};

} // namespace duckdb

#endif // REFRESH_DAEMON_HPP
