#ifndef REFRESH_DAEMON_HPP
#define REFRESH_DAEMON_HPP

#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {

// Background thread that periodically refreshes materialized views with a REFRESH EVERY interval.
// There is one daemon per DatabaseInstance, stored as a non-evictable entry of that database's
// ObjectCache. The database owns it, so destroying the database stops and joins its thread.
// The thread holds only a weak reference to the database and locks it for one scheduling cycle
// at a time; it wakes every openivm_daemon_wake_seconds (default 30), checks which views are due,
// and refreshes them after acquiring the shared OpenIVM mutation gate. A database closed mid-cycle is
// destroyed by the daemon thread when the cycle ends. Nothing stops a daemon thread at process exit
// (waiting would run it after the host destroyed state its queries use, e.g. the CLI's): exiting
// during a cycle can crash the process; the deltas of an unfinished refresh stay for the next refresh.
class RefreshDaemon : public ObjectCacheEntry {
public:
	static constexpr const char *WAKE_SECONDS_SETTING = "openivm_daemon_wake_seconds";
	static constexpr int64_t DEFAULT_WAKE_SECONDS = 30;
	static constexpr int64_t MAX_WAKE_SECONDS = 86400;

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
	// Without restart (extension load): start this database's daemon unless it runs; it skips its cycles
	// while openivm_disable_daemon is set. With restart (PRAGMA refresh_start_daemon): stop a running
	// daemon, start a new one (resetting backoff) that ignores openivm_disable_daemon, and wake it now.
	static void Start(DatabaseInstance &db, bool restart = false);

	~RefreshDaemon() override;

	// Get the effective interval for a view (may be larger than configured due to backoff).
	int64_t GetEffectiveInterval(const string &view_name) const;

	// Check if a view is currently being refreshed by the daemon.
	bool IsRefreshing(const string &view_name) const;

	// Apply a new openivm_daemon_wake_seconds value and run a cycle now.
	void SetWakeSeconds(int64_t seconds);

private:
	// State shared by the database-owned handle and its thread. The thread keeps its own
	// reference, so it never reads freed state even when the handle is destroyed on that thread.
	struct State {
		State(weak_ptr<DatabaseInstance> db_p, bool explicit_start_p, int64_t wake_seconds_p)
		    : db(std::move(db_p)), explicit_start(explicit_start_p), wake_seconds(wake_seconds_p) {
		}
		weak_ptr<DatabaseInstance> db;
		// Started by PRAGMA refresh_start_daemon: runs even while openivm_disable_daemon is set.
		const bool explicit_start;
		std::mutex cv_mutex;
		std::condition_variable cv;
		bool shutdown = false;       // guarded by cv_mutex
		bool wake_requested = false; // guarded by cv_mutex
		int64_t wake_seconds;        // guarded by cv_mutex

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

	void StartThread(DatabaseInstance &db, bool restart);
	// Signal the thread and join it. Holds lifecycle_mutex_. On the daemon thread itself (the thread
	// destroyed the database by dropping its last reference, or a refresh hook it runs restarted the
	// daemon), detach instead: the thread exits after its current cycle and touches only its State.
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

	static constexpr int64_t MAX_BACKOFF_SECONDS = 86400; // 24 hours
};

} // namespace duckdb

#endif // REFRESH_DAEMON_HPP
