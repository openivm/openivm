#include "core/refresh_daemon.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/connection.hpp"

#include <chrono>
#include <unordered_set>

namespace duckdb {

shared_ptr<RefreshDaemon> RefreshDaemon::Get(DatabaseInstance &db) {
	return db.GetObjectCache().Get<RefreshDaemon>(ObjectType());
}

void RefreshDaemon::Start(DatabaseInstance &db, bool restart, bool wake) {
	auto daemon = db.GetObjectCache().GetOrCreate<RefreshDaemon>(ObjectType());
	if (!daemon) {
		throw InternalException("OpenIVM refresh daemon cache key is used by another object");
	}
	daemon->StartThread(db, restart, wake);
}

void RefreshDaemon::StartThread(DatabaseInstance &db, bool restart, bool wake) {
	std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
	if (restart) {
		StopThread();
	}
	if (!state_) {
		auto state = make_shared_ptr<State>(weak_ptr<DatabaseInstance>(db.shared_from_this()));
		thread_ = std::thread(&RefreshDaemon::Run, state);
		std::lock_guard<std::mutex> guard(state_mutex_);
		state_ = std::move(state);
	}
	if (wake) {
		state_->Signal(false);
	}
}

void RefreshDaemon::StopThread() {
	if (!state_) {
		return;
	}
	state_->Signal(true);
	if (thread_.joinable()) {
		if (thread_.get_id() == std::this_thread::get_id()) {
			// The daemon thread dropped the last reference to its database, so the database (and this
			// handle, which it owns) is being destroyed on that thread. A thread cannot join itself:
			// detach it instead. Run sees the shutdown flag as soon as the destructor returns and touches
			// only the State it co-owns, never this handle or the database.
			thread_.detach();
		} else {
			thread_.join();
		}
	}
	std::lock_guard<std::mutex> guard(state_mutex_);
	state_.reset();
}

RefreshDaemon::~RefreshDaemon() {
	OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Database shutting down; stopping its daemon\n");
	std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
	StopThread();
}

void RefreshDaemon::State::Signal(bool stop) {
	{
		// Set under the wait mutex so the thread cannot miss the notification and sleep a full wake interval.
		std::lock_guard<std::mutex> guard(cv_mutex);
		(stop ? shutdown : wake_requested) = true;
	}
	cv.notify_all();
}

bool RefreshDaemon::State::ShuttingDown() {
	std::lock_guard<std::mutex> guard(cv_mutex);
	return shutdown;
}

int64_t RefreshDaemon::State::GetEffectiveInterval(const string &view_name) const {
	std::lock_guard<std::mutex> guard(backoff_mutex);
	auto it = effective_intervals.find(view_name);
	return it == effective_intervals.end() ? -1 : it->second; // -1: not backed off, use configured interval
}

shared_ptr<RefreshDaemon::State> RefreshDaemon::CurrentState() const {
	std::lock_guard<std::mutex> guard(state_mutex_);
	return state_;
}

int64_t RefreshDaemon::GetEffectiveInterval(const string &view_name) const {
	auto state = CurrentState();
	return state ? state->GetEffectiveInterval(view_name) : -1;
}

bool RefreshDaemon::IsRefreshing(const string &view_name) const {
	auto state = CurrentState();
	if (!state) {
		return false;
	}
	std::lock_guard<std::mutex> guard(state->refreshing_mutex);
	return state->currently_refreshing == view_name;
}

void RefreshDaemon::Run(shared_ptr<State> state) {
	OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Started\n");
	while (true) {
		{
			std::unique_lock<std::mutex> lock(state->cv_mutex);
			state->cv.wait_for(lock, std::chrono::seconds(WAKE_INTERVAL_SECONDS),
			                   [&] { return state->shutdown || state->wake_requested; });
			if (state->shutdown) {
				break;
			}
			state->wake_requested = false;
		}
		// Hold the database only for one cycle. If its owner closed it meanwhile, this thread drops the
		// last reference at the end of the iteration and destroys it here; that stops this daemon.
		auto db = state->db.lock();
		if (!db) {
			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Database closed; stopping\n");
			break;
		}
		RunCycle(*state, *db);
	}
	OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Stopped\n");
}

void RefreshDaemon::RunCycle(State &state, DatabaseInstance &db) {
	try {
		Connection con(db);
		OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Woke up\n");

		// Read cascade setting from the DB config
		auto cascade_mode =
		    StringUtil::Lower(SqlUtils::GetStringSetting(*con.context, "openivm_cascade_refresh", "downstream"));

		// Read adaptive backoff setting
		state.adaptive_backoff =
		    SqlUtils::GetBoolSetting(*con.context, "openivm_adaptive_backoff", state.adaptive_backoff);

		RefreshMetadata metadata(con);
		auto scheduled = metadata.GetScheduledViews();
		OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Scheduled views: %lu\n", (unsigned long)scheduled.size());

		// Track views already refreshed this cycle (directly or via cascade) to avoid double work
		std::unordered_set<string> refreshed_this_cycle;

		for (auto &sv : scheduled) {
			if (state.ShuttingDown()) {
				break;
			}

			// Cascade lookups below read the location that registered this view.
			MetadataLocation location;
			location.catalog = sv.metadata_catalog;
			location.schema = sv.metadata_schema;
			RefreshMetadata::UseLocation(con, location);
			auto view_key = SqlUtils::FullName(sv.catalog_name, sv.schema_name, sv.view_name);

			// Skip if already refreshed via cascade from an earlier view in this cycle
			if (refreshed_this_cycle.count(view_key)) {
				continue;
			}

			// Determine effective interval (may be backed off)
			auto backed_off = state.GetEffectiveInterval(view_key);
			int64_t interval = backed_off < 0 ? sv.interval_seconds : backed_off;

			// Check if the view is due for refresh
			if (!sv.last_update.empty()) {
				auto elapsed_result = con.Query("SELECT EXTRACT(EPOCH FROM (" + string(openivm::UTC_NOW_SQL) + " - '" +
				                                sv.last_update + "'::TIMESTAMP))");
				if (!elapsed_result->HasError() && elapsed_result->RowCount() > 0 &&
				    !elapsed_result->Collection().GetValue(0, 0).IsNull()) {
					auto elapsed_seconds = elapsed_result->Collection().GetValue(0, 0).GetValue<double>();
					if (elapsed_seconds < static_cast<double>(interval)) {
						continue; // not due yet
					}
				}
			}

			{
				std::lock_guard<std::mutex> guard(state.refreshing_mutex);
				state.currently_refreshing = view_key;
			}

			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Refreshing '%s'\n", sv.view_name.c_str());
			auto before = std::chrono::steady_clock::now();

			bool refresh_succeeded = false;
			try {
				Connection refresh_con(db);
				auto result = refresh_con.Query("PRAGMA refresh_options('" + SqlUtils::EscapeValue(sv.catalog_name) +
				                                "', '" + SqlUtils::EscapeValue(sv.schema_name) + "', '" +
				                                SqlUtils::EscapeValue(sv.view_name) + "')");
				if (result->HasError()) {
					Printer::Print("Warning: auto-refresh of '" + sv.view_name + "' failed: " + result->GetError());
				} else {
					refresh_succeeded = true;
				}
			} catch (std::exception &e) {
				Printer::Print("Warning: auto-refresh of '" + sv.view_name + "' failed: " + string(e.what()));
			}

			{
				std::lock_guard<std::mutex> guard(state.refreshing_mutex);
				state.currently_refreshing.clear();
			}
			if (!refresh_succeeded) {
				continue;
			}

			// Mark this view and any cascaded views as done for this cycle
			for (auto &node : metadata.CascadeOrder(sv.view_name, cascade_mode)) {
				refreshed_this_cycle.insert(SqlUtils::FullName(sv.catalog_name, sv.schema_name, node));
			}

			auto after = std::chrono::steady_clock::now();
			auto duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(after - before).count();

			// Adaptive backoff
			if (state.adaptive_backoff && duration_seconds > sv.interval_seconds) {
				int64_t new_interval = std::min(interval * 2, MAX_BACKOFF_SECONDS);
				{
					std::lock_guard<std::mutex> guard(state.backoff_mutex);
					state.effective_intervals[view_key] = new_interval;
				}
				Printer::Print("Warning: refresh of '" + sv.view_name + "' took " + to_string(duration_seconds) +
				               "s (interval: " + to_string(sv.interval_seconds) +
				               "s). Increasing effective interval to " + to_string(new_interval) +
				               "s. Set openivm_adaptive_backoff = false to disable.");
			} else if (state.adaptive_backoff) {
				std::lock_guard<std::mutex> guard(state.backoff_mutex);
				state.effective_intervals.erase(view_key);
			}
		}
	} catch (std::exception &e) {
		OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Error: %s\n", e.what());
	}
}

} // namespace duckdb
