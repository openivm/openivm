// Refresh daemon lifetime (#34): every database has its own daemon, and a database
// can be destroyed at any point of its daemon's cycle without the daemon touching it.
#include "duckdb.hpp"
#include "core/openivm_constants.hpp"
#include "duckdb/main/db_instance_cache.hpp"

#include <chrono>
#include <iostream>
#include <thread>

using namespace duckdb;
using std::chrono::steady_clock;

static void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Daemon lifetime SQL failed: %s; SQL: %s", result->GetError(), sql);
	}
}

static Value Scalar(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Daemon lifetime SQL failed: %s; SQL: %s", result->GetError(), sql);
	}
	return result->Collection().GetValue(0, 0);
}

// Bag equality of a materialized view and its query (EXCEPT ALL in both directions).
static bool BagEqual(Connection &con, const string &view, const string &query) {
	return Scalar(con, "SELECT NOT EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + query + ") AND NOT EXISTS (" +
	                       query + " EXCEPT ALL SELECT * FROM " + view + ")")
	    .GetValue<bool>();
}

static void CheckBag(Connection &con, const string &view, const string &query) {
	if (!BagEqual(con, view, query)) {
		throw InvalidInputException("Materialized view %s differs from its query", view);
	}
}

static void Configure(Connection &con, const string &directory) {
	// The daemon reads settings through its own connection, so they must be global.
	Execute(con, "LOAD openivm; SET GLOBAL openivm_files_path=" + Value(directory).ToSQLString() + ";");
}

static const char *AGG_QUERY = "SELECT k, SUM(v) AS s, COUNT(*) AS c FROM src GROUP BY k";
static const char *PROJ_QUERY = "SELECT id, v FROM src WHERE v > 10";

// Make the scheduled view due now instead of a minute after its last refresh.
static void MakeDue(Connection &con) {
	Execute(con, "UPDATE openivm_delta_tables SET last_update = " + string(openivm::UTC_NOW_SQL) +
	                 " - INTERVAL '2 minutes';");
}

// Many conflicting changes to the same rows, consolidated by one scheduled refresh.
static void ChangeSource(Connection &con, int round) {
	auto r = to_string(round);
	Execute(con, "INSERT INTO src SELECT i, i % 7, i * " + r +
	                 " FROM range(1000, 1200) t(i); "
	                 "UPDATE src SET v = v + " +
	                 r +
	                 " WHERE id % 3 = 0; "
	                 "DELETE FROM src WHERE id % 5 = 0; "
	                 "INSERT INTO src SELECT i, i % 7, " +
	                 r +
	                 " FROM range(0, 50) t(i); "
	                 "UPDATE src SET k = (k + 1) % 7 WHERE id % 11 = 0; "
	                 "DELETE FROM src WHERE id BETWEEN 1100 AND 1110;");
	MakeDue(con);
}

static void CreateScheduledView(Connection &con, const string &query) {
	Execute(con, "CREATE TABLE src(id INTEGER, k INTEGER, v BIGINT); "
	             "INSERT INTO src SELECT i, i % 7, i FROM range(0, 1000) t(i);");
	Execute(con, "CREATE MATERIALIZED VIEW mv REFRESH EVERY '1 minute' AS " + query + ";");
	CheckBag(con, "mv", query);
}

// Wait for the views' own daemons to refresh them, without waking or restarting
// any daemon: one wake interval (30 seconds) plus slack.
static void WaitForScheduledRefresh(vector<std::pair<Connection *, const char *>> views, const string &what) {
	auto deadline = steady_clock::now() + std::chrono::seconds(75);
	while (true) {
		bool all = true;
		for (auto &view : views) {
			all = BagEqual(*view.first, "mv", view.second) && all;
		}
		if (all) {
			return;
		}
		if (steady_clock::now() >= deadline) {
			throw InvalidInputException("Scheduled refresh never reached %s", what);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
}

// Two databases in one process both keep their REFRESH EVERY views fresh, and closing
// one does not stop the other's daemon.
static void TestTwoDatabases(const string &directory) {
	std::cerr << "CASE two databases keep their own daemons" << std::endl;
	auto a = make_uniq<DuckDB>(nullptr);
	auto con_a = make_uniq<Connection>(*a);
	Configure(*con_a, directory);
	CreateScheduledView(*con_a, AGG_QUERY);
	// Loading OpenIVM in a second database must not take over the first one's scheduling.
	DuckDB b(nullptr);
	Connection con_b(b);
	Configure(con_b, directory);
	CreateScheduledView(con_b, PROJ_QUERY);
	ChangeSource(*con_a, 1);
	ChangeSource(con_b, 2);
	if (BagEqual(*con_a, "mv", AGG_QUERY) || BagEqual(con_b, "mv", PROJ_QUERY)) {
		throw InvalidInputException("The changes did not affect the views");
	}
	WaitForScheduledRefresh({{con_a.get(), AGG_QUERY}, {&con_b, PROJ_QUERY}}, "both databases");

	// Closing the first database stops only its daemon.
	con_a.reset();
	a.reset();
	ChangeSource(con_b, 3);
	if (BagEqual(con_b, "mv", PROJ_QUERY)) {
		throw InvalidInputException("The changes did not affect the view");
	}
	WaitForScheduledRefresh({{&con_b, PROJ_QUERY}}, "the remaining database");
}

// A database whose daemon is idle closes promptly: its destructor wakes and joins the thread.
static void TestCloseIdle(const string &directory) {
	std::cerr << "CASE close a database with an idle daemon" << std::endl;
	for (int i = 0; i < 20; i++) {
		auto db = make_uniq<DuckDB>(directory + "/idle.db");
		{
			Connection con(*db);
			Configure(con, directory);
			if (i == 0) {
				CreateScheduledView(con, AGG_QUERY);
			}
			ChangeSource(con, i);
		}
		weak_ptr<DatabaseInstance> instance = db->instance;
		auto before = steady_clock::now();
		db.reset();
		if (!instance.expired()) {
			throw InvalidInputException("An idle daemon kept its database alive");
		}
		if (steady_clock::now() - before > std::chrono::seconds(5)) {
			throw InvalidInputException("Closing the database waited for the daemon's wake interval");
		}
	}
	DuckDB db(directory + "/idle.db");
	Connection con(db);
	Configure(con, directory);
	Execute(con, "PRAGMA refresh('mv');");
	CheckBag(con, "mv", AGG_QUERY);
}

// The owner closes the database while its daemon is refreshing. The daemon then holds the
// last reference, destroys the database on its own thread when the refresh ends, and stops.
static void TestCloseDuringRefresh(const string &directory) {
	std::cerr << "CASE close a database while its daemon refreshes" << std::endl;
	auto path = directory + "/busy.db";
	// Open through the instance cache, as client APIs do: it reopens a file only after the
	// previous instance's destructor has finished.
	DBInstanceCache cache;
	DBConfig config;
	weak_ptr<DatabaseInstance> instance;
	{
		auto db = cache.GetOrCreateInstance(path, config, true);
		auto con = make_uniq<Connection>(*db);
		Configure(*con, directory);
		CreateScheduledView(*con, AGG_QUERY);
		// A large pending delta keeps the scheduled refresh running long enough to observe it.
		Execute(*con, "INSERT INTO src SELECT i, i % 100000, i FROM range(2000, 4000000) t(i); "
		              "UPDATE src SET v = v + 1 WHERE id % 2 = 0; DELETE FROM src WHERE id % 9 = 0;");
		MakeDue(*con);
		Execute(*con, "PRAGMA refresh_start_daemon;");
		auto deadline = steady_clock::now() + std::chrono::seconds(30);
		while (true) {
			auto status = con->Query("PRAGMA refresh_status('mv');");
			if (status->HasError()) {
				throw InvalidInputException(status->GetError());
			}
			if (status->Collection().GetValue(4, 0).ToString() == "refreshing") {
				break;
			}
			if (steady_clock::now() >= deadline) {
				throw InvalidInputException("The daemon never started refreshing");
			}
		}
		instance = db->instance;
		con.reset();
		db.reset();
	}
	// The daemon's cycle holds the last reference: the database is still open.
	if (instance.expired()) {
		throw InvalidInputException("The database closed before its daemon's refresh ended");
	}
	// It is destroyed on the daemon thread once the refresh ends, which also stops that daemon.
	auto deadline = steady_clock::now() + std::chrono::seconds(120);
	while (!instance.expired()) {
		if (steady_clock::now() >= deadline) {
			throw InvalidInputException("The daemon never released its closed database");
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
	auto reopened = cache.GetOrCreateInstance(path, config, true);
	Connection con(*reopened);
	Configure(con, directory);
	// The refresh in flight at close committed: the view is fresh without another refresh.
	CheckBag(con, "mv", AGG_QUERY);
}

// After the closes above, a new database's daemon keeps running and refreshing.
static void TestKeepRunning(const string &directory) {
	std::cerr << "CASE a later database keeps refreshing" << std::endl;
	DuckDB db(nullptr);
	Connection con(db);
	Configure(con, directory);
	CreateScheduledView(con, PROJ_QUERY);
	auto end = steady_clock::now() + std::chrono::seconds(5);
	for (int round = 1; steady_clock::now() < end; round++) {
		ChangeSource(con, round);
		Execute(con, "PRAGMA refresh_start_daemon;");
		auto deadline = steady_clock::now() + std::chrono::seconds(30);
		while (!BagEqual(con, "mv", PROJ_QUERY)) {
			if (steady_clock::now() >= deadline) {
				throw InvalidInputException("The later database's daemon did not refresh");
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
	}
}

int main(int argc, char **argv) {
	if (argc != 2) {
		return 2;
	}
	try {
		TestCloseIdle(argv[1]);
		TestCloseDuringRefresh(argv[1]);
		TestKeepRunning(argv[1]);
		TestTwoDatabases(argv[1]);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
