#include "upsert/refresh_internal.hpp"

#include "core/compiled_sql_archive.hpp"
#include "core/vector_utils.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/plan_rewrite.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/planner.hpp"
#include "rules/column_hider.hpp"

namespace duckdb {

// `SELECT DISTINCT <key_cols>` over the UNION ALL of the per-source changed-key arms.
static string ChangedPartitionKeysSQL(const string &key_cols, const vector<string> &arms) {
	return "SELECT DISTINCT " + key_cols + " FROM (" + StringUtil::Join(arms, " UNION ALL ") +
	       ") openivm_changed_partitions";
}

static string AffectedPartitionsTempTable(const string &view_name) {
	return SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name);
}

static bool DeltaHasColumn(Connection &con, const RefreshMetadata::SourceLocation &source, const string &column_name) {
	auto col_result = con.Query("SELECT 1 FROM information_schema.columns WHERE table_name = '" +
	                            SqlUtils::EscapeValue(source.delta_table_name) + "' AND table_catalog = '" +
	                            SqlUtils::EscapeValue(source.catalog_name) + "' AND table_schema = '" +
	                            SqlUtils::EscapeValue(source.delta_schema_name) + "' AND lower(column_name) = lower('" +
	                            SqlUtils::EscapeValue(column_name) + "') LIMIT 1");
	return !col_result->HasError() && col_result->RowCount() > 0;
}

static vector<WindowPartitionDeltaSpec> BuildWindowPartitionDeltaSpecs(RefreshMetadata &metadata, Connection &con,
                                                                       const string &view_name,
                                                                       const vector<string> &delta_table_names,
                                                                       const vector<string> &partition_cols) {
	vector<WindowPartitionDeltaSpec> partition_delta_specs;
	for (auto &raw_partition_col : partition_cols) {
		auto parsed = RefreshMetadata::ParseWindowColumn(raw_partition_col);
		for (auto &dt : delta_table_names) {
			if (DeltaHasColumn(con, metadata.GetSourceLocation(view_name, dt), parsed.second)) {
				string delta_table_sql = metadata.ResolveDeltaQualifiedName(view_name, dt);
				partition_delta_specs.push_back({dt, delta_table_sql, parsed.first, parsed.second});
			}
		}
	}
	return partition_delta_specs;
}

static bool AnyDuckLakeSource(RefreshMetadata &metadata, const string &view_name,
                              const vector<string> &delta_table_names) {
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			return true;
		}
	}
	return false;
}

static string ResolveStandardTrackedTableSQL(RefreshMetadata &metadata, const string &view_name,
                                             const vector<string> &delta_table_names, const string &table_name,
                                             bool delta_table, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	string metadata_key = ResolveDeltaMetadataKey(metadata, view_name, table_name, delta_table_names);
	string fallback_catalog = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
	string fallback_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
	auto loc = metadata.GetSourceLocation(view_name, metadata_key, fallback_catalog, fallback_schema);
	// A delta may live in an internal schema under a schema-qualified name.
	string resolved_name = delta_table ? loc.delta_table_name : BaseTableNameFromDeltaKey(loc.table_name);
	string resolved_schema = delta_table ? loc.delta_schema_name : loc.schema_name;
	if (resolved_name.empty()) {
		return "";
	}
	if (loc.catalog_name.empty()) {
		return SqlUtils::QuoteIdentifier(resolved_name);
	}
	if (resolved_schema.empty()) {
		resolved_schema = "main";
	}
	return SqlUtils::FullName(loc.catalog_name, resolved_schema, resolved_name);
}

static string BuildLineageColumnExpr(const string &column_name, const string &cast_type, const string &alias = "") {
	string column =
	    alias.empty() ? SqlUtils::QuoteIdentifier(column_name) : alias + "." + SqlUtils::QuoteIdentifier(column_name);
	return SqlUtils::ApplyCastSpec(column, cast_type);
}

static string ProjectWindowKey(const string &expression, const string &output_col, const string &collation) {
	return expression + (collation.empty() ? "" : " COLLATE " + SqlUtils::QuoteIdentifier(collation)) + " AS " +
	       SqlUtils::QuoteIdentifier(output_col);
}

static string DescribeColumnType(Connection &con, const string &table_sql, const string &column_name) {
	auto result = con.Query("DESCRIBE SELECT " + SqlUtils::QuoteIdentifier(column_name) + " FROM " + table_sql);
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return StringUtil::Upper(result->Collection().GetValue(1, 0).ToString());
}

static bool LookupCastMetadataIsSafe(Connection &con, const string &delta_table, const string &source_col,
                                     const string &lookup_table, const string &lookup_col, const string &source_cast,
                                     const string &lookup_cast) {
	if (!source_cast.empty() || !lookup_cast.empty()) {
		return true;
	}
	string source_type = DescribeColumnType(con, delta_table, source_col);
	string lookup_type = DescribeColumnType(con, lookup_table, lookup_col);
	return !source_type.empty() && !lookup_type.empty() && StringUtil::CIEquals(source_type, lookup_type);
}

static string BuildStandardChangedValuesSQL(RefreshMetadata &metadata, const string &view_name,
                                            const vector<string> &delta_table_names, const string &table_name,
                                            const string &source_col, const string &source_cast,
                                            const string &output_col, const string &key_collation,
                                            const string &delta_ts_filter, const string &view_catalog_name,
                                            const string &view_schema_name, const string &attached_db_catalog_name,
                                            const string &attached_db_schema_name, const string &data_table,
                                            const string &order_col, const string &order_out) {
	string delta_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, table_name,
	                                                    /*delta_table=*/true, view_catalog_name, view_schema_name,
	                                                    attached_db_catalog_name, attached_db_schema_name);
	if (delta_table.empty()) {
		return "";
	}
	if (!data_table.empty() && !order_col.empty() && !order_out.empty() && source_cast.empty()) {
		// A key whose pending rows all sort strictly after its materialized rank-1 row keeps that row.
		string changed_key = BuildLineageColumnExpr(source_col, "", "openivm_chg");
		string changed_order = BuildLineageColumnExpr(order_col, "", "openivm_chg");
		string current_key = BuildLineageColumnExpr(output_col, "", "openivm_cur");
		string current_order = BuildLineageColumnExpr(order_out, "", "openivm_cur");
		return "SELECT DISTINCT " + ProjectWindowKey(changed_key, output_col, key_collation) + " FROM " + delta_table +
		       " AS openivm_chg WHERE " + (delta_ts_filter.empty() ? "" : delta_ts_filter + " AND ") +
		       "NOT EXISTS (SELECT 1 FROM " + data_table + " AS openivm_cur WHERE " + current_key +
		       " IS NOT DISTINCT FROM " + changed_key + " AND " + changed_order + " < " + current_order + ")";
	}
	string filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	return "SELECT DISTINCT " +
	       ProjectWindowKey(BuildLineageColumnExpr(source_col, source_cast), output_col, key_collation) + " FROM " +
	       delta_table + filter;
}

static string BuildStandardLookupChangedKeysSQL(RefreshMetadata &metadata, const string &view_name, Connection &con,
                                                const vector<string> &delta_table_names,
                                                const RefreshMetadata::WindowPartitionLineageOp &op,
                                                const string &key_collation, const string &delta_ts_filter,
                                                const string &view_catalog_name, const string &view_schema_name,
                                                const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name) {
	string delta_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.source,
	                                                    /*delta_table=*/true, view_catalog_name, view_schema_name,
	                                                    attached_db_catalog_name, attached_db_schema_name);
	string lookup_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.lookup,
	                                                     /*delta_table=*/false, view_catalog_name, view_schema_name,
	                                                     attached_db_catalog_name, attached_db_schema_name);
	if (delta_table.empty() || lookup_table.empty()) {
		return "";
	}
	if (!LookupCastMetadataIsSafe(con, delta_table, op.source_col, lookup_table, op.lookup_col, op.source_cast,
	                              op.lookup_cast)) {
		OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION lookup cast metadata is incomplete for '%s' — "
		                    "full recompute fallback\n",
		                    view_name.c_str());
		return "";
	}
	string source_expr = BuildLineageColumnExpr(op.source_col, op.source_cast);
	string lookup_expr = BuildLineageColumnExpr(op.lookup_col, op.lookup_cast);
	string lookup_out = BuildLineageColumnExpr(op.lookup_out, op.lookup_out_cast);
	string filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	// A session collation can merge source keys that the lookup's explicit
	// collation distinguishes. Let the lookup perform matching before any deduplication.
	string source_keys = "SELECT " +
	                     string(Settings::Get<DefaultCollationSetting>(*con.context).empty() ? "DISTINCT " : "") +
	                     source_expr;
	if (op.null_safe) {
		string null_safe_lookup_expr = BuildLineageColumnExpr(op.lookup_col, op.lookup_cast, "openivm_lookup");
		string null_safe_lookup_out = BuildLineageColumnExpr(op.lookup_out, op.lookup_out_cast, "openivm_lookup");
		return "SELECT DISTINCT " + ProjectWindowKey(null_safe_lookup_out, op.output_col, key_collation) + " FROM " +
		       lookup_table + " AS openivm_lookup JOIN (" + source_keys + " AS openivm_join_key FROM " + delta_table +
		       filter + ") openivm_changed ON " + null_safe_lookup_expr +
		       " IS NOT DISTINCT FROM openivm_changed.openivm_join_key";
	}
	return "SELECT DISTINCT " + ProjectWindowKey(lookup_out, op.output_col, key_collation) + " FROM " + lookup_table +
	       " WHERE " + lookup_expr + " IN (" + source_keys + " FROM " + delta_table + filter + ")";
}

enum class LineageAffectedKeysResult : uint8_t { UNAVAILABLE, AVAILABLE, UNSAFE };

static bool CanUseCoalesceLineage(RefreshMetadata &metadata, const string &view_name,
                                  const vector<string> &delta_tables,
                                  const RefreshMetadata::WindowPartitionLineage &lineage, const string &column,
                                  optional_ptr<const DeltaActivityResult> activity) {
	if (!activity || activity->sources.size() != delta_tables.size() || lineage.coalesce_preserved_source.empty() ||
	    std::find(lineage.coalesce_columns.begin(), lineage.coalesce_columns.end(), StringUtil::Lower(column)) ==
	        lineage.coalesce_columns.end()) {
		return false;
	}
	auto preserved = ResolveDeltaMetadataKey(metadata, view_name, lineage.coalesce_preserved_source, delta_tables);
	bool found_preserved = false;
	for (const auto &source : activity->sources) {
		if (source.delta_table_name == preserved) {
			found_preserved = true;
		} else if (!source.ok || source.has_changes) {
			return false;
		}
	}
	return found_preserved;
}

// The plan below the window is linear (projections, filters, UNION ALL over scans), so evaluating
// the view query with every source occurrence replaced by that source's delta rows (both images)
// yields every partition key whose input rows changed. Any base reference left behind would
// evaluate an arm over current rows only and miss deleted keys, so it falls back instead.
static LineageAffectedKeysResult
BuildLinearAffectedKeysSQL(RefreshMetadata &metadata, ClientContext &context, const string &view_name,
                           const string &view_query_sql, const vector<string> &delta_table_names,
                           const vector<RefreshMetadata::WindowPartitionLineageOp> &key_ops, const string &key_col,
                           const string &delta_ts_filter, const string &view_catalog_name,
                           const string &view_schema_name, const string &attached_db_catalog_name,
                           const string &attached_db_schema_name, string &affected_keys_sql) {
	auto decline = [&view_name](LineageAffectedKeysResult result, const char *reason) {
		OPENIVM_DEBUG_PRINT("[LINEAR WINDOW] affected keys declined for '%s': %s\n", view_name.c_str(), reason);
		return result;
	};
	string scoped = view_query_sql;
	case_insensitive_map_t<string> replaced_tables;
	unordered_set<string> seen_sources;
	unordered_set<string> covered_sources;
	for (auto &op : key_ops) {
		if (op.kind != WindowLineageKind::LINEAR) {
			return decline(LineageAffectedKeysResult::UNSAFE, "mixed lineage kinds");
		}
		if (!seen_sources.insert(StringUtil::Lower(op.source)).second) {
			continue;
		}
		string delta_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.source,
		                                                    /*delta_table=*/true, view_catalog_name, view_schema_name,
		                                                    attached_db_catalog_name, attached_db_schema_name);
		string base_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.source,
		                                                   /*delta_table=*/false, view_catalog_name, view_schema_name,
		                                                   attached_db_catalog_name, attached_db_schema_name);
		if (delta_table.empty() || base_table.empty()) {
			return decline(LineageAffectedKeysResult::UNSAFE, "could not resolve tracked base or delta table");
		}
		string base_ref = SqlUtils::FindTableReference(scoped, base_table);
		if (base_ref.empty()) {
			return decline(LineageAffectedKeysResult::UNSAFE, "could not locate base table reference");
		}
		auto delta_rows = BuildStandardDeltaRowsSQL(delta_table, "", delta_ts_filter);
		scoped = SqlUtils::ReplaceTableReferences(scoped, base_ref, delta_rows);
		auto base_name = QualifiedName::Parse(base_table).Name().GetIdentifierName();
		auto delta_name = QualifiedName::Parse(delta_table).Name().GetIdentifierName();
		if (!replaced_tables.emplace(std::move(base_name), std::move(delta_name)).second) {
			return decline(LineageAffectedKeysResult::UNSAFE, "ambiguous source table names");
		}
		covered_sources.insert(ResolveDeltaMetadataKey(metadata, view_name, op.source, delta_table_names));
	}
	auto count_sources = [&](const string &sql, case_insensitive_map_t<idx_t> &counts) {
		Parser parser(context);
		try {
			parser.ParseQuery(sql);
		} catch (const ParserException &ex) {
			OPENIVM_DEBUG_PRINT("[LINEAR WINDOW] could not parse scope: %s\n", ex.what());
			return false;
		}
		if (parser.statements.size() != 1 || parser.statements.front()->type != StatementType::SELECT_STATEMENT) {
			return false;
		}
		SqlUtils::VisitSourceReferences(*parser.statements.front(),
		                                [&](BaseTableRef &ref) { counts[ref.Table().GetIdentifierName()]++; });
		return true;
	};
	case_insensitive_map_t<idx_t> original_sources;
	case_insensitive_map_t<idx_t> scoped_sources;
	if (!count_sources(view_query_sql, original_sources) || !count_sources(scoped, scoped_sources)) {
		return decline(LineageAffectedKeysResult::UNSAFE, "expected one parseable SELECT per scope");
	}
	for (auto &entry : replaced_tables) {
		if (scoped_sources.count(entry.first)) {
			return decline(LineageAffectedKeysResult::UNSAFE,
			               "a base table reference remains after delta substitution");
		}
		if (original_sources[entry.first] == 0 || original_sources[entry.first] != scoped_sources[entry.second]) {
			return decline(LineageAffectedKeysResult::UNSAFE,
			               "delta substitution did not cover exactly the base references");
		}
	}
	for (auto &dt : delta_table_names) {
		if (!covered_sources.count(dt)) {
			return decline(LineageAffectedKeysResult::UNAVAILABLE, "a tracked source has no lineage coverage");
		}
	}
	if (covered_sources.empty()) {
		return decline(LineageAffectedKeysResult::UNAVAILABLE, "no covered sources");
	}
	affected_keys_sql =
	    "SELECT DISTINCT " + SqlUtils::QuoteIdentifier(key_col) + " FROM (" + scoped + ") openivm_changed_partitions";
	return LineageAffectedKeysResult::AVAILABLE;
}

// Build one key-selection arm per applicable lineage op and require coverage of every source.
static LineageAffectedKeysResult BuildLineageAffectedKeyArms(
    const string &output_col, const vector<RefreshMetadata::WindowPartitionLineageOp> &lineage_ops,
    const vector<string> &required_sources,
    const std::function<LineageAffectedKeysResult(const RefreshMetadata::WindowPartitionLineageOp &, string &,
                                                  string &)> &build_arm,
    vector<string> &arms) {
	unordered_set<string> covered_sources;
	for (auto &op : lineage_ops) {
		if (!StringUtil::CIEquals(op.output_col, output_col)) {
			continue;
		}
		string arm_sql;
		string source;
		auto result = build_arm(op, arm_sql, source);
		if (result == LineageAffectedKeysResult::UNSAFE) {
			return result;
		}
		if (result == LineageAffectedKeysResult::AVAILABLE) {
			arms.push_back(std::move(arm_sql));
			covered_sources.insert(std::move(source));
		}
	}
	for (auto &source : required_sources) {
		if (!covered_sources.count(source)) {
			return LineageAffectedKeysResult::UNAVAILABLE;
		}
	}
	if (arms.empty()) {
		return LineageAffectedKeysResult::UNAVAILABLE;
	}
	return LineageAffectedKeysResult::AVAILABLE;
}

static LineageAffectedKeysResult BuildLineageStandardAffectedKeysSQL(
    RefreshMetadata &metadata, Connection &con, const string &view_name, const string &view_query_sql,
    const vector<string> &delta_table_names, const string &output_col, const string &key_collation,
    const string &delta_ts_filter, const string &view_catalog_name, const string &view_schema_name,
    const string &attached_db_catalog_name, const string &attached_db_schema_name, const string &data_table,
    bool require_proof, optional_ptr<const DeltaActivityResult> activity,
    const RefreshMetadata::WindowPartitionLineage &lineage, string &affected_keys_sql) {
	if (lineage.ops.empty()) {
		return LineageAffectedKeysResult::UNAVAILABLE;
	}
	bool proven = lineage.IsProven(output_col);
	bool use_coalesce =
	    !proven && CanUseCoalesceLineage(metadata, view_name, delta_table_names, lineage, output_col, activity);
	if ((require_proof || lineage.proven_columns) && !proven && !use_coalesce) {
		return LineageAffectedKeysResult::UNAVAILABLE;
	}
	vector<RefreshMetadata::WindowPartitionLineageOp> key_ops;
	bool has_linear = false;
	for (auto &op : lineage.ops) {
		if (StringUtil::CIEquals(op.output_col, output_col)) {
			has_linear |= op.kind == WindowLineageKind::LINEAR;
			key_ops.push_back(op);
		}
	}
	if (has_linear) {
		return BuildLinearAffectedKeysSQL(metadata, *con.context, view_name, view_query_sql, delta_table_names, key_ops,
		                                  output_col, delta_ts_filter, view_catalog_name, view_schema_name,
		                                  attached_db_catalog_name, attached_db_schema_name, affected_keys_sql);
	}
	const auto trim_data_table =
	    SqlUtils::GetBoolSetting(*con.context, "openivm_late_append_window_trim", false) ? data_table : "";
	auto build_arm = [&](const RefreshMetadata::WindowPartitionLineageOp &op, string &arm, string &source) {
		if (op.kind == WindowLineageKind::DIRECT) {
			arm = BuildStandardChangedValuesSQL(metadata, view_name, delta_table_names, op.source, op.source_col,
			                                    op.source_cast, op.output_col, key_collation, delta_ts_filter,
			                                    view_catalog_name, view_schema_name, attached_db_catalog_name,
			                                    attached_db_schema_name, trim_data_table, op.order_col, op.order_out);
		} else if (op.kind == WindowLineageKind::LOOKUP) {
			arm = BuildStandardLookupChangedKeysSQL(metadata, view_name, con, delta_table_names, op, key_collation,
			                                        delta_ts_filter, view_catalog_name, view_schema_name,
			                                        attached_db_catalog_name, attached_db_schema_name);
		}
		if (arm.empty()) {
			return LineageAffectedKeysResult::UNSAFE;
		}
		arm = "(" + arm + ")";
		source = ResolveDeltaMetadataKey(metadata, view_name, op.source, delta_table_names);
		return LineageAffectedKeysResult::AVAILABLE;
	};
	vector<string> arms;
	auto result = BuildLineageAffectedKeyArms(output_col, lineage.ops, delta_table_names, build_arm, arms);
	if (result != LineageAffectedKeysResult::AVAILABLE) {
		return result;
	}
	auto key_cols = SqlUtils::QuoteIdentifier(output_col);
	if (use_coalesce) {
		// The unchanged nullable inputs supply the alternative keys. A
		// changed preserved row can also move into or out of the NULL partition.
		arms.push_back("(SELECT NULL AS " + key_cols + ")");
	}

	affected_keys_sql = ChangedPartitionKeysSQL(key_cols, arms);
	return LineageAffectedKeysResult::AVAILABLE;
}

static string BuildDuckLakeLookupChangedKeysSQL(const DuckLakeSourceSpec &source_spec,
                                                const DuckLakeSourceSpec &lookup_spec,
                                                const RefreshMetadata::WindowPartitionLineageOp &op) {
	string changed = BuildDuckLakeChangedValuesSQL(source_spec, op.source_col, op.source_cast, "openivm_join_key");
	string lookup_table =
	    SqlUtils::FullName(lookup_spec.loc.catalog_name, lookup_spec.loc.schema_name, lookup_spec.loc.table_name);
	string lookup_col = BuildLineageColumnExpr(op.lookup_col, op.lookup_cast, "l");
	string lookup_out = BuildLineageColumnExpr(op.lookup_out, op.lookup_out_cast, "l");
	string output_col = SqlUtils::QuoteIdentifier(op.output_col);
	string join_operator = op.null_safe ? " IS NOT DISTINCT FROM " : " = ";
	string current_lookup = "SELECT " + lookup_out + " AS " + output_col + " FROM " + changed + " c JOIN " +
	                        lookup_table + " AS l AT (VERSION => " + to_string(lookup_spec.current_snap) + ") ON " +
	                        lookup_col + join_operator + "c.openivm_join_key";
	string old_lookup = "SELECT " + lookup_out + " AS " + output_col + " FROM " + changed + " c JOIN " + lookup_table +
	                    " AS l AT (VERSION => " + to_string(lookup_spec.old_snap) + ") ON " + lookup_col +
	                    join_operator + "c.openivm_join_key";
	return "(" + current_lookup + " UNION ALL " + old_lookup + ")";
}

// cascade_delta names the view's own delta table when a downstream MV needs this refresh's signed row changes.
static string BuildAffectedPartitionRefreshSQL(const string &view_name, const string &data_table,
                                               const string &view_query_sql, const string &affected_keys_sql,
                                               const vector<string> &partition_cols,
                                               RefreshPublicationScope *publication_scope,
                                               const string &cascade_delta) {
	auto affected_temp_table = AffectedPartitionsTempTable(view_name);
	auto output_columns = PartitionOutputColumns(partition_cols);
	string target_match = SqlUtils::BuildNullSafeMatch(output_columns, "openivm_aff", "openivm_target");
	string recompute_match = SqlUtils::BuildNullSafeMatch(output_columns, "openivm_aff", "openivm_recompute");
	if (publication_scope) {
		publication_scope->columns = output_columns;
	}
	if (!cascade_delta.empty()) {
		// Snapshot the affected partitions before and after so their signed difference feeds the view delta.
		OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION emits its affected-partition delta for '%s'\n",
		                    view_name.c_str());
		string sql = "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_keys_sql + ";\n\n";
		sql += BuildSnapshotDeltaRefreshSQL(
		    data_table, view_query_sql, cascade_delta,
		    SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name),
		    SqlUtils::QuoteIdentifier("openivm_new_" + view_name),
		    "EXISTS (SELECT 1 FROM " + affected_temp_table + " openivm_aff WHERE " + target_match + ")", false);
		auto cleanup = "DROP TABLE IF EXISTS " + affected_temp_table + ";\n";
		if (!publication_scope) {
			return sql + cleanup;
		}
		publication_scope->rows = affected_temp_table;
		publication_scope->cleanup_sql = cleanup;
		return sql;
	}
	return BuildAffectedKeyRefreshSQL(data_table, view_query_sql, affected_keys_sql, "openivm_target",
	                                  "openivm_recompute", "openivm_aff", target_match, recompute_match,
	                                  affected_temp_table, {}, "", publication_scope);
}

static bool AddWindowRowKeyColumns(const vector<string> &specs, const vector<string> &visible_columns,
                                   vector<string> &row_keys) {
	for (auto &spec : specs) {
		auto output_column = RefreshMetadata::ParseWindowColumn(spec).first;
		auto visible = std::find_if(visible_columns.begin(), visible_columns.end(),
		                            [&](const string &column) { return StringUtil::CIEquals(column, output_column); });
		if (visible == visible_columns.end()) {
			return false;
		}
		AddUniqueCI(row_keys, *visible);
	}
	return true;
}

struct WindowOutput {
	const vector<string> &column_names;
	const vector<LogicalType> &column_types;
	SqlDialect dialect;
	string default_collation;
	// The view's own delta table when this refresh must emit its signed row changes, else empty.
	string cascade_delta;

	string KeyCollation(const string &column) const {
		D_ASSERT(column_names.size() == column_types.size());
		if (dialect != SqlDialect::DUCKDB) {
			return "";
		}
		for (idx_t i = 0; i < column_names.size(); i++) {
			if (column_types[i].id() == LogicalTypeId::VARCHAR && StringUtil::CIEquals(column, column_names[i])) {
				// Deduplication in every source arm must use the destination key's collation.
				const auto &collation = StringType::GetCollation(column_types[i]);
				return collation.empty() ? (default_collation.empty() ? "binary" : default_collation) : collation;
			}
		}
		return "";
	}

	vector<string> Identities(const string &prefix = "") const {
		D_ASSERT(column_names.size() == column_types.size());
		vector<string> identities;
		for (idx_t i = 0; i < column_names.size(); i++) {
			if (IncrementalTableNames::IsInternalColumn(column_names[i])) {
				continue;
			}
			auto identity = SqlUtils::ValueIdentitySQL(prefix + SqlUtils::QuoteIdentifier(column_names[i]),
			                                           column_types[i], dialect, default_collation);
			if (!identity.empty()) {
				identities.push_back(std::move(identity));
			}
		}
		return identities;
	}
};

static string BuildDuckLakeWindowRowDiffRefreshSQL(const string &view_name, const string &data_table,
                                                   const string &view_query_sql, const string &affected_keys_sql,
                                                   const vector<string> &partition_cols,
                                                   const vector<string> &order_cols, const WindowOutput &output,
                                                   RefreshPublicationScope *publication_scope) {
	vector<string> visible_columns;
	for (auto &column : output.column_names) {
		// A stored column named rowid shadows the physical row identifier.
		if (StringUtil::CIEquals(column, "rowid")) {
			return "";
		}
		if (!IncrementalTableNames::IsInternalColumn(column)) {
			visible_columns.push_back(column);
		}
	}
	vector<string> row_keys;
	if (visible_columns.empty() || !AddWindowRowKeyColumns(partition_cols, visible_columns, row_keys) ||
	    !AddWindowRowKeyColumns(order_cols, visible_columns, row_keys) || row_keys.empty()) {
		return "";
	}

	string affected_table = AffectedPartitionsTempTable(view_name);
	string recompute_table = SqlUtils::QuoteIdentifier("openivm_window_recompute_" + view_name);
	string changed_table = SqlUtils::QuoteIdentifier("openivm_window_changed_" + view_name);
	auto partition_output_columns = PartitionOutputColumns(partition_cols);
	string target_affected = SqlUtils::BuildNullSafeMatch(partition_output_columns, "openivm_aff", "openivm_target");
	string recompute_affected =
	    SqlUtils::BuildNullSafeMatch(partition_output_columns, "openivm_aff", "openivm_recompute");
	string row_key_match = SqlUtils::BuildNullSafeMatch(row_keys, "openivm_old", "openivm_new");
	string old_columns = SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_target");
	string new_columns = SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_recompute");
	string changed_new_columns = SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_new");
	string insert_columns = SqlUtils::JoinQuotedColumns(visible_columns);
	string distinct_rows = "(" + SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_old") +
	                       ") IS DISTINCT FROM (" +
	                       SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_new") + ")";
	auto old_identities = output.Identities("openivm_old.");
	auto new_identities = output.Identities("openivm_new.");
	for (idx_t i = 0; i < old_identities.size(); i++) {
		distinct_rows += " OR ((" + old_identities[i] + ") IS DISTINCT FROM (" + new_identities[i] + "))";
	}

	// Retain both row images from the diff already required for maintenance.
	// Publication can then apply the exact signed bag without comparing again.
	string old_publication_columns;
	if (publication_scope) {
		vector<string> old_projection;
		vector<string> scope_projection;
		for (idx_t i = 0; i < visible_columns.size(); i++) {
			auto column = SqlUtils::QuoteIdentifier(visible_columns[i]);
			auto old_column = SqlUtils::QuoteIdentifier("openivm_old_value_" + to_string(i));
			old_publication_columns += "openivm_old." + column + " AS " + old_column + ", ";
			old_projection.push_back(old_column + " AS " + column);
			if (std::find(row_keys.begin(), row_keys.end(), visible_columns[i]) != row_keys.end()) {
				scope_projection.push_back("COALESCE(" + column + ", " + old_column + ") AS " + column);
			}
		}
		publication_scope->columns = row_keys;
		publication_scope->rows =
		    "(SELECT " + StringUtil::Join(scope_projection, ", ") + " FROM " + changed_table + ")";
		publication_scope->signed_rows = "(SELECT " + StringUtil::Join(old_projection, ", ") + ", -1::INTEGER AS " +
		                                 string(openivm::MULTIPLICITY_COL) + " FROM " + changed_table +
		                                 " WHERE openivm_old_rowid IS NOT NULL UNION ALL SELECT " + insert_columns +
		                                 ", 1::INTEGER AS " + string(openivm::MULTIPLICITY_COL) + " FROM " +
		                                 changed_table + " WHERE openivm_new_present)";
		publication_scope->cleanup_sql = "DROP TABLE IF EXISTS " + changed_table + ";\n";
	}
	string old_partition_by = SqlUtils::JoinQualifiedQuotedColumns(row_keys, "openivm_target");
	string new_partition_by = SqlUtils::JoinQualifiedQuotedColumns(row_keys, "openivm_recompute");
	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + affected_table + " AS\n" + affected_keys_sql + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + recompute_table + " AS\nSELECT * FROM (" + view_query_sql +
	       ") openivm_recompute\nWHERE EXISTS (SELECT 1 FROM " + affected_table + " openivm_aff WHERE " +
	       recompute_affected + ");\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + changed_table + " AS\nWITH openivm_old AS (\n  SELECT " + old_columns +
	       ", openivm_target.rowid AS openivm_old_rowid,\n    ROW_NUMBER() OVER (PARTITION BY " + old_partition_by +
	       ") AS openivm_match_id\n  FROM " + data_table + " openivm_target\n  WHERE EXISTS (SELECT 1 FROM " +
	       affected_table + " openivm_aff WHERE " + target_affected + ")\n), openivm_new AS (\n  SELECT " +
	       new_columns + ", TRUE AS openivm_new_present,\n    ROW_NUMBER() OVER (PARTITION BY " + new_partition_by +
	       ") AS openivm_match_id\n  FROM " + recompute_table + " openivm_recompute\n)\nSELECT " +
	       "openivm_old.openivm_old_rowid, openivm_new.openivm_new_present, " + old_publication_columns +
	       changed_new_columns + "\nFROM openivm_old\nFULL OUTER JOIN openivm_new ON " + row_key_match +
	       " AND openivm_old.openivm_match_id = openivm_new.openivm_match_id\nWHERE " +
	       "openivm_old.openivm_old_rowid IS NULL OR openivm_new.openivm_new_present IS NULL OR " + distinct_rows +
	       ";\n\n";
	sql += "DELETE FROM " + data_table + " WHERE rowid IN (SELECT openivm_old_rowid FROM " + changed_table +
	       " WHERE openivm_old_rowid IS NOT NULL);\n\n";
	sql += "INSERT INTO " + data_table + " (" + insert_columns + ")\nSELECT " +
	       SqlUtils::JoinQualifiedQuotedColumns(visible_columns, "openivm_changed") + "\nFROM " + changed_table +
	       " openivm_changed\nWHERE openivm_new_present;\n\n";
	if (!publication_scope) {
		sql += "DROP TABLE IF EXISTS " + changed_table + ";\n";
	}
	sql += "DROP TABLE IF EXISTS " + recompute_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + affected_table + ";\n";
	OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION DuckLake compact row diff for %s (%zu row keys)\n",
	                    view_name.c_str(), row_keys.size());
	return sql;
}

static bool BuildLineageDuckLakeAffectedKeysSQL(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                const vector<string> &delta_table_names,
                                                const vector<string> &partition_cols, const string &view_catalog_name,
                                                const string &view_schema_name, const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name,
                                                const RefreshMetadata::WindowPartitionLineage &lineage,
                                                string &affected_keys_sql) {
	if (partition_cols.size() != 1) {
		return false;
	}
	auto output_col = RefreshMetadata::ParseWindowColumn(partition_cols[0]).first;
	if ((lineage.proven_columns || delta_table_names.size() == 1) && !lineage.IsProven(output_col)) {
		return false;
	}

	vector<DuckLakeSourceSpec> specs;
	if (!BuildDuckLakeSourceSpecs(metadata, con, view_name, delta_table_names, view_catalog_name, view_schema_name,
	                              attached_db_catalog_name, attached_db_schema_name, specs)) {
		return false;
	}
	vector<string> required_sources;
	for (auto &spec : specs) {
		required_sources.push_back(StripOpenIVMDataPrefix(spec.metadata_key));
	}
	auto build_arm = [&](const RefreshMetadata::WindowPartitionLineageOp &op, string &arm, string &source) {
		auto *source_spec = FindDuckLakeSourceSpec(specs, op.source);
		if (source_spec && op.kind == WindowLineageKind::DIRECT) {
			arm = BuildDuckLakeChangedValuesSQL(*source_spec, op.source_col, op.source_cast, op.output_col);
		} else if (source_spec && op.kind == WindowLineageKind::LOOKUP) {
			auto *lookup_spec = FindDuckLakeSourceSpec(specs, op.lookup);
			if (lookup_spec) {
				arm = BuildDuckLakeLookupChangedKeysSQL(*source_spec, *lookup_spec, op);
			}
		}
		if (arm.empty()) {
			return LineageAffectedKeysResult::UNAVAILABLE;
		}
		source = StripOpenIVMDataPrefix(source_spec->metadata_key);
		return LineageAffectedKeysResult::AVAILABLE;
	};
	vector<string> arms;
	if (BuildLineageAffectedKeyArms(output_col, lineage.ops, required_sources, build_arm, arms) !=
	    LineageAffectedKeysResult::AVAILABLE) {
		return false;
	}
	affected_keys_sql = ChangedPartitionKeysSQL(SqlUtils::QuoteIdentifier(output_col), arms);
	return true;
}

static bool HasDirectDuckLakeWindowPartitionOps(const RefreshMetadata::WindowPartitionLineage &lineage,
                                                const string &partition_spec) {
	auto partition = RefreshMetadata::ParseWindowColumn(partition_spec);
	bool found = false;
	for (const auto &op : lineage.ops) {
		if (!StringUtil::CIEquals(op.output_col, partition.first)) {
			continue;
		}
		if (!StringUtil::CIEquals(op.source_col, partition.second) || !op.source_cast.empty() ||
		    (op.kind == WindowLineageKind::LOOKUP && (!StringUtil::CIEquals(op.lookup_col, op.lookup_out) ||
		                                              !op.lookup_cast.empty() || !op.lookup_out_cast.empty()))) {
			return false;
		}
		found = true;
	}
	return found;
}

struct ReboundWindowPartition {
	RefreshMetadata::WindowPartitionLineage lineage;
	bool requires_result_diff = true;
};

static ReboundWindowPartition RebindWindowPartitionLineage(Connection &con, const string &view_query_sql,
                                                           const vector<string> &column_names,
                                                           vector<string> &partitions) {
	ReboundWindowPartition rebound;
	try {
		con.context->RunFunctionInTransaction([&] {
			auto parser = Parser::GetBuiltinParser();
			parser.ParseQuery(view_query_sql);
			if (parser.statements.size() != 1) {
				return;
			}
			Planner planner(*con.context);
			PlanWithSourceColumns(planner, std::move(parser.statements[0]));
			if (!planner.plan) {
				return;
			}
			InlineCtesIfPresent(*con.context, *planner.binder, planner.plan);
			auto facts =
			    BuildCreateMVPlanFacts(planner.plan.get(), SqlUtils::SessionDefaultLocation(*con.context).catalog);
			BuildWindowPartitionLineageOps(facts, partitions, column_names, rebound.lineage);
			if (!rebound.lineage.proven_columns || rebound.lineage.proven_columns->empty()) {
				BuildLinearWindowPartitionLineageOps(facts, partitions, column_names, rebound.lineage);
			}
			rebound.requires_result_diff =
			    WindowPlanRequiresResultDiff(facts, partitions, column_names, rebound.lineage);
		});
	} catch (const std::exception &error) {
		OPENIVM_DEBUG_PRINT("[UPSERT] Could not prove legacy window key: %s\n", error.what());
	}
	return rebound;
}

static string BuildSingleSourceDuckLakeWindowRefresh(
    RefreshMetadata &metadata, Connection &con, const string &view_name, const string &view_query_sql,
    const vector<string> &partition_cols, const vector<string> &order_cols, const string &data_table,
    const string &view_catalog_name, const string &view_schema_name, const string &attached_db_catalog_name,
    const string &attached_db_schema_name, const string &base_name, const WindowOutput &output,
    RefreshPublicationScope *publication_scope) {
	int64_t old_snap = metadata.GetLastSnapshotId(view_name, base_name);
	auto loc = ResolveDuckLakeSourceLocation(con, view_name, base_name, view_catalog_name, view_schema_name,
	                                         attached_db_catalog_name, attached_db_schema_name);
	int64_t current_snap = old_snap;
	auto snapshot_id = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
	if (snapshot_id >= 0) {
		current_snap = snapshot_id;
	}

	string affected_cols = SqlUtils::JoinQuotedColumns(PartitionOutputColumns(partition_cols));
	string affected_select = StringUtil::Join(partition_cols, partition_cols.size(), ", ", [](const string &spec) {
		auto parsed = RefreshMetadata::ParseWindowColumn(spec);
		return SqlUtils::QuoteIdentifier(parsed.second) + " AS " + SqlUtils::QuoteIdentifier(parsed.first);
	});
	auto changes = SqlUtils::DuckLakeChangesSQL(affected_select, loc.catalog_name, loc.schema_name, loc.table_name,
	                                            old_snap, current_snap);
	string affected_keys = ChangedPartitionKeysSQL(affected_cols, {"(" + changes + ")"});
	// The compact row diff applies changes without a view delta, so cascading uses the partition snapshot.
	if (!order_cols.empty() && output.cascade_delta.empty()) {
		auto compact_diff = BuildDuckLakeWindowRowDiffRefreshSQL(view_name, data_table, view_query_sql, affected_keys,
		                                                         partition_cols, order_cols, output, publication_scope);
		if (!compact_diff.empty()) {
			return compact_diff;
		}
	}
	string upsert_query = BuildAffectedPartitionRefreshSQL(view_name, data_table, view_query_sql, affected_keys,
	                                                       partition_cols, publication_scope, output.cascade_delta);
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake change-feed, %zu "
	                    "partition cols, old_snap=%ld, current_snap=%ld)\n",
	                    partition_cols.size(), (long)old_snap, (long)current_snap);
	return upsert_query;
}

static vector<string> ResultDiffScope(const vector<string> &partition_cols, const vector<string> &column_names) {
	vector<string> scope;
	optional<vector<string>> sanitized_names;
	for (const auto &partition : partition_cols) {
		auto output = RefreshMetadata::ParseWindowColumn(partition).first;
		if (ContainsCI(column_names, output)) {
			AddUniqueCI(scope, partition);
			continue;
		}
		// DuckLake may materialize the original SQL while metadata names were
		// prepared for LPTS. Recover the physical column at the same position.
		if (!sanitized_names) {
			sanitized_names = PrepareOutputNames(nullptr, column_names);
		}
		auto match = std::find_if(sanitized_names->begin(), sanitized_names->end(),
		                          [&](const string &name) { return StringUtil::CIEquals(name, output); });
		if (match != sanitized_names->end()) {
			const auto &physical_name = column_names[match - sanitized_names->begin()];
			AddUniqueCI(scope, RefreshMetadata::SerializeWindowColumn(physical_name, physical_name));
		}
	}
	if (scope.empty()) {
		// Full result comparison can group the changed rows by any output key.
		D_ASSERT(!column_names.empty());
		scope.push_back(RefreshMetadata::SerializeWindowColumn(column_names.front(), column_names.front()));
	}
	return scope;
}

static string BuildChangedWindowPartitionsSQL(const string &data_table, const string &view_query_sql,
                                              const vector<string> &partition_cols, const WindowOutput &output) {
	auto key_cols = SqlUtils::JoinQuotedColumns(PartitionOutputColumns(partition_cols));
	string current_rows = view_query_sql;
	string previous_rows = "SELECT * FROM " + data_table + " openivm_old_rows";
	auto identities = output.Identities();
	if (!identities.empty()) {
		auto projection = "SELECT *, " + StringUtil::Join(identities, ", ");
		current_rows = projection + " FROM (" + view_query_sql + ") openivm_current_rows";
		previous_rows = projection + " FROM " + data_table + " openivm_old_rows";
	}
	// Compare the two bags with witnesses for values that SQL equality merges.
	return "WITH openivm_window_current AS (" + current_rows + "), openivm_window_previous AS (" + previous_rows +
	       ") SELECT DISTINCT " + key_cols +
	       " FROM ((SELECT * FROM openivm_window_current EXCEPT ALL SELECT * FROM openivm_window_previous) UNION ALL "
	       "(SELECT * FROM openivm_window_previous EXCEPT ALL SELECT * FROM openivm_window_current)) "
	       "openivm_changed_rows";
}

struct WindowResultDiff {
	string query;
	string affected_keys;
	string setup;
	string cleanup;
};

static WindowResultDiff BuildWindowResultDiff(const string &view_name, const string &data_table,
                                              const string &view_query_sql, const vector<string> &partition_cols,
                                              const WindowOutput &output) {
	WindowResultDiff diff;
	if (output.dialect == SqlDialect::DUCKDB) {
		// Both statements need the same current rows. In particular, a LIMIT can
		// prevent the affected-partition predicate from being pushed into the query.
		auto current_table =
		    SqlUtils::FullName(TEMP_CATALOG, DEFAULT_SCHEMA, "openivm_window_current_" + NewCompilationId(view_name));
		diff.setup = "CREATE TEMP TABLE " + current_table + " AS\n" + view_query_sql + ";\n";
		diff.query = "SELECT * FROM " + current_table;
		diff.cleanup = "DROP TABLE IF EXISTS " + current_table + ";\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Reusing materialized window results for '%s'\n", view_name.c_str());
	} else {
		diff.query = view_query_sql;
	}
	diff.affected_keys = BuildChangedWindowPartitionsSQL(data_table, diff.query, partition_cols, output);
	return diff;
}

static string BuildResultDiffPartitionRefreshSQL(const string &view_name, const string &data_table,
                                                 const string &view_query_sql, const vector<string> &partition_cols,
                                                 const WindowOutput &output,
                                                 RefreshPublicationScope *publication_scope) {
	auto diff = BuildWindowResultDiff(view_name, data_table, view_query_sql, partition_cols, output);
	return diff.setup +
	       BuildAffectedPartitionRefreshSQL(view_name, data_table, diff.query, diff.affected_keys, partition_cols,
	                                        publication_scope, output.cascade_delta) +
	       diff.cleanup;
}

static string BuildDuckLakeWindowRefreshWithLineage(
    RefreshMetadata &metadata, Connection &con, const string &view_name, const string &view_query_sql,
    const vector<string> &delta_table_names, const vector<string> &partition_cols, const string &data_table,
    const string &view_catalog_name, const string &view_schema_name, const string &attached_db_catalog_name,
    const string &attached_db_schema_name, const RefreshMetadata::WindowPartitionLineage &lineage,
    const WindowOutput &output, RefreshPublicationScope *publication_scope) {
	string affected_keys;
	if (!lineage.ops.empty() &&
	    BuildLineageDuckLakeAffectedKeysSQL(metadata, con, view_name, delta_table_names, partition_cols,
	                                        view_catalog_name, view_schema_name, attached_db_catalog_name,
	                                        attached_db_schema_name, lineage, affected_keys)) {
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake lineage change-feed, %zu "
		                    "sources)\n",
		                    delta_table_names.size());
		// Conservative lineage can over-include partitions, but must cover every changed source.
		// If lineage is incomplete, the full logical view diff below preserves correctness.
		return BuildAffectedPartitionRefreshSQL(view_name, data_table, view_query_sql, affected_keys, partition_cols,
		                                        publication_scope, output.cascade_delta);
	}

	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake view-diff, %zu "
	                    "partition cols, %zu sources)\n",
	                    partition_cols.size(), delta_table_names.size());
	return BuildResultDiffPartitionRefreshSQL(view_name, data_table, view_query_sql, partition_cols, output,
	                                          publication_scope);
}

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const vector<LogicalType> &column_types,
                                   const string &data_table, const string &delta_ts_filter,
                                   const string &internal_catalog_prefix, const string &view_catalog_name,
                                   const string &view_schema_name, const string &attached_db_catalog_name,
                                   const string &attached_db_schema_name, bool cross_system, bool has_join,
                                   optional_ptr<const DeltaActivityResult> activity, bool caller_transaction,
                                   bool emit_cascade_delta, bool running_window_incremental, bool *uses_running_suffix,
                                   RefreshPublicationScope *publication_scope) {
	if (uses_running_suffix) {
		*uses_running_suffix = false;
	}
	auto dialect = openivm::CompileFactsContextSlot::Get(*con.context).target_dialect;
	const WindowOutput output {
	    column_names, column_types, dialect, Settings::Get<DefaultCollationSetting>(*con.context),
	    emit_cascade_delta ? internal_catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name)) : ""};
	optional<bool> saved_result_diff;
	auto partition_cols = metadata.GetWindowPartitionColumns(view_name, &saved_result_diff);
	auto order_cols = metadata.GetWindowOrderColumns(view_name);
	bool any_ducklake = AnyDuckLakeSource(metadata, view_name, delta_table_names);
	bool require_lineage = has_join || delta_table_names.size() > 1;
	auto ducklake_scope = any_ducklake ? DuckLakeWindowScope(partition_cols, column_names) : vector<string>();
	optional<ReboundWindowPartition> rebound;
	auto get_rebound = [&]() -> ReboundWindowPartition & {
		if (!rebound) {
			auto &scope = ducklake_scope.empty() ? partition_cols : ducklake_scope;
			rebound = RebindWindowPartitionLineage(con, view_query_sql, column_names, scope);
			if (any_ducklake && ducklake_scope.empty()) {
				ducklake_scope = DuckLakeWindowScope(partition_cols, column_names);
			}
		}
		return *rebound;
	};
	auto read_lineage = [&](bool require_proof) {
		RefreshMetadata::WindowPartitionLineage lineage;
		metadata.GetWindowPartitionLineage(view_name, lineage);
		// Current views store their proof, even an empty one. Only legacy views are planned again to recover it.
		if (require_proof && !lineage.proven_columns) {
			auto &bound = get_rebound();
			lineage = bound.requires_result_diff ? RefreshMetadata::WindowPartitionLineage() : std::move(bound.lineage);
		}
		return lineage;
	};
	// Legacy metadata never recorded whether a LIMIT or DISTINCT ON could
	// evict a row in an unchanged source partition.
	bool requires_result_diff = saved_result_diff.value_or(false);
	if (!saved_result_diff && !partition_cols.empty()) {
		requires_result_diff = get_rebound().requires_result_diff;
	}
	if (any_ducklake && requires_result_diff && !partition_cols.empty()) {
		auto scope = ResultDiffScope(partition_cols, column_names);
		return BuildResultDiffPartitionRefreshSQL(view_name, data_table, view_query_sql, scope, output,
		                                          publication_scope);
	}
	if (!ducklake_scope.empty()) {
		// Narrowing a composite scope needs the same proof as a join:
		// the selected output must represent the source key at every window.
		bool require_proof = require_lineage || partition_cols.size() > 1;
		RefreshMetadata::WindowPartitionLineage lineage;
		if (require_proof) {
			lineage = read_lineage(require_proof);
		}
		if (delta_table_names.size() == 1 &&
		    (!require_proof || (lineage.IsProven(RefreshMetadata::ParseWindowColumn(ducklake_scope[0]).first) &&
		                        HasDirectDuckLakeWindowPartitionOps(lineage, ducklake_scope[0])))) {
			return BuildSingleSourceDuckLakeWindowRefresh(metadata, con, view_name, view_query_sql, ducklake_scope,
			                                              order_cols, data_table, view_catalog_name, view_schema_name,
			                                              attached_db_catalog_name, attached_db_schema_name,
			                                              delta_table_names[0], output, publication_scope);
		}
		return BuildDuckLakeWindowRefreshWithLineage(
		    metadata, con, view_name, view_query_sql, delta_table_names, ducklake_scope, data_table, view_catalog_name,
		    view_schema_name, attached_db_catalog_name, attached_db_schema_name, lineage, output, publication_scope);
	}
	if (any_ducklake) {
		OPENIVM_DEBUG_PRINT(
		    "[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake, full recompute fallback)\n");
		return CompileFullRecompute(view_name, view_query_sql, internal_catalog_prefix, emit_cascade_delta);
	}
	string affected_keys_sql;
	auto lineage_result = LineageAffectedKeysResult::UNAVAILABLE;
	string composite_scope;
	bool composite = partition_cols.size() > 1;
	if (!requires_result_diff && !partition_cols.empty() && (!composite || require_lineage)) {
		auto lineage = read_lineage(require_lineage);
		for (const auto &partition : partition_cols) {
			auto output_col = RefreshMetadata::ParseWindowColumn(partition).first;
			// A proven common key covers every affected partition under that key.
			if (composite && !lineage.IsProven(output_col)) {
				continue;
			}
			auto result = BuildLineageStandardAffectedKeysSQL(
			    metadata, con, view_name, view_query_sql, delta_table_names, output_col,
			    output.KeyCollation(output_col), delta_ts_filter, view_catalog_name, view_schema_name,
			    attached_db_catalog_name, attached_db_schema_name, data_table, require_lineage, activity, lineage,
			    affected_keys_sql);
			if (!composite || result == LineageAffectedKeysResult::AVAILABLE) {
				lineage_result = result;
				if (composite) {
					composite_scope = partition;
				}
				break;
			}
		}
	}
	if (lineage_result == LineageAffectedKeysResult::UNSAFE) {
		OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION lineage is unsafe for '%s' — full recompute fallback\n",
		                    view_name.c_str());
		return CompileFullRecompute(view_name, view_query_sql, internal_catalog_prefix, emit_cascade_delta);
	}
	bool use_result_diff = (require_lineage || requires_result_diff) &&
	                       lineage_result != LineageAffectedKeysResult::AVAILABLE && !partition_cols.empty();
	WindowResultDiff result_diff;
	if (use_result_diff) {
		// Matching column names in delta tables do not prove a partition relationship.
		partition_cols = ResultDiffScope(partition_cols, column_names);
		result_diff = BuildWindowResultDiff(view_name, data_table, view_query_sql, partition_cols, output);
		affected_keys_sql = std::move(result_diff.affected_keys);
		OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION uses result-diff keys for '%s'\n", view_name.c_str());
	}
	vector<WindowPartitionDeltaSpec> partition_delta_specs;
	if (!composite_scope.empty()) {
		partition_cols = {std::move(composite_scope)};
	} else if (!use_result_diff) {
		partition_delta_specs =
		    BuildWindowPartitionDeltaSpecs(metadata, con, view_name, delta_table_names, partition_cols);
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (%zu partition cols, lineage keys: %s)\n",
	                    partition_cols.size(), lineage_result == LineageAffectedKeysResult::AVAILABLE ? "yes" : "no");
	vector<string> running_window_column_names = column_names;
	if (running_window_incremental) {
		auto internal_location = metadata.GetInternalLocation(view_name, view_catalog_name, view_schema_name);
		auto data_cols = metadata.GetTableColumns(internal_location.catalog_name, internal_location.schema_name,
		                                          IncrementalTableNames::DataTableName(view_name));
		if (!data_cols.empty()) {
			running_window_column_names = std::move(data_cols);
		}
	}
	bool use_data_modifying_ctes = false;
	// DuckDB's data-modifying CTEs cannot be replayed by Spark. The compile bridge
	// can target Spark even when its temporary catalog is native DuckDB.
	if (!cross_system && dialect == SqlDialect::DUCKDB) {
		con.context->RunFunctionInTransaction([&]() {
			use_data_modifying_ctes =
			    Catalog::GetCatalog(*con.context, Identifier(view_catalog_name)).SupportsMultipleDMLCTEs();
		});
	}
	auto refresh_sql = CompileWindowRecompute(
	    view_name, use_result_diff ? result_diff.query : view_query_sql, delta_ts_filter, internal_catalog_prefix,
	    partition_cols, partition_delta_specs, emit_cascade_delta, affected_keys_sql, running_window_column_names,
	    running_window_incremental, uses_running_suffix, use_data_modifying_ctes,
	    caller_transaction && dialect == SqlDialect::DUCKDB);
	if (use_result_diff) {
		return result_diff.setup + refresh_sql + result_diff.cleanup;
	}
	return refresh_sql;
}

} // namespace duckdb
