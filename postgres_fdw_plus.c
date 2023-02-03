#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "postgres_fdw_plus.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/xid8.h"

/*
 * GUC parameters
 */
bool		pgfdw_two_phase_commit = false;
bool		pgfdw_skip_commit_phase = false;
bool		pgfdw_track_xact_commits = true;

/*
 * Define GUC parameters for postgres_fdw_plus.
 */
void
DefineCustomVariablesForPgFdwPlus(void)
{
	DefineCustomBoolVariable("postgres_fdw.two_phase_commit",
							 "Uses two phase commit to commit foreign transactions.",
							 NULL,
							 &pgfdw_two_phase_commit,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("postgres_fdw.skip_commit_phase",
							 "Performs only prepare phase in two phase commit.",
							 NULL,
							 &pgfdw_skip_commit_phase,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("postgres_fdw.track_xact_commits",
							 "Collects transaction commits information.",
							 NULL,
							 &pgfdw_track_xact_commits,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

bool
pgfdw_exec_cleanup_query_begin(ConnCacheEntry *entry, const char *query)
{
	/*
	 * If it takes too long to execute a cleanup query, assume the connection
	 * is dead.  It's fairly likely that this is why we aborted in the first
	 * place (e.g. statement timeout, user cancel), so the timeout shouldn't
	 * be too long.
	 */
	entry->endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

	/*
	 * Submit a query.  Since we don't use non-blocking mode, this also can
	 * block.  But its risk is relatively small, so we ignore that for now.
	 */
	if (!PQsendQuery(entry->conn, query))
	{
		pgfdw_report_error(WARNING, NULL, entry->conn, false, query);
		return false;
	}

	return true;
}

bool
pgfdw_exec_cleanup_query_end(ConnCacheEntry *entry, const char *query,
							 bool ignore_errors)
{
	PGresult   *result = NULL;
	bool		timed_out;

	/* Get the result of the query. */
	if (pgfdw_get_cleanup_result(entry->conn, entry->endtime, &result,
								 &timed_out))
	{
		if (timed_out)
			ereport(WARNING,
					(errmsg("could not get query result due to timeout"),
					 query ? errcontext("remote SQL command: %s", query) : 0));
		else
			pgfdw_report_error(WARNING, NULL, entry->conn, false, query);

		return false;
	}

	/* Issue a warning if not successful. */
	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		pgfdw_report_error(WARNING, result, entry->conn, true, query);
		return ignore_errors;
	}
	PQclear(result);

	return true;
}

void
pgfdw_abort_cleanup(ConnCacheEntry *entry, bool toplevel)
{
	char		sql[100];

	if (toplevel)
		snprintf(sql, sizeof(sql), "ABORT TRANSACTION");
	else
		snprintf(sql, sizeof(sql),
				 "ROLLBACK TO SAVEPOINT s%d; RELEASE SAVEPOINT s%d",
				 entry->xact_depth, entry->xact_depth);

	pgfdw_abort_cleanup_with_sql(entry, sql, toplevel);
}

void
pgfdw_abort_cleanup_with_sql(ConnCacheEntry *entry, const char *sql,
							 bool toplevel)
{
	/*
	 * Don't try to clean up the connection if we're already in error
	 * recursion trouble.
	 */
	if (in_error_recursion_trouble())
		entry->changing_xact_state = true;

	/*
	 * If connection is already unsalvageable, don't touch it further.
	 */
	if (entry->changing_xact_state)
		return;

	/*
	 * Mark this connection as in the process of changing transaction state.
	 */
	entry->changing_xact_state = true;

	/* Assume we might have lost track of prepared statements */
	entry->have_error = true;

	/*
	 * If a command has been submitted to the remote server by using an
	 * asynchronous execution function, the command might not have yet
	 * completed.  Check to see if a command is still being processed by the
	 * remote server, and if so, request cancellation of the command.
	 */
	if (PQtransactionStatus(entry->conn) == PQTRANS_ACTIVE &&
		!pgfdw_cancel_query(entry->conn))
		return;					/* Unable to cancel running query */

	if (!pgfdw_exec_cleanup_query(entry->conn, sql, false))
		return;					/* Unable to abort remote (sub)transaction */

	if (toplevel)
	{
		if (entry->have_prep_stmt && entry->have_error &&
			!pgfdw_exec_cleanup_query(entry->conn,
									  "DEALLOCATE ALL",
									  true))
			return;				/* Trouble clearing prepared statements */

		entry->have_prep_stmt = false;
		entry->have_error = false;
	}

	/*
	 * If pendingAreq of the per-connection state is not NULL, it means that
	 * an asynchronous fetch begun by fetch_more_data_begin() was not done
	 * successfully and thus the per-connection state was not reset in
	 * fetch_more_data(); in that case reset the per-connection state here.
	 */
	if (entry->state.pendingAreq)
		memset(&entry->state, 0, sizeof(entry->state));

	/* Disarm changing_xact_state if it all worked */
	entry->changing_xact_state = false;
}

void
pgfdw_prepare_xacts(ConnCacheEntry *entry, List **pending_entries_prepare)
{
	char		sql[256];

	Assert(!FullTransactionIdIsValid(entry->fxid));
	entry->fxid = GetTopFullTransactionId();

	PreparedXactCommand(sql, "PREPARE TRANSACTION", entry);

	entry->changing_xact_state = true;
	if (entry->parallel_commit)
	{
		do_sql_command_begin(entry->conn, sql);
		*pending_entries_prepare = lappend(*pending_entries_prepare, entry);
		return;
	}

	do_sql_command(entry->conn, sql);
	entry->changing_xact_state = false;
}

void
pgfdw_finish_prepare_cleanup(List *pending_entries_prepare)
{
	ConnCacheEntry *entry;
	char		sql[256];
	ListCell   *lc;

	Assert(pending_entries_prepare);

	foreach(lc, pending_entries_prepare)
	{
		entry = (ConnCacheEntry *) lfirst(lc);

		Assert(entry->changing_xact_state);

		/*
		 * We might already have received the result on the socket, so pass
		 * consume_input=true to try to consume it first
		 */
		PreparedXactCommand(sql, "PREPARE TRANSACTION", entry);
		do_sql_command_end(entry->conn, sql, true);
		entry->changing_xact_state = false;
	}
}

void
pgfdw_commit_prepared(ConnCacheEntry *entry,
					  List **pending_entries_commit_prepared)
{
	char		sql[256];
	bool		success = true;

	if (!FullTransactionIdIsValid(entry->fxid))
		return;

	Assert(pgfdw_two_phase_commit);

	if (!pgfdw_skip_commit_phase)
	{
		PreparedXactCommand(sql, "COMMIT PREPARED", entry);

		entry->changing_xact_state = true;
		if (entry->parallel_commit)
		{
			if (pgfdw_exec_cleanup_query_begin(entry, sql))
				*pending_entries_commit_prepared =
					lappend(*pending_entries_commit_prepared, entry);
			return;
		}
		success = pgfdw_exec_cleanup_query(entry->conn, sql, false);
		entry->changing_xact_state = false;
	}

	/*
	 * If COMMIT PREPARED fails, we don't do a DEALLOCATE ALL because it's
	 * also likely to fail or may get stuck (especially when
	 * pgfdw_exec_cleanup_query() reports failure because of a timeout).
	 */
	if (success)
		pgfdw_deallocate_all(entry);
}

void
pgfdw_finish_commit_prepared_cleanup(List *pending_entries_commit_prepared)
{
	ConnCacheEntry *entry;
	char		sql[256];
	ListCell   *lc;

	Assert(pending_entries_commit_prepared);

	foreach(lc, pending_entries_commit_prepared)
	{
		bool		success;

		entry = (ConnCacheEntry *) lfirst(lc);

		Assert(entry->changing_xact_state);

		PreparedXactCommand(sql, "COMMIT PREPARED", entry);
		success = pgfdw_exec_cleanup_query_end(entry, sql, false);
		entry->changing_xact_state = false;

		if (success)
			pgfdw_deallocate_all(entry);
	}
}

bool
pgfdw_rollback_prepared(ConnCacheEntry *entry)
{
	char		sql[256];

	if (!FullTransactionIdIsValid(entry->fxid))
		return false;

	Assert(pgfdw_two_phase_commit);

	if (!pgfdw_skip_commit_phase)
	{
		PreparedXactCommand(sql, "ROLLBACK PREPARED", entry);
		pgfdw_abort_cleanup_with_sql(entry, sql, true);
	}
	else
		pgfdw_deallocate_all(entry);

	return true;
}

/*
 * Do a DEALLOCATE ALL to make sure we get rid of all prepared statements.
 * See comments in pgfdw_xact_callback().
 */
void
pgfdw_deallocate_all(ConnCacheEntry *entry)
{
	if (entry->have_prep_stmt && entry->have_error)
		pgfdw_exec_cleanup_query(entry->conn, "DEALLOCATE ALL", true);

	entry->have_prep_stmt = false;
	entry->have_error = false;
}

/* Macros for pgfdw_plus.xact_commits table to track transaction commits */
#define PGFDW_PLUS_SCHEMA	"pgfdw_plus"
#define PGFDW_PLUS_XACT_COMMITS_TABLE	"xact_commits"
#define PGFDW_PLUS_XACT_COMMITS_COLS	2

/*
 * Insert the following two information about the current local
 * transaction into PGFDW_PLUS_XACT_COMMITS_TABLE table.
 *
 * 1. The full transaction ID of the current local transaction.
 * 2. Array of user mapping OID corresponding to a foreign transaction
 *    that the current local transaction started. The list of
 *    these user mapping OIDs needs to be specified in the argument
 *    "umids". This list must not be NIL.
 *
 * Note that PGFDW_PLUS_XACT_COMMITS_TABLE, as it is named,
 * eventually contains only the information of committed transactions.
 * If the transaction is rollbacked, the record inserted by this function
 * obviously gets unvisiable.
 */
void
pgfdw_insert_xact_commits(List *umids)
{
	Datum		values[PGFDW_PLUS_XACT_COMMITS_COLS];
	bool		nulls[PGFDW_PLUS_XACT_COMMITS_COLS];
	Datum	   *datums;
	int			ndatums;
	ArrayType  *arr;
	ListCell   *lc;
	Oid			namespaceId;
	Oid			relId;
	Relation	rel;
	HeapTuple	tup;

	Assert(umids != NIL);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[0] = FullTransactionIdGetDatum(GetTopFullTransactionId());

	/* Convert a list of umid to an oid[] Datum */
	ndatums = list_length(umids);
	datums = (Datum *) palloc(ndatums * sizeof(Datum));
	ndatums = 0;
	foreach(lc, umids)
	{
		Oid			umid = lfirst_oid(lc);

		datums[ndatums++] = ObjectIdGetDatum(umid);
	}
	arr = construct_array(datums, ndatums, OIDOID, sizeof(Oid),
						  true, TYPALIGN_INT);
	values[1] = PointerGetDatum(arr);

	/*
	 * Look up the schema and table to store transaction commits information.
	 * Note that we don't verify we have enough permissions on them, nor run
	 * object access hooks for them.
	 */
	namespaceId = get_namespace_oid(PGFDW_PLUS_SCHEMA, false);
	relId = get_relname_relid(PGFDW_PLUS_XACT_COMMITS_TABLE, namespaceId);
	if (!OidIsValid(relId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s.%s\" does not exist",
						PGFDW_PLUS_SCHEMA,
						PGFDW_PLUS_XACT_COMMITS_TABLE)));

	rel = table_open(relId, RowExclusiveLock);
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);

	table_close(rel, NoLock);
}
