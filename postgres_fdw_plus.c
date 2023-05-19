#include "postgres.h"

#include "access/table.h"
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
bool		pgfdw_use_read_committed = false;

/*
 * Global variables
 */

/*
 * This flag indicates whether the current local transaction uses
 * the read committed isolation level when starting remote transactions.
 * The flag is set when the first remote transaction is started and is
 * based on the value of the pgfdw_use_read_committed parameter.
 * The flag remains constant for the duration of the local transaction,
 * even if pgfdw_use_read_committed is changed during that time.
 */
bool		pgfdw_use_read_committed_in_xact = false;

/*
 * This saves the command ID that was retrieved the last time a PGconn
 * was obtained, i.e., GetConnection() is called. The saved command ID
 * is used to detect cases where a single local query requires multiple
 * accesses to remote servers, which is not allowed when the read committed
 * isolation level is used for remote transactions.
 */
CommandId	pgfdw_last_cid = InvalidCommandId;

/*
 * This list has ConnCacheEntry that are parallel_commit=on and have already
 * sent PREPARED TRANSACTION. On a transaction abort, PQgetResult should be
 * called to these connections before sending ROLLBACK PREPARED.
 */
List	   *pending_entries_prepare = NIL;

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

	DefineCustomBoolVariable("postgres_fdw.use_read_committed",
							 "Use READ COMMITTED isolation level on remote transactions.",
							 NULL,
							 &pgfdw_use_read_committed,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

/*
 * Connection cache (initialized on first use)
 */
HTAB *ConnectionHash = NULL;

void
pgfdw_abort_cleanup(ConnCacheEntry *entry, bool toplevel)
{
	char		sql[100];

	CONSTRUCT_ABORT_COMMAND(sql, entry, toplevel);
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
pgfdw_arrange_read_committed(bool xact_got_connection)
{
	/*
	 * Determine whether the current local transaction uses the read
	 * committed isolation level when starting remote transactions.
	 */
	if (!xact_got_connection)
	{
		pgfdw_use_read_committed_in_xact = pgfdw_use_read_committed;
		pgfdw_last_cid = InvalidCommandId;
	}

	/*
	 * When using the read committed isolation level for remote transactions,
	 * a single query should perform only one foreign scan to maintain
	 * consistency. If a query performs multiple foreign scans, it triggers
	 * an error. This is detected by checking how many times GetConnection()
	 * is called with the same command ID.
	 */
	if (pgfdw_use_read_committed_in_xact)
	{
		CommandId	cid = GetCurrentCommandId(true);

		if (pgfdw_last_cid == cid)
			ereport(ERROR,
					(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
					 errmsg("could not initiate multiple foreign scans in a single query"),
					 errdetail("Multiple foreign scans are not allowed in a single query when using read committed level for remote transactions to maintain consistency."),
					 errhint("Disable postgres_fdw.use_read_committed or modify query to perform only a single foreign scan.")));
		pgfdw_last_cid = cid;
	}
}

bool
pgfdw_xact_two_phase(XactEvent event)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	List	   *umids = NIL;
	List	   *pending_entries_commit_prepared = NIL;

	/* Quick exit if not two-phase commit case */
	switch (event)
	{
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
			if (!pgfdw_two_phase_commit)
				return false;
			break;
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			return false;
		default:
			break;
	}

	if ((event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		&& pending_entries_prepare)
		pgfdw_cleanup_pending_entries();

	if (pending_entries_prepare)
	{
		list_free(pending_entries_prepare);
		pending_entries_prepare = NIL;
	}

	/*
	 * Scan all connection cache entries to find open remote transactions, and
	 * close them.
	 */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry->conn == NULL)
			continue;

		/* If it has an open remote transaction, try to close it */
		if (entry->xact_depth > 0)
		{
			elog(DEBUG3, "closing remote transaction on connection %p",
				 entry->conn);

			switch (event)
			{
				case XACT_EVENT_PARALLEL_PRE_COMMIT:
				case XACT_EVENT_PRE_COMMIT:
					Assert(pgfdw_two_phase_commit);

					/*
					 * If abort cleanup previously failed for this connection,
					 * we can't issue any more commands against it.
					 */
					pgfdw_reject_incomplete_xact_state_change(entry);

					pgfdw_prepare_xacts(entry);
					if (pgfdw_track_xact_commits)
						umids = lappend_oid(umids, (Oid) entry->key);
					continue;
				case XACT_EVENT_PARALLEL_COMMIT:
				case XACT_EVENT_COMMIT:
					pgfdw_commit_prepared(entry,
										  &pending_entries_commit_prepared);
					if (entry->parallel_commit && !pgfdw_skip_commit_phase)
						continue;
					break;

				case XACT_EVENT_PARALLEL_ABORT:
				case XACT_EVENT_ABORT:
					if (pgfdw_rollback_prepared(entry))
						break;
					pgfdw_abort_cleanup(entry, true);
					break;

				default:
					Assert(false);	/* can't happen */
					break;
			}
		}

		/* Reset state to show we're out of a transaction */
		entry->fxid = InvalidFullTransactionId;
		pgfdw_reset_xact_state(entry, true);
	}

	if (pending_entries_prepare)
	{
		Assert(event == XACT_EVENT_PARALLEL_PRE_COMMIT ||
			   event == XACT_EVENT_PRE_COMMIT);
		pgfdw_finish_prepare_cleanup();
	}

	if (pending_entries_commit_prepared)
	{
		Assert(event == XACT_EVENT_PARALLEL_COMMIT ||
			   event == XACT_EVENT_COMMIT);
		pgfdw_finish_commit_prepared_cleanup(pending_entries_commit_prepared);
	}

	if (umids)
	{
		Assert(event == XACT_EVENT_PARALLEL_PRE_COMMIT ||
			   event == XACT_EVENT_PRE_COMMIT);
		pgfdw_insert_xact_commits(umids);
	}

	return true;
}

void
pgfdw_prepare_xacts(ConnCacheEntry *entry)
{
	char		sql[256];

	Assert(!FullTransactionIdIsValid(entry->fxid));
	entry->fxid = GetTopFullTransactionId();

	PreparedXactCommand(sql, "PREPARE TRANSACTION", entry);

	entry->changing_xact_state = true;
	if (entry->parallel_commit)
	{
		do_sql_command_begin(entry->conn, sql);
		pending_entries_prepare = lappend(pending_entries_prepare, entry);
		return;
	}

	do_sql_command(entry->conn, sql);
	entry->changing_xact_state = false;
}

void
pgfdw_finish_prepare_cleanup(void)
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
pgfdw_cleanup_pending_entries(void)
{
	ConnCacheEntry *entry;
	char		sql[256];
	ListCell   *lc;

	foreach(lc, pending_entries_prepare)
	{
		TimestampTz endtime;
		bool		success;

		entry = (ConnCacheEntry *) lfirst(lc);

		/* If this connection has problem or is cleaned up already, skip it */
		if (PQstatus(entry->conn) != CONNECTION_OK ||
			!entry->changing_xact_state)
			continue;

		endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											  CONNECTION_CLEANUP_TIMEOUT);
		PreparedXactCommand(sql, "ROLLBACK PREPARED", entry);
		success = pgfdw_exec_cleanup_query_end(entry->conn, sql, endtime,
											   true, false);
		if (success)
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
			if (pgfdw_exec_cleanup_query_begin(entry->conn, sql))
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
		TimestampTz endtime;

		entry = (ConnCacheEntry *) lfirst(lc);

		Assert(entry->changing_xact_state);

		/*
		 * Set end time.  We do this now, not before issuing the command like
		 * in normal mode, for the same reason as for the cancel_requested
		 * entries.
		 */
		endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											  CONNECTION_CLEANUP_TIMEOUT);

		PreparedXactCommand(sql, "COMMIT PREPARED", entry);
		success = pgfdw_exec_cleanup_query_end(entry->conn, sql, endtime,
											   false, false);
		entry->changing_xact_state = false;

		if (success)
			pgfdw_deallocate_all(entry);

		entry->fxid = InvalidFullTransactionId;
		pgfdw_reset_xact_state(entry, true);
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
