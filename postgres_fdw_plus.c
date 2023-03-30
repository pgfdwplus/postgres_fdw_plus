#include "postgres.h"

#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "postgres_fdw_plus.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/xid8.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

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

/*
 * Connection cache (initialized on first use)
 */
HTAB *ConnectionHash = NULL;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(pgfdw_plus_verify_connection_states);
PG_FUNCTION_INFO_V1(pgfdw_plus_can_verify_connection_states);

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

bool
pgfdw_xact_two_phase(XactEvent event)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	List	   *umids = NIL;
	List	   *pending_entries_prepare = NIL;
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

					pgfdw_prepare_xacts(entry, &pending_entries_prepare);
					if (pgfdw_track_xact_commits)
						umids = lappend_oid(umids, (Oid) entry->key);
					continue;

				case XACT_EVENT_PARALLEL_COMMIT:
				case XACT_EVENT_COMMIT:
					pgfdw_commit_prepared(entry,
										  &pending_entries_commit_prepared);
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
		pgfdw_finish_prepare_cleanup(pending_entries_prepare);
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

/*
 * Check whether check_connection_health() can work on this platform.
 *
 * This function returns true if this platform can use check_connection_health(),
 * otherwise false.
 */
bool
connection_checkable()
{
#if defined(HAVE_POLL) && defined(POLLRDHUP)
	return true;
#else
	return false;
#endif
}

/*
 * Check whether the socket peer closed the connection or not.
 *
 * This function returns >0 if the remote peer closed the connection, 0 if
 * disconnection is not detected or this function is not supported on this
 * platform, -1 if an error occurred.
 */
int
check_connection_health(PGconn *conn)
{
#if defined(HAVE_POLL) && defined(POLLRDHUP)
	struct	pollfd input_fd;
	int		sock = PQsocket(conn);
	int		result;
	int		errflags = POLLERR | POLLHUP | POLLNVAL;

	if (conn == NULL || sock == PGINVALID_SOCKET)
		return -1;

	input_fd.fd = sock;
	input_fd.events = POLLRDHUP;

	do
		result = poll(&input_fd, 1, 0);
	while (result < 0 && errno == EINTR);

	/* revents field is filld, but error state */
	if (result > 0 && (input_fd.revents & errflags))
		return -1;

	return result;
#else
	return 0;
#endif
}

/*
 * Workhorse to verify cached connections.
 *
 * This function scans all the connection cache entries and verifies the
 * connections whose foreign server OID matches with the specified one. If
 * InvalidOid is specified, it verifies all the cached connections.
 *
 * This function emits warnings if a disconnection is found. This returns false
 * if disconnections are found, otherwise returns true.
 *
 * checked will be set to true if check_connection_health() is called at least once.
 */
bool
verify_cached_connections(Oid serverid, bool *checked)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	bool		all = !OidIsValid(serverid);
	bool		result = true;
	StringInfoData str;

	*checked = false;

	Assert(ConnectionHash);

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (!entry->conn)
			continue;

		/* Skip if the entry is invalidated */
		if (entry->invalidated)
			continue;

		if (all || entry->serverid == serverid)
		{
			if (check_connection_health(entry->conn))
			{
				/* A foreign server might be down, so construct a message */
				ForeignServer *server = GetForeignServer(entry->serverid);

				if (result)
				{
					/*
					 * Initialize and add a prefix if this is the first
					 * disconnection we found.
					 */
					initStringInfo(&str);
					appendStringInfo(&str, "could not connect to server ");

					result = false;
				}
				else
					appendStringInfo(&str, ", ");

				appendStringInfo(&str, "\"%s\"", server->servername);
			}

			/* Set a flag to notify the caller */
			*checked = true;
		}
	}

	/* Raise a warning if disconnections are found */
	if (!result)
	{
		Assert(str.len);
		ereport(WARNING,
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("%s", str.data),
				errdetail("Connection close is detected."),
				errhint("Plsease check the health of server."));
		pfree(str.data);
	}

	return result;
}

/*
 * Verify the specified cached connections.
 *
 * This function verifies the connections that are established by postgres_fdw
 * from the local session to the foreign server with the given name.
 *
 * This function emits a warning if a disconnection is found. This returns true
 * if existing connection is not closed by the remote peer. false is returned
 * if the local session seems to be disconnected from other servers. NULL is
 * returned if a valid connection to the specified foreign server is not
 * established or this function is not available on this platform.
 */
Datum
pgfdw_plus_verify_connection_states(PG_FUNCTION_ARGS)
{
	ForeignServer *server;
	char	   *servername;
	bool		result;
	bool		checked = false;

	/* quick exit if the checking does not work well on this platfrom */
	if (!connection_checkable())
		PG_RETURN_NULL();

	/* quick exit if connection cache has not been initialized yet */
	if (!ConnectionHash)
		PG_RETURN_NULL();

	servername = text_to_cstring(PG_GETARG_TEXT_PP(0));
	server = GetForeignServerByName(servername, false);

	result = verify_cached_connections(server->serverid, &checked);

	/* Return the result if checking function was called, otherwise NULL */
	if (checked)
		PG_RETURN_BOOL(result);
	else
		PG_RETURN_NULL();
}

/*
 * Check whether functions for verifying cached connections work well or not
 */
Datum
pgfdw_plus_can_verify_connection_states(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(connection_checkable());
}
