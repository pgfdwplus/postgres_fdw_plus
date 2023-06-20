#ifndef POSTGRES_FDW_PLUS_H
#define POSTGRES_FDW_PLUS_H

#include "access/xact.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "postgres_fdw/postgres_fdw.h"
#include "utils/guc.h"

/*
 * GUC parameters
 */
extern bool pgfdw_two_phase_commit;
extern bool pgfdw_skip_commit_phase;
extern bool pgfdw_track_xact_commits;
extern bool pgfdw_use_read_committed;

/*
 * Global variables
 */
extern bool pgfdw_use_read_committed_in_xact;
extern CommandId pgfdw_last_cid;

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the user mapping OID. We use just one
 * connection per user mapping ID, which ensures that all the scans use the
 * same snapshot during a query.  Using the user mapping OID rather than
 * the foreign server OID + user OID avoids creating multiple connections when
 * the public user mapping applies to all user OIDs.
 *
 * The "conn" pointer can be NULL if we don't currently have a live connection.
 * When we do have a connection, xact_depth tracks the current depth of
 * transactions and subtransactions open on the remote side.  We need to issue
 * commands at the same nesting depth on the remote as we're executing at
 * ourselves, so that rolling back a subtransaction will kill the right
 * queries and not the wrong ones.
 */
typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	PGconn	   *conn;			/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open, 2 =
								 * one level of subxact open, etc */
	bool		have_prep_stmt; /* have we prepared any stmts in this xact? */
	bool		have_error;		/* have any subxacts aborted in this xact? */
	bool		changing_xact_state;	/* xact state change in process */
	bool		parallel_commit;	/* do we commit (sub)xacts in parallel? */
	bool		parallel_abort;	/* do we abort (sub)xacts in parallel? */
	bool		invalidated;	/* true if reconnect is pending */
	bool		keep_connections;	/* setting value of keep_connections
									 * server option */
	Oid			serverid;		/* foreign server OID used to get server name */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
	PgFdwConnState state;		/* extra per-connection state */

	/* postgres_fdw_plus */
	FullTransactionId fxid;
} ConnCacheEntry;

extern HTAB *ConnectionHash;

/*
 * Milliseconds to wait to cancel an in-progress query or execute a cleanup
 * query; if it takes longer than 30 seconds to do these, we assume the
 * connection is dead.
 */
#define CONNECTION_CLEANUP_TIMEOUT	30000

/* Macro for constructing abort command to be sent */
#define CONSTRUCT_ABORT_COMMAND(sql, entry, toplevel) \
	do { \
		if (toplevel) \
			snprintf((sql), sizeof(sql), \
					 "ABORT TRANSACTION"); \
		else \
			snprintf((sql), sizeof(sql), \
					 "ROLLBACK TO SAVEPOINT s%d; RELEASE SAVEPOINT s%d", \
					 (entry)->xact_depth, (entry)->xact_depth); \
	} while(0)

/* option.c */
extern void DefineCustomVariablesForPgFdwPlus(void);

/* connection.c */
extern void do_sql_command_begin(PGconn *conn, const char *sql);
extern void do_sql_command_end(PGconn *conn, const char *sql,
							   bool consume_input);

extern void pgfdw_reject_incomplete_xact_state_change(ConnCacheEntry *entry);
extern void pgfdw_reset_xact_state(ConnCacheEntry *entry, bool toplevel);
extern bool pgfdw_cancel_query(PGconn *conn);
extern bool pgfdw_cancel_query_begin(PGconn *conn);
extern bool pgfdw_cancel_query_end(PGconn *conn, TimestampTz endtime,
								   bool consume_input);
extern bool pgfdw_exec_cleanup_query(PGconn *conn, const char *query,
									 bool ignore_errors);
extern bool pgfdw_exec_cleanup_query_begin(PGconn *conn, const char *query);
extern bool pgfdw_exec_cleanup_query_end(PGconn *conn, const char *query,
										 TimestampTz endtime,
										 bool consume_input,
										 bool ignore_errors);
extern bool pgfdw_get_cleanup_result(PGconn *conn, TimestampTz endtime,
									 PGresult **result, bool *timed_out);
extern void pgfdw_abort_cleanup(ConnCacheEntry *entry, bool toplevel);
extern void pgfdw_abort_cleanup_with_sql(ConnCacheEntry *entry,
										 const char *sql, bool toplevel);
extern bool pgfdw_abort_cleanup_begin(ConnCacheEntry *entry, bool toplevel,
									  List **pending_entries,
									  List **cancel_requested);
extern void pgfdw_finish_abort_cleanup(List *pending_entries,
									   List *cancel_requested,
									   bool toplevel);

extern void pgfdw_arrange_read_committed(bool xact_got_connection);
extern bool pgfdw_xact_two_phase(XactEvent event);
extern void pgfdw_prepare_xacts(ConnCacheEntry *entry,
								List **pending_entries_prepare);
extern void pgfdw_finish_prepare_cleanup(List **pending_entries_prepare);
extern void pgfdw_commit_prepared(ConnCacheEntry *entry,
								  List **pending_entries_commit_prepared);
extern void pgfdw_finish_commit_prepared_cleanup(
	List *pending_entries_commit_prepared);
extern bool pgfdw_rollback_prepared(ConnCacheEntry *entry);
extern void pgfdw_deallocate_all(ConnCacheEntry *entry);
extern void pgfdw_insert_xact_commits(List *umids);

/*
 * Construct the prepared transaction command like PREPARE TRANSACTION
 * that's issued to the foreign server. It consists of full transaction ID,
 * user mapping OID, process ID and cluster name.
 */
#define PreparedXactCommand(sql, cmd, entry)	\
	snprintf(sql, sizeof(sql), "%s 'pgfdw_" UINT64_FORMAT "_%u_%d_%s'",	\
			 cmd, U64FromFullTransactionId(entry->fxid),	\
			 (Oid) entry->key, MyProcPid,	\
			 (*cluster_name == '\0') ? "null" : cluster_name)

#endif							/* POSTGRES_FDW_PLUS_H */
