#ifndef POSTGRES_FDW_PLUS_H
#define POSTGRES_FDW_PLUS_H

#include "nodes/pg_list.h"
#include "postgres_fdw.h"
#include "utils/guc.h"

extern bool pgfdw_two_phase_commit;
extern bool pgfdw_skip_commit_phase;
extern bool pgfdw_track_xact_commits;

extern void DefineCustomVariablesForPgFdwPlus(void);

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
