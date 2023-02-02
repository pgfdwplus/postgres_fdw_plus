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
