#include "postgres.h"

#include "postgres_fdw.h"
#include "utils/guc.h"

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
