#ifndef POSTGRES_FDW_PLUS_H
#define POSTGRES_FDW_PLUS_H

extern bool pgfdw_two_phase_commit;
extern bool pgfdw_skip_commit_phase;
extern bool pgfdw_track_xact_commits;

extern void DefineCustomVariablesForPgFdwPlus(void);

#endif							/* POSTGRES_FDW_PLUS_H */
