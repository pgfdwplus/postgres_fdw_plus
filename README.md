# postgres_fdw_plus

This extension is a fork of
[postgres_fdw](https://www.postgresql.org/docs/devel/postgres-fdw.html)
contrib module provided by PostgreSQL.
It provides the same feature as postgres_fdw, and additionally aims to
support global transaction feature on multiple foreign PostgreSQL servers
(but currently it only allows us to use two-phase commit protocol
when committing foreign transaction on them).

The postgres_fdw_plus is released under
the [PostgreSQL License](https://opensource.org/licenses/postgresql),
a liberal Open Source license, similar to the BSD or MIT licenses.

## Install

Download the source archive of postgres_fdw_plus from
[here](https://github.com/MasaoFujii/postgres_fdw_plus),
and then build and install it. postgres_fdw_plus requires
PostgreSQL 16 or later.

```
$ cd postgres_fdw_plus
$ make USE_PGXS=1 PG_CONFIG=/opt/pgsql-X.Y.Z/bin/pg_config
$ su
# make USE_PGXS=1 PG_CONFIG=/opt/pgsql-X.Y.Z/bin/pg_config install
# exit
```

USE_PGXS=1 must be always specified when building this extension.
The path to [pg_config](https://www.postgresql.org/docs/devel/app-pgconfig.html)
(which exists in the bin directory of PostgreSQL installation)
needs be specified in PG_CONFIG.
However, if the PATH environment variable contains the path to pg_config,
PG_CONFIG doesn't need to be specified.

## Prepare for remote access using postgres_fdw_plus

1. Install the postgres_fdw_plus extension using CREATE EXTENSION.

   ```
   =# CREATE EXTENSION postgres_fdw_plus;
   ```

   Note that postgres_fdw or postgres_fdw_plus but not both can be installed
   on the same database.

2. Create a foreign server, a user mapping and a foreign table in the same way
as you do when using postgres_fdw. For example,

   ```
   =# CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw;
   =# CREATE USER MAPPING FOR public SERVER loopback;
   =# CREATE FOREIGN TABLE ft (i int) SERVER loopback OPTIONS (table_name 't');
   ```

   Note that the foreign data wrapper that postgres_fdw_plus provides is
   **postgres_fdw** not postgres_fdw_plus. That is, when creating a foreign
   server, postgres_fdw must be specified as a foreign data wrapper.

Then you can access and modify the data stored in remote servers
using SELECT, INSERT, etc on the foreign tables, in the same way as
you do using postgres_fdw.

## Configuration parameters

### postgres_fdw.two_phase_commit (boolean)
If true, all foreign transactions are committed by using
two-phase commit protocol (2PC).
If false (default), all foreign transactions are committed as
postgres_fdw currently does, i.e., 2PC is not used at all.

Any users can change this setting.

### postgres_fdw.skip_commit_phase (boolean)
If true, foreign transactions to be processed via two phase commit
protocol are marked just as prepared, but not actually committed,
i.e., prepare phase is performed but commit phase not. This setting
is useful to create foreign prepared transactions purposely for
the test or debug, for example. Those foreign prepared transactions
are expected to be committed or rollbacked by the functions to
resolve them. If false (default), both phases are performed in 2PC.
This setting has no effect if postgres_fdw.two_phase_commit is false.

Any users can change this setting.

### postgres_fdw.track_xact_commits (boolean)
If true (default), information about transactions that used two phase
commit protocol and was successfully committed are collected in
pgfdw_plus.xact_commits table. If false, no such information is collected.
This setting has no effect if postgres_fdw.two_phase_commit is false.

Any users can change this setting.

## Functions

### SETOF resolve_foreign_prepared_xacts pgfdw_plus_resolve_foreign_prepared_xacts (server name, force boolean)
Resolve (commit or rollback) foreign transactions prepared but not
committed yet on the specified remote servers. This function issues
COMMIT PREPARED for a foreign transaction if the local transaction
that started it was committed, and ROLLBACK PREPARED otherwise.

Note that it may be impossible to determine whether a local transaction
is committed or rollbacked when its full transaction ID is not found
in pgfdw_plus.xact_commits. There can be two cases where a local
transaction was committed but its ID was not recorded because
postgres_fdw.track_xact_commits was disabled, and it was rollbacked
so its ID was not recorded. If force is false (default), this function
does nothing for foreign prepared transactions of such "uncertain"
local transaction. Those foreign transactions need to be resolved by hand.
If force is true, this function determines that the local transaction
was aborted and rollbacks those foreign prepared transactions. If it's
guaranteed that all local transactions are tracked in
pgfdw_plus.xact_commits, e.g., because postgres_fdw.track_xact_commits
was never disabled, force can be safely set to true.

This function is restricted to superusers by default,
but other users can be granted EXECUTE to run the function.

Only users having valid user mapping to the specified server is allowed
to execute this function.

This function returns one row per foreign prepared transaction that
it successfully resolved, shown in the table below.

| Column Name   | Data Type | Description                                    |
|---------------|-----------|------------------------------------------------|
| status          | text      | transaction status (committed or aborted) of this foreign prepared transaction |
| server        | name      | name of server that this foreign prepared transaction existed |
| transaction         | xid   | transaction ID of this foreign prepared transaction |
| gid   | text    | global transaction ID that was assigned to this foreign prepared transaction |
| prepared | timestamptz    | time at which this foreign transaction was prepared for commit |
| owner    | name    | name of the user that executed this foreign transaction |
| database   | name    | name of the database in which this foreign transaction was executed |

### SETOF resolve_foreign_prepared_xacts pgfdw_plus_resolve_foreign_prepared_xacts_all (force boolean)
Execute pgfdw_plus_resolve_foreign_prepared_xacts() for all defined servers,
i.e., it resolves (commits or rollbacks) foreign transactions prepared
but not committed yet on those servers. This function ignores a server
whose foreign data wrapper is not postgres_fdw.

This function is restricted to superusers by default,
but other users can be granted EXECUTE to run the function.

Whenever this function executes pgfdw_plus_resolve_foreign_prepared_xacts()
for each server, it finds the proper user having a valid user mapping to
the server and changes the current user to the user by using SET ROLE.
So the caller of this function must be a superuser or a user having
memberships of all those "proper" users.

Please see the descriptions of pgfdw_plus_resolve_foreign_prepared_xacts() for
the details about the argument "force" and the return values.

### SETOF pgfdw_plus.xact_commits pgfdw_plus_vacuum_xact_commits()
Delete any records no longer necessary to resolve foreign prepared
transactions, from pgfdw_plus.xact_commits. This function returns
records that it deleted from pgfdw_plus.xact_commits.

This function is restricted to superusers by default,
but other users can be granted EXECUTE to run the function.

The caller of this function must be a superuser or a user having
memberships of all users having valid user mappings to all defined servers.

### boolean pgfdw_plus_verify_connection_states(server_name text)
Checks the status of remote connections established by postgres_fdw from the
local session to the foreign server with the given name. This check is performed
by polling the socket and allows long-running transactions to be aborted sooner
if the kernel reports that the connection is closed. This function is currently
available only on systems that support the non-standard POLLRDHUP extension to
the poll system call, including Linux. This returns true if existing connection
is not closed by the remote peer. false is returned if the local session seems
to be disconnected from other servers. NULL is returned if a valid connection
to the specified foreign server is not established or this function is not
available on this platform. If no foreign server with the given name is found,
an error is reported.

### boolean pgfdw_plus_can_verify_connection_states()
This function checks whether pgfdw_plus_verify_connection_states works well or
not. This returns true if it can be used, otherwise returns false.
