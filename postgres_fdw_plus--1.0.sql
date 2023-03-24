/* contrib/postgres_fdw_plus/postgres_fdw_plus--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION postgres_fdw_plus" to load this file. \quit

/*
 * Copy-and-paste the object definitions from postgres_fdw--1.0.sql.
 */
 CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION postgres_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER postgres_fdw
  HANDLER postgres_fdw_handler
  VALIDATOR postgres_fdw_validator;

/*
 * Copy-and-paste the object definitions from postgres_fdw--1.0--1.1.sql.
 */
CREATE FUNCTION postgres_fdw_get_connections (OUT server_name text,
    OUT valid boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_disconnect (text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_disconnect_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

/*
 * The object definitions for global transactions.
 */
 
 /*
 * Information about transactions that used two phase commit protocol
 * and were successfully committed are collected in this table.
 */
CREATE SCHEMA pgfdw_plus;
CREATE TABLE pgfdw_plus.xact_commits (
  fxid xid8 primary key,
  umids oid[]
);


/*
 * Retrieve information about foreign prepared transactions
 * from given server.
 */
CREATE FUNCTION pgfdw_plus_foreign_prepared_xacts (server name)
  RETURNS SETOF pg_prepared_xacts AS $$
DECLARE
  sql text;
BEGIN
  /*
   * Is there user mapping so that current user can connect to given server? */
  PERFORM * FROM pg_user_mappings
    WHERE srvname = server AND (usename = current_user OR usename = 'public');
  IF NOT FOUND THEN
    RAISE EXCEPTION 'user mapping for server "%" and user "%" not found',
      server, current_user;
  END IF;

  /* Is foreign data wrapper of given server "postgres_fdw"? */
  PERFORM * FROM pg_foreign_server fs, pg_foreign_data_wrapper fdw
    WHERE fs.srvfdw = fdw.oid AND fs.srvname = server AND
      fdw.fdwname = 'postgres_fdw';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'foreign data wrapper of specified server must be "postgres_fdw"';
  END IF;

  /* Has dblink already been installed? */
  PERFORM * FROM pg_extension WHERE extname = 'dblink';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'extension "dblink" must be installed';
  END IF;

  /* Collect only foreign prepared transactions that current user can handle */
  sql := 'SELECT * FROM pg_prepared_xacts ' ||
    'WHERE owner = current_user AND database = current_database() ' ||
    'AND gid LIKE ''pgfdw_%_%_%_' || current_setting('cluster_name') || '''';
  RETURN QUERY SELECT * FROM
    dblink(server, sql) AS t1
    (transaction xid, gid text, prepared timestamp with time zone,
      owner name, database name);
END;
$$ LANGUAGE plpgsql;


/*
 * Return true if backend with given PID may be still running
 * transaction with given XID, false otherwise.
 *
 * Note that here transaction is considered as running
 * until it and its all foreign transactions have been completed.
 */
CREATE FUNCTION pgfdw_plus_xact_is_running(target_xid xid, target_pid integer)
  RETURNS boolean AS $$
DECLARE
  r record;
BEGIN
  /* Quick exit if target backend is not running */
  SELECT * INTO r FROM pg_stat_get_activity(target_pid);
  IF NOT FOUND THEN RETURN false; END IF;

  /*
   * Return true if pg_stat_get_activity() reports target backend is
   * running target transaction.
   *
   * There is corner case where backend_xid and target XID are the same
   * but their epoch is different. In this case target transaction is not
   * running but true is returned incorrectly. For now we don't fix
   * this corner case because it's very unlikely to happen and there is
   * no good idea for the fix yet.
   */
  IF r.backend_xid = target_xid THEN RETURN true; END IF;

  /*
   * Even while pg_stat_get_activity() is reporting backend_xid is NULL,
   * if backend_xmin is NULL and also state is 'active', backend may be
   * still running and in commit phase of two phase commit protocol.
   * But since backend_xid is NULL, pg_stat_activity cannot tell whether
   * target transaction is running or other one is.
   *
   * In this case pg_locks is used next. While in commit phase,
   * pg_locks should have record for lock on transactionid with given PID
   * and XID. If it's found, transaction can be considered as running.
   *
   * Note that at first pg_stat_get_activity() is checked and then
   * pg_locks is. Because pg_stat_get_activity() is faster and enough for
   * many cases.
   */
  IF r.backend_xid IS NULL AND r.backend_xmin IS NULL AND r.state = 'active' THEN
    PERFORM * FROM pg_locks WHERE locktype = 'transactionid' AND
      pid = target_pid AND transactionid = target_xid AND
      mode = 'ExclusiveLock' AND granted = true;
    IF FOUND THEN RETURN true; END IF;
  END IF;

  RETURN false;
END;
$$ LANGUAGE plpgsql;


/*
 * Data type for record that pgfdw_plus_resolve_foreign_prepared_xacts()
 * and _all() return.
 */
CREATE TYPE type_resolve_foreign_prepared_xacts AS
  (status text, server name, transaction xid, gid text,
    prepared timestamp with time zone, owner name, database name);


/*
 * Resolve foreign prepared transactions on given server.
 */
CREATE FUNCTION
  pgfdw_plus_resolve_foreign_prepared_xacts (server name, force boolean DEFAULT false)
  RETURNS SETOF type_resolve_foreign_prepared_xacts AS $$
DECLARE
  r type_resolve_foreign_prepared_xacts;
  sql text;
  full_xid xid8;
  pid integer;
  connected boolean := false;
BEGIN
  FOR r IN SELECT NULL AS status, server, *
    FROM pgfdw_plus_foreign_prepared_xacts(server) LOOP
    sql := NULL;

    BEGIN
      full_xid := split_part(r.gid, '_', 2)::xid8;
      pid := split_part(r.gid, '_', 4)::integer;

      /*
       * Skip resolving foreign prepared transactions if backend that
       * started them may be still committing or rollbacking them.
       * Otherwise backend and this function may try to resolve foreign
       * prepared transactions at the same time, and which would cause
       * either of them to fail with an error.
       */
      CONTINUE WHEN pgfdw_plus_xact_is_running(full_xid::xid, pid);

      /*
       * At first use pg_xact_status() to check commit status of transaction
       * with full_xid. Because pg_xact_status() can be used for that purpose
       * even when track_xact_commits is off, and using it is faster than
       * lookup on xact_commits table.
       */
      r.status := pg_xact_status(full_xid);
      CASE r.status
        WHEN 'committed' THEN sql := 'COMMIT PREPARED ''' || r.gid || '''';
        WHEN 'aborted' THEN sql := 'ROLLBACK PREPARED ''' || r.gid || '''';
      END CASE;
    EXCEPTION WHEN OTHERS THEN
    END;

    /*
     * Look up xact_commits table if pg_xact_status() fails to report commit
     * status. If entry for full_xid is found in it, transaction with full_xid
     * should be considered as committed. Otherwise as rollbacked
     * unless track_xact_commits had been disabled so far. Note that
     * foreign prepared transaction is kept as it is by default if its entry
     * is not found, in case of track_xact_commits=off. To forcibly
     * rollback it, argument 'force' needs to be enabled.
     */
    IF sql IS NULL THEN
      PERFORM * FROM pgfdw_plus.xact_commits WHERE fxid = full_xid LIMIT 1;
      IF FOUND THEN
        r.status := 'committed';
        sql := 'COMMIT PREPARED ''' || r.gid || '''';
      ELSIF force THEN
        r.status := 'aborted';
        sql := 'ROLLBACK PREPARED ''' || r.gid || '''';
      ELSE
        RAISE NOTICE 'could not resolve foreign prepared transaction with gid "%"',
          r.gid USING HINT = 'Commit status of transaction with fxid "' ||
          full_xid || '" not found';
      END IF;
    END IF;

    IF sql IS NOT NULL THEN
      RETURN NEXT r;

    /*
     * Use dblink_connect() and dblink_exec() here instead of dblink()
     * so that new connection is made only once and is reused to execute
     * transaction command. This would decrease the number of connection
     * establishments and improve performance of this function especially
     * when there are lots of foreign prepared transactions to resolve.
     */
      IF NOT connected THEN
        PERFORM dblink_connect('pgfdw_plus_conn', server);
	connected := true;
      END IF;
      PERFORM dblink_exec('pgfdw_plus_conn', sql);
    END IF;
  END LOOP;

  IF connected THEN
    PERFORM dblink_disconnect('pgfdw_plus_conn');
  END IF;
END;
$$ LANGUAGE plpgsql;


/*
 * Set current user to given user.
 *
 * Return true if successful, false otherwise. Note that false is returned
 * if given user is NULL or 'public'.
 */
CREATE FUNCTION pgfdw_plus_set_current_user(usename name) RETURNS boolean AS $$
DECLARE
  errmsg text;
BEGIN
  /* Quick exit and return false if given user is NULL or 'public' */
  IF usename IS NULL OR usename = 'public' THEN RETURN false; END IF;

  /* Quick exit and return true if given user is the same as current user */
  IF usename = current_user THEN RETURN true; END IF;

  /*
   * Return false if current user has neither direct nor indirect membership
   * in given user (that is, doesn't have the right to do SET ROLE).
   */
  IF NOT pg_has_role(usename, 'MEMBER') THEN RETURN false; END IF;

  /*
   * Report NOTICE message and return false if SET ROLE fails even when
   * current user is allowed to be set to given user.
   */
  BEGIN
    EXECUTE 'SET ROLE ' || usename;
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
    RAISE NOTICE 'could not set current user to user "%"',
      usename USING DETAIL = 'Error message: ' || errmsg;
    RETURN false;
  END;

  RETURN true;
END;
$$ LANGUAGE plpgsql;


/*
 * Find out user that can use public mapping to given server.
 *
 * Return name of such user if found, NULL otherwise.
 */
CREATE FUNCTION pgfdw_plus_user_for_public_mapping(server name)
  RETURNS name AS $$
DECLARE
  target_user name := NULL;
BEGIN
  /*
   * Find user 1) who has right to use given server,
   * 2) whom current user has membership in, and
   * 3) who has no user mapping for itself to given server.
   *
   * Use pg_roles intead of pg_user because even role without login
   * privilege can use user mappings and dblink to foreign servers.
   *
   * Note that superuser is chosen preferentially if both superuser
   * and non-superuser are picked up as user who can use public mapping.
   * Because this case happens only when superuser calls this function
   * and which means that it basically wants to use superuser for
   * processing as much as possible.
   */
  SELECT r.rolname INTO target_user FROM pg_roles r WHERE
    has_server_privilege(r.rolname, server, 'USAGE') AND
    pg_has_role(r.rolname, 'MEMBER') AND
    r.rolname NOT IN
      (SELECT um.usename FROM pg_user_mappings um
        WHERE um.usename <> 'public' AND um.srvname = server) AND
    r.rolname NOT LIKE 'pg\_%'
    ORDER BY r.rolsuper DESC, r.rolname LIMIT 1;
  RETURN target_user;
END;
$$ LANGUAGE plpgsql;

/*
 * Resolve foreign prepared transactions on all servers.
 */
CREATE FUNCTION
  pgfdw_plus_resolve_foreign_prepared_xacts_all (force boolean DEFAULT false)
  RETURNS SETOF type_resolve_foreign_prepared_xacts AS $$
DECLARE
  r RECORD;
  orig_user name := current_user;
  target_user name;
  errmsg text;
BEGIN
  FOR r IN
    SELECT * FROM pg_foreign_data_wrapper fdw,
      pg_foreign_server fs, pg_user_mappings um
    WHERE fdw.fdwname = 'postgres_fdw' AND
      fs.srvfdw = fdw.oid AND fs.oid = um.srvid
    ORDER BY fs.srvname
  LOOP
    /*
     * If target user is 'public', find out user who can use this public
     * mapping and reset target user to it. If no such user is found,
     * target user is reset to NULL and subsequent pgfdw_plus_set_current_user()
     * returns false.
     */
    target_user := r.usename;
    IF target_user = 'public' THEN
      target_user := pgfdw_plus_user_for_public_mapping(r.srvname);
    END IF;

    /*
     * Change current user in order to connect to foreign server based on
     * user mapping. If fail, resolutions of foreign prepared transactions on
     * this server are skipped.
     */
    IF NOT pgfdw_plus_set_current_user(target_user) THEN
      RAISE NOTICE 'skipping server "%" with user mapping for "%"',
        r.srvname, r.usename;
      CONTINUE;
    END IF;

    /* Resolve foreign prepared transactions on one server */
    BEGIN
      RETURN QUERY SELECT *
        FROM pgfdw_plus_resolve_foreign_prepared_xacts(r.srvname, force);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
      RAISE NOTICE 'could not resolve foreign prepared transactions on server "%"',
        r.srvname USING DETAIL = 'Error message: ' || errmsg;
    END;

    /*
     * Back to orignal user. RESET ROLE should not be used for the case
     * where session user and current user are different.
     */
    EXECUTE 'SET ROLE ' || orig_user;
  END LOOP;

  /* Current user must be the same before and after executing this function */
  ASSERT orig_user = current_user;
END;
$$ LANGUAGE plpgsql;


/*
 * Get minimum value of full transaction IDs assigned to running
 * local transactions and foreign prepared (unresolved yet) ones.
 *
 * This function also returns a list of user mapping OIDs corresponding to
 * server that it successfully fetched minimum full transaction ID from.
 */
CREATE FUNCTION
  pgfdw_plus_min_fxid_foreign_prepared_xacts_all(OUT fxmin xid8, OUT umids oid[])
  RETURNS record AS $$
DECLARE
  r RECORD;
  fxid xid8;
  orig_user name := current_user;
  target_user name;
  errmsg text;
BEGIN
  fxmin := pg_snapshot_xmin(pg_current_snapshot());
  FOR r IN
    SELECT * FROM pg_foreign_data_wrapper fdw,
      pg_foreign_server fs, pg_user_mappings um
    WHERE fdw.fdwname = 'postgres_fdw' AND
      fs.srvfdw = fdw.oid AND fs.oid = um.srvid
    ORDER BY fs.srvname
  LOOP
    /*
     * If target user is 'public', find out user who can use this public
     * mapping and reset target user to it. If no such user is found,
     * target user is reset to NULL and subsequent pgfdw_plus_set_current_user()
     * returns false.
     */
    target_user := r.usename;
    IF target_user = 'public' THEN
      target_user := pgfdw_plus_user_for_public_mapping(r.srvname);
    END IF;

    /*
     * Change current user in order to connect to foreign server based on
     * user mapping. If fail, we skip getting min fxid of foreign prepared
     * transactions on this server.
     */
    IF NOT pgfdw_plus_set_current_user(target_user) THEN
      RAISE NOTICE 'skipping server "%" with user mapping for "%"',
        r.srvname, r.usename;
      CONTINUE;
    END IF;

    BEGIN
      /*
       * Use min(bigint)::text::xid8 to calculate minimum value of
       * full transaction IDs, for now, instead of min(xid8) because
       * PostgreSQL core hasn't supported yet min(xid8) aggregation function.
       */
      SELECT min(split_part(gid, '_', 2)::bigint)::text::xid8 INTO fxid
        FROM pgfdw_plus_foreign_prepared_xacts(r.srvname);
      IF fxid IS NOT NULL AND fxid < fxmin THEN
        fxmin := fxid;
      END IF;
      umids := array_append(umids, r.umid);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS errmsg = MESSAGE_TEXT;
      RAISE NOTICE 'could not retrieve minimum full transaction ID from server "%"',
        r.srvname USING DETAIL = 'Error message: ' || errmsg;
    END;

    /*
     * Back to orignal user. RESET ROLE should not be used for the case
     * where session user and current user are different.
     */
    EXECUTE 'SET ROLE ' || orig_user;
  END LOOP;

  /* Current user must be the same before and after executing this function */
  ASSERT orig_user = current_user;
END;
$$ LANGUAGE plpgsql;

/*
 * Delete records no longer necessary to resolve foreign prepared
 * transactions, from xact_commits. This function returns
 * deleted records.
 */
CREATE FUNCTION pgfdw_plus_vacuum_xact_commits()
  RETURNS SETOF pgfdw_plus.xact_commits AS $$
DECLARE
  r record;
BEGIN
  IF current_setting('transaction_read_only')::boolean THEN
    RAISE EXCEPTION 'cannot execute pgfdw_plus_vacuum_xact_commits() in a read-only transaction';
  END IF;
  SELECT * INTO r FROM pgfdw_plus_min_fxid_foreign_prepared_xacts_all();
  RETURN QUERY DELETE FROM pgfdw_plus.xact_commits xc
    WHERE xc.fxid < r.fxmin AND xc.umids <@ r.umids RETURNING *;
END;
$$ LANGUAGE plpgsql;

/*
 * The object definitions for connection check.
 */

/*
 * Check the health of the connection to given server.
 */
CREATE FUNCTION pgfdw_plus_verify_connection_states (text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

/*
 * Check the health of the connection to all servers.
 */
CREATE FUNCTION pgfdw_plus_verify_connection_states_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

/*
 * Return true if connection check is supported on this platform.
 */
CREATE FUNCTION pgfdw_plus_can_verify_connection_states ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL SAFE;
