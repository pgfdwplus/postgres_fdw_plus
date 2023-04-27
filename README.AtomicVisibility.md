# Atomic Visibility

Atomic Visibility is a characteristic of distributed transactions that
guarantees that a global transaction can see the consistent results
of another global transaction across different PostgreSQL servers.
With Atomic Visibility, a transaction can only see the committed or
uncommitted status of another transaction on all participating servers.
It ensures that a transaction cannot observe different states of
another transaction on different servers, which could cause
inconsistencies.

## How to enable Atomic Visibility

To enable Atomic Visibility, follow these steps:

1. Apply the patch (patch/0001-Add-GUC-parameter-for-Atomic-Visibility-feature.patch) that postgres_fdw_plus provides to PostgreSQL, and build the patched PostgreSQL. For example,

   ```
   $ git clone -b master --depth 1 https://github.com/postgres/postgres.git
   $ cd postgres
   $ git am 0001-Add-GUC-parameter-for-Atomic-Visibility-feature.patch
   $ ./configure --prefix=/opt/pgsql
   $ make
   $ make install
   ```

2. Create the database cluster, append the setting to enable Atomic Visibility (```atomic_visibility = on```) to the configuration file, and start the PostgreSQL server. For example,

   ```
   $ cd /opt/pgsql
   $ initdb -D $PGDATA --locale=C --encoding=UTF8
   $ echo "atomic_visibility = on" >> $PGDATA/postgresql.conf
   $ pg_ctl -D $PGDATA start
   ```

3. Build postgres_fdw_plus with the patched version of PostgreSQL. Please refer to the README.md file for instructions on how to compile and install postgres_fdw_plus.

4. Connect to the PostgreSQL server and run the command ```CREATE EXTENSION postgres_fdw_plus``` to register the extension. For example,

   ```
   $ psql
   =# CREATE EXTENSION postgres_fdw_plus;
   ```

5. Enable both postgres_fdw.two_phase_commit and postgres_fdw.use_read_committed, and reload the configuration file. For example,

   ```
   =# ALTER SYSTEM SET postgres_fdw.two_phase_commit TO on;
   =# ALTER SYSTEM SET postgres_fdw.use_read_committed TO on;
   =# SELECT pg_reload_conf();
   ```
