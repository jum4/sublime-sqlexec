##
# libsys.sql - SQL to support driver features
##
-- Queries for dealing with the PostgreSQL catalogs for supporting the driver.

[lookup_type::first]
SELECT
 ns.nspname as namespace,
 bt.typname,
 bt.typtype,
 bt.typlen,
 bt.typelem,
 bt.typrelid,
 ae.oid AS ae_typid,
 ae.typreceive::oid != 0 AS ae_hasbin_input,
 ae.typsend::oid != 0 AS ae_hasbin_output
FROM pg_catalog.pg_type bt
 LEFT JOIN pg_type ae
  ON (
   bt.typlen = -1 AND
   bt.typelem != 0 AND
   bt.typelem = ae.oid
  )
 LEFT JOIN pg_catalog.pg_namespace ns
  ON (ns.oid = bt.typnamespace)
WHERE bt.oid = $1

[lookup_composite]
-- Get the type Oid and name of the attributes in `attnum` order.
SELECT
 CAST(atttypid AS oid) AS atttypid,
 CAST(attname AS text) AS attname,
 tt.typtype = 'd'      AS is_domain
FROM
 pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_attribute a
  ON (t.typrelid = a.attrelid)
 LEFT JOIN pg_type tt ON (a.atttypid = tt.oid)
WHERE
 attrelid = $1 AND NOT attisdropped AND attnum > 0
ORDER BY attnum ASC

[lookup_basetype_recursive]
SELECT
  (CASE WHEN tt.typtype = 'd' THEN
       (WITH RECURSIVE typehierarchy(typid, depth) AS (
          SELECT
               t2.typbasetype,
               0
           FROM
               pg_type t2
           WHERE
               t2.oid = tt.oid
          UNION ALL
          SELECT
               t2.typbasetype,
               th.depth + 1
           FROM
               pg_type t2,
               typehierarchy th
           WHERE
               th.typid = t2.oid
               AND t2.typbasetype != 0
       ) SELECT typid FROM typehierarchy ORDER BY depth DESC LIMIT 1)

       ELSE NULL
 END)             AS basetypid
FROM
  pg_catalog.pg_type tt
WHERE
  tt.oid = $1

[lookup_basetype]
SELECT
  tt.typbasetype
FROM
  pg_catalog.pg_type tt
WHERE
  tt.oid = $1

[lookup_procedures]
SELECT
 pg_proc.oid,
 pg_proc.*,
 pg_proc.oid::regproc AS _proid,
 pg_proc.oid::regprocedure as procedure_id,
 COALESCE(string_to_array(trim(replace(textin(oidvectorout(proargtypes)), ',', ' '), '{}'), ' ')::oid[], '{}'::oid[])
  AS proargtypes,
 (pg_type.oid = 'record'::regtype or pg_type.typtype = 'c') AS composite
FROM
 pg_catalog.pg_proc LEFT JOIN pg_catalog.pg_type ON (
  pg_proc.prorettype = pg_type.oid
 )

[lookup_procedure_oid::first]
*[lookup_procedures]
 WHERE pg_proc.oid = $1

[lookup_procedure_rp::first]
*[lookup_procedures]
 WHERE pg_proc.oid = regprocedurein($1)

[lookup_prepared_xacts::first]
SELECT
	COALESCE(ARRAY(
		SELECT
			gid::text
		FROM
			pg_catalog.pg_prepared_xacts
		WHERE
			database = current_database()
			AND (
				owner = $1::text
				OR (
					(SELECT rolsuper FROM pg_roles WHERE rolname = $1::text)
				)
			)
		ORDER BY prepared ASC
	), ('{}'::text[]))

[regtypes::column]
SELECT pg_catalog.regtypein(pg_catalog.textout(($1::text[])[i]))::oid AS typoid
FROM pg_catalog.generate_series(1, array_upper($1::text[], 1)) AS g(i)

[xact_is_prepared::first]
SELECT TRUE FROM pg_catalog.pg_prepared_xacts WHERE gid::text = $1

[get_statement_source::first]
SELECT statement FROM pg_catalog.pg_prepared_statements WHERE name = $1

[setting_get]
SELECT setting FROM pg_catalog.pg_settings WHERE name = $1

[setting_set::first]
SELECT pg_catalog.set_config($1, $2, false)

[setting_len::first]
SELECT count(*) FROM pg_catalog.pg_settings

[setting_item]
SELECT name, setting FROM pg_catalog.pg_settings WHERE name = $1

[setting_mget]
SELECT name, setting FROM pg_catalog.pg_settings WHERE name = ANY ($1)

[setting_keys]
SELECT name FROM pg_catalog.pg_settings ORDER BY name

[setting_values]
SELECT setting FROM pg_catalog.pg_settings ORDER BY name

[setting_items]
SELECT name, setting FROM pg_catalog.pg_settings ORDER BY name

[setting_update]
SELECT
	($1::text[][])[i][1] AS key,
	pg_catalog.set_config(($1::text[][])[i][1], $1[i][2], false) AS value
FROM
	pg_catalog.generate_series(1, array_upper(($1::text[][]), 1)) g(i)

[startup_data:transient:first]
-- 8.2 and greater
SELECT
 pg_catalog.version()::text AS version,
 backend_start::text,
 client_addr::text,
 client_port::int
FROM pg_catalog.pg_stat_activity WHERE procpid = pg_catalog.pg_backend_pid()
UNION ALL SELECT
 pg_catalog.version()::text AS version,
 NULL::text AS backend_start,
 NULL::text AS client_addr,
 NULL::int AS client_port
LIMIT 1;

[startup_data_92:transient:first]
-- 9.2 and greater
SELECT
 pg_catalog.version()::text AS version,
 backend_start::text,
 client_addr::text,
 client_port::int
FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()
UNION ALL SELECT
 pg_catalog.version()::text AS version,
 NULL::text AS backend_start,
 NULL::text AS client_addr,
 NULL::int AS client_port
LIMIT 1;

[startup_data_no_start:transient:first]
-- 8.1 only, but is unused as often the backend's activity row is not
-- immediately present.
SELECT
 pg_catalog.version()::text AS version,
 NULL::text AS backend_start,
 client_addr::text,
 client_port::int
FROM pg_catalog.pg_stat_activity WHERE procpid = pg_catalog.pg_backend_pid();

[startup_data_only_version:transient:first]
-- In 8.0, there's nothing there.
SELECT
 pg_catalog.version()::text AS version,
 NULL::text AS backend_start,
 NULL::text AS client_addr,
 NULL::int AS client_port;

[terminate_backends:transient:column]
-- Terminate all except mine.
SELECT
	procpid, pg_catalog.pg_terminate_backend(procpid)
FROM
	pg_catalog.pg_stat_activity
WHERE
	procpid != pg_catalog.pg_backend_pid()

[terminate_backends_92:transient:column]
-- Terminate all except mine. 9.2 and later
SELECT
	pid, pg_catalog.pg_terminate_backend(pid)
FROM
	pg_catalog.pg_stat_activity
WHERE
	pid != pg_catalog.pg_backend_pid()

[cancel_backends:transient:column]
-- Cancel all except mine.
SELECT
	procpid, pg_catalog.pg_cancel_backend(procpid)
FROM
	pg_catalog.pg_stat_activity
WHERE
	procpid != pg_catalog.pg_backend_pid()

[cancel_backends_92:transient:column]
-- Cancel all except mine. 9.2 and later
SELECT
	pid, pg_catalog.pg_cancel_backend(pid)
FROM
	pg_catalog.pg_stat_activity
WHERE
	pid != pg_catalog.pg_backend_pid()

[sizeof_db:transient:first]
SELECT pg_catalog.pg_database_size(current_database())::bigint

[sizeof_cluster:transient:first]
SELECT SUM(pg_catalog.pg_database_size(datname))::bigint FROM pg_database

[sizeof_relation::first]
SELECT pg_catalog.pg_relation_size($1::text)::bigint

[pg_reload_conf:transient:]
SELECT pg_reload_conf()

[languages:transient:column]
SELECT lanname FROM pg_catalog.pg_language

[listening_channels:transient:column]
SELECT channel FROM pg_catalog.pg_listening_channels() AS x(channel)

[listening_relations:transient:column]
-- listening_relations: old version of listening_channels.
SELECT relname as channel FROM pg_catalog.pg_listener
WHERE listenerpid = pg_catalog.pg_backend_pid();

[notify::first]
-- 9.0 and greater
SELECT
	COUNT(pg_catalog.pg_notify(($1::text[])[i][1], $1[i][2]) IS NULL)
FROM
	pg_catalog.generate_series(1, array_upper($1, 1)) AS g(i)

[release_advisory_shared]
SELECT
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_advisory_unlock_shared(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_advisory_unlock_shared($2[i])
	END AS released
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)

[acquire_advisory_shared]
SELECT COUNT((
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_advisory_lock_shared(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_advisory_lock_shared($2[i])
	END
) IS NULL) AS acquired
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)

[try_advisory_shared]
SELECT
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_try_advisory_lock_shared(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_try_advisory_lock_shared($2[i])
	END AS acquired
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)

[release_advisory_exclusive]
SELECT
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_advisory_unlock(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_advisory_unlock($2[i])
	END AS released
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)

[acquire_advisory_exclusive]
SELECT COUNT((
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_advisory_lock(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_advisory_lock($2[i])
	END
) IS NULL) AS acquired -- Guaranteed to be acquired once complete.
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)

[try_advisory_exclusive]
SELECT
	CASE WHEN ($2::int8[])[i] IS NULL
	THEN
		pg_catalog.pg_try_advisory_lock(($1::int4[])[i][1], $1[i][2])
	ELSE
		pg_catalog.pg_try_advisory_lock($2[i])
	END AS acquired
FROM
	pg_catalog.generate_series(1, COALESCE(array_upper($2::int8[], 1), array_upper($1::int4[], 1))) AS g(i)
