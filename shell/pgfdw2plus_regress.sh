#!/bin/sh

cat postgres_fdw/sql/postgres_fdw.sql \
  | sed s/"EXTENSION postgres_fdw"/"EXTENSION postgres_fdw_plus"/g \
  | sed s/"extension postgres_fdw"/"extension postgres_fdw_plus"/g \
  | sed s/"extensions 'postgres_fdw'"/"extensions 'postgres_fdw_plus'"/g \
	> sql/postgres_fdw_plus.sql
