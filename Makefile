# contrib/postgres_fdw_plus/Makefile

MODULE_big = postgres_fdw_plus
OBJS = \
	$(WIN32RES) \
	connection.o \
	postgres_fdw/deparse.o \
	option.o \
	postgres_fdw.o \
	postgres_fdw/shippable.o
PGFILEDESC = "postgres_fdw_plus - foreign data wrapper for PostgreSQL, supporting global transaction"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = postgres_fdw_plus
DATA = postgres_fdw_plus--1.0.sql

REGRESS = postgres_fdw postgres_fdw_plus
EXTRA_INSTALL = contrib/dblink

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/postgres_fdw_plus
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
