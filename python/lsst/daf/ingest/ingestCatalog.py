#
# LSST Data Management System
#
# Copyright 2008-2016  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
"""This module provides a |task| for LSST |afw catalog| ingestion.

:class:`.IngestCatalogTask` is able to ingest a catalog with an arbitrary
|schema| into a MySQL table. Also provided are associated |configuration| and
|runner| classes, as well as helpers for formatting fields and determining
field aliases.

.. |afw catalog|   replace::  :class:`afw catalog <lsst.afw.table.BaseCatalog>`
.. |afw table|     replace::  :mod:`afw table <lsst.afw.table>`
.. |alias map|     replace::  :class:`alias map <lsst.afw.table.AliasMap>`
.. |configuration| replace::  :class:`configuration <IngestCatalogConfig>`
.. |formatter|     replace::  :class:`formatter <.FieldFormatter>`
.. |run|           replace::  :meth:`~IngestCatalogTask.run`
.. |runner|        replace::  :class:`runner <IngestCatalogRunner>`
.. |schema|        replace::  :class:`schema <lsst.afw.table.Schema>`
.. |task|          replace::  :class:`~lsst.pipe.base.Task`
"""
from builtins import object
from contextlib import closing
import MySQLdb
import math
import re
import struct

import lsst.afw.table as afw_table
from lsst.daf.persistence import DbAuth
import lsst.pex.config as pex_config
import lsst.pipe.base as pipe_base


__all__ = (
    "FieldFormatter",
    "field_formatters",
    "canonicalize_field_name",
    "quote_mysql_identifier",
    "aliases_for",
    "IngestCatalogConfig",
    "IngestCatalogRunner",
    "IngestCatalogTask",
)


class FieldFormatter(object):
    """Formatter for fields in an |afw catalog|.

    This class is a container for a function that maps an |afw table| field to
    a MySQL type, and a function that maps a field value to a literal suitable
    for use in a MySQL ``INSERT`` or ``REPLACE`` statement.
    """

    def __init__(self, sql_type_callable, format_value_callable):
        """Store the field formatting information."""
        self.sql_type_callable = sql_type_callable
        self.format_value_callable = format_value_callable

    def sql_type(self, field):
        """Return the SQL type of values for `field`."""
        return self.sql_type_callable(field)

    def format_value(self, value):
        """Return a string representation of `value`.

        The return value will be suitable for use as a literal in an
        ``INSERT``/``REPLACE`` statement.  ``None`` values are always
        converted to ``"NULL"``.
        """
        if value is None:
            return "NULL"
        return self.format_value_callable(value)


def _format_number(format_string, number):
    """Format a number for use as a literal in a SQL statement.

    NaNs and infinities are converted to ``"NULL"``, because MySQL does not
    support storing such values in ``FLOAT`` or ``DOUBLE`` columns. Otherwise,
    `number` is formatted according to the given `format string`_.

    .. _format string:
        https://docs.python.org/library/string.html#format-string-syntax
    """
    if math.isnan(number) or math.isinf(number):
        return "NULL"
    return format_string.format(number)


def _format_string(string):
    """Format a string for use as a literal in a SQL statement.

    The input is quoted, and embedded backslashes and single quotes are
    backslash-escaped.
    """
    return "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _format_array(format_char, array):
    """Format an array for use as a literal in a SQL statement.

    The array elements are packed into a sequence of bytes, with bytes
    comprising individual elements arranged in little-endian order. This
    sequence is then transformed into a MySQL hexadecimal literal and returned.

    Parameters
    ----------

    format_char : str
        One of the `format characters`_ defined by the :mod:`struct` module.

    array : sequence
        A homogeneous sequence.

    .. _format characters:
        https://docs.python.org/library/struct.html#format-characters
    """
    byte_string = struct.pack("<" + str(len(array)) + format_char, *array)
    return "x'" + byte_string.encode("hex_codec") + "'"


def _sql_type_for_string(field):
    """Compute the SQL column type of a string valued field."""
    sz = field.getSize()
    if sz > 65535:
        # If necessary, longer strings could be ingested as TEXT.
        raise RuntimeError("String field is too large for ingestion")
    elif sz == 0:
        raise RuntimeError("String field has zero size")
    # A string containing trailing spaces cannot round-trip to a CHAR
    # column and back. Therefore, use VARCHAR. Also, assume strings are
    # ASCII for now.
    return ("VARCHAR({}) CHARACTER SET ascii COLLATE ascii_bin NOT NULL"
            .format(sz))


def _sql_type_for_array(format_char, field):
    """Compute the SQL column type of an array valued |afw table| field.

    Parameters
    ----------

    format_char : str
        One of the `format characters`_ defined by the :mod:`struct` module.

    field : field
        A descriptor for an array-valued field (e.g. a |Field_ArrayF|).

    .. _format characters:
        https://docs.python.org/library/struct.html#format-characters
    .. |Field_ArrayF|  replace::  :class:`~lsst.afw.table.Field_ArrayF`
    """
    sz = field.getSize()
    if sz == 0:
        return "BLOB NOT NULL"
    sz *= struct.calcsize("<" + format_char)
    if sz > 65535:
        raise RuntimeError("Array field is too large for ingestion")
    return "BINARY({}) NOT NULL".format(sz)


"""A mapping from |afw table| field type strings to field |formatter|s.

This mapping is used by :class:`.IngestCatalogTask` to determine how to format
|afw table| field values from the input catalog.  If new field types are added
to the |afw table| library, they should also be added here.
"""
field_formatters = dict(
    U=FieldFormatter(lambda f: "SMALLINT UNSIGNED NOT NULL",
                     lambda v: str(v)),
    I=FieldFormatter(lambda f: "INT NOT NULL",
                     lambda v: str(v)),
    L=FieldFormatter(lambda f: "BIGINT NOT NULL",
                     lambda v: str(v)),
    F=FieldFormatter(lambda f: "FLOAT",
                     lambda v: _format_number("{:.9g}", v)),
    D=FieldFormatter(lambda f: "DOUBLE",
                     lambda v: _format_number("{:.17g}", v)),
    Flag=FieldFormatter(lambda f: "BIT NOT NULL",
                        lambda v: "1" if v else "0"),
    Angle=FieldFormatter(lambda f: "DOUBLE",
                         lambda v: _format_number("{:.17g}", v.asDegrees())),
    String=FieldFormatter(_sql_type_for_string,
                          _format_string),
    ArrayU=FieldFormatter(lambda f: _sql_type_for_array("H", f),
                          lambda v: _format_array("H", v)),
    ArrayI=FieldFormatter(lambda f: _sql_type_for_array("i", f),
                          lambda v: _format_array("i", v)),
    ArrayF=FieldFormatter(lambda f: _sql_type_for_array("f", f),
                          lambda v: _format_array("f", v)),
    ArrayD=FieldFormatter(lambda f: _sql_type_for_array("d", f),
                          lambda v: _format_array("d", v)),
)


def canonicalize_field_name(field_name):
    """Return a MySQL-compatible version of the given field name.

    For now, the implementation simply changes all non-word characters to
    underscores.
    """
    return re.sub(r"[^\w]", "_", field_name)


def quote_mysql_identifier(identifier):
    """Return a MySQL compatible version of the given identifier.

    The given string is quoted with back-ticks, and any embedded back-ticks
    are doubled up.
    """
    return "`" + identifier.replace("`", "``") + "`"


def aliases_for(name, mappings):
    """Compute the set of possible aliases for the given field name.

    The |afw table| library processes a given field name F by replacing the
    longest prefix of F that can be found in a |schema|'s |alias map| with
    the corresponding target.  Replacement is repeated until either no such
    prefix is found, or N replacements have been made (where N is the size of
    the |alias map|).

    Parameters
    ----------

    name: str
        Field name to compute aliases for.

    mappings: sequence of (str, str)
        A sorted sequence of substitutions. Each substitution is a 2-tuple
        of strings (prefix, target).

    Returns
    -------

    set of str
        The set of aliases for `name`.
    """
    # TODO: DM-3401 may revisit afw table aliases, and this code should be
    #       updated in accordance with any changes introduced there.
    n = 0
    aliases, names = set(), set()
    names.add(name)
    while n < len(mappings) and len(names) > 0:
        # Perform one round of reverse alias substitution
        n += 1
        new_names = set()
        for name in names:
            for i, (source, target) in enumerate(mappings):
                if name.startswith(target):
                    alias = source + name[len(target):]
                    # alias is only valid if source is its longest prefix in
                    # mappings. A prefix strictly longer than source must occur
                    # after it in the sorted list of mappings.
                    valid = True
                    for s, _ in mappings[i + 1:]:
                        if not s.startswith(source):
                            break
                        if alias.startswith(s):
                            valid = False
                            break
                    if valid:
                        aliases.add(alias)
                        new_names.add(alias)
        names = new_names
    return aliases


class IngestCatalogConfig(pex_config.Config):
    """Configuration for :class:`~IngestCatalogTask`."""

    allow_replace = pex_config.Field(
        "Allow replacement of existing rows with the same unique IDs",
        bool, default=False
    )

    max_query_len = pex_config.Field(
        "Maximum length of a query string. None means use a non-standard, "
        "database-specific way to get the maximum.",
        int, optional=True, default=None
    )

    max_column_len = pex_config.RangeField(
        "Maximum length of a database column name or alias. Fields "
        "that map to longer column names will not be ingested.",
        int, default=64, min=1, max=64, inclusiveMin=True, inclusiveMax=True
    )

    id_field_name = pex_config.Field(
        "Name of the unique ID field",
        str, optional=True, default="id"
    )

    remap = pex_config.DictField(
        "A mapping from afw table field names to desired SQL column names. "
        "Column names containing special characters must be quoted.",
        keytype=str, itemtype=str, optional=True, default={}
    )

    extra_columns = pex_config.Field(
        "Extra column definitions, comma-separated, to put into the "
        "CREATE TABLE statement (if the table is being created)",
        str, optional=True, default=""
    )


class IngestCatalogRunner(pipe_base.TaskRunner):
    """Runner for :class:`~IngestCatalogTask`."""

    @staticmethod
    def getTargetList(parsed_cmd):
        """Add additional |run| method arguments by overloading |getTargetList|.

        .. |getTargetList| replace::
            :meth:`~lsst.pipe.base.TaskRunner.getTargetList`
        """
        return pipe_base.TaskRunner.getTargetList(
            parsed_cmd,
            dstype=parsed_cmd.dstype,
            table_name=parsed_cmd.table_name,
            view_name=parsed_cmd.view_name,
            host=parsed_cmd.host,
            db=parsed_cmd.db,
            port=parsed_cmd.port,
            user=parsed_cmd.user
        )

    def precall(self, parsed_cmd):
        """Prepare for task execution.

        This override of |precall|:

        - sets the task's name appropriately
        - does not write task schemata
        - attempts to write a task configuration (success is not required)

        .. |precall| replace:: :meth:`~lsst.pipe.base.TaskRunner.precall`
        """
        self.TaskClass._DefaultName += "_" + parsed_cmd.dstype
        task = self.TaskClass(config=self.config, log=self.log)
        try:
            task.writeConfig(parsed_cmd.butler, clobber=self.clobberConfig)
        except Exception as e:
            # Often no mapping for config, but in any case just skip
            task.log.warn("Could not persist config: %s" % (e,))
        return True


class IngestCatalogTask(pipe_base.CmdLineTask):
    r"""A |task| for ingesting an |afw catalog| into a MySQL table.

    Any |afw catalog| subclass (with an arbitrary schema) can be ingested
    into a MySQL database table.

    This task contacts a MySQL server using connection information given
    through command line or :meth:`.run` arguments.  It attempts to use a
    ``my.cnf`` file if present (by omitting a password from the connection
    parameters) and falls back to using credentials obtained via the |DbAuth|
    interface if not.

    If run from the command line, it will ingest each catalog specified by a
    data id and dataset type. As usual for tasks, multiple ``--id`` options may
    be specified, or ranges and lists of values can be specified for data id
    keys.

    There are also two methods (:meth:`.ingest` and :meth:`.run_file`) that can
    be manually called to ingest catalogs, either by passing the catalog
    explicitly or by passing the name of a FITS file containing the catalog.
    Both, like :meth:`.run`, require database connection information.

    The ingestion process creates the destination table in the database if it
    doesn't already exist.  The database schema is translated from the input
    catalog's |schema|, and may contain a (configurable) unique identifier
    field.  The only index provided is a unique one on this field.  (Additional
    ones can be created later, of course.)  Additionally, a database view that
    provides the field aliases of the input catalog's schema can be created.

    Rows are inserted into the database via ``INSERT`` statements.  As many
    rows as possible are packed into each ``INSERT`` to maximize throughput.
    The limit on ``INSERT`` statement length is either set by configuration or
    determined by querying the database (in a MySQL-specific way).  This may
    not be as efficient in its use of the database as converting to CSV and
    doing a bulk load, but it eliminates the use of (often shared) disk
    resources.  The use of ``INSERT`` (committed once at the end) may not be
    fully parallelizable (particularly if a unique id index exists), but tests
    seem to indicate that it is at least not much slower to execute many
    ``INSERT`` statements in parallel compared with executing them all
    sequentially. This remains an area for future optimization.

    The important |configuration| parameters are:

    |id_field_name|:
        A unique identifier field in the input catalog. If it is specified and
        the field exists, a unique index is created for the corresponding
        column.

    |max_column_len|:
        Fields and field aliases with database names longer than the value of
        this parameter are automatically dropped.

    |remap|:
        If a |canonicalized| field name is problematic (say because it is too
        long, or because it matches a SQL keyword), then it can be changed by
        providing a more suitable name via this parameter.

    |extra_columns|:
        Extra columns (e.g. ones to be filled in later by spatial indexing
        code) can be added to the database table via this parameter.

    Examples
    --------

    A sample task invocation that ingests some of the LSST DM stack demo output
    is:

    .. prompt:: bash

        $DAF_INGEST_DIR/bin/ingestCatalog.py \
                $LSST_DM_STACK_DEMO_DIR/output \
                --host lsst-db.ncsa.illinois.edu \
                --database $USER_test \
                --table Source \
                --dstype src \
                --id filter=g

    .. |canonicalized|  replace:: :func:`.canonicalize_field_name`
    .. |DbAuth|         replace:: :class:`~lsst.daf.persistence.DbAuth`
    .. |extra_columns|  replace:: :attr:`~.IngestCatalogConfig.extra_columns`
    .. |id_field_name|  replace:: :attr:`~.IngestCatalogConfig.id_field_name`
    .. |max_column_len| replace:: :attr:`~.IngestCatalogConfig.max_column_len`
    .. |remap|          replace:: :attr:`~.IngestCatalogConfig.remap`
    """

    ConfigClass = IngestCatalogConfig
    _DefaultName = "ingest_catalog"
    RunnerClass = IngestCatalogRunner

    @classmethod
    def _makeArgumentParser(cls):
        """Extend the default argument parser.

        Database-specific arguments and the dataset type of the catalogs to
        read are added in.
        """
        parser = pipe_base.ArgumentParser(name=cls._DefaultName)
        parser.add_argument(
            "--host", dest="host", required=True,
            help="Database hostname")
        parser.add_argument(
            "--database", dest="db", required=True,
            help="Database name")
        parser.add_argument(
            "--user", dest="user",
            help="Database username (optional)", default=None)
        parser.add_argument(
            "--port", dest="port", type=int,
            help="Database port number (optional)", default=3306)
        parser.add_argument(
            "--table", dest="table_name", required=True,
            help="Table to ingest into")
        parser.add_argument(
            "--view", dest="view_name",
            help="View to create containing column aliases")
        # Use DatasetArgument to require dataset type be specified on
        # the command line
        parser.add_id_argument(
            "--id", pipe_base.DatasetArgument("dstype"),
            help="Dataset data id to ingest")
        return parser

    def run_file(self, file_name, table_name, host, db,
                 port=3306, user=None, view_name=None):
        """Ingest an |afw catalog| specified by a filename."""
        cat = afw_table.BaseCatalog.readFits(file_name)
        self.ingest(cat, table_name, host, db, port, user, view_name)

    def run(self, data_ref, dstype, table_name, host, db,
            port=3306, user=None, view_name=None):
        """Ingest an |afw catalog| specified by a data ref and dataset type."""
        self.ingest(data_ref.get(dstype), table_name, host, db,
                    port, user, view_name)

    @pipe_base.timeMethod
    def ingest(self, cat, table_name, host, db,
               port=3306, user=None, view_name=None):
        """Ingest an |afw catalog| passed as an object.

        Parameters
        ----------

        cat : lsst.afw.table.BaseCatalog or subclass
            Catalog to ingest.

        table_name : str
            Name of the database table to create.

        host : str
            Name of the database host machine.

        db : str
            Name of the database to ingest into.

        port : int
            Port number on the database host.

        user : str
            User name to use when connecting to the database.

        view_name : str
            Name of the database view to create.
        """
        table_name = quote_mysql_identifier(table_name)
        view_name = quote_mysql_identifier(view_name) if view_name else None
        with closing(self.connect(host, port, db, user)) as conn:
            # Determine the maximum query length (MySQL-specific) if not
            # configured.
            if self.config.max_query_len is None:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        """SELECT variable_value
                        FROM information_schema.session_variables
                        WHERE variable_name = 'max_allowed_packet'
                        """
                    )
                    max_query_len = int(cursor.fetchone()[0])
            else:
                max_query_len = self.config.max_query_len
            self.log.debug("max_query_len: %d", max_query_len)
            self._create_table(conn, table_name, cat.schema)
            if view_name is not None:
                self._create_view(conn, table_name, view_name, cat.schema)
            self._ingest(conn, cat, table_name, max_query_len)

    @staticmethod
    def connect(host, port, db, user=None):
        """Connect to the specified MySQL database server."""
        kwargs = dict(host=host, port=port, db=db)
        if user is not None:
            kwargs["user"] = user
        try:
            # See if we can connect without a password (e.g. via my.cnf)
            return MySQLdb.connect(**kwargs)
        except:
            # Fallback to DbAuth
            kwargs["user"] = DbAuth.username(host, str(port))
            kwargs["passwd"] = DbAuth.password(host, str(port))
            return MySQLdb.connect(**kwargs)

    def _execute_sql(self, conn, sql):
        """Execute a SQL query with no expectation of a result."""
        self.log.debug(sql)
        conn.query(sql)

    def _schema_items(self, schema):
        """Yield ingestible schema items."""
        for item in schema:
            field = item.field
            if field.getTypeString() not in field_formatters:
                self.log.warn("Skipping field %s: type %s not supported",
                              field.getName(), field.getTypeString())
            else:
                column = self._column_name(field.getName())
                if len(column) > self.config.max_column_len:
                    self.log.warn("Skipping field %s: column name %d too long",
                                  field.getName(), column)
                else:
                    yield item

    def _ingest(self, conn, cat, table_name, max_query_len):
        """Ingest an afw catalog.

        This is accomplished by converting it to one or more (large) INSERT or
        REPLACE statements, executing those statements, and committing the
        result.
        """
        sql_prefix = "REPLACE" if self.config.allow_replace else "INSERT"
        sql_prefix += " INTO {} (".format(table_name)
        keys = []
        column_names = []
        for item in self._schema_items(cat.schema):
            keys.append((item.key, field_formatters[item.field.getTypeString()]))
            column_names.append(self._column_name(item.field.getName()))
        sql_prefix += ",".join(column_names)
        sql_prefix += ") VALUES "
        pos = 0
        while pos < len(cat):
            sql = sql_prefix
            initial_pos = pos
            max_value_len = max_query_len - len(sql)
            while pos < len(cat):
                row = cat[pos]
                value = "("
                value += ",".join([f.format_value(row.get(k)) for (k, f) in keys])
                value += "),"
                max_value_len -= len(value)
                if max_value_len < 0:
                    break
                else:
                    sql += value
                    pos += 1
            if pos == initial_pos:
                # Have not made progress
                raise RuntimeError("Single row is too large to insert")
            self._execute_sql(conn, sql[:-1])
        conn.commit()

    def _column_name(self, field_name):
        """Return the SQL column name for the given afw table field."""
        if field_name in self.config.remap:
            return self.config.remap[field_name]
        return canonicalize_field_name(field_name)

    def _column_def(self, field):
        """Return the SQL column definition for the given afw table field."""
        sql_type = field_formatters[field.getTypeString()].sql_type(field)
        return self._column_name(field.getName()) + " " + sql_type

    def _create_table(self, conn, table_name, schema):
        """Create a table corresponding to the given afw table schema.

        Any extra columns specified in the task config are added in. If a
        unique id column exists, it is given a key.
        """
        fields = [item.field for item in self._schema_items(schema)]
        names = [f.getName() for f in fields]
        equivalence_classes = {}
        for name in names:
            equivalence_classes.setdefault(name.lower(), []).append(name)
        clashes = ',\n'.join('\t{' + ', '.join(c) + '}'
                             for c in equivalence_classes.values() if len(c) > 1)
        if clashes:
            raise RuntimeError(
                "Schema contains columns that differ only by non-word "
                "characters and/or case:\n{}\nIn the database, these cannot "
                "be distinguished and hence result in column name duplicates. "
                "Use the remap configuration parameter to resolve this "
                "ambiguity.".format(clashes)
            )
        sql = "CREATE TABLE IF NOT EXISTS {} (\n\t".format(table_name)
        sql += ",\n\t".join(self._column_def(field) for field in fields)
        if self.config.extra_columns:
            sql += ",\n\t" + self.config.extra_columns
        if self.config.id_field_name:
            if self.config.id_field_name in names:
                sql += ",\n\tUNIQUE({})".format(
                    self._column_name(self.config.id_field_name))
            else:
                self.log.warn(
                    "No field matches the configured unique ID field name "
                    "(%s)", self.config.id_field_name)
        sql += "\n)"
        self._execute_sql(conn, sql)

    def _create_view(self, conn, table_name, view_name, schema):
        """Create a view allowing columns to be referred to by their aliases."""
        sql = ("CREATE OR REPLACE "
               "ALGORITHM = MERGE SQL SECURITY INVOKER "
               "VIEW {} AS SELECT\n\t").format(view_name)
        with closing(conn.cursor()) as cursor:
            cursor.execute("SHOW COLUMNS FROM " + table_name)
            column_names = [row[0] for row in cursor.fetchall()]
        sql += ",\n\t".join(column_names)
        # Technically, this isn't quite right. In afw, it appears to be legal
        # for an alias to shadow an actual field name. So for full rigor,
        # shadowed field names would have to be removed from the column_names
        # list.
        #
        # For now, construct an invalid view and fail in this case.
        mappings = sorted((s, t) for (s, t) in schema.getAliasMap().items())
        for item in self._schema_items(schema):
            field_name = item.field.getName()
            aliases = sorted(aliases_for(field_name, mappings))
            column = self._column_name(field_name)
            for a in aliases:
                alias = self._column_name(a)
                if len(alias) > self.config.max_column_len:
                    self.log.warn("Skipping alias %s for %d: "
                                  "alias too long", alias, column)
                    continue
                sql += ",\n\t{} AS {}".format(column, alias)
        sql += "\nFROM "
        sql += table_name
        self._execute_sql(conn, sql)
