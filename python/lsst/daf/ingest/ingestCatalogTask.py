#
# LSST Data Management System
#
# Copyright 2008-2015  AURA/LSST.
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

from contextlib import closing
import MySQLdb
import math
import re
import struct

import lsst.afw.table as afwTable
from lsst.daf.persistence import DbAuth
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

## \addtogroup LSST_task_documentation
## \{
## \page daf_ingest_IngestCatalogTask
## \ref IngestCatalogTask_ "IngestCatalogTask"
##      \copybrief IngestCatalogTask
## \}

class FieldFormatter(object):
    """A class for formatting fields in an afw BaseCatalog.

    This class is a container for a function that maps a Field to a MySQL type,
    and a function that maps a field value to a literal suitable for use in a
    MySQL INSERT/REPLACE statement.
    """

    def __init__(self, sqlTypeCallable, formatValueCallable):
        """Store the field formatting information.
        """
        self.sqlTypeCallable = sqlTypeCallable
        self.formatValueCallable = formatValueCallable

    def sqlType(self, field):
        """Return the SQL type for the given field's values.
        """
        return self.sqlTypeCallable(field)

    def formatValue(self, value):
        """Return a string representation of value suitable for use in an
        INSERT/REPLACE statement (not a CSV file).  Nones are always
        transformed to SQL NULLs.
        """
        if value is None:
            return "NULL"
        return self.formatValueCallable(value)


def _formatNumber(formatString, number):
    """Helper function for formatting floating point numbers. NaNs and
    infinities are converted to NULL, because MySQL does not seem to support
    NaN/Inf in FLOAT or DOUBLE columns.
    """
    if math.isnan(number) or math.isinf(number):
        return "NULL"
    return formatString.format(number)

def _formatString(string):
    """Helper function for formatting a string. Embedded backslashes and single
    quotes are backslash-escaped.
    """
    return "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'"

def _formatArray(formatChar, array):
    """Auxiliary function for formatting an array. The array elements are packed
    into a sequence of bytes, with bytes comprising individual elements arranged
    in little endian order. This sequence is then transformed into a MySQL
    hexadecimal literal and returned.
    """
    bytestring = struct.pack("<" + str(len(array)) + formatChar, *array)
    return "x'" + bytestring.encode('hex_codec') + "'"

def _sqlTypeForString(field):
    """Helper function for computing the SQL column type of a string valued field.
    """
    sz = field.getSize()
    if sz > 65535:
        # If necessary, longer strings could be ingested as TEXT.
        raise RuntimeError("String field is too large for ingestion")
    elif sz == 0:
        raise RuntimeError("String field has zero size")
    # A string containing trailing spaces cannot round-trip to a CHAR
    # column and back. Therefore, use VARCHAR. Also, assume strings are
    # ASCII for now.
    return "VARCHAR({}) CHARACTER SET ascii COLLATE ascii_bin NOT NULL".format(sz)

def _sqlTypeForArray(formatChar, field):
    """Helper function for computing the SQL column type of an array valued field.
    See https://docs.python.org/2/library/struct.html#format-characters for possible
    format characters. The formatChar passed in must correspond to the array element
    type.
    """
    sz = field.getSize()
    if sz == 0:
        return "BLOB NOT NULL"
    sz *= struct.calcsize("<" + formatChar)
    if sz > 65535:
        raise RuntimeError("Array field is too large for ingestion")
    return "BINARY({}) NOT NULL".format(sz)


"""A dictionary mapping afw field type strings to corresponding formatters.

This dictionary is used by IngestCatalogTask to determine how to format each
type of field in the input catalog.  If new field types are added to the afw
table library, they should also be added here.
"""
fieldFormatters = dict(
    U = FieldFormatter(lambda f: "SMALLINT UNSIGNED NOT NULL", lambda v: str(v)),
    I = FieldFormatter(lambda f: "INT NOT NULL", lambda v: str(v)),
    L = FieldFormatter(lambda f: "BIGINT NOT NULL", lambda v: str(v)),
    F = FieldFormatter(lambda f: "FLOAT", lambda v: _formatNumber("{:.9g}", v)),
    D = FieldFormatter(lambda f: "DOUBLE", lambda v: _formatNumber("{:.17g}", v)),
    Flag = FieldFormatter(lambda f: "BIT NOT NULL", lambda v: "1" if v else "0"),
    Angle = FieldFormatter(lambda f: "DOUBLE", lambda v: _formatNumber("{:.17g}", v.asDegrees())),
    String = FieldFormatter(_sqlTypeForString, _formatString),
    ArrayU = FieldFormatter(lambda f: _sqlTypeForArray("H", f), lambda v: _formatArray("H", v)),
    ArrayI = FieldFormatter(lambda f: _sqlTypeForArray("i", f), lambda v: _formatArray("i", v)),
    ArrayF = FieldFormatter(lambda f: _sqlTypeForArray("f", f), lambda v: _formatArray("f", v)),
    ArrayD = FieldFormatter(lambda f: _sqlTypeForArray("d", f), lambda v: _formatArray("d", v)),
)


def canonicalizeFieldName(fieldName):
    """Return a MySQL-compatible version of the schema field name, for now by
    changing any non-word characters to underscores.
    """
    return re.sub(r"[^\w]", "_", fieldName)

def quoteIdentifier(identifier):
    """Return a MySQL compatible version of the given identifier by quoting
    it with back-ticks (and escaping embedded back-ticks).
    """
    return "`" + identifier.replace("`", "``") + "`"

def aliasesFor(name, mappings):
    """Compute and return the set of possible aliases for the given field name.

    The afw table library processes a given name F by replacing the longest
    prefix of F that can be found in a schema's alias map with the corresponding
    alias target. Replacement is repeated until either no such prefix is found,
    or N replacements have been made (where N is the size of the alias map).

    The mappings argument must be a sorted list of substitutions, where a single
    substitution is a 2-tuple of strings (source, target).
    """
    # TODO: DM-3401 may revisit afw table aliases, and this code should be
    #       updated in accordance with any changes introduced there.
    n = 0
    aliases, names = set(), set()
    names.add(name)
    while n < len(mappings) and len(names) > 0:
        # Perform one round of reverse alias substitution
        n += 1
        newNames = set()
        for name in names:
            for i, (source, target) in enumerate(mappings):
                if name.startswith(target):
                    alias = source + name[len(target):]
                    # alias is only valid if source is its longest prefix in mappings.
                    # A prefix strictly longer than source must occur after it in the
                    # sorted list of mappings.
                    valid = True
                    for s, _ in mappings[i + 1:]:
                        if not s.startswith(source):
                            break;
                        if alias.startswith(s):
                            valid = False
                            break
                    if valid:
                        aliases.add(alias)
                        newNames.add(alias)
        names = newNames
    return aliases


class IngestCatalogConfig(pexConfig.Config):
    """Configuration for the IngestCatalogTask.
    """

    allowReplace = pexConfig.Field(
        "Allow replacement of existing rows with the same unique IDs",
        bool, default=False
    )

    maxQueryLen = pexConfig.Field(
        "Maximum length of a query string. None means use a non-standard, "
        "database-specific way to get the maximum.",
        int, optional=True, default=None
    )

    maxColumnLen = pexConfig.RangeField(
        "Maximum length of a database column name or alias. Fields "
        "that map to longer column names will not be ingested.",
        int, default=64, min=1, max=64, inclusiveMin=True, inclusiveMax=True
    )

    idFieldName = pexConfig.Field(
        "Name of the unique ID field",
        str, optional=True, default="id"
    )

    remap = pexConfig.DictField(
        "A mapping from afw table field names to desired SQL column names. "
        "The user must quote desired names containing special characters.",
        keytype=str, itemtype=str, optional=True, default={}
    )

    extraColumns = pexConfig.Field(
        "Extra column definitions, comma-separated, to put into the "
        "CREATE TABLE statement if the table is being created",
        str, optional=True, default=""
    )


class IngestCatalogRunner(pipeBase.TaskRunner):
    @staticmethod
    def getTargetList(parsedCmd):
        """Override the target list to add additional run() method parameters.
        """
        return pipeBase.TaskRunner.getTargetList(
            parsedCmd,
            dstype=parsedCmd.dstype,
            tableName=parsedCmd.tableName,
            viewName=parsedCmd.viewName,
            host=parsedCmd.host,
            db=parsedCmd.db,
            port=parsedCmd.port,
            user=parsedCmd.user
        )

    def precall(self, parsedCmd):
        """Override the precall to not write schemas, not require writing of
        configs, and set the task's name appropriately.
        """
        self.TaskClass._DefaultName += "_" + parsedCmd.dstype
        task = self.TaskClass(config=self.config, log=self.log)
        try:
            task.writeConfig(parsedCmd.butler, clobber=self.clobberConfig)
        except Exception, e:
            # Often no mapping for config, but in any case just skip
            task.log.warn("Could not persist config: %s" % (e,))
        return True


class IngestCatalogTask(pipeBase.CmdLineTask):
    """!
    \anchor IngestCatalogTask_
    \brief Ingest an afw catalog into a database table.

    \section daf_ingest_IngestCatalogTask_Contents Contents

    - \ref daf_ingest_IngestCatalogTask_Purpose
    - \ref daf_ingest_IngestCatalogTask_Config
    - \ref daf_ingest_IngestCatalogTask_Example

    \section daf_ingest_IngestCatalogTask_Purpose Description

    This task ingests an afw catalog (of any catalog subclass and with an
    arbitrary schema) into a database table.

    It contacts a MySQL server using connection information given through
    command line arguments or \ref IngestCatalogTask.run "run" parameters.
    It attempts to use a my.cnf file if present (by omitting a password from
    the connection parameters) and falls back to using credentials obtained via
    the DbAuth interface if not.

    If run from the command line, it will ingest each catalog specified by a
    data id and dataset type. As usual for tasks, multiple --id options may be
    specified, or ranges and lists of values can be specified for data id keys.

    There are also two methods (\ref IngestCatalogTask.ingest "ingest" and
    \ref IngestCatalogTask.runFile "runFile") that can be manually called to
    ingest catalogs, either by passing the catalog explicitly or by passing
    the name of a FITS file containing the catalog.  Both, like
    \ref IngestCatalogTask.run "run", require database connection information.

    The ingestion process creates the destination table in the database if it
    doesn't exist.  The schema is translated from the input catalog's schema,
    and may contain a (configurable) unique identifier field.  The only index
    provided is a unique one on this field.  (Additional ones can be created
    later, of course.)  Additionally, a database view that provides the field
    aliases of the input catalog's schema can be created.

    Rows are inserted into the database via INSERT statements.  As many rows
    as possible are packed into each INSERT to maximize throughput.  The limit
    on INSERT statement length is either set by configuration or determined by
    querying the database (in a MySQL-specific way).  This may not be as
    efficient in its use of the database as converting to CSV and doing a bulk
    load, but it eliminates the use of (often shared) disk resources.  The use
    of INSERTs (committed once at the end) may not be fully parallelizable
    (particularly if a unique id index exists), but tests seem to indicate that
    it is at least not much slower to execute many such INSERTs in parallel
    compared with executing them all sequentially.  This remains an area for
    future optimization.

    \section daf_ingest_IngestCatalogTask_Config Configuration Parameters

    The optional idFieldName configuration parameter identifies a unique
    identifier field in the source table. If it is specified and the
    field exists, a unique index is created for the corresponding column.

    Fields and field aliases with database names longer than the maxColumnLen
    configuration parameter are automatically dropped.

    If a \ref canonicalizeFieldName "canonicalized" afw table field name is
    problematic (say because it is too long, or because it matches a SQL
    keyword), then it can be changed by providing a more suitable name via the
    remap configuration parameter.

    Extra columns (e.g. ones to be filled in later by spatial indexing code)
    can be added to the database table via the extraColumns configuration
    parameter.

    \section daf_ingest_IngestCatalogTask_Example Example

    A sample invocation of IngestCatalogTask for ingesting some of the LSST
    DM stack demo output is:

    \code
        $DAF_INGEST_DIR/bin/ingestCatalog.py \
                $LSST_DM_STACK_DEMO_DIR/output \
                --host lsst-db.ncsa.illinois.edu \
                --database $USER_test \
                --table Source \
                --dstype src \
                --id filter=g
    \endcode
    """

    ConfigClass = IngestCatalogConfig
    _DefaultName = "ingestCatalog"
    RunnerClass = IngestCatalogRunner

    @classmethod
    def _makeArgumentParser(cls):
        """Extend the default argument parser with database-specific
        arguments and the dataset type for the catalogs to be read.
        """
        parser = pipeBase.ArgumentParser(name=cls._DefaultName)
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
            "--table", dest="tableName", required=True,
            help="Table to ingest into")
        parser.add_argument(
            "--view", dest="viewName",
            help="View to create containing column aliases")
        # Use DatasetArgument to require dataset type be specified on
        # the command line
        parser.add_id_argument(
            "--id", pipeBase.DatasetArgument("dstype"),
            help="Dataset data id to ingest")
        return parser

    def runFile(self, fileName, tableName, host, db, port=3306, user=None, viewName=None):
        """Ingest a BaseCatalog specified by a filename.
        """
        cat = afwTable.BaseCatalog.readFits(fileName)
        self.ingest(cat, tableName, host, db, port, user, viewName)

    def run(self, dataRef, dstype, tableName, host, db, port=3306, user=None, viewName=None):
        """Ingest a BaseCatalog specified by a dataref and dataset type.
        """
        self.ingest(dataRef.get(dstype), tableName, host, db, port, user, viewName)

    @pipeBase.timeMethod
    def ingest(self, cat, tableName, host, db, port=3306, user=None, viewName=None):
        """Ingest a BaseCatalog passed as an object.

        @param cat (BaseCatalog or derivative) Catalog to ingest.
        @param tableName (str)   Name of the database table to create.
        @param host (str)        Name of the database host machine.
        @param db (str)          Name of the database to ingest into.
        @param port (int)        Port number on the database host.
        @param user (str)        User name to use for the database.
        @param viewName (str)    Name of the database view to create.
        """
        tableName = quoteIdentifier(tableName)
        viewName = quoteIdentifier(viewName) if viewName else None
        with closing(self.connect(host, port, db, user)) as conn:
            # Determine the maximum query length (MySQL-specific) if not
            # configured.
            if self.config.maxQueryLen is None:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        """SELECT variable_value
                        FROM information_schema.session_variables
                        WHERE variable_name = 'max_allowed_packet'
                        """
                    )
                    maxQueryLen = int(cursor.fetchone()[0])
            else:
                maxQueryLen = self.config.maxQueryLen
            self.log.logdebug("maxQueryLen: {}".format(maxQueryLen))
            self._createTable(conn, tableName, cat.schema)
            if viewName is not None:
                self._createView(conn, tableName, viewName, cat.schema)
            self._ingest(conn, cat, tableName, maxQueryLen)

    @staticmethod
    def connect(host, port, db, user=None):
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

    def _executeSql(self, conn, sql):
        """Execute a SQL query with no expectation of a result.
        """
        self.log.logdebug(sql)
        conn.query(sql)

    def _schemaItems(self, schema):
        """A generator over ingestible schema items.
        """
        for item in schema:
            field = item.field
            if field.getTypeString() not in fieldFormatters:
                self.log.warn(
                    "Skipping field {}: type {} not supported".format(
                        field.getName(), field.getTypeString()))
            else:
                column = self._columnName(field.getName())
                if len(column) > self.config.maxColumnLen:
                    self.log.warn(
                        "Skipping field {}: column name {} too long".format(
                            field.getName(), column))
                else:
                    yield item

    def _ingest(self, conn, cat, tableName, maxQueryLen):
        """Ingest a BaseCatalog by converting it to one or more (large) INSERT or
        REPLACE statements, executing those statements, and committing the result.
        """
        sqlPrefix = "REPLACE" if self.config.allowReplace else "INSERT"
        sqlPrefix += " INTO {} (".format(tableName)
        keys = []
        columnNames = []
        for item in self._schemaItems(cat.schema):
            keys.append((item.key, fieldFormatters[item.field.getTypeString()]))
            columnNames.append(self._columnName(item.field.getName()))
        sqlPrefix += ",".join(columnNames)
        sqlPrefix += ") VALUES "
        pos = 0
        while pos < len(cat):
            sql = sqlPrefix
            initialPos = pos
            maxValueLen = maxQueryLen - len(sql)
            while pos < len(cat):
                row = cat[pos]
                value = "("
                value += ",".join([f.formatValue(row.get(k)) for (k, f) in keys])
                value += "),"
                maxValueLen -= len(value)
                if maxValueLen < 0:
                    break
                else:
                    sql += value
                    pos += 1
            if pos == initialPos:
                # Have not made progress
                raise RuntimeError("Single row is too large to insert")
            self._executeSql(conn, sql[:-1])
        conn.commit()

    def _columnName(self, fieldName):
        """Return the SQL column name for a given afw table field.
        """
        if fieldName in self.config.remap:
            return self.config.remap[fieldName]
        return canonicalizeFieldName(fieldName)

    def _columnDef(self, field):
        """Return the SQL column definition for a given afw table field.
        """
        sqlType = fieldFormatters[field.getTypeString()].sqlType(field)
        return self._columnName(field.getName()) + " " + sqlType

    def _createTable(self, conn, tableName, schema):
        """Create a table using column definitions based on the provided afw
        table schema, adding in any extra columns specified in the config.  If
        one exists, the unique id column is given a key.
        """
        fields = [item.field for item in self._schemaItems(schema)]
        names = [f.getName() for f in fields]
        equivalenceClasses = {}
        for name in names:
            equivalenceClasses.setdefault(name.lower(), []).append(name)
        clashes = ',\n'.join('\t{' + ', '.join(c) + '}'
                             for c in equivalenceClasses.itervalues() if len(c) > 1)
        if clashes:
            raise RuntimeError(
                "Schema contains columns that differ only by non-word characters "
                "and/or case:\n{}\nIn the database, these cannot be distinguished "
                "and hence result in column name duplicates. Use the remap "
                "configuration parameter to resolve this ambiguity.".format(clashes))
        sql = "CREATE TABLE IF NOT EXISTS {} (\n\t".format(tableName)
        sql += ",\n\t".join(self._columnDef(field) for field in fields)
        if self.config.extraColumns:
            sql += ",\n\t" + self.config.extraColumns
        if self.config.idFieldName:
            if self.config.idFieldName in names:
                sql += ",\n\tUNIQUE({})".format(self._columnName(self.config.idFieldName))
            else:
                self.log.warn(
                    "No field matches the configured unique ID field name "
                    "({})".format(self.config.idFieldName))
        sql += "\n)"
        self._executeSql(conn, sql)

    def _createView(self, conn, tableName, viewName, schema):
        """Create a view that allows table columns to be referred to by their
        afw table field aliases.
        """
        sql = ("CREATE OR REPLACE "
               "ALGORITHM = MERGE SQL SECURITY INVOKER "
               "VIEW {} AS SELECT\n\t").format(viewName)
        with closing(conn.cursor()) as cursor:
            cursor.execute("SHOW COLUMNS FROM " + tableName)
            columnNames = [row[0] for row in cursor.fetchall()]
        sql += ",\n\t".join(columnNames)
        # Technically, this isn't quite right. In afw, it appears to be legal
        # for an alias to shadow an actual field name. So for full rigor,
        # shadowed field names would have to be removed from the columnNames
        # list.
        #
        # For now, construct an invalid view and fail in this case.
        mappings = sorted((s, t) for (s, t) in schema.getAliasMap().iteritems())
        for item in self._schemaItems(schema):
            fieldName = item.field.getName()
            aliases = sorted(aliasesFor(fieldName, mappings))
            column = self._columnName(fieldName)
            for a in aliases:
                alias = self._columnName(a)
                if len(alias) > self.config.maxColumnLen:
                    self.log.warn("Skipping alias {} for {}: alias too long".format(alias, column))
                    continue
                sql += ",\n\t{} AS {}".format(column, alias)
        sql += "\nFROM "
        sql += tableName
        self._executeSql(conn, sql)
