import MySQLdb
import math
import re

import lsst.afw.table as afwTable
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

class ColumnFormatter(object):
    """A class to format a column in an afw.SourceCatalog.

    A little tricky because a column's values may be composite entities
    (coordinates, matrixes, etc.).

    This class is basically a container for a SQL type, a function returning
    SQL column names, and a function returning the formatted value of a
    column."""

    def __init__(self, sqlType, columnNameCallable, formatValueCallable):
        """Store the column formatting information."""
        self.sqlType = sqlType
        self.columnNameCallable = columnNameCallable
        self.formatValueCallable = formatValueCallable

    def getSqlType(self):
        """Return the SQL type (e.g. BIGINT, DOUBLE) for the column's basic
        values."""
        return self.sqlType

    def getColumnNames(self, baseName):
        """Return an iterable of the names that should be used for columns in
        SQL given a SQL-compatible base name derived from the catalog's column
        name."""
        return self.columnNameCallable(baseName)

    def formatValue(self, value):
        """Return a string suitable for inclusion in an INSERT/REPLACE
        statement (not a CSV file) resulting from formatting the column's
        value.  One value should be provided for each of the column names that
        had been returned, of course.  Values should be separated by commas.
        This method also handles changing "None" values to SQL NULLs."""
        if value is None:
            return "NULL"
        return self.formatValueCallable(value)

        
def _formatNumber(fmt, number):
    """Auxiliary function for formatting a number, handling conversion of NaN
    and infinities to NULL."""
    if math.isnan(number) or math.isinf(number):
        return "NULL"
    return fmt % (number,)

def _formatList(fmt, list):
    """Auxiliary function for formatting a list of numbers using a common
    format, joining the results with commas."""
    return ", ".join([_formatNumber(fmt, x) for x in list])

"""Describe how to handle each of the column types. Array and Cov (plain)
types are not yet processed."""
columnFormatters = dict(
        Flag = ColumnFormatter("BIT", lambda x: (x,),
            lambda v: "1" if v else "0"),
        I = ColumnFormatter("INT", lambda x: (x,),
            lambda v: str(v)),
        L = ColumnFormatter("BIGINT", lambda x: (x,),
            lambda v: str(v)),
        F = ColumnFormatter("FLOAT", lambda x: (x,),
            lambda v: _formatNumber("%.9g", v)),
        D = ColumnFormatter("DOUBLE", lambda x: (x,),
            lambda v: _formatNumber("%.17g", v)),
        Angle = ColumnFormatter("DOUBLE", lambda x: (x,),
            lambda v: _formatNumber("%.17g", v.asDegrees())),
        Coord = ColumnFormatter("DOUBLE", lambda x: (x + "_ra", x + "_dec"),
            lambda v: _formatList("%.17g",
                (v.getRa().asDegrees(), v.getDec().asDegrees()))),
        PointI = ColumnFormatter("INT", lambda x: (x + "_x", x + "_y"),
            lambda v: _formatList("%d", (v[0], v[1]))),
        PointF = ColumnFormatter("FLOAT", lambda x: (x + "_x", x + "_y"),
            lambda v: _formatList("%.9g", (v[0], v[1]))),
        PointD = ColumnFormatter("DOUBLE", lambda x: (x + "_x", x + "_y"),
            lambda v: _formatList("%.17g", (v[0], v[1]))),
        MomentsF = ColumnFormatter("FLOAT",
            lambda x: (x + "_xx", x + "_xy", x + "_yy"),
            lambda v: _formatList("%.9g", 
                (v.getIxx(), v.getIxy(), v.getIyy()))),
        MomentsD = ColumnFormatter("DOUBLE",
            lambda x: (x + "_xx", x + "_xy", x + "_yy"),
            lambda v: _formatList("%.17g", 
                (v.getIxx(), v.getIxy(), v.getIyy()))),
        CovPointF = ColumnFormatter("FLOAT",
            lambda x: (x + "_xx", x + "_xy", x + "_yy"),
            lambda v: _formatList("%.9g", (v[0, 0], v[0, 1], v[1, 1]))),
        CovPointD = ColumnFormatter("DOUBLE",
            lambda x: (x + "_xx", x + "_xy", x + "_yy"),
            lambda v: _formatList("%.17g", (v[0, 0], v[0, 1], v[1, 1]))),
        CovMomentsF = ColumnFormatter("FLOAT",
            lambda x: (x + "_xx_xx", x + "_xx_xy", x + "_xx_yy",
                x + "_xy_xy", x + "_xy_yy", x + "_yy_yy"),
            lambda v: _formatList("%.9g",
                (v[0, 0], v[0, 1], v[0, 2], v[1, 1], v[1, 2], v[2, 2]))),
        CovMomentsD = ColumnFormatter("DOUBLE",
            lambda x: (x + "_xx_xx", x + "_xx_xy", x + "_xx_yy",
                x + "_xy_xy", x + "_xy_yy", x + "_yy_yy"),
            lambda v: _formatList("%.17g",
                (v[0, 0], v[0, 1], v[0, 2], v[1, 1], v[1, 2], v[2, 2])))
        )

class IngestSourcesConfig(pexConfig.Config):
    """Configuration for the IngestSourcesTask."""
    allowReplace = pexConfig.Field(
            "Allow replacement of existing rows with the same source IDs",
            bool, default=False)
    idColumnName = pexConfig.Field(
            "Name of unique identifier column",
            str, default="id")

class IngestSourcesTask(pipeBase.CmdLineTask):
    """Task to ingest a SourceCatalog of arbitrary schema into a database
    table."""

    ConfigClass = IngestSourcesConfig
    _DefaultName = "ingestSources"

    @classmethod
    def _makeArgumentParser(cls):
        """Extend the default argument parser with database-specific
        arguments and the dataset type for the Sources to be read.""" 
        parser = pipeBase.ArgumentParser(name=cls._DefaultName)
        parser.add_argument("-H", "--host", dest="host", required=True,
                help="Database hostname")
        parser.add_argument("-D", "--database", dest="db", required=True,
                help="Database name")
        parser.add_argument("-U", "--user", dest="user",
                help="Database username (optional)", default=None)
        parser.add_argument("-P", "--port", dest="port",
                help="Database port number (optional)", default=3306)
        parser.add_argument("-t", "--table", dest="tableName", required=True,
                help="Table to ingest into")
        parser.add_argument("-d", "--dataset-type", dest="datasetType",
                required=True,
                help="Dataset type of Sources to ingest")
        return parser

    @classmethod
    def runParsedCmd(cls, parsedCmd):
        """Override the default method for running the parsed command.
        Necessary because the task needs to be instantiated with more than
        just the data id and because we want to connect to the database only
        once for all data ids specified.  Prevents the use of
        multiprocessing.  Note that the config and metadata are written using
        the first data id given, not each of them."""

        task = cls(tableName=parsedCmd.tableName,
                datasetType=parsedCmd.datasetType,
                host=parsedCmd.host, db=parsedCmd.db,
                port=parsedCmd.port, user=parsedCmd.user)
        if len(parsedCmd.dataRefList) == 0:
            return
        task.writeConfig(parsedCmd.dataRefList[0])
        for dataRef in parsedCmd.dataRefList:
            if parsedCmd.doraise:
                task.run(dataRef)
            else:
                try:
                    task.run(dataRef)
                except Exception, e:
                    self.log.log(self.log.FATAL, "Failed on dataId=%s: %s" %
                            (dataRef.dataId, e))
                    if not isinstance(e, TaskError):
                        traceback.print_exc(file=sys.stderr)
        task.writeMetadata(parsedCmd.dataRefList[0])

    def __init__(self, tableName, datasetType, host, db,
            port=3306, user=None, **kwargs):
        super(IngestSourcesTask, self).__init__(**kwargs)
        try:
            self.db = MySQLdb.connect(host=host, port=port, user=user, db=db)
        except:
            user = dafPersist.DbAuth.username(host, str(port))
            passwd = dafPersist.DbAuth.password(host, str(port))
            self.db = MySQLdb.connect(host=host, port=port,
                    user=user, passwd=passwd, db=db)
        self.tableName = tableName
        self.datasetType = datasetType

    def _executeSql(self, sql):
        self.log.info("executeSql: " + sql)
        self.db.query(sql)

    def _getSqlScalar(self, sql):
        cur = self.db.cursor()
        self.log.info("getSqlScalar: " + sql)
        rows = cur.execute(sql)
        if rows != 1:
            raise RuntimeError(
                    "Wrong number of rows (%d) for scalar query: %s" %
                    (rows, sql))
        row = cur.fetchone()
        self.log.info("Result: " + str(row))
        return row[0]

    @pipeBase.timeMethod
    def run(self, dataRef):
        cat = dataRef.get(self.datasetType)
        self.runCatalog(cat)

    def runFile(self, fileName):
        cat = afwTable.SourceCatalog.readFits(fileName)
        self.runCatalog(cat)

    def runCatalog(self, cat):
        tableName = self.db.escape_string(self.tableName)
        self._checkTable(tableName, cat)
        if self.config.allowReplace:
            sql = "REPLACE"
        else:
            sql = "INSERT"
        sql += " INTO `%s` (" % (tableName,)
        keys = []
        firstCol = True
        for col in cat.schema:
            formatter = columnFormatters[col.field.getTypeString()]
            keys.append((col.key, formatter))
            if firstCol:
                firstCol = False
            else:
                sql += ", "
            sql += self._columnDef(col, includeTypes=False)
        sql += ") VALUES "
        firstSource = True
        for source in cat:
            if firstSource:
                firstSource = False
                sql += "("
            else:
                sql += "), ("
            sql += ", ".join([formatter.formatValue(source.get(key))
                for (key, formatter) in keys])
        sql += ");"
        self._executeSql(sql)
        self.db.commit()

    def _checkTable(self, tableName, cat):
        sampleId = cat[0][self.config.idColumnName]
        count = 0
        try:
            count = self._getSqlScalar(
                    "SELECT COUNT(*) FROM `%s` WHERE %s = %d;" % (
                        tableName, self.config.idColumnName, sampleId))
        except RuntimeError, e:
            raise e
        except:
            pass
        if count == 0:
            self._createTable(tableName, cat.schema)
        elif self.config.allowReplace:
            self.log.warn("Overwriting existing rows")
        else:
            raise RuntimeError("Row exists: {name}={id}".format(
                name=self.config.idColumnName, id=sampleId))

    def _createTable(self, tableName, schema):
        sql = "CREATE TABLE IF NOT EXISTS `%s` (" % (tableName,)
        sql += ", ".join([self._columnDef(col) for col in schema])
        sql += ", UNIQUE(%s)" % (self.config.idColumnName,)
        sql += ");"
        self._executeSql(sql)

    def _columnDef(self, col, includeTypes=True):
        formatter = columnFormatters[col.field.getTypeString()]
        baseName = self._canonicalizeName(col.field.getName())
        columnType = " " + formatter.getSqlType() if includeTypes else ""
        return ", ".join(["%s%s" % (columnName, columnType)
            for columnName in formatter.getColumnNames(baseName)])

    def _canonicalizeName(self, colName):
        return re.sub(r'\.', '_', colName)
