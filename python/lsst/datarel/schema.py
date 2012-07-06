# 
# LSST Data Management System
# Copyright 2012 LSST Corporation.
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
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import re

import lsst.pex.config
import lsst.meas.algorithms
import lsst.ap.cluster
import lsst.ap.utils

__all__ = ['makeMysqlCsvConfig',
           'DbMappingConfig',
           'genericTableSql',
           'sourceTableSql',
           'objectTableSql',
           'coaddSourceTableSql',
          ]

_dbType = {
    'I': 'INTEGER',
    'L': 'BIGINT',
    'F': 'FLOAT',
    'D': 'DOUBLE',
    'Angle': 'DOUBLE',
    'Flag': 'BIT(1)',
}

def _pk(cols):
    if not isinstance(cols, basestring):
        cols = ', '.join(colName)
    return str.format('PRIMARY KEY ({0})', cols)

def _k(cols):
    if not isinstance(cols, basestring):
        idx = '_'.join(cols)
        cols = ', '.join(cols)
    else:
        idx = cols
    return str.format('KEY IDX_{0} ({0})', idx, cols)


def makeMysqlCsvConfig():
    """Return the lsst.ap.utils.CsvConfig to use when writing out CSV files
    that must be loaded into MySQL.
    """
    cfg = lsst.ap.utils.CsvConfig()
    cfg.quoting = 'QUOTE_NONE' # C++ tables cannot contain strings (yet)
    cfg.delimiter = ','
    cfg.escapeChar = '\\'
    cfg.quoteChar = ''
    cfg.skipInitialSpace = False
    cfg.doubleQuote = False
    cfg.standardEscapes = True
    cfg.trailingDelimiter = False
    cfg.nonfiniteAsNull = True
    return cfg


class DbMappingConfig(lsst.pex.config.Config):
    sourceConversion = lsst.pex.config.ConfigField(
        dtype = lsst.ap.utils.CsvConversionConfig,
        doc = "C++ source table to CSV file conversion parameters.")

    objectConversion = lsst.pex.config.ConfigField(
        dtype = lsst.ap.utils.CsvConversionConfig,
        doc = "C++ source cluster table to CSV file conversion parameters.")

    asView = lsst.pex.config.Field(dtype=bool, default=True,
        doc="Create canonical Source/Object tables as VIEWs over the "
            "run-specific tables? False means they are materialized.")


def genericTableSql(schema, csvConversionConfig, indexedFields):
    """Return a pair of SQL template strings (createStmt, loadStmt).

    createStmt : a format string for the CREATE TABLE statement corresponding
                 to the given schema and desired indexes. To generate valid
                 SQL, tableName must be supplied, e.g.:

                 str.format(create, tableName='MyTable')

    loadStmt   : a format string for the corresponding LOAD DATA statement.
                 To generate valid SQL, tableName and fileName must be supplied,
                 e.g.:

                 str.format(load, tableName='MyTable', fileName='MyTable.csv')

    Note that the generated LOAD statement will never REPLACE data, and assumes
    that CSV files conform to the format returned by makeMysqlCsvConfig().

    @param schema               lsst.afw.table.Schema describing the C++ table
                                to map to a MySQL database table.
    @param csvConversionConfig  lsst.ap.utils.CsvConversionConfig describing
                                the C++ table to CSV file conversion options. 
    @param indexedFields        List or set of C++ field names to create indexes on
    """
    dbkeys = []
    coldefs = []
    columns = []
    setexprs = []

    def _append(dbcol, dbty, suffixes):
        for suffix in suffixes:
            coldefs.append(str.format('{}{} {} NULL', dbcol, suffix, dbty))
            columns.append(dbcol + suffix)

    for item in schema.asList():
        name = item.field.getName()
        dbcol = name.replace('.', '_')
        ty = item.key.getTypeString()
        if ty in _dbType:
            dbty = _dbType[ty]
            constraint = 'NULL'
            if name == 'id':
                dbkeys.append(_pk(dbcol))
                constraint = 'NOT NULL'
            elif ty == 'Flag':
                if not csvConversionConfig.flagsAsBits:
                    continue # we will deal with flags later
                constraint = 'NOT NULL'
            elif ty == 'I' or ty == 'L':
                if name in csvConversionConfig.nullableIntegers:
                    constraint = 'NULL'
                else:
                    constraint = 'NOT NULL'
            else:
                constraint = 'NULL'
            coldefs.append(' '.join([dbcol, dbty, constraint]))
            if ty == 'Flag':
                columns.append('@' + dbcol)
                setexprs.append(str.format('{0} = CAST(@{0} AS UNSIGNED)', dbcol))
            else: 
                columns.append(dbcol)
            if name != 'id' and name in indexedFields:
                dbkeys.append(_k(dbcol))
        elif ty == 'Coord':
            if name == "coord":
                # the Coord slot field
                coldefs.append('coord_ra DOUBLE NULL')
                coldefs.append('coord_decl DOUBLE NULL')
                coldefs.append('coord_htmId20 BIGINT NULL')
                columns.append('coord_ra')
                columns.append('coord_decl')
                setexprs.append('coord_htmId20 = scisql_s2HtmId(coord_ra, coord_decl, 20)')
                dbkeys.append(_k('coord_htmId20'))
                dbkeys.append(_k('coord_decl'))
            else:
                 _append(dbcol, 'DOUBLE', ['_ra', '_decl'])
        elif ty == 'ArrayF' or ty == 'ArrayD':
            dbty = _dbType[ty[-1]]
            for i in xrange(1, item.key.getSize() + 1):
                coldefs.append(str.format('{}_{} {} NULL', dbcol, i, dbty))
                columns.append(str.format('{}_{}', dbcol, i))
        elif ty == 'PointI' or ty == 'PointF' or ty == 'PointD':
            dbty = _dbType[ty[-1]]
            _append(dbcol, dbty, ['_x', '_y'])
        elif ty == 'MomentsF' or ty == 'MomentsD':
            dbty = _dbType[ty[-1]]
            _append(dbcol, dbty, ['_Ixx', '_Iyy', '_Ixy'])
        elif ty == 'CovF' or ty == 'CovD':
            dbty = _dbType[ty[-1]]
            sz = item.key.getSize()
            for i in xrange(1, sz + 1):
                for j in xrange(i, sz + 1):
                    coldefs.append(str.format('{}_{}_{} {} NULL', dbcol, i, j, dbty))
                    columns.append(str.format('{}_{}_{}', dbcol, i, j))
        elif ty == 'CovPointF' or ty == 'CovPointD':
            dbty = _dbType[ty[-1]]
            if name.endswith('.err'):
                dbcol = dbcol[:-4]
                if item.field.getUnits() == 'rad^2':
                    # HACK: this is a coordinate covariance matrix
                    _append(dbcol, dbty, ['_raVar', '_radeclCov',
                                                      '_declVar'])
                    continue
            _append(dbcol, dbty, ['_xVar', '_xyCov',
                                            '_yVar'])
        elif ty == 'CovMomentsF' or ty == 'CovMomentsD':
            dbty = _dbType[ty[-1]]
            if name.endswith('.err'):
                dbcol = dbcol[:-4]
            _append(dbcol, dbty, ['_IxxVar', '_IxxIyyCov', '_IxxIxyCov',
                                                '_IyyVar', '_IyyIxyCov',
                                                              '_IxyVar'])
        else:
            raise RuntimeError(ty + ' is not a recognized AFW field type string!')
    if not csvConversionConfig.flagsAsBits:
        # add BIGINT flag columns
        n = (schema.getFlagFieldCount() + 62) / 63
        for i in xrange(1, n + 1):
            coldefs.append(str.format('runFlags{} BIGINT NOT NULL', i))
            columns.append(str.format('runFlags{}', i))
        # add BIGINT flag columns for canonical flags in canonical order
        n = (len(csvConversionConfig.canonicalFlags) + 62) / 63
        for i in xrange(1, n + 1):
            coldefs.append(str.format('flags{} BIGINT NOT NULL', i))
            columns.append(str.format('flags{}', i))
    # Finally, create schema SQL and LOAD DATA templates
    createStmt = 'CREATE TABLE IF NOT EXISTS {tableName} (\n\t'
    createStmt += ',\n\t'.join(coldefs + dbkeys)
    createStmt += '\n) ENGINE=MyISAM;\n'

    loadStmt = ("LOAD DATA LOCAL INFILE '{fileName}'\n"
                "\tINTO TABLE {tableName}\n"
                "\tFIELDS TERMINATED BY ','\n"
                "(\n\t")
    loadStmt += ',\n\t'.join(columns)
    if len(setexprs) == 0:
        loadStmt += '\n);'
    else:
        loadStmt += '\n) SET\n\t'
        loadStmt += ',\n\t'.join(setexprs)
        loadStmt += ';'
    return createStmt, loadStmt


def _sourceIndexes(sourceProcessingConfig):
    """Return the list of C++ Source field names to create indexes on.

    @param sourceProcessingConfig     lsst.ap.cluster.SourceProcessingConfig;
                                      describes source processing performed by
                                      SourceAssoc.
    """
    indexes = set()
    indexes.add("parent")
    if sourceProcessingConfig.exposurePrefix:
       indexes.add(sourceProcessingConfig.exposurePrefix + ".id")
       if not sourceProcessingConfig.multiBand:
           indexes.add(sourceProcessingConfig.exposurePrefix + ".filter.id")
    if sourceProcessingConfig.clusterPrefix:
        indexes.add(sourceProcessingConfig.clusterPrefix + ".id")
    return indexes
 

# mappings from run-specific table column names to canonical Source columns
_sourceMappings = [
    ("id", "sourceId"), # from minimal schema, no prefix
    ("parent", "parentSourceId"), # from minimal schema, no prefix
    ("{exposurePrefix}_id", "scienceCcdExposureId"),
    ("{exposurePrefix}_filter_id", "filterId"),
    ("{clusterPrefix}_id", "objectId"),
    ("{clusterPrefix}_coord_ra", "objectRa"),
    ("{clusterPrefix}_coord_decl", "objectDecl"),
    ("coord_ra", "ra"), # from minimal schema, no prefix
    ("coord_decl", "decl"), # from minimal schema, no prefix
    ("coord_raVar", "raVar"), # from source association, no prefix
    ("coord_declVar", "declVar"), # from source association, no prefix
    ("coord_radeclCov", "radeclCov"), # from source association, no prefix
    ("coord_htmId20", "htmId20"), # from ingest, no prefix
    ("{measPrefix}{centroid}_x", "x"),
    ("{measPrefix}{centroid}_y", "y"),
    ("{measPrefix}{centroid}_xVar", "xVar"),
    ("{measPrefix}{centroid}_yVar", "yVar"),
    ("{measPrefix}{centroid}_xyCov", "xyCov"),
    ("{exposurePrefix}_time_mid", "timeMid"),
    ("{exposurePrefix}_time", "expTime"),
    ("{measPrefix}{psfFlux}", "psfFlux"),
    ("{measPrefix}{psfFlux}_err", "psfFluxSigma"),
    ("{measPrefix}{apFlux}", "apFlux"),
    ("{measPrefix}{apFlux}_err", "apFluxSigma"),
    ("{measPrefix}{modelFlux}", "modelFlux"),
    ("{measPrefix}{modelFlux}_err", "modelFluxSigma"),
    ("{measPrefix}{instFlux}", "instFlux"),
    ("{measPrefix}{instFlux}_err", "instFluxSigma"),
    ("aperturecorrection", "apCorrection"), # from measurement, no prefix (!?)
    ("aperturecorrection_err", "apCorrectionSigma"), # from measurement, no prefix (!?)
    ("{measPrefix}{shape}_centroid_x", "shapeIx"),
    ("{measPrefix}{shape}_centroid_y", "shapeIy"),
    ("{measPrefix}{shape}_centroid_xVar", "shapeIxVar"),
    ("{measPrefix}{shape}_centroid_yVar", "shapeIyVar"),
    ("{measPrefix}{shape}_centroid_xyCov", "shapeIxIyCov"),
    ("{measPrefix}{shape}_Ixx", "shapeIxx"),
    ("{measPrefix}{shape}_Iyy", "shapeIyy"),
    ("{measPrefix}{shape}_Ixy", "shapeIxy"),
    ("{measPrefix}{shape}_IxxVar", "shapeIxxVar"),
    ("{measPrefix}{shape}_IyyVar", "shapeIyyVar"),
    ("{measPrefix}{shape}_IxyVar", "shapeIxyVar"),
    ("{measPrefix}{shape}_IxxIyyCov", "shapeIxxIyyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIxxIxyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIyyIxyCov"),
    ("{measPrefix}classification_extendedness", "extendedness"),
    ("flags_negative", "flagNegative"), # from detection, no prefix
    ("{measPrefix}flags_badcentroid", "flagBadMeasCentroid"),
    ("{measPrefix}flags_pixel_edge", "flagPixEdge"),
    ("{measPrefix}flags_pixel_interpolated_any", "flagPixInterpAny"),
    ("{measPrefix}flags_pixel_interpolated_center", "flagPixInterpCen"),
    ("{measPrefix}flags_pixel_saturated_any", "flagPixSaturAny"),
    ("{measPrefix}flags_pixel_saturated_center", "flagPixSaturCen"),
    ("{measPrefix}{psfFlux}_flags", "flagBadPsfFlux"),
    ("{measPrefix}{apFlux}_flags", "flagBadApFlux"),
    ("{measPrefix}{modelFlux}_flags", "flagBadModelFlux"),
    ("{measPrefix}{instFlux}_flags", "flagBadInstFlux"),
    ("{measPrefix}{centroid}_flags", "flagBadCentroid"),
    ("{measPrefix}{shape}_flags", "flagBadShape"),
]

def _colToField(col):
    """Turn a database column name back into a C++ table field name"""
    i = col.rfind('_')
    if i == -1:
        return col
    field = col[:i].replace('_', '.')
    suffix = col[i+1:]
    if suffix in ['x', 'y', 'Ixx', 'Iyy', 'Ixy', 'ra', 'decl']:
        return field
    if suffix.endswith('Var') or suffix.endswith('Cov'):
       return field + '.err'
    # TODO: generic Cov<T> or Array<T> fields not supported yet,
    #       but so far no such fields are mapped to canonical
    #       Source/Object table columns.
    return col.replace('_', '.')
     
def _getMappingKw(slots, sourceProcessingConfig, measPrefix=None):
    """Return substitution parameters for mapping table entries.
    """
    kw = dict()
    kw['measPrefix'] = (measPrefix or '').replace('.', '_')
    kw['exposurePrefix'] = sourceProcessingConfig.exposurePrefix.replace('.', '_')
    kw['clusterPrefix'] = sourceProcessingConfig.clusterPrefix.replace('.', '_')
    kw['centroid'] = slots.centroid.replace('.', '_') if slots.centroid else '__X__'
    kw['shape'] = slots.shape.replace('.', '_') if slots.shape else '__X__'
    kw['psfFlux'] = slots.psfFlux.replace('.', '_') if slots.psfFlux else '__X__'
    kw['apFlux'] = slots.apFlux.replace('.', '_') if slots.apFlux else '__X__'
    kw['modelFlux'] = slots.modelFlux.replace('.', '_') if slots.modelFlux else '__X__'
    kw['instFlux'] = slots.instFlux.replace('.', '_') if slots.instFlux else '__X__'
    return kw

def sourceTableSql(schema, dbMappingConfig, sourceAssocConfig):
    """Return a tuple of SQL statements (createStmt, loadStmt, sourceStmt)
    for the Source table.

    createStmt :    CREATE TABLE statement for the RunSource table, which
                    includes all fields from the run-specific
                    lsst.afw.table.Schema for source tables output by the
                    pipelines.

    loadStmt :      LOAD DATA statement for the RunSource table. This is
                    a format string; to generate valid SQL a fileName must
                    be supplied, e.g.:

                    loadStmt.format(fileName='source.csv')

    sourceStmt :    Map the RunSource table to the canonical Source schema.
                    This will either create a VIEW, or INSERT into the
                    materialized equivalent.

    @param schema               lsst.afw.table.Schema for sources
    @param dbMappingConfig      lsst.datarel.DbMappingConfig
    @param sourceAssocConfig    lsst.ap.tasks.sourceAssoc.SourceAssocConfig
    """
    # Generate SQL for run specific table
    createStmt, loadStmt = genericTableSql(
        schema,
        dbMappingConfig.sourceConversion,
        _sourceIndexes(sourceAssocConfig.sourceProcessing))
    # build substitution parameters for mapping table
    kw = _getMappingKw(
        sourceAssocConfig.measSlots,
        sourceAssocConfig.sourceProcessing,
        sourceAssocConfig.measPrefix)
    # build selection/output column lists
    selcols = []
    outcols = []
    for runFmt, srcCol in _sourceMappings:
        runCol = runFmt.format(**kw)
        field = _colToField(runCol)
        isFlag = srcCol.startswith('flag')
        if isFlag and not dbMappingConfig.sourceConversion.flagsAsBits:
            continue
        if field in schema or runCol == 'coord_htmId20':
            selcols.append(runCol)
        elif isFlag:
            selcols.append("b'0'")
        else:
            selcols.append('NULL')
        outcols.append(srcCol)
    if not dbMappingConfig.sourceConversion.flagsAsBits:
        # Deal with canonical flags packed into BIGINTs
        n = (len(dbMappingConfig.sourceConversion.canonicalFlags) + 62) / 63
        if n == 1:
            selcols.append('flags')
            outcols.append('flags')
        else:
            for i in xrange(1, n + 1):
                c = 'flags{}'.format(i)
                selcols.append(c)
                outcols.append(c)
    if dbMappingConfig.asView:
        # Replace the official version of Source with an equivalent VIEW
        sourceStmt = 'CREATE OR REPLACE VIEW Source AS SELECT\n\t'
        sourceStmt += ',\n\t'.join(a + ' AS ' + b for a,b in zip(selcols, outcols))
        sourceStmt += '\nFROM RunSource;'
    else:
        # Use the definition of Source from cat (i.e. the one used by the
        # schema browser for documentation purposes). This should cause
        # ingest to fail if this code and the canonical schema are not in sync.
        sourceStmt = 'INSERT INTO Source (\n\t'
        sourceStmt += ',\n\t'.join(outcols)
        sourceStmt += ')\nSELECT\n\t'
        sourceStmt += ',\n\t'.join(selcols)
        sourceStmt += '\nFROM RunSource;\n'
    return (createStmt.format(tableName='RunSource'),
            loadStmt.format(tableName='RunSource', fileName='{fileName}'),
            sourceStmt) 

_objectMappings = [
    ("id", "objectId"),
    ("coord_ra", "ra"),
    ("coord_decl", "decl"),
    ("coord_raVar", "raVar"),
    ("coord_declVar", "declVar"),
    ("coord_radeclCov", "radeclCov"),
    ("coord_htmId20", "htmId20"),
    ("coord_weightedmean_ra", "wmRa"),
    ("coord_weightedmean_decl", "wmDecl"),
    ("coord_weightedmean_raVar", "wmRaVar"),
    ("coord_weightedmean_declVar", "wmDeclVar"),
    ("coord_weightedmean_radeclCov", "wmRadeclCov"),
    ("obs_count", "obsCount"),
    ("obs_time_min", "obsTimeMin"),
    ("obs_time_max", "obsTimeMax"),
    ("obs_time_mean", "obsTimeMean"),
    ("flag_noise", "flagNoise"),
]

_filterMappings = [
    ("{filter}_obs_count", "{filter}ObsCount"),
    ("{filter}_obs_time_min", "{filter}ObsTimeMin"),
    ("{filter}_obs_time_max", "{filter}ObsTimeMax"),
    ("{filter}_{measPrefix}{psfFlux}", "{filter}PsfFlux"),
    ("{filter}_{measPrefix}{psfFlux}_err", "{filter}PsfFluxSigma"),
    ("{filter}_{measPrefix}{psfFlux}_count", "{filter}PsfFluxCount"),
    ("{filter}_{measPrefix}{apFlux}", "{filter}ApFlux"),
    ("{filter}_{measPrefix}{apFlux}_err", "{filter}ApFluxSigma"),
    ("{filter}_{measPrefix}{apFlux}_count", "{filter}ApFluxCount"),
    ("{filter}_{measPrefix}{modelFlux}", "{filter}ModelFlux"),
    ("{filter}_{measPrefix}{modelFlux}_err", "{filter}ModelFluxSigma"),
    ("{filter}_{measPrefix}{modelFlux}_count", "{filter}ModelFluxCount"),
    ("{filter}_{measPrefix}{instFlux}", "{filter}InstFlux"),
    ("{filter}_{measPrefix}{instFlux}_err", "{filter}InstFluxSigma"),
    ("{filter}_{measPrefix}{instFlux}_count", "{filter}InstFluxCount"),
    ("{filter}_{measPrefix}{shape}_Ixx", "{filter}ShapeIxx"),
    ("{filter}_{measPrefix}{shape}_Iyy", "{filter}ShapeIyy"),
    ("{filter}_{measPrefix}{shape}_Ixy", "{filter}ShapeIxy"),
    ("{filter}_{measPrefix}{shape}_IxxVar", "{filter}ShapeIxxVar"),
    ("{filter}_{measPrefix}{shape}_IyyVar", "{filter}ShapeIyyVar"),
    ("{filter}_{measPrefix}{shape}_IxyVar", "{filter}ShapeIxyVar"),
    ("{filter}_{measPrefix}{shape}_IxxIyyCov", "{filter}ShapeIxxIyyCov"),
    ("{filter}_{measPrefix}{shape}_IxxIxyCov", "{filter}ShapeIxxIxyCov"),
    ("{filter}_{measPrefix}{shape}_IyyIxyCov", "{filter}ShapeIyyIxyCov"),
    ("{filter}_{measPrefix}{shape}_count", "{filter}ShapeCount"),
]

def objectTableSql(schema, dbMappingConfig, sourceAssocConfig, filters):
    """Return a tuple of SQL statements (createStmt, loadStmt, objectStmt)
    for the Object table.

    createStmt :    CREATE TABLE statement for the RunObject table, which
                    includes all fields from the run-specific
                    lsst.afw.table.Schema for source cluster tables output
                    by the SourceAssoc pipeline.

    loadStmt :      LOAD DATA statement for the RunObject table. This is
                    a format string; to generate valid SQL a fileName must
                    be supplied, e.g.:

                    loadStmt.format(fileName='object.csv')

    objectStmt :    Map the RunObject table to the canonical Object schema.
                    This will either create a VIEW, or INSERT into its
                    materialized equivalent.

    @param schema               lsst.afw.table.Schema for objects (source clusters)
    @param dbMappingConfig      lsst.datarel.DbMappingConfig
    @param sourceAssocConfig    lsst.ap.tasks.sourceAssoc.SourceAssocConfig
    @param filters              Iterable over the filter names included in the
                                canonical Object table.
    """
    # Generate SQL for run specific table
    createStmt, loadStmt = genericTableSql(
        schema, dbMappingConfig.objectConversion, set())
    # build substitution parameters for mapping table
    kw = _getMappingKw(
        sourceAssocConfig.measSlots,
        sourceAssocConfig.sourceProcessing,
        sourceAssocConfig.measPrefix)
    # build selection/output column lists
    selcols = []
    outcols = []
    for runFmt, objCol in _objectMappings:
        runCol = runFmt.format(**kw)
        field = _colToField(runCol)
        isFlag = objCol.startswith('flag')
        if isFlag and not dbMappingConfig.objectConversion.flagsAsBits:
            continue
        if field in schema or runCol == 'coord_htmId20':
            selcols.append(runCol)
        elif isFlag:
            selcols.append("b'0'")
        else:
            selcols.append('NULL')
        outcols.append(objCol)
    for filter in filters:
        kw['filter'] = filter
        for runFmt, objFmt in _filterMappings:
            runCol = runFmt.format(**kw)
            objCol = objFmt.format(filter=filter)
            field = _colToField(runCol)
            isFlag = objCol.startswith('flag')
            if isFlag and not dbMappingConfig.objectConversion.flagsAsBits:
                continue
            if field in schema:
                selcols.append(runCol)
            elif isFlag:
                selcols.append("b'0'")
            else:
                selcols.append('NULL')
            outcols.append(objCol)
    if not dbMappingConfig.objectConversion.flagsAsBits:
        # Deal with canonical flags packed into BIGINTs
        n = (len(dbMappingConfig.objectConversion.canonicalFlags) + 62) / 63
        if n == 1:
            selcols.append('flags')
            outcols.append('flags')
        else:
            for i in xrange(1, n + 1):
                c = 'flags{}'.format(i)
                selcols.append(c)
                outcols.append(c)
    if dbMappingConfig.asView:
        # Replace the official version of Object with an equivalent VIEW
        objectStmt = 'CREATE OR REPLACE VIEW Object AS SELECT\n\t'
        objectStmt += ',\n\t'.join(a + ' AS ' + b for a,b in zip(selcols, outcols))
        objectStmt += '\nFROM RunObject;'
    else:
        # Use the definition of Object from cat (i.e. the one used by the
        # schema browser for documentation purposes). This should cause
        # ingest to fail if this code and the canonical schema are not in sync.
        objectStmt = 'INSERT INTO Object (\n\t'
        objectStmt += ',\n\t'.join(outcols)
        objectStmt += ')\nSELECT\n\t'
        objectStmt += ',\n\t'.join(selcols)
        objectStmt += '\nFROM RunObject;'
    return (createStmt.format(tableName='RunObject'),
            loadStmt.format(tableName='RunObject', fileName='{fileName}'),
            objectStmt)


_coaddSourceMappings = [
    ("id", "{coaddName}SourceId"), # from minimal schema, no prefix
    ("parent", "parent{CoaddName}SourceId"), # from minimal schema, no prefix
    ("{exposurePrefix}_id", "{coaddName}CoaddId"),
    ("{exposurePrefix}_filter_id", "filterId"),
    ("coord_ra", "ra"), # from minimal schema, no prefix
    ("coord_decl", "decl"), # from minimal schema, no prefix
    ("coord_raVar", "raVar"), # from source association, no prefix
    ("coord_declVar", "declVar"), # from source association, no prefix
    ("coord_radeclCov", "radeclCov"), # from source association, no prefix
    ("coord_htmId20", "htmId20"), # from ingest, no prefix
    ("{measPrefix}{centroid}_x", "x"),
    ("{measPrefix}{centroid}_y", "y"),
    ("{measPrefix}{centroid}_xVar", "xVar"),
    ("{measPrefix}{centroid}_yVar", "yVar"),
    ("{measPrefix}{centroid}_xyCov", "xyCov"),
    ("{measPrefix}{psfFlux}", "psfFlux"),
    ("{measPrefix}{psfFlux}_err", "psfFluxSigma"),
    ("{measPrefix}{apFlux}", "apFlux"),
    ("{measPrefix}{apFlux}_err", "apFluxSigma"),
    ("{measPrefix}{modelFlux}", "modelFlux"),
    ("{measPrefix}{modelFlux}_err", "modelFluxSigma"),
    ("{measPrefix}{instFlux}", "instFlux"),
    ("{measPrefix}{instFlux}_err", "instFluxSigma"),
    ("aperturecorrection", "apCorrection"), # from measurement, no prefix (!?)
    ("aperturecorrection_err", "apCorrectionSigma"), # from measurement, no prefix (!?)
    ("{measPrefix}{shape}_centroid_x", "shapeIx"),
    ("{measPrefix}{shape}_centroid_y", "shapeIy"),
    ("{measPrefix}{shape}_centroid_xVar", "shapeIxVar"),
    ("{measPrefix}{shape}_centroid_yVar", "shapeIyVar"),
    ("{measPrefix}{shape}_centroid_xyCov", "shapeIxIyCov"),
    ("{measPrefix}{shape}_Ixx", "shapeIxx"),
    ("{measPrefix}{shape}_Iyy", "shapeIyy"),
    ("{measPrefix}{shape}_Ixy", "shapeIxy"),
    ("{measPrefix}{shape}_IxxVar", "shapeIxxVar"),
    ("{measPrefix}{shape}_IyyVar", "shapeIyyVar"),
    ("{measPrefix}{shape}_IxyVar", "shapeIxyVar"),
    ("{measPrefix}{shape}_IxxIyyCov", "shapeIxxIyyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIxxIxyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIyyIxyCov"),
    ("{measPrefix}classification_extendedness", "extendedness"),
    ("flags_negative", "flagNegative"), # from detection, no prefix
    ("{measPrefix}flags_badcentroid", "flagBadMeasCentroid"),
    ("{measPrefix}flags_pixel_edge", "flagPixEdge"),
    ("{measPrefix}flags_pixel_interpolated_any", "flagPixInterpAny"),
    ("{measPrefix}flags_pixel_interpolated_center", "flagPixInterpCen"),
    ("{measPrefix}flags_pixel_saturated_any", "flagPixSaturAny"),
    ("{measPrefix}flags_pixel_saturated_center", "flagPixSaturCen"),
    ("{measPrefix}{psfFlux}_flags", "flagBadPsfFlux"),
    ("{measPrefix}{apFlux}_flags", "flagBadApFlux"),
    ("{measPrefix}{modelFlux}_flags", "flagBadModelFlux"),
    ("{measPrefix}{instFlux}_flags", "flagBadInstFlux"),
    ("{measPrefix}{centroid}_flags", "flagBadCentroid"),
    ("{measPrefix}{shape}_flags", "flagBadShape"),
]

def coaddSourceTableSql(coaddName,
                        schema,
                        sourceConversionConfig,
                        asView,
                        sourceProcessingConfig,
                        slotConfig,
                        measPrefix):
    """Return a tuple of SQL statements (createStmt, loadStmt, sourceStmt)
    for a coadd source table. The canonical table name is obtained by
    captilizing the first letter of coaddName and appending 'Source'. The
    run specific table name is derived from the former by prepending 'Run'.

    createStmt :    CREATE TABLE statement for the Run<CoaddName>Source table,
                    which includes all fields from the run-specific
                    lsst.afw.table.Schema for source tables output by the
                    pipelines.

    loadStmt :      LOAD DATA statement for the Run<CoaddName>Source table.
                    This is a format string; to generate valid SQL a fileName
                    must be supplied, e.g.:

                    loadStmt.format(fileName='source.csv')

    sourceStmt :    Map the Run<CoaddName>Source table to the canonical
                    <CoaddName>Source schema. This will either create a VIEW,
                    or INSERT into the materialized equivalent.

    @param coaddName
        Coadd name (camel-case), e.g. 'deep' or 'goodSeeing'.
    @param schema
        lsst.afw.table.Schema for coadd-sources.   
    @param sourceConversionConfig
        lsst.ap.utils.CsvConversionConfig - parameters used for
        C++ to CSV conversion.
    @param asView
        True if the canonical table should be constructed as a VIEW on
        top of the run-specific table.
    @param sourceProcessingConfig
        lsst.ap.cluster.SourceProcessingConfig - parameters used to
        denormalize the C++ schema produced by the pipeline.
    @param slotConfig
        lsst.meas.algorithms.SlotConfig - pipeline slot mappings.
    @param measPrefix
        Prefix for measurement field names.
    """
    # Generate SQL for run specific table
    createStmt, loadStmt = genericTableSql(
        schema,
        sourceConversionConfig,
        _sourceIndexes(sourceProcessingConfig))
    # build substitution parameters for mapping table
    kw = _getMappingKw(
        slotConfig,
        sourceProcessingConfig,
        measPrefix)
    # build selection/output column lists
    selcols = []
    outcols = []
    CoaddName = coaddName[0].upper() + coaddName[1:]
    for runFmt, srcCol in _coaddSourceMappings:
        runCol = runFmt.format(**kw)
        srcCol = srcCol.format(coaddName=coaddName, CoaddName=CoaddName)
        if sourceProcessingConfig.multiBand and srcCol == 'filterId':
            continue # multi-band source has no filterId
        field = _colToField(runCol)
        isFlag = srcCol.startswith('flag')
        if isFlag and not sourceConversionConfig.flagsAsBits:
            continue
        if field in schema or runCol == 'coord_htmId20':
            selcols.append(runCol)
        elif isFlag:
            selcols.append("b'0'")
        else:
            selcols.append('NULL')
        outcols.append(srcCol)
    if not sourceConversionConfig.flagsAsBits:
        # Deal with canonical flags packed into BIGINTs
        n = (len(sourceConversionConfig.canonicalFlags) + 62) / 63
        if n == 1:
            selcols.append('flags')
            outcols.append('flags')
        else:
            for i in xrange(1, n + 1):
                c = 'flags{}'.format(i)
                selcols.append(c)
                outcols.append(c)
    tableName = CoaddName + 'Source'
    runTableName = 'Run' + tableName
    if asView:
        # Replace the official version of <CoaddName>Source with an equivalent VIEW
        sourceStmt = 'CREATE OR REPLACE VIEW {} AS SELECT\n\t'.format(tableName)
        sourceStmt += ',\n\t'.join(a + ' AS ' + b for a,b in zip(selcols, outcols))
        sourceStmt += '\nFROM {};'.format(runTableName)
    else:
        # Use the definition of Source from cat (i.e. the one used by the
        # schema browser for documentation purposes). This should cause
        # ingest to fail if this code and the canonical schema are not in sync.
        sourceStmt = 'INSERT INTO {} (\n\t'.format(tableName)
        sourceStmt += ',\n\t'.join(outcols)
        sourceStmt += ')\nSELECT\n\t'
        sourceStmt += ',\n\t'.join(selcols)
        sourceStmt += '\nFROM {};\n'.format(runTableName)
    return (createStmt.format(tableName=runTableName),
            loadStmt.format(tableName=runTableName, fileName='{fileName}'),
            sourceStmt)

# mappings from run-specific table column names to canonical ForcedSource columns
_forcedSourceMappings = [
    ("id", "{coaddName}ForcedSourceId"), # from minimal schema, no prefix
    ("{exposurePrefix}_id", "scienceCcdExposureId"),
    ("{exposurePrefix}_filter_id", "filterId"),
    ("{exposurePrefix}_time_mid", "timeMid"),
    ("{exposurePrefix}_time", "expTime"),
    ("objectId", "{coaddName}SourceId"),
    ("coord_ra", "ra"), # from minimal schema, no prefix
    ("coord_decl", "decl"), # from minimal schema, no prefix
    ("coord_raVar", "raVar"), # from source association, no prefix
    ("coord_declVar", "declVar"), # from source association, no prefix
    ("coord_radeclCov", "radeclCov"), # from source association, no prefix
    ("coord_htmId20", "htmId20"), # from ingest, no prefix
    ("{measPrefix}{centroid}_x", "x"),
    ("{measPrefix}{centroid}_y", "y"),
    ("{measPrefix}{centroid}_xVar", "xVar"),
    ("{measPrefix}{centroid}_yVar", "yVar"),
    ("{measPrefix}{centroid}_xyCov", "xyCov"),
    ("{measPrefix}{psfFlux}", "psfFlux"),
    ("{measPrefix}{psfFlux}_err", "psfFluxSigma"),
    ("{measPrefix}{apFlux}", "apFlux"),
    ("{measPrefix}{apFlux}_err", "apFluxSigma"),
    ("{measPrefix}{modelFlux}", "modelFlux"),
    ("{measPrefix}{modelFlux}_err", "modelFluxSigma"),
    ("{measPrefix}{instFlux}", "instFlux"),
    ("{measPrefix}{instFlux}_err", "instFluxSigma"),
    ("aperturecorrection", "apCorrection"), # from measurement, no prefix (!?)
    ("aperturecorrection_err", "apCorrectionSigma"), # from measurement, no prefix (!?)
    ("{measPrefix}{shape}_centroid_x", "shapeIx"),
    ("{measPrefix}{shape}_centroid_y", "shapeIy"),
    ("{measPrefix}{shape}_centroid_xVar", "shapeIxVar"),
    ("{measPrefix}{shape}_centroid_yVar", "shapeIyVar"),
    ("{measPrefix}{shape}_centroid_xyCov", "shapeIxIyCov"),
    ("{measPrefix}{shape}_Ixx", "shapeIxx"),
    ("{measPrefix}{shape}_Iyy", "shapeIyy"),
    ("{measPrefix}{shape}_Ixy", "shapeIxy"),
    ("{measPrefix}{shape}_IxxVar", "shapeIxxVar"),
    ("{measPrefix}{shape}_IyyVar", "shapeIyyVar"),
    ("{measPrefix}{shape}_IxyVar", "shapeIxyVar"),
    ("{measPrefix}{shape}_IxxIyyCov", "shapeIxxIyyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIxxIxyCov"),
    ("{measPrefix}{shape}_IxxIxyCov", "shapeIyyIxyCov"),
    ("{measPrefix}classification_extendedness", "extendedness"),
    ("flags_negative", "flagNegative"), # from detection, no prefix
    ("{measPrefix}flags_badcentroid", "flagBadMeasCentroid"),
    ("{measPrefix}flags_pixel_edge", "flagPixEdge"),
    ("{measPrefix}flags_pixel_interpolated_any", "flagPixInterpAny"),
    ("{measPrefix}flags_pixel_interpolated_center", "flagPixInterpCen"),
    ("{measPrefix}flags_pixel_saturated_any", "flagPixSaturAny"),
    ("{measPrefix}flags_pixel_saturated_center", "flagPixSaturCen"),
    ("{measPrefix}{psfFlux}_flags", "flagBadPsfFlux"),
    ("{measPrefix}{apFlux}_flags", "flagBadApFlux"),
    ("{measPrefix}{modelFlux}_flags", "flagBadModelFlux"),
    ("{measPrefix}{instFlux}_flags", "flagBadInstFlux"),
    ("{measPrefix}{centroid}_flags", "flagBadCentroid"),
    ("{measPrefix}{shape}_flags", "flagBadShape"),
]

def forcedSourceTableSql(coaddName,
                        schema,
                        sourceConversionConfig,
                        asView,
                        sourceProcessingConfig,
                        slotConfig,
                        measPrefix):
    """Return a tuple of SQL statements (createStmt, loadStmt, sourceStmt)
    for a forced source table. The canonical table name is obtained by
    capitalizing the first letter of coaddName and appending 'ForcedSource'.
    The run specific table name is derived from the former by prepending 'Run'.

    createStmt :    CREATE TABLE statement for the Run<CoaddName>ForcedSource
                    table, which includes all fields from the run-specific
                    lsst.afw.table.Schema for source tables output by the
                    pipelines.

    loadStmt :      LOAD DATA statement for the Run<CoaddName>ForcedSource
                    table.  This is a format string; to generate valid SQL a
                    fileName must be supplied, e.g.:

                    loadStmt.format(fileName='source.csv')

    sourceStmt :    Map the Run<CoaddName>ForcedSource table to the canonical
                    <CoaddName>ForcedSource schema. This will either create a
                    VIEW, or INSERT into the materialized equivalent.

    @param coaddName
        Coadd name (camel-case), e.g. 'deep' or 'goodSeeing'.
    @param schema
        lsst.afw.table.Schema for forced sources.   
    @param sourceConversionConfig
        lsst.ap.utils.CsvConversionConfig - parameters used for
        C++ to CSV conversion.
    @param asView
        True if the canonical table should be constructed as a VIEW on
        top of the run-specific table.
    @param sourceProcessingConfig
        lsst.ap.cluster.SourceProcessingConfig - parameters used to
        denormalize the C++ schema produced by the pipeline.
    @param slotConfig
        lsst.meas.algorithms.SlotConfig - pipeline slot mappings.
    @param measPrefix
        Prefix for measurement field names.
    """
    # Generate SQL for run specific table
    createStmt, loadStmt = genericTableSql(
        schema,
        sourceConversionConfig,
        _sourceIndexes(sourceProcessingConfig))
    # build substitution parameters for mapping table
    if sourceProcessingConfig.clusterPrefix is None:
        sourceProcessingConfig.clusterPrefix = ""
    kw = _getMappingKw(
        slotConfig,
        sourceProcessingConfig,
        measPrefix)
    # build selection/output column lists
    selcols = []
    outcols = []
    CoaddName = coaddName[0].upper() + coaddName[1:]
    for runFmt, srcCol in _forcedSourceMappings:
        runCol = runFmt.format(**kw)
        srcCol = srcCol.format(coaddName=coaddName, CoaddName=CoaddName)
        if sourceProcessingConfig.multiBand and srcCol == 'filterId':
            continue # multi-band source has no filterId
        field = _colToField(runCol)
        isFlag = srcCol.startswith('flag')
        if isFlag and not sourceConversionConfig.flagsAsBits:
            continue
        if field in schema or runCol == 'coord_htmId20':
            selcols.append(runCol)
        elif isFlag:
            selcols.append("b'0'")
        else:
            selcols.append('NULL')
        outcols.append(srcCol)
    if not sourceConversionConfig.flagsAsBits:
        # Deal with canonical flags packed into BIGINTs
        n = (len(sourceConversionConfig.canonicalFlags) + 62) / 63
        if n == 1:
            selcols.append('flags')
            outcols.append('flags')
        else:
            for i in xrange(1, n + 1):
                c = 'flags{}'.format(i)
                selcols.append(c)
                outcols.append(c)
    tableName = CoaddName + 'ForcedSource'
    runTableName = 'Run' + tableName
    if asView:
        # Replace the official version of <CoaddName>ForcedSource with an equivalent VIEW
        sourceStmt = 'CREATE OR REPLACE VIEW {} AS SELECT\n\t'.format(tableName)
        sourceStmt += ',\n\t'.join(a + ' AS ' + b for a,b in zip(selcols, outcols))
        sourceStmt += '\nFROM {};'.format(runTableName)
    else:
        # Use the definition of Source from cat (i.e. the one used by the
        # schema browser for documentation purposes). This should cause
        # ingest to fail if this code and the canonical schema are not in sync.
        sourceStmt = 'INSERT INTO {} (\n\t'.format(tableName)
        sourceStmt += ',\n\t'.join(outcols)
        sourceStmt += ')\nSELECT\n\t'
        sourceStmt += ',\n\t'.join(selcols)
        sourceStmt += '\nFROM {};\n'.format(runTableName)
    return (createStmt.format(tableName=runTableName),
            loadStmt.format(tableName=runTableName, fileName='{fileName}'),
            sourceStmt)
