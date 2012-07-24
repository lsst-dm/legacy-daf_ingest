#! /usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

import argparse
import os
import string

from lsst.datarel.mysqlExecutor import MysqlExecutor, addDbOptions
from prepareDb import loadTables
import transposeMetadata

# SQL script that updates an key-value metadata table such that
# metadata keys with inconsistent type have consistent type.
#
# This has been observed to happen in the following cases:
#   * When a FITS header card that nominally has floating point values happens
#     to have an integer value (i.e. was printed without a decimal point)
#   * When a FITS header card that nominally has a numeric value is set to
#     a string value to indicate a missing value (e.g. "Null")
#
#
# Cleaner but slower:
#
# UPDATE $table
# SET doubleValue = intValue, intValue = NULL
# WHERE intValue IS NOT NULL AND metadataKey IN (
#    SELECT DISTINCT metadataKey FROM XXX_Exposure_Metadata
#    WHERE doubleValue IS NOT NULL AND metadataKey IN (
#        SELECT DISTINCT metadataKey FROM XXX_Exposure_Metadata
#        WHERE intValue IS NOT NULL
#    )
# );
#
# UPDATE $table 
# SET stringValue = NULL
# WHERE stringValue RLIKE '[[:blank:]]*[Nn][Uu][Ll]{2}[[:blank:]]*' AND metadataKey IN (
#     SELECT DISTINCT metadataKey FROM XXX_Exposure_Metadata
#     WHERE (doubleValue IS NOT NULL OR intValue IS NOT NULL) AND metadataKey IN (
#         SELECT DISTINCT metadataKey FROM Raw_Amp_Exposure_Metadata
#         WHERE stringValue IS NOT NULL
#     )
# );
fixupTemplate = string.Template("""
    SET myisam_sort_buffer_size=1000000000;

    CREATE TABLE _Keys1 (metadataKey VARCHAR(255) NOT NULL PRIMARY KEY) ENGINE=MEMORY;
    CREATE TABLE _Keys2 (metadataKey VARCHAR(255) NOT NULL PRIMARY KEY) ENGINE=MEMORY;

    INSERT INTO _Keys1
        SELECT DISTINCT metadataKey
        FROM $table WHERE intValue IS NOT NULL;

    INSERT INTO _Keys2
        SELECT DISTINCT a.metadataKey
        FROM $table AS a INNER JOIN
             _Keys1 AS b ON (a.metadataKey = b.metadataKey)
        WHERE a.doubleValue IS NOT NULL;

    UPDATE $table AS a INNER JOIN
           _Keys2 AS b ON (a.metadataKey = b.metadataKey)
    SET a.doubleValue = a.intValue,
        a.intValue    = NULL
    WHERE a.intValue IS NOT NULL;

    TRUNCATE TABLE _Keys1;
    TRUNCATE TABLE _Keys2;

    INSERT INTO _Keys1
        SELECT DISTINCT metadataKey
        FROM $table WHERE stringValue IS NOT NULL;

    INSERT INTO _Keys2
        SELECT DISTINCT a.metadataKey
        FROM $table AS a INNER JOIN
             _Keys1 AS b ON (a.metadataKey = b.metadataKey)
        WHERE a.doubleValue IS NOT NULL OR a.intValue IS NOT NULL;

    UPDATE Raw_Amp_Exposure_Metadata AS a INNER JOIN
           _Keys2 AS b ON (a.metadataKey = b.metadataKey)
    SET a.stringValue = NULL
    WHERE a.stringValue RLIKE '[[:blank:]]*[Nn][Uu][Ll]{2}[[:blank:]]*';

    DROP TABLE _Keys1;
    DROP TABLE _Keys2;
    """)


metadataTables = {
    "lsstsim" : ["Raw_Amp_Exposure_Metadata", "Science_Ccd_Exposure_Metadata"],
    "sdss"    : ["Science_Ccd_Exposure_Metadata"],
}


def findInconsistentMetadataTypes(sql, camera):
    needsFix = []
    for table in metadataTables[camera]:
        keys = sql.runQuery(str.format("""
            SET myisam_sort_buffer_size=1000000000;

            SELECT t.metadataKey, count(*) AS n,
                   GROUP_CONCAT(t.type SEPARATOR ',') AS types
            FROM (
                SELECT DISTINCT metadataKey, IF(stringValue IS NOT NULL, "string",
                    IF(intValue IS NOT NULL, "int", "double")) AS type
                FROM {} 
                WHERE stringValue IS NOT NULL OR
                      intValue IS NOT NULL OR
                      doubleValue IS NOT NULL
            ) AS t
            GROUP BY t.metadataKey
            HAVING n > 1;
            """, table))
        if len(keys) > 0:
            print table + " has inconsistent types for metadata keys:"
            for k in keys:
                print "{}:\t{}".format(k[0], k[2])
            sys.stdout.flush()
            needsFix.append(table)
    return needsFix


def main():
    parser = argparse.ArgumentParser(description=
        "Program which runs post-processing steps on an LSST run database, "
        "including enabling the table indexes that prepareDb.py disables to "
        "speed up loading. Metadata tables are checked for type consistency "
        "and fixed up if necessary, and key-value metadata tables are optionally "
        "transposed into column-per-value tables.")
    addDbOptions(parser)
    parser.add_argument("--camera", dest="camera", default="lsstSim",
        help="Name of desired camera (defaults to %(default)s)")
    parser.add_argument(
        "-t", "--transpose", action="store_true", dest="transpose",
        help="Flag that causes key-value metadata tables to be transposed to "
             "column-per-value metadata tables for easier metadata queries.")
    parser.add_argument("database", help="Name of database to post-process.")

    ns = parser.parse_args()
    if ns.user == None:
        parser.error("No database user name specified and $USER is undefined or empty")
    sql = MysqlExecutor(ns.host, ns.database, ns.user, ns.port)
    camera = ns.camera.lower()
    if camera not in loadTables:
        parser.error("Unknown camera: {}. Choices (not case sensitive): {}".format(
            camera, loadTables.keys()))
    # Enable indexes on tables for faster queries
    tables = loadTables[camera] + ["Logs", "RunSource", "RunObject",
            "RunGoodSeeingSource", "RunGoodSeeingForcedSource"]
    for table in tables:
        if sql.isView(table) or not sql.exists(table):
            continue
        sql.execStmt(str.format("SET myisam_sort_buffer_size=1000000000; "
                                "ALTER TABLE {} ENABLE KEYS;", table))
    # fixup metadata tables if necessary
    fixTables = findInconsistentMetadataTypes(sql, camera)
    if len(fixTables) > 0:
        print "\nattempting to fix type inconsistencies"
    for table in fixTables:
        stmt = fixupTemplate.substitute({"table": table})
        sql.execStmt(stmt)
    if len(fixTables) > 0:
        print "\nVerifying that all inconsistencies were fixed..."
        fixTables = findInconsistentMetadataTypes(sql, camera)
    if len(fixTables) > 0:
        print "\n... inconsistencies remain!"
        if ns.transpose:
            print "\nCannot transpose metadata tables with inconsistent types!"
    elif ns.transpose:
        # Generate transposed metadata tables
        if "Raw_Amp_Exposure_Metadata" in metadataTables[camera]:
            rawAmpSkipCols = set(['NAXIS1', 'NAXIS2',
                                  'MJD-OBS', 'EXPTIME',
                                  'FILTER',
                                  'RA_DEG', 'DEC_DEG',
                                  'EQUINOX', 'RADESYS',
                                  'CTYPE1', 'CTYPE2',
                                  'CRPIX1', 'CRPIX2',
                                  'CRVAL1', 'CRVAL2',
                                  'CD1_1', 'CD1_2',
                                  'CD2_1', 'CD2_2',
                                  'AIRMASS', 'DARKTIME', 'ZENITH'])
            transposeMetadata.run(ns.host, ns.port, ns.user,
                                  sql.password, ns.database,
                                  "Raw_Amp_Exposure_Metadata",
                                  "rawAmpExposureId",
                                  "Raw_Amp_Exposure_Extra",
                                  rawAmpSkipCols, True)
        if "Science_Ccd_Exposure_Metadata" in metadataTables[camera]:
            sciCcdSkipCols = set(['NAXIS1', 'NAXIS2',
                                  'MJD-OBS',
                                  'FILTER',
                                  'RA_DEG', 'DEC_DEG',
                                  'EQUINOX', 'RADESYS',
                                  'CTYPE1', 'CTYPE2',
                                  'CRPIX1', 'CRPIX2',
                                  'CRVAL1', 'CRVAL2',
                                  'CD1_1', 'CD1_2',
                                  'CD2_1', 'CD2_2',
                                  'TIME-MID', 'EXPTIME',
                                  'RDNOISE', 'SATURATE', 'GAINEFF',
                                  'FLUXMAG0', 'FLUXMAG0ERR',
                                 ])
            transposeMetadata.run(ns.host, ns.port, ns.user,
                                  sql.password, ns.database,
                                  "Science_Ccd_Exposure_Metadata",
                                  "scienceCcdExposureId",
                                  "Science_Ccd_Exposure_Extra",
                                  sciCcdSkipCols, True)

if __name__ == "__main__":
    main()

