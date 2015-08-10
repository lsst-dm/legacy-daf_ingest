#!/usr/bin/env python

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

from __future__ import division

import unittest

from contextlib import closing
import math
import numpy as np
import os
import struct
import sys
import uuid

import lsst.utils.tests as utilsTests
import lsst.afw.table as afwTable

from lsst.afw.geom import Angle
from lsst.daf.ingest.ingestCatalogTask import IngestCatalogTask, IngestCatalogConfig
from lsst.daf.persistence import DbAuth


class IngestCatalogTest(unittest.TestCase):
    """Test for ingesting afw catalogs into a database table.
    """

    def setUp(self):
        """Create a BaseCatalog to run through the ingestion process.
        """
        # First, connect to the database.
        self.host = os.environ.get("TEST_MYSQL_HOST", "lsst-db.ncsa.illinois.edu")
        self.db = os.environ.get("TEST_MYSQL_DB", "test")
        self.port = int(os.environ.get("TEST_MYSQL_PORT", "3306"))
        self.conn = None
        try:
            self.conn = IngestCatalogTask.connect(host=self.host, port=self.port, db=self.db)
        except:
            pass
        # Create a schema containing one of every kind of afw field
        schema = afwTable.Schema()
        keys = (
            # Scalar fields
            schema.addField("scalar.u", type="U"),
            schema.addField("scalar.i", type="I"),
            schema.addField("scalar.l", type="L"),
            schema.addField("scalar.f", type="F"),
            schema.addField("scalar.d", type="D"),
            schema.addField("scalar.flag", type="Flag"),
            schema.addField("scalar.angle", type="Angle"),
            # Fixed-length array types
            schema.addField("fix.string", type="String", size=4),
            schema.addField("fix.array.u", type="ArrayU", size=2),
            schema.addField("fix.array.i", type="ArrayI", size=2),
            schema.addField("fix.array.f", type="ArrayF", size=2),
            schema.addField("fix.array.d", type="ArrayD", size=2),
            # Variable-length array types
            schema.addField("var.array.u", type="ArrayU", size=0),
            schema.addField("var.array.i", type="ArrayI", size=0),
            schema.addField("var.array.f", type="ArrayF", size=0),
            schema.addField("var.array.d", type="ArrayD", size=0),
        )
        # Setup schema aliases
        aliases = dict(
            S="scalar",
            F="fix",
            af="fix.array",
            V="var",
            av="var.array",
            vla="av",
        )
        for source, target in aliases.iteritems():
            schema.getAliasMap().set(source, target)
        # Create two rows of fake data...
        self.rows = (
            (0, -2147483648, -9223372036854775808, 1.0, math.pi,
             False, Angle(1.0),
             "ab ",
             np.array([0, 65535], dtype=np.uint16),
             np.array([-2147483648, 2147483647], dtype=np.int32),
             np.array([1.0, 2.0], dtype=np.float32),
             np.array([math.pi, math.e], dtype=np.float64),
             np.array(range(0), dtype=np.uint16),
             np.array(range(1), dtype=np.int32),
             np.array(range(3), dtype=np.float32),
             np.array(range(4), dtype=np.float64),
            ),
            (65535, 2147483647, 9223372036854775807, 2.0, math.e,
             True, Angle(2.0), "",
             np.array(range(2), dtype=np.uint16),
             np.array(range(2), dtype=np.int32),
             np.array(range(2), dtype=np.float32),
             np.array(range(2), dtype=np.float64),
             np.array([], dtype=np.uint16),
             np.array([], dtype=np.int32),
             np.array([], dtype=np.float32),
             np.array([], dtype=np.float64),
            )
        )
        # and a corresponding catalog
        self.catalog = afwTable.BaseCatalog(schema)
        for row in self.rows:
            record = self.catalog.addNew()
            for i, k in enumerate(keys):
                record.set(k, row[i])
        # Finally, choose table/view names that are unique with very high
        # probability.
        suffix = uuid.uuid4().hex
        self.tableName = "catalog_" + suffix
        self.viewName = "view_" + suffix

    def tearDown(self):
        """Remove the database table and view created during testing.
        """
        self.catalog = None
        if self.conn is not None:
            self.conn.query("DROP TABLE IF EXISTS " + self.tableName)
            self.conn.query("DROP VIEW IF EXISTS " + self.viewName)
            self.conn.close()
            self.conn = None

    _formatChars = {np.uint16: "H", np.int32: "i", np.float32: "f", np.float64: "d"}

    def _compareValues(self, originalValue, roundtripValue):
        """Compare the original value of a field to the value of that field after
        a roundtrip through the database. This is tricky for a couple reasons:

        - angles are ingested in degrees rather than radians
        - arrays are packed into binary string columns
        """
        if isinstance(originalValue, np.ndarray):
            # Arrays are ingested into binary strings. Convert those
            # binary strings back into arrays before comparison to the
            # original data.
            formatChar = self._formatChars[originalValue.dtype.type]
            sz = struct.calcsize("<" + formatChar)
            n = len(roundtripValue) // sz
            roundtripValue = np.array(
                struct.unpack("<" + str(n) + formatChar, roundtripValue),
                dtype=originalValue.dtype.type
            )
            self.assertTrue(np.array_equal(originalValue, roundtripValue))
        elif isinstance(originalValue, Angle):
            # The ingest code always converts angles to degrees.
            self.assertEqual(originalValue.asDegrees(), roundtripValue)
        elif isinstance(originalValue, bool):
            # MySQLdb maps BIT column values to '\x00' or '\x01'.
            self.assertEqual(originalValue, roundtripValue != '\x00')
        else:
            self.assertEqual(originalValue, roundtripValue)

    def testIngest(self):
        """Test the ingest task.
        """
        # Skip if no database connection available
        if self.conn is None:
            self.skipTest("Could not connect to database")

        # Run the catalog ingestion task.
        config = IngestCatalogConfig()
        config.extraColumns = "htmId20 BIGINT, otherColumn DOUBLE DEFAULT 2.0"
        config.maxQueryLen = 100000
        task = IngestCatalogTask(config=config)
        task.ingest(self.catalog, self.tableName, self.host, self.db,
                    port=self.port, viewName=self.viewName)

        with closing(self.conn.cursor()) as cursor:
            # Check that the rows were correctly loaded.
            try:
                cursor.execute("SELECT * FROM " + self.tableName)
            except:
                self.fail("Could not query ingested database table")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)
            for (originalRow, roundtripRow) in zip(self.rows, rows):
                for originalValue, roundtripValue in zip(originalRow, roundtripRow):
                    self._compareValues(originalValue, roundtripValue)
            # Check the extra htmId20 column.
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM {} WHERE htmId20 IS NULL".format(self.tableName))
            except:
                self.fail("Could not find column htmId20 in ingested database table")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 2)
            # Check the extra double column with a default.
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM {} WHERE otherColumn <> 2.0".format(self.tableName))
            except:
                self.fail("Could not find column otherColumn in ingested database table")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 0)
            # Check that aliases are faithfully provided.
            try:
                cursor.execute(
                    """SELECT COUNT(*) FROM {} WHERE
                        (s_flag = scalar_flag)
                    AND (f_string = fix_string OR fix_string IS NULL)
                    AND (af_u = fix_array_u OR F_array_u IS NULL)
                    AND (var_array_d = vla_d OR av_d IS NULL)
                    """.format(self.viewName))
            except:
                self.fail("Missing aliases in view of ingested database table")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 2)


def suite():
    """Returns a suite containing all the test cases in this module.
    """
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(IngestCatalogTest)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
