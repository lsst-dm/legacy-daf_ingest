#!/usr/bin/env python

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

import unittest

import MySQLdb
import sys
import time

import lsst.utils.tests as utilsTests

from lsst.datarel.ingestSourcesTask import IngestSourcesTask, IngestSourcesConfig
from lsst.daf.persistence import DbAuth

class IngestSourcesTest(unittest.TestCase):
    """
    Test for ingesting sources into a database table.
    """

    def setUp(self):
        # Make sure we can connect to the database first.
        self.tableName = "Source_%d" % (time.time())
        self.host = "lsst10.ncsa.illinois.edu"
        self.db = "test"
        port = 3306
        try:
            self.conn = MySQLdb.connect(host=self.host, port=port,
                    user=DbAuth.username(self.host, str(port)),
                    passwd=DbAuth.password(self.host, str(port)), db=self.db)
        except:
            print >>sys.stderr, "*** Could not connect to database, skipping test."
            sys.exit(0)

    def tearDown(self):
        # Clean up by removing the database table.
        self.conn.query("DROP TABLE %s;" % (self.tableName,))

    
    def testIngest(self):
        """Test the ingest task."""

        # First run the task.
        config = IngestSourcesConfig()
        config.extraColumns = "htmid20 INT, otherColumn DOUBLE DEFAULT 2.0"
        config.maxQueryLen = 100000
        task = IngestSourcesTask(self.tableName, self.host, self.db,
                config=config)
        task.runFile("tests/data/src.fits")

        # Check that we actually loaded the rows into the table.
        cur = self.conn.cursor()
        try:
            rows = cur.execute("SELECT COUNT(*) FROM %s;" % (self.tableName,))
        except:
            self.fail("Could not access ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 1080)

        # Check that a certain known column was loaded with correct values.
        cur = self.conn.cursor()
        try:
            rows = cur.execute(
                    "SELECT COUNT(*) FROM %s WHERE centroid_sdss_x < 1000;" %
                    (self.tableName,))
        except:
            self.fail("Could not find column centrolid_sdss_x in ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 82)

        # Check the extra htmid20 column.
        cur = self.conn.cursor()
        try:
            rows = cur.execute(
                    "SELECT COUNT(*) FROM %s WHERE htmid20 IS NULL;" %
                    (self.tableName,))
        except:
            self.fail("Could not find column htmid20 in ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 1080)

        # Check the extra double column with a default.
        cur = self.conn.cursor()
        try:
            rows = cur.execute(
                    "SELECT COUNT(*) FROM %s WHERE otherColumn <> 2.0;" %
                    (self.tableName,))
        except:
            self.fail("Could not find column otherColumn in ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 0)

#####

def suite():
    """Returns a suite containing all the test cases in this module."""
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(IngestSourcesTest)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
