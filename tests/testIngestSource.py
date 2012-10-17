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
import time

import lsst.utils.tests as utilsTests

from lsst.datarel.ingestSourcesTask import IngestSourcesTask
from lsst.daf.persistence import DbAuth

class IngestSourcesTest(unittest.TestCase):
    """
    Test for ingesting sources into a database table.
    """
    
    def testIngest(self):
        tableName = "Source_%d" % (time.time())
        host = "lsst10.ncsa.illinois.edu"
        db = "test"
        port = 3306
        task = IngestSourcesTask(tableName, host, db)
        task.runFile("tests/data/src.fits")
        conn = MySQLdb.connect(host=host, port=port,
                user=DbAuth.username(host, str(port)),
                passwd=DbAuth.password(host, str(port)), db=db)
        cur = conn.cursor()
        try:
            rows = cur.execute("SELECT COUNT(*) FROM %s;" % (tableName,))
        except:
            self.fail("Could not access ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 1080)
        cur = conn.cursor()
        try:
            rows = cur.execute(
                    "SELECT COUNT(*) FROM %s WHERE centroid_sdss_x < 1000;" %
                    (tableName,))
        except:
            self.fail("Could not find column in ingested database table")
        self.assertEqual(rows, 1)
        self.assertEqual(cur.fetchone()[0], 82)
        conn.query("DROP TABLE %s;" % (tableName,))

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
