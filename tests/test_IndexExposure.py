#!/usr/bin/env python

#
# LSST Data Management System
#
# Copyright 2016 AURA/LSST.
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
"""Unit tests for the exposure indexing task."""
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import object

import unittest

import math
try:
    import cPickle as pickle
except:
    import pickle
import random
import sqlite3

import lsst.utils.tests
import lsst.daf.base as daf_base
import lsst.afw.image as afw_image
import lsst.pipe.base as pipe_base
import lsst.sphgeom as sphgeom
from lsst.log import Log
from lsst.daf.ingest.indexExposure import (
    create_exposure_tables,
    find_intersecting_exposures,
    store_exposure_info,
    IndexExposureConfig,
    IndexExposureRunner,
    IndexExposureTask,
)


class MockDataRef(object):
    """A :class:`lsst.daf.persistence.ButlerDataRef` impostor.

    This class avoids the need to stand up a butler for command
    line task unit testing.
    """

    def __init__(self, data_id, value):
        """Store a data id and an associated value."""
        self.dataId = data_id
        self.value = value

    def get(self, *args, **kwargs):
        """Return the value pointed to by this data ref."""
        return self.value


class IndexExposureTest(unittest.TestCase):
    """Test for spatial indexing of afw exposures."""

    def test_basic(self):
        """Perform basic correctness testing."""
        ps = []
        # Construct property sets for two exposures centered on the equator
        for center in ((0.0, 0.0), (180.0, 0.0)):
            props = daf_base.PropertySet()
            props.add("NAXIS1", 9)
            props.add("NAXIS2", 9)
            props.add("RADECSYS", "ICRS")
            props.add("EQUINOX", 2000.0)
            props.add("CTYPE1", "RA---TAN")
            props.add("CTYPE2", "DEC--TAN")
            props.add("CRPIX1", 5.0)
            props.add("CRPIX2", 5.0)
            props.add("CRVAL1", center[0])
            props.add("CRVAL2", center[1])
            props.add("CD1_1", 1.0)
            props.add("CD2_1", 0.0)
            props.add("CD1_2", 0.0)
            props.add("CD2_2", 1.0)
            ps.append(props)
        # Retain one as is, and create an exposure from the other
        inputs = [
            ps[0],
            afw_image.ExposureF(8, 8, afw_image.makeWcs(ps[1]))
        ]
        # Test data-ids are just integers.
        refs = [MockDataRef(i, v) for i, v in enumerate(inputs)]
        config = IndexExposureConfig()
        config.allow_replace = True
        config.defer_writes = True
        config.init_statements = ['PRAGMA page_size = 4096']
        database = sqlite3.connect(":memory:")
        # Avoid the command line parser.
        parsed_cmd = pipe_base.Struct(
            config=config,
            log=Log.getDefaultLogger(),
            doraise=True,
            clobberConfig=False,
            noBackupConfig=False,
            database=database,
            dstype="bogus",
            id=pipe_base.Struct(refList=refs),
        )
        runner = IndexExposureRunner(IndexExposureTask, parsed_cmd)
        runner.run(parsed_cmd)
        # Re-ingest to test that allow_replace=True works. Toggle off
        # the deferred writes to test that as well.
        runner.config.defer_writes = False
        runner.run(parsed_cmd)
        # Re-ingest to test that allow_replace=False raises an exception.
        runner.config.allow_replace = False
        with self.assertRaises(Exception):
            runner.run(parsed_cmd)
        # Now, verify the contents of the database. First, check that
        # data ids are recoverable.
        data_ids = sorted(pickle.loads(str(r[0])) for r in database.execute(
            "SELECT pickled_data_id FROM exposure"))
        self.assertEqual(data_ids, [0, 1])
        # Next, run a spatial query and check that it returns the
        # expected results.
        center = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(4.0, 1.0))
        circle = sphgeom.Circle(center, sphgeom.Angle.fromDegrees(1.5))
        results = find_intersecting_exposures(database, circle)
        self.assertEqual(len(results), 1)
        info = results[0]
        # The first input exposure should have been returned, and
        # should intersect the query region
        self.assertEqual(info.data_id, 0)
        self.assertEqual(circle.relate(info.boundary), sphgeom.INTERSECTS)
        database.close()

    def test_search(self):
        """Test that brute-force and R*Tree search give the same results."""
        # Generate metadata for exposures with centers distributed uniformly
        # at random over the sky.
        task = IndexExposureTask(config=IndexExposureConfig())
        random.seed(31415926)
        results = []
        for data_id in range(1000):
            ra = random.uniform(0.0, 360.0)
            dec = math.degrees(math.asin(random.uniform(-1.0, 1.0)))
            props = daf_base.PropertySet()
            props.add("NAXIS1", 9)
            props.add("NAXIS2", 9)
            props.add("RADECSYS", "ICRS")
            props.add("EQUINOX", 2000.0)
            props.add("CTYPE1", "RA---TAN")
            props.add("CTYPE2", "DEC--TAN")
            props.add("CRPIX1", 5.0)
            props.add("CRPIX2", 5.0)
            props.add("CRVAL1", ra)
            props.add("CRVAL2", dec)
            props.add("CD1_1", 0.25)
            props.add("CD2_1", 0.0)
            props.add("CD1_2", 0.0)
            props.add("CD2_2", 0.25)
            results.append(task.index(props, data_id, None))
        # Persist results
        database = sqlite3.connect(":memory:")
        create_exposure_tables(database)
        store_exposure_info(database, False, results)
        # Compare brute force and R*Tree search results.
        circle = sphgeom.Circle(sphgeom.UnitVector3d.Z(),
                                sphgeom.Angle.fromDegrees(10.0))
        brute_ids = self._brute_search(database, circle)
        rtree_ids = sorted(e.data_id for e in
                           find_intersecting_exposures(database, circle))
        self.assertEqual(brute_ids, rtree_ids)
        database.close()

    def _brute_search(self, conn, region):
        results = []
        query = "SELECT pickled_data_id, encoded_polygon FROM exposure"
        for row in conn.execute(query):
            poly = sphgeom.ConvexPolygon.decode(str(row[1]))
            if region.relate(poly) != sphgeom.DISJOINT:
                results.append(pickle.loads(str(row[0])))
        results.sort()
        return results


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
