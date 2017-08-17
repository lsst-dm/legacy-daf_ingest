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
"""This module provides a |task| for spatial afw |exposure| indexing.

:class:`.IndexExposureTask` extracts the WCS from an input exposure and uses
it to compute a corresponding spherical bounding polygon. The exposure data-id
and bounding polygon are then written to an SQLite 3 database.  Fast spatial
queries are supported by maintaining an `R*Tree`_ index over exposures.

.. _`R*Tree`:      https://www.sqlite.org/rtree.html

.. |allow_replace| replace::  :attr:`~.IndexExposureConfig.allow_replace`
.. |configuration| replace::  :class:`configuration <.IndexExposureConfig>`
.. |defer_writes|  replace::  :attr:`~.IndexExposureConfig.defer_writes`
.. |encoded|       replace::  :meth:`encoded <lsst.sphgeom.Region.encode>`
.. |exposure|      replace::  :class:`exposure <lsst.afw.image.ExposureF>`
.. |metadata|      replace::  :class:`metadata <lsst.daf.base.PropertySet>`
.. |pad_pixels|    replace::  :attr:`~.IndexExposureConfig.pad_pixels`
.. |polygon|       replace::  :class:`polygon <lsst.sphgeom.ConvexPolygon>`
.. |run|           replace::  :meth:`~.IndexExposureTask.run`
.. |runner|        replace::  :class:`runner <.IndexExposureRunner>`
.. |task|          replace::  :class:`~lsst.pipe.base.Task`
"""

from collections import namedtuple
import math
try:
    import cPickle as pickle
except:
    import pickle
import sqlite3
import sys
import traceback

import lsst.daf.base as daf_base
import lsst.afw.geom as afw_geom
import lsst.afw.image as afw_image
import lsst.pex.config as pex_config
import lsst.pipe.base as pipe_base
from lsst.log import Log
from lsst.sphgeom import Angle, ConvexPolygon, DISJOINT, UnitVector3d


__all__ = (
    "quote_sqlite3_identifier",
    "create_exposure_tables",
    "ExposureInfo",
    "store_exposure_info",
    "find_intersecting_exposures",
    "IndexExposureConfig",
    "IndexExposureRunner",
    "IndexExposureTask",
)


def quote_sqlite3_identifier(s):
    """Safely quote a string `s` for use as an SQLite 3 identifier.

    Note that `s` is required to be valid UTF-8 (or encodable as such), and
    may not contain embedded NUL characters.

    This function exists because Python DB-API parameter substitution does not
    work for table and column names. Without proper quoting, they present
    an opportunity for SQL injection.
    """
    if isinstance(s, unicode):
        # Convert to a UTF-8 string
        ident = s.encode('utf-8')
    else:
        # Make sure s is valid UTF-8
        ident = s.decode('utf-8').encode('utf-8')
    if ident.find('\x00') >= 0:
        raise RuntimeError('The NUL character is not legal '
                           'in SQLite 3 identifiers')
    # Quote the identifier. Embedded quotes are escaped by doubling them up.
    return '"' + ident.replace('"', '""') + '"'


def create_exposure_tables(database, init_statements=[]):
    """Create SQLite 3 exposure index tables.

    One table, ``exposure``, contains exposure data-ids and boundaries,
    and the other, ``exposure_rtree``, is an `R*Tree`_ of 3-D exposure
    bounding boxes.

    Parameters
    ----------

    database : sqlite3.Connection or str
        A connection to (or filename of) a SQLite 3 database.

    init_statements : iterable
        A series of database initialization statements (strings) to execute.

    .. _`R*Tree`:      https://www.sqlite.org/rtree.html
    """
    if isinstance(database, sqlite3.Connection):
        conn = database
    else:
        conn = sqlite3.connect(database)
    with conn:
        for statement in init_statements:
            conn.execute(statement)
        conn.execute(
            'CREATE VIRTUAL TABLE IF NOT EXISTS exposure_rtree USING rtree(\n'
            '    rowid,\n'
            '    x_min, x_max,\n'
            '    y_min, y_max,\n'
            '    z_min, z_max\n'
            ')'
        )
        conn.execute(
            'CREATE TABLE IF NOT EXISTS exposure (\n'
            '    rowid INTEGER PRIMARY KEY,\n'
            '    pickled_data_id BLOB NOT NULL UNIQUE,\n'
            '    encoded_polygon BLOB NOT NULL\n'
            ')'
        )


ExposureInfo = namedtuple('ExposureInfo', ['data_id', 'boundary'])


def store_exposure_info(database, allow_replace, exposure_info):
    """Store exposure data-ids and bounding polygons in the given database.

    The database is assumed to have been initialized via
    :func:`.create_exposure_tables`.

    Parameters
    ----------

    database : sqlite3.Connection or str
        A connection to (or filename of) a SQLite 3 database.

    allow_replace : bool
        If ``True``, information for previously stored exposures with matching
        data-ids will be overwritten.

    exposure_info : iterable or lsst.daf.ingest.indexExposure.ExposureInfo
        One or more :class:`.ExposureInfo` objects to persist. Their
        ``data_id`` attributes must be pickled data-ids, and their
        ``boundary`` attributes must be |encoded| |polygon| objects.
    """
    if isinstance(database, sqlite3.Connection):
        conn = database
    else:
        conn = sqlite3.connect(database)
    with conn:
        cursor = conn.cursor()
        if isinstance(exposure_info, ExposureInfo):
            exposure_info = (exposure_info,)
        # Insert or update information in database
        for info in exposure_info:
            if info is None:
                continue
            # In Python 2, the sqlite3 module maps between Python buffer
            # objects and BLOBs. When migrating to Python 3, the buffer()
            # calls should be removed (sqlite3 maps bytes objects to BLOBs).
            pickled_data_id = buffer(info.data_id)
            encoded_polygon = buffer(info.boundary)
            bbox = ConvexPolygon.decode(info.boundary).getBoundingBox3d()
            if allow_replace:
                # See if there is already an entry for the given data id.
                cursor.execute(
                    'SELECT rowid FROM exposure WHERE pickled_data_id = ?',
                    (pickled_data_id,)
                )
                results = cursor.fetchall()
                if len(results) > 0:
                    # If so, update spatial information for the exposure.
                    row_id = results[0][0]
                    cursor.execute(
                        'UPDATE exposure\n'
                        '    SET encoded_polygon = ?\n'
                        '    WHERE rowid = ?',
                        (encoded_polygon, row_id)
                    )
                    cursor.execute(
                        'UPDATE exposure_rtree SET\n'
                        '    x_min = ?, x_max = ?,\n'
                        '    y_min = ?, y_max = ?,\n'
                        '    z_min = ?, z_max = ?\n'
                        'WHERE rowid = ?',
                        (bbox.x().getA(), bbox.x().getB(),
                         bbox.y().getA(), bbox.y().getB(),
                         bbox.z().getA(), bbox.z().getB(),
                         row_id)
                    )
                    return
            # Insert the data id and corresponding spatial information.
            cursor.execute(
                'INSERT INTO exposure\n'
                '    (pickled_data_id, encoded_polygon)\n'
                '    VALUES (?, ?)',
                (pickled_data_id, encoded_polygon)
            )
            row_id = cursor.lastrowid
            cursor.execute(
                'INSERT INTO exposure_rtree\n'
                '     (rowid, x_min, x_max, y_min, y_max, z_min, z_max)\n'
                '     VALUES (?, ?, ?, ?, ?, ?, ?)',
                (row_id,
                 bbox.x().getA(), bbox.x().getB(),
                 bbox.y().getA(), bbox.y().getB(),
                 bbox.z().getA(), bbox.z().getB())
            )


def find_intersecting_exposures(database, region):
    """Find exposures that intersect a spherical region.

    Parameters
    ----------

    database : sqlite3.Connection or str
        A connection to (or filename of) a SQLite 3 database containing
        an exposure index.

    region : lsst.sphgeom.Region
        The spherical region of interest.

    Returns
    -------

        A list of :class:`.ExposureInfo` objects corresponding to the
        exposures intersecting `region`.  Their ``data_id`` attributes
        are data-id objects that can be passed to a butler to retrieve
        the corresponding exposure, and their ``boundary`` attributes
        are |polygon| objects.
    """
    if isinstance(database, sqlite3.Connection):
        conn = database
    else:
        conn = sqlite3.connect(database)
    query = ("SELECT pickled_data_id, encoded_polygon\n"
             "FROM exposure JOIN exposure_rtree USING (rowid)\n"
             "WHERE x_min < ? AND x_max > ? AND\n"
             "      y_min < ? AND y_max > ? AND\n"
             "      z_min < ? AND z_max > ?")
    bbox = region.getBoundingBox3d()
    params = (bbox.x().getB(), bbox.x().getA(),
              bbox.y().getB(), bbox.y().getA(),
              bbox.z().getB(), bbox.z().getA())
    results = []
    for row in conn.execute(query, params):
        # Note that in Python 2, BLOB columns are mapped to Python buffer
        # objects, and so a conversion to str is necessary. In Python 3,
        # BLOBs are mapped to bytes directly, and the str() calls must
        # be removed.
        poly = ConvexPolygon.decode(str(row[1]))
        if region.relate(poly) != DISJOINT:
            results.append(ExposureInfo(pickle.loads(str(row[0])), poly))
    return results


class IndexExposureConfig(pex_config.Config):
    """Configuration for :class:`.IndexExposureTask`."""

    allow_replace = pex_config.Field(
        "Allow replacement of previously indexed exposures",
        bool, default=False
    )

    init_statements = pex_config.ListField(
        "List of initialization statements (e.g. PRAGMAs) to run when the "
        "SQLite 3 database is first created. Useful for performance tuning.",
        str, default=[]
    )

    defer_writes = pex_config.Field(
        "If False, then exposure information is inserted directly into the "
        "database by IndexExposureTask. Otherwise, all exposure index "
        "information is collected by IndexExposureRunner and inserted after "
        "task processing is complete. This avoids an implicit serialization "
        "point in task processing (SQLite 3 does not support concurrent "
        "database writes), and amortizes the overhead of connecting and "
        "running a transaction over many inserts.",
        bool, default=True
    )

    pad_pixels = pex_config.Field(
        "Number of pixels by which the pixel-space bounding box of an "
        "exposure is grown before it is converted to a spherical polygon. "
        "Positive values expand the box, while negative values shrink it. "
        "If the padded box is empty, the corresponding exposure will not "
        "appear in the index.",
        int, default=0
    )


class IndexExposureRunner(pipe_base.TaskRunner):
    """Runner for :class:`.IndexExposureTask`."""

    @staticmethod
    def getTargetList(parsed_cmd):
        """Add additional |run| method arguments by overloading |getTargetList|.

        .. |getTargetList| replace::
            :meth:`~lsst.pipe.base.TaskRunner.getTargetList`
        """
        return pipe_base.TaskRunner.getTargetList(
            parsed_cmd,
            dstype=parsed_cmd.dstype,
            database=parsed_cmd.database
        )

    def precall(self, parsed_cmd):
        """Prepare for task execution.

        This override of |precall|:

        - sets the task's name appropriately
        - does not write task schemata
        - attempts to write a task configuration (success is not required)
        - initializes the SQLite 3 output database

        .. |precall| replace:: :meth:`~lsst.pipe.base.TaskRunner.precall`
        """
        self.TaskClass._DefaultName += "_" + parsed_cmd.dstype
        task = self.makeTask()
        try:
            task.writeConfig(parsed_cmd.butler, clobber=self.clobberConfig)
        except Exception, e:
            # Often no mapping for config, but in any case just skip
            task.log.warn("Could not persist config: %s" % (e,))
        create_exposure_tables(parsed_cmd.database,
                               self.config.init_statements)
        return True

    def run(self, parsed_cmd):
        """Run the task on all targets.

        If the |defer_writes| configuration parameter is ``True``, then
        exposure information is stored after all targets have finished
        running.
        """
        results = pipe_base.TaskRunner.run(self, parsed_cmd)
        if self.config.defer_writes:
            store_exposure_info(
                parsed_cmd.database, self.config.allow_replace, results)

    def __call__(self, args):
        """Run the task on a single target.

        This implementation is nearly equivalent to the overridden one, but
        it never writes out metadata and always returns results. For memory
        efficiency reasons, the return value is exactly the one of |run|,
        rather than a :class:`~lsst.pipe.base.Struct` wrapped around it.
        """
        data_ref, kwargs = args
        if self.log is None:
            self.log = Log.getDefaultLogger()
        if hasattr(data_ref, "dataId"):
            self.log.MDC("LABEL", str(data_ref.dataId))
        elif isinstance(data_ref, (list, tuple)):
            self.log.MDC("LABEL", str([ref.dataId for ref in data_ref if hasattr(ref, "dataId")]))
        task = self.makeTask(args=args)
        result = None
        try:
            result = task.run(data_ref, **kwargs)
        except Exception, e:
            if self.doRaise:
                raise
            if hasattr(data_ref, "dataId"):
                task.log.fatal("Failed on dataId=%s: %s" % (data_ref.dataId, e))
            elif isinstance(data_ref, (list, tuple)):
                task.log.fatal("Failed on dataId=[%s]: %s" %
                               (",".join([str(_.dataId) for _ in data_ref]), e))
            else:
                task.log.fatal("Failed on dataRef=%s: %s" % (data_ref, e))
            if not isinstance(e, pipe_base.TaskError):
                traceback.print_exc(file=sys.stderr)
        return result


class IndexExposureTask(pipe_base.CmdLineTask):
    r"""A |task| for spatially indexing an afw |exposure| using SQLite 3.

    This task extracts the WCS from an input exposure and uses it to compute
    a corresponding spherical bounding polygon. The exposure data-id and
    bounding polygon are ether written to an SQLite 3 database or returned.
    Both values are stored as binary strings - the data-id is pickled, and
    the bounding polygon is |encoded|.

    This avoids the complexity of performing ORM on the data-id, but
    also limits the kinds of queries that can be run on the database
    to essentially just spatial ones. (Adding ORM for the data-id would allow
    additional potentially useful query constraints to be expressed, like
    restrictions on filter, visit number, etc...)

    Additionally, a 3-D bounding box for each exposure is stored in an SQLite
    `R*Tree`_, allowing for fast spatial exposure queries.

    If run from the command line, this task will index each exposure specified
    by a data id and dataset type. As usual for tasks, multiple ``--id``
    options may be specified, or ranges and lists of values can be specified
    for data id keys.

    Note that while |exposure| objects can be processed directly, it is more
    efficient to process exposure |metadata| (e.g. by specifying a dataset
    type of ``"calexp_md"`` rather than ``"calexp"``) since that avoids
    reading pixel data not needed by this task.

    The :meth:`.index` method can also be called manually, in which case an
    |exposure| or |metadata| object must be passed in directly. Note that a
    data id is still required, just as for :meth:`.run`, and that the database
    must be initialized beforehand by calling :func:`.create_exposure_tables`.

    Once an exposure index has been produced, other pipeline tasks (like the
    ones responsible for coaddition) can use it to quickly locate exposures
    overlapping a particular part of the sky by calling
    :func:`.find_intersecting_exposures`.

    To allow pre-existing exposure index information to be overwritten, set
    the |allow_replace| |configuration| parameter to ``True``. By default,
    attempting to index the same exposure twice will result in an error.

    The |pad_pixels| parameter can be used to grow (or shrink, if the value
    is negative) the pixel space bounding box for an exposure before it is
    converted to a spherical bounding polygon.

    Finally, set |defer_writes| to ``False`` to execute SQLite database writes
    directly from the task. Normally, database writes are executed by
    :class:`.IndexTaskRunner` after all bounding polygons have been computed.
    This allows for parallel task execution and speeds up database writes (since
    many rows can be inserted in a single transaction, and since SQLite 3 does
    not support concurrent writers).

    Examples
    --------

    A sample task invocation that indexes some of the LSST DM stack demo output
    is:

    .. prompt:: bash

        $DAF_INGEST_DIR/bin/indexExposure.py \
                $LSST_DM_STACK_DEMO_DIR/output \
                --database calexp.sqlite3 \
                --dstype calexp_md \
                --id filter=g

    .. _`R*Tree`:      https://www.sqlite.org/rtree.html
    """

    ConfigClass = IndexExposureConfig
    _DefaultName = "indexExposure"
    RunnerClass = IndexExposureRunner

    @classmethod
    def _makeArgumentParser(cls):
        """Extend the default argument parser.

        Arguments specifying the output SQLite database and exposure dataset
        type are added in.
        """
        parser = pipe_base.ArgumentParser(name=cls._DefaultName)
        parser.add_argument(
            '--database', dest='database', required=True,
            help='SQLite 3 database file name')
        # Use DatasetArgument to require dataset type be specified on
        # the command line
        parser.add_id_argument(
            '--id', pipe_base.DatasetArgument('dstype'),
            help='Dataset data id to index')
        return parser

    def run(self, data_ref, dstype, database):
        """Index an exposure specified by a data ref and dataset type."""
        return self.index(data_ref.get(dstype), data_ref.dataId, database)

    def index(self, exposure_or_metadata, data_id, database):
        """Spatially index an |exposure| or |metadata| object.

        Parameters
        ----------

        exposure_or_metadata : lsst.afw.image.Exposure[DFILU] or lsst.daf.base.PropertySet
            An afw |exposure| or corresponding |metadata| object.

        data_id : object
            An object identifying a single exposure (e.g. as used by the
            butler). It must be possible to pickle `data_id`.

        database : sqlite3.Connection or str
            A connection to (or filename of) a SQLite 3 database.

        Returns
        -------

        ``None``, unless the |defer_writes| coniguration parameter is ``True``.
        In that case, an :class:`.ExposureInfo` object containing a pickled
        data-id and an |encoded| |polygon| is returned.
        """
        # Get a pixel index bounding box for the exposure.
        if isinstance(exposure_or_metadata, daf_base.PropertySet):
            md = exposure_or_metadata
            # Map (LTV1, LTV2) to LSST (x0, y0). LSST convention says that
            # (x0, y0) is the location of the sub-image origin (the bottom-left
            # corner) relative to the origin of the parent, whereas LTVi encode
            # the origin of the parent relative to the origin of the subimage.
            pixel_bbox = afw_image.bboxFromMetadata(md)
            wcs = afw_image.makeWcs(md, False)
        else:
            pixel_bbox = exposure_or_metadata.getBBox()
            wcs = exposure_or_metadata.getWcs()
        # Pad the box by a configurable amount and bail if the result is empty.
        pixel_bbox.grow(self.config.pad_pixels)
        if pixel_bbox.isEmpty():
            self.log.warn("skipping exposure indexing for dataId=%s: "
                          "empty bounding box", data_id)
            return
        corners = []
        for c in pixel_bbox.getCorners():
            # Convert the box corners from pixel indexes to pixel positions,
            # and then to sky coordinates.
            c = wcs.pixelToSky(afw_image.indexToPosition(c.getX()),
                               afw_image.indexToPosition(c.getY()))
            c = (c.getLongitude().asRadians(), c.getLatitude().asRadians())
            # Bail if any coordinate is not finite.
            if any(math.isinf(x) or math.isnan(x) for x in c):
                self.log.warn("skipping exposure indexing for dataId=%s: "
                              "NaN or Inf in bounding box sky coordinate(s)"
                              " - bad WCS?", data_id)
                return
            # Convert from sky coordinates to unit vectors.
            corners.append(UnitVector3d(Angle.fromRadians(c[0]),
                                        Angle.fromRadians(c[1])))
        # Create a convex polygon containing the exposure pixels. When sphgeom
        # gains support for non-convex polygons, this could be changed to map
        # exposure.getPolygon() to a spherical equivalent, or to subdivide box
        # edges in pixel space to account for non linear projections. This
        # would have higher accuracy than the current approach of connecting
        # corner sky coordinates with great circles.
        poly = ConvexPolygon(corners)
        # Finally, persist or return the exposure information.
        info = ExposureInfo(pickle.dumps(data_id), poly.encode())
        if self.config.defer_writes:
            return info
        store_exposure_info(database, self.config.allow_replace, info)
