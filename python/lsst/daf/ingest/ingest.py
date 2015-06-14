# 
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2012 LSST Corporation.
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
from itertools import izip
import os, os.path
import re
import shlex
import sys

import lsst.daf.persistence as dafPersist
from .mysqlExecutor import addDbOptions
from .datasetScanner import parseDataIdRules

__all__ = ["makeArgumentParser",
           "makeRules",
          ]

def _line_to_args(self, line):
    for arg in shlex.split(line, comments=True, posix=True):
        if not arg.strip():
            continue
        yield arg


def makeArgumentParser(description, inRootsRequired=True, addRegistryOption=True):
    parser = argparse.ArgumentParser(
        description=description,
        fromfile_prefix_chars="@",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Data IDs are expected to be of the form:\n"
               "  --id k1=v11[^v12[...]] [k2=v21[^v22[...]]\n"
               "\n"
               "Examples:\n"
               "  1. --id visit=12345 raft=1,1\n"
               "  2. --id skyTile=12345\n"
               "  3. --id visit=12 raft=1,2^2,2 sensor=1,1^1,3\n"
               "\n"
               "The first example identifies all sensors in raft 1,1 of visit "
               "12345. The second identifies all sources/objects in sky-tile "
               "12345. The cross product is computed for keys with multiple "
               "values, so the third example is equivalent to:\n"
               "  --id visit=12 raft=1,2 sensor=1,1\n"
               "  --id visit=12 raft=1,2 sensor=1,3\n"
               "  --id visit=12 raft=2,2 sensor=1,1\n"
               "  --id visit=12 raft=2,2 sensor=1,3\n"
               "\n"
               "Redundant specification of a data ID will *not* result in "
               "loads of duplicate data - data IDs are de-duped before ingest "
               "starts. Note also that the keys allowed in data IDs are "
               "specific to the type of data the ingestion script deals with. "
               "For example, one cannot load sensor metadata by sky tile ID, nor "
               "sources by sensor (CCD) ID.\n"
               "\n"
               "Any omitted keys are assumed to take all legal values. So, "
               "`--id raft=1,2` identifies all sensors of raft 1,2 for all "
               "available visits.\n"
               "\n"
               "Finally, calling an ingestion script multiple times is safe. "
               "Attempting to load the same data item twice will result "
               "in an error or (if --strict is specified) or cause previously "
               "loaded data item to be skipped. Database activity is strictly "
               "append only.")
    parser.convert_arg_line_to_args = _line_to_args
    addDbOptions(parser)
    parser.add_argument(
        "-d", "--database", dest="database",
        help="MySQL database to load CSV files into.")
    parser.add_argument(  
        "-s", "--strict", dest="strict", action="store_true",
        help="Error out if previously ingested, incomplete, or otherwise "
             "suspicious data items are encountered (by default, these are "
             "skipped). Note that if --database is not supplied, detecting "
             "previously ingested data is not possible.")
    parser.add_argument(
        "-i", "--id", dest="id", nargs="+", default=None, action="append",
        help="Data ID specifying what to ingest. May appear multiple times.")
    if addRegistryOption:
        parser.add_argument(
            "-R", "--registry", dest="registry", help="Input registry path; "
            "used for all input roots. If omitted, a file named registry.sqlite3 "
            "must exist in each input root.")
    parser.add_argument(
        "outroot", help="Output directory for CSV files")
    parser.add_argument(
        "inroot", nargs="+" if inRootsRequired else "*",
        help="One or more input root directories")
    return parser


def makeRules(dataIdSpecs, camera, validKeys):
    """Return a list of data ID rules from command line --id specifications.
    Ensures only the given keys are referenced by the data id specs."""
    if not dataIdSpecs:
        return None
    rules = []
    for id in dataIdSpecs:
        r = parseDataIdRules(id, camera)
        for k in r:
            if k not in validKeys:
                raise RuntimeError(k + " is not a legal data ID key for this ingest script")
        rules.append(r)
    return rules

