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

"""readRegistry.py INPUTREGISTRY 

This command reads an ImSim INPUTREGISTRY and generates the input file,  
required by the JOBOFFICE, which  contains one line per visit/raft/ccd and terminates with 10 lines of null entries

"""
import subprocess
from optparse import OptionParser
import os

# sql = 'sqlite3 -column /lsst2/imsim-TuesdayRuns/imSim/registry.sqlite3 \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\''
#sql = 'sqlite3 -column /lsst2/imsim-VariabilityTest/imSim/registry.sqlite3 \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\''
#sql = 'sqlite3 -column /lsst2/imsim-TuesdayRuns/imSim-05Apr2011/registry.sqlite3 \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\''

def main(inputRegistry):
    #sql = 'sqlite3 -column %s \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\'' %(inputRegistry)
    sql = 'sqlite3 -column %s \'select r.visit, r.raft, r.sensor, s.skyTile, r.visit %% 10 as clflag  from raw as r, raw_skyTile as s where (r.id = s.id) and (r.channel="0,0") and (r.snap=0) group by r.id order by clflag desc, s.skyTile;\'' %(inputRegistry)
    p = subprocess.Popen(sql, shell=True, stdout=subprocess.PIPE)
    results = p.stdout.readlines()
    p.stdout.close()

    print ">intids visit"
    for result in results:
        visit, raft, ccd, skytile, clflag = result.split()
        print "raw visit=%s raft=%s sensor=%s" % (visit, raft, ccd)
    for i in range(0,10):
        print "raw visit=0         raft=0   sensor=0"
    

if __name__ == "__main__":
    parser = OptionParser(usage="""\
usage: %prog INPUTREGISTRY 

INPUTREGISTRY contains image data identification.""")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Missing input registry")
    inputRegistry = args[0]
    if not os.path.exists(inputRegistry):
        raise RuntimeError, "Missing input registry : %s" % (inputRegistry,)
    main(args[0])

