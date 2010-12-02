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

""" jobsleft.py JOB_DIRECTORY_NAME

Take the jobs that are left in the jobsInProgress and jobsAvailable of the joboffice, sort them, and create a new input list to be used to feed to orca
The result is written to stdout.


example:

$ jobsleft.py /cfs/scratch/users/daues/lsst/datarel-runs/pt1prod_im0063
>intids visit
raw sensor=0,0 visit=85661762 raft=0,1
raw sensor=0,1 visit=85661762 raft=0,1
raw sensor=0,2 visit=85661762 raft=0,1
raw sensor=1,0 visit=85661762 raft=0,1
raw sensor=1,1 visit=85661762 raft=0,1
raw sensor=1,2 visit=85661762 raft=0,1
raw sensor=2,0 visit=85661762 raft=0,1
raw sensor=2,1 visit=85661762 raft=0,1
raw sensor=2,2 visit=85661762 raft=0,1
raw sensor=0,0 visit=85661762 raft=0,2
raw sensor=0,1 visit=85661762 raft=0,2
raw sensor=0,2 visit=85661762 raft=0,2
raw sensor=1,0 visit=85661762 raft=0,2
raw sensor=1,1 visit=85661762 raft=0,2
raw sensor=1,2 visit=85661762 raft=0,2
raw sensor=2,0 visit=85661762 raft=0,2
raw sensor=2,1 visit=85661762 raft=0,2
raw sensor=2,2 visit=85661762 raft=0,2
raw sensor=0,0 visit=85661762 raft=0,3
raw sensor=0,1 visit=85661762 raft=0,3
$

"""

import os, sys
import re
import operator

from lsst.pex.policy import Policy

rundir = sys.argv[1]

jobOfficeDir = "work/joboffices_1/PT1Pipe-joboffice"
jobsInProgress = rundir+"/"+jobOfficeDir+"/jobsInProgress"

# jobs in progress
filenames = os.listdir(jobsInProgress)
jobs = []
for filename in filenames:
	matchObj = re.match(r'(.*)\.paf', filename)
	# if this is a paf file, add it to the list
	if matchObj != None:
		jobObj = re.match(r'Job-(.*)\.paf', filename)
		jobs.append((jobsInProgress+"/"+filename, int(jobObj.group(1))))


# jobs available
jobsAvailable = rundir+"/"+jobOfficeDir+"/jobsAvailable"
filenames = os.listdir(jobsAvailable)
for filename in filenames:
	matchObj = re.match(r'(.*)\.paf', filename)
	# if this is a paf file, add it to the list
	if matchObj != None:
		jobObj = re.match(r'Job-(.*)\.paf', filename)
		jobs.append((jobsAvailable+"/"+filename, int(jobObj.group(1))))

# sort the job files based on the value of the job number
jobs = sorted(jobs, key=lambda val:val[1])

# for each of the jobs in this sorted list, open them up and get
# the INPUT policy out of each of them.

print ">intids visit"
for job in jobs:
	filename = job[0]
	pol = Policy(filename)
	type = pol.get("INPUT.type")
        ids = pol.get("INPUT.ids")
	print type,
	for name in ids.names():
		val = ids.get(name)
		print name+"="+str(val),
	print
