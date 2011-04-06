import subprocess

# sql = 'sqlite3 -column /lsst2/imsim-TuesdayRuns/imSim/registry.sqlite3 \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\''
sql = 'sqlite3 -column /lsst2/imsim-VariabilityTest/imSim/registry.sqlite3 \'select visit,raft,sensor from raw where channel="0,0" and snap=0;\''

p = subprocess.Popen(sql, shell=True, stdout=subprocess.PIPE)
results = p.stdout.readlines()
p.stdout.close()

for result in results:
    visit, raft, ccd = result.split()
    print "raw visit=%s raft=%s sensor=%s" % (visit, raft, ccd)
    
