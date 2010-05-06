#! /bin/sh
set -e
if [ $# -gt 0 ]; then
   RUNID=$1
else 
   RUNID=ca42
fi
RunDir="CFHTRun"
repository=/lsst/DC3/data/obstest/CFHTLS

pipeline=CA
broker=lsst8
availtopic=RawAvailable
stoptopic=JobOfficeStop
jobofficepol=cfht-ca-joboffice.paf

echo Running $pipeline pipeline in directory, $RUNID

# doin' some orchestration
mkdir -p $RunDir
(cd $RunDir && mkdir -p output scratch update work)
[ -e "$RunDir/input/D2" ] || ln -s $repository/D2 $RunDir/input/D2
if [ -e "$RunDir/work/$pipeline-joboffice" ]; then
   rm -rf $RunDir/work/$pipeline-joboffice
fi

joboffice.py -D -L verb2 -r $RUNID -b $broker -d $RunDir/work $jobofficepol
sleep 2
launchPipeline.py -L debug cfht-ca-master.paf $RUNID $pipeline | grep -v Shutdown &

set +e
announceDataset.py -r $RUNID -b $broker -t $availtopic cfht-ca-inputdata.txt
sleep 15

# ps -auxww | grep runPipeline.py | grep $RUNID
#pid=`ps -auxww | grep runPipeline.py | grep $RUNID | awk '{print $2}'`
#echo kill $pid
#kill $pid
#sendevent.py -n $pipeline -b $broker -r $RUNID stop $stoptopic
#sleep 5
#rm -rf $pipeline-joboffice
