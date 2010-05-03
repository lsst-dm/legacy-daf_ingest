#! /bin/sh
set -e
RUNID=isr43
repository=/lsst/DC3/data/obstest/ImSim

pipeline=ISR
broker=lsst8
availtopic=RawAvailable
stoptopic=JobOfficeStop
jobofficepol=imsim-isr-joboffice.paf

echo Running $pipeline pipeline in directory, $RUNID

# doin' some orchestration
mkdir -p $RUNID
(cd $RUNID && mkdir -p input output scratch update work)
[ -e "$RUNID/input/raw" ]  || ln -s $repository/raw  $RUNID/input/raw
[ -e "$RUNID/input/dark" ] || ln -s $repository/dark $RUNID/input/dark
[ -e "$RUNID/input/bias" ] || ln -s $repository/bias $RUNID/input/bias
[ -e "$RUNID/input/flat" ] || ln -s $repository/flat $RUNID/input/flat
if [ -e "$RUNID/work/$pipeline-joboffice" ]; then
   rm -rf $RUNID/work/$pipeline-joboffice
fi

joboffice.py -D -L verb2 -r $RUNID -b $broker -d $RUNID/work $jobofficepol
sleep 2
launchPipeline.py -L debug imsim-isr-master.paf $RUNID | grep -v Shutdown &

set +e
announceDataset.py -r $RUNID -b $broker -t $availtopic inputdata.txt
sleep 15

# ps -auxww | grep runPipeline.py | grep $RUNID
pid=`ps -auxww | grep runPipeline.py | grep $RUNID | awk '{print $2}'`
echo kill $pid
kill $pid
sendevent.py -b $broker -r $RUNID stop $stoptopic
sleep 5
rm -rf $pipeline-joboffice
