#!/bin/sh

#set -x

rm -f *.out

for i in `seq 1 100`
do
  # make sure 2 digits for filename
  ii=`printf '%02d' $i`
  ls -l li-awp-$ii.yaml

  /root/zhangli/data/stresstest/loop.sh $ii 1 > $ii.out 2>&1 &

done

