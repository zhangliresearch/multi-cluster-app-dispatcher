#!/bin/sh

#set -x

rm -f out
for i in `seq 1 100`
do
  ii=`printf '%02d' $i`
  ij=`printf '%02d' $((100-i))`
  echo li-awp-$ii.yaml
  rm -f li-awp-$ii.yaml
  cat li-awp-START.yaml | sed "s/START/$ii/g" | sed "s/SLOPE/$ij/g" > li-awp-$ii.yaml 
done

