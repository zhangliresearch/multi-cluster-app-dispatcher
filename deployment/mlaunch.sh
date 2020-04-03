#!/bin/sh

#set -x

rm -f out

pods=`kubectl get pods | awk '{if (NR>1) print $1}'`
echo `date` " -- All pods:" $pods
set -- $pods
echo `date` " -- Number of pods:" "$#"
while [ "$#" -gt 0 ] 
do 
  cpods=`kubectl get pods | grep "Completed" | sed 's/hello//' | awk 'BEGIN{FS="-"}{print $1 "-" $2 "-" $3}' | sort -u`
  set -- $cpods
  echo `date` " -- Completed pods:" $@
  for name in $@
  do
    awfile=$name.yaml
    echo `date` " -- Deleting " $awfile
    kubectl delete -f $awfile
    /root/zhangli/code/src/github/IBM/multi-cluster-app-dispatcher/deployment/sleeplaunch.sh $awfile 10 > out &>/dev/null & 
  done
  sleep 2
  pods=`kubectl get pods | awk '{if (NR>1) print $1}'`
  echo `date` " -- All pods:" $pods
  set -- $pods
  echo `date` " -- Number of pods=" $#
done
echo "No pods left."
