#!/bin/sh

#set -x

# make sure 2 digits for file name
echo "file=li-awp-$1.yaml loops=$2"

# delete li-awp-$1.yaml first
pod=`kubectl get pods | grep "awp-$1-"`
echo "j=$j current pod=$pod|"
if [ -n "$pod" ]; then
  echo "kubectl delete -f li-awp-$1.yaml"
  kubectl delete -f li-awp-$1.yaml
fi

# create and delete $2 times
for j in `seq 1 $2`
do
  # check there is no current pod
  pod=`kubectl get pods | grep "awp-$1-"`
  echo "j=$j current pod=$pod|"
  while [ -n "$pod" ]
  do
    sleep 1
    pod=`kubectl get pods | grep "awp-$1-"`
    echo "j=$j current pod=$pod|"
  done
  echo "no current pod=$pod| create li-apw-$1.yaml"

  sleep 1
  # create from file li-awp-$1.yaml
  kubectl create -f li-awp-$1.yaml

  # wait until complete
  cpod=""
  while [ -z "$cpod" ]
  do
    pod=`kubectl get pods | grep "awp-$1-"`
    echo "any=$pod"
    cpod=`kubectl get pods | grep "awp-$1-" | grep "Completed" | awk '{print $1}'`
    echo "Completed=$cpod|"
    sleep 1
  done

  # delete li-awp-$1.yaml after completion
  echo "j=$j completed pod=$cpod delete li-awp-$1.yaml"
  kubectl delete -f li-awp-$1.yaml
done  # for j

