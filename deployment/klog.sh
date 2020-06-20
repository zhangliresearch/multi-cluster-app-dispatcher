#!/bin/sh

#set -x

name=`kubectl get pods -n kube-system | grep xqueuejob | awk '{print $1}'`

echo "kubectl logs $name -n kube-system > $1"
      kubectl logs $name -n kube-system > $1
# echo "grep 100-90 $1 > 100"
#       grep "100-90" $1 > 100
 
