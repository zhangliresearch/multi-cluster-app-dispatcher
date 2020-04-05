#!/bin/sh

#set -x

name=`kubectl get pods -n kube-system | grep xqueuejob | awk '{print $1}'`

echo "kubectl delete pod $name -n kube-system"
      kubectl delete pod $name -n kube-system 
 
