#!/bin/sh

#set -x

#kubectl get appwrappers -o yaml | grep "system\|queue\|name:"| grep -v "       " | awk '{a[NR%3]=$0 ; if (NR%3==0) print a[1] "\t" a[2] "\t" a[0]}'
kubectl get appwrappers -o yaml | grep "system\|queue\|name:"| grep -v "       " | awk -f demo.awk -

 
