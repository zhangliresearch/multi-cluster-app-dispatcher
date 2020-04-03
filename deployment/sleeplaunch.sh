#!/bin/sh
set -x

echo `date` "begin sleep"

sleep $2

echo `date` "end   sleep"

kubectl create -f $1

