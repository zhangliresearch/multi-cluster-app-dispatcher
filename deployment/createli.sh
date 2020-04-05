#!/bin/sh

set -x

kubectl create -f li-awp-100.yaml &
kubectl create -f li-awp-200.yaml &
kubectl create -f li-awp-300.yaml &
kubectl create -f li-awp-400.yaml &
kubectl create -f li-awp-500.yaml &
kubectl create -f li-awp-600.yaml &
kubectl create -f li-awp-700.yaml &
kubectl create -f li-awp-800.yaml &


