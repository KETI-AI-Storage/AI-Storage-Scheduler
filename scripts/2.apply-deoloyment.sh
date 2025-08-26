#!/usr/bin/env bash
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# $1 is apply/a or delete/d

if [ "$1" == "delete" ] || [ "$1" == "d" ]; then    
    echo "Running: kubectl delete -f $dir/../deployments"
    kubectl delete -f "$dir/../deployments"

elif [ "$1" == "apply" ] || [ "$1" == "a" ]; then
    echo "Running: kubectl apply -f $dir/../deployments"
    kubectl apply -f "$dir/../deployments"

else
    echo "Usage: $0 [apply|a|delete|d]"
    exit 1
fi