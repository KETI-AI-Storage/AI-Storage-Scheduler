#!/usr/bin/env bash

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# $1 is apply/a or delete/d

if [ "$1" == "delete" ] || [ "$1" == "d" ]; then
    echo "================================"
    echo "Deleting AI Storage Scheduler deployment..."
    echo "================================"
    kubectl delete -f "$dir/../deployments/ai-storage-scheduler.yaml"

    if [ $? -eq 0 ]; then
        echo "Deployment deleted successfully"
    else
        echo "Error: Deployment deletion failed"
        exit 1
    fi

elif [ "$1" == "apply" ] || [ "$1" == "a" ]; then
    echo "================================"
    echo "Applying AI Storage Scheduler deployment..."
    echo "================================"
    kubectl apply -f "$dir/../deployments/ai-storage-scheduler.yaml"

    if [ $? -eq 0 ]; then
        echo "Deployment applied successfully"
        echo ""
        echo "Checking pod status..."
        sleep 3
        kubectl get pods -n keti -l app=ai-storage-scheduler
    else
        echo "Error: Deployment apply failed"
        exit 1
    fi

else
    echo "Usage: $0 [apply|a|delete|d]"
    echo ""
    echo "Examples:"
    echo "  $0 apply    # Apply deployment"
    echo "  $0 a        # Apply deployment (short)"
    echo "  $0 delete   # Delete deployment"
    echo "  $0 d        # Delete deployment (short)"
    exit 1
fi
