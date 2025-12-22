#!/bin/bash
# AI Storage Scheduler - 로그 조회 스크립트

APP_NAME="ai-storage-scheduler"
NAMESPACE="keti"

# Pod 찾기
POD=$(kubectl get pods -n $NAMESPACE -l app=$APP_NAME -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$POD" ]; then
    echo "Error: $APP_NAME pod not found in namespace $NAMESPACE"
    exit 1
fi

echo "=== $APP_NAME 로그 ==="
echo "Pod: $POD"
echo "Namespace: $NAMESPACE"
echo "========================"
echo ""

# 옵션 처리
if [ "$1" == "-f" ] || [ "$1" == "--follow" ]; then
    kubectl logs -n $NAMESPACE $POD -f
elif [ "$1" == "--tail" ]; then
    LINES=${2:-100}
    kubectl logs -n $NAMESPACE $POD --tail=$LINES
else
    kubectl logs -n $NAMESPACE $POD --tail=100
fi
