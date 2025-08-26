#!/bin/bash

NAMESPACE=ai-storage
COMPONENT=ai-storage-scheduler

while [ -z "$PODNAME" ]
do
    PODNAME=$(kubectl get po -n ${NAMESPACE} -o name --field-selector=status.phase=Running | grep ${COMPONENT})
    PODNAME=${PODNAME##pod/}
done

kubectl logs $PODNAME -n ${NAMESPACE} -f