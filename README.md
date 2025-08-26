# Introduction of Deployment for AI Storage Scheduler
-------------
AI Storage Scheduler for feature classification and learning-based, policy-driven scheduling

Developed by KETI

![Architecture](image.png)

## Contents
-------------
[1. Requirement](#requirement)

[2. Node Requirement](#node-requirement)

[3. How To Run](#module-description-and-how-to-run-pod)

[4. How To Debug](#how-to-debug)

[5. Governance](#governance)


## Requirement
>  kubernetes - <br>
>  containerd - <br>
>  kubeflow - <br>
>  go - <br>

## Node Requirement
### Control Plain Requirement
```bash
kubectl label nodes ai-storage-master layer=orchestration
kubectl label nodes ai-storage-master node-role.kubernetes.io/control-plane=
```
### Compute Node Requirement
```bash
kubectl label nodes {nodename} layer=compute
kubectl label node {nodename} node-role.kubernetes.io/worker=
```
### Storage Node Requirement
```bash
kubectl label nodes {nodename} layer=storage
kubectl label node {nodename} node-role.kubernetes.io/worker=
```

## Module Description And How To Run Pod
```bash
git clone https://github.com/KETI-AI-Storage/AI-Storage-API-Server.git
cd AI-Storage-API-Server
```

### OpenCSD AI Storage Scheduler
-
```bash
-
```

## Acknowledgements
This work was supported by the Institute of Information & Communications Technology Planning & Evaluation(IITP) grant funded by the Korea government(MSIT) (No.RS-2024-00461572, Development of High-efficiency Parallel Storage SW Technology Oprimized for AI Computational Accelerators)