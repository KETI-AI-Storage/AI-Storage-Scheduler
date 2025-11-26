# AI Storage Scheduler Scripts

This directory contains utility scripts for building, deploying, and monitoring the AI Storage Scheduler.

## Scripts

### 1. Build and Push Image
**File:** `1.build-image.sh`

Builds the scheduler binary and Docker image, then pushes to Docker Hub.

```bash
./scripts/1.build-image.sh
```

**What it does:**
- Builds static Go binary with CGO disabled
- Creates Docker image
- Tags image as `ketidevit2/keti-ai-storage-scheduler:latest`
- Pushes to Docker Hub registry

### 2. Apply/Delete Deployment
**File:** `2.apply-deployment.sh`

Manages Kubernetes deployment of the scheduler.

```bash
# Apply deployment
./scripts/2.apply-deployment.sh apply
./scripts/2.apply-deployment.sh a        # short version

# Delete deployment
./scripts/2.apply-deployment.sh delete
./scripts/2.apply-deployment.sh d        # short version
```

**What it does:**
- Applies or deletes the scheduler deployment in Kubernetes
- Shows pod status after applying
- Provides helpful error messages

### 3. Trace Logs
**File:** `3.trace-log.sh`

Monitors scheduler pod logs in real-time.

```bash
./scripts/3.trace-log.sh
```

**What it does:**
- Waits for scheduler pod to be running
- Tails the last 50 lines of logs
- Follows logs in real-time (Ctrl+C to exit)

## Usage Example

Complete workflow:

```bash
# 1. Build and push new image
./scripts/1.build-image.sh

# 2. Deploy to Kubernetes
./scripts/2.apply-deployment.sh apply

# 3. Monitor logs
./scripts/3.trace-log.sh
```

## Prerequisites

- Go 1.21+ installed
- Docker installed and logged in to Docker Hub
- kubectl configured with cluster access
- Kubernetes cluster with `keti` namespace
