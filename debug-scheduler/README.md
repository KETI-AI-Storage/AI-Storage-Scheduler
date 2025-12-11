# AI Storage Scheduler - GPU Scheduling Debug

This directory contains test files and scripts for debugging GPU scheduling functionality of the ai-storage-scheduler.

## Files

- **gpu-test-pod.yaml** - Single GPU (1) test pod
- **multi-gpu-test-pod.yaml** - Multi-GPU (2) test pod
- **no-gpu-test-pod.yaml** - No GPU test pod (CPU only)
- **test.sh** - Automated test script

## Quick Start

### Run All Tests
```bash
cd debug-scheduler
./test.sh
```

### Run Individual Tests

**Test 1: Single GPU Pod**
```bash
kubectl apply -f gpu-test-pod.yaml
kubectl get pod gpu-test-pod -n keti -o wide
```

**Test 2: Multi-GPU Pod**
```bash
kubectl apply -f multi-gpu-test-pod.yaml
kubectl get pod multi-gpu-test-pod -n keti -o wide
```

**Test 3: No GPU Pod**
```bash
kubectl apply -f no-gpu-test-pod.yaml
kubectl get pod no-gpu-test-pod -n keti -o wide
```

## Test Script Usage

```bash
# Run all tests
./test.sh

# Cleanup test pods
./test.sh cleanup

# Show node GPU resources
./test.sh nodes

# Show scheduler logs for a specific pod
./test.sh logs gpu-test-pod

# Show test pod status
./test.sh status
./test.sh status gpu-test-pod

# Show help
./test.sh help
```

## Expected Results

### Single GPU Pod (gpu-test-pod)
- ✅ Should be scheduled to **gpu-server-03** (has 2 GPUs)
- ✅ Should NOT be scheduled to nodes without GPUs
- ✅ NodeResourcesFit plugin should filter out non-GPU nodes
- ✅ LeastAllocated plugin should give higher score to nodes with more available GPUs

### Multi-GPU Pod (multi-gpu-test-pod)
- ✅ Should be scheduled to **gpu-server-03** (has 2 GPUs)
- ❌ Should fail if all GPUs are allocated
- ✅ Should show "Insufficient nvidia.com/gpu" error if no node has 2 GPUs available

### No GPU Pod (no-gpu-test-pod)
- ✅ Can be scheduled to any node
- ✅ Should be distributed based on LeastAllocated and BalancedAllocation scores

## Debugging Commands

### Check Scheduler Logs
```bash
kubectl logs -n keti -l app=ai-storage-scheduler --tail=100
kubectl logs -n keti -l app=ai-storage-scheduler -f  # Follow mode
```

### Check Pod Events
```bash
kubectl describe pod gpu-test-pod -n keti
```

### Check Node GPU Resources
```bash
# Show all nodes with GPU info
kubectl get nodes -o custom-columns=\
NAME:.metadata.name,\
CPU:.status.allocatable.cpu,\
MEMORY:.status.allocatable.memory,\
GPU:.status.allocatable."nvidia\.com/gpu"

# Detailed GPU info for specific node
kubectl get node gpu-server-03 -o json | jq '.status.allocatable["nvidia.com/gpu"]'
kubectl get node gpu-server-03 -o json | jq '.status.capacity["nvidia.com/gpu"]'
```

### Check Current GPU Usage
```bash
# Show all GPU pods
kubectl get pods -A -o json | jq -r '.items[] | select(.spec.containers[].resources.requests["nvidia.com/gpu"] != null) | "\(.metadata.namespace)/\(.metadata.name) -> \(.spec.nodeName)"'

# Calculate GPU usage per node
kubectl describe nodes | grep -A 5 "Allocated resources"
```

## Troubleshooting

### Pod Stuck in Pending
1. Check scheduler is running:
   ```bash
   kubectl get pods -n keti -l app=ai-storage-scheduler
   ```

2. Check scheduler logs:
   ```bash
   kubectl logs -n keti -l app=ai-storage-scheduler --tail=50
   ```

3. Check pod events:
   ```bash
   kubectl describe pod <pod-name> -n keti
   ```

4. Verify GPU resources on nodes:
   ```bash
   kubectl get nodes -o json | jq '.items[] | {name: .metadata.name, gpu: .status.allocatable["nvidia.com/gpu"]}'
   ```

### Pod Scheduled to Wrong Node
1. Check if pod has `schedulerName: ai-storage-scheduler`
2. Check scheduler filter logs
3. Verify NodeResourcesFit plugin is checking GPU resources
4. Check LeastAllocated scoring logic

### GPU Not Detected
1. Check NVIDIA device plugin is running:
   ```bash
   kubectl get daemonset -n kube-system nvidia-device-plugin-daemonset
   kubectl get pods -n kube-system -l name=nvidia-device-plugin-ds
   ```

2. Check node labels:
   ```bash
   kubectl get node gpu-server-03 --show-labels | grep gpu
   ```

3. Verify GPU capacity:
   ```bash
   kubectl get node gpu-server-03 -o json | jq '.status.capacity'
   ```

## Plugin Behavior

### Filter Plugins (Executed in Order)
1. **NodeName** - Checks spec.nodeName
2. **NodeUnschedulable** - Filters unschedulable nodes
3. **TaintToleration** - Checks taints/tolerations
4. **NodeAffinity** - Checks node selectors and affinity
5. **NodeResourcesFit** - Checks CPU, Memory, **GPU** availability

### Score Plugins (Executed for Feasible Nodes)
1. **LeastAllocated** (weight: 1)
   - GPU has weight: 2 (higher priority)
   - Prefers nodes with more available resources
2. **BalancedAllocation** (weight: 1)
   - Prefers balanced CPU/Memory/GPU usage
3. **ImageLocality** (weight: 1)
   - Prefers nodes with container images already present

## Cleanup

Remove all test pods:
```bash
kubectl delete pods -n keti -l test=scheduler-debug
```

Or use the script:
```bash
./test.sh cleanup
```
