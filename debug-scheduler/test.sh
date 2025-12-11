#!/bin/bash

# AI Storage Scheduler GPU Scheduling Debug Test Script
# This script tests GPU scheduling functionality of ai-storage-scheduler

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="keti"
SCHEDULER_LABEL="app=ai-storage-scheduler"
TEST_LABEL="test=scheduler-debug"

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}AI Storage Scheduler GPU Test${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for pod to be scheduled
wait_for_pod_scheduled() {
    local pod_name=$1
    local timeout=60
    local elapsed=0

    print_info "Waiting for pod '$pod_name' to be scheduled..."

    while [ $elapsed -lt $timeout ]; do
        node=$(kubectl get pod $pod_name -n $NAMESPACE -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "")
        if [ -n "$node" ]; then
            print_success "Pod '$pod_name' scheduled to node: $node"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done

    print_error "Pod '$pod_name' not scheduled within ${timeout}s"
    return 1
}

# Function to check pod status
check_pod_status() {
    local pod_name=$1

    echo ""
    print_info "Pod Status for '$pod_name':"
    kubectl get pod $pod_name -n $NAMESPACE -o wide

    echo ""
    print_info "Pod Details:"
    kubectl describe pod $pod_name -n $NAMESPACE | grep -A 10 "Events:" || echo "No events"

    node=$(kubectl get pod $pod_name -n $NAMESPACE -o jsonpath='{.spec.nodeName}')
    if [ -n "$node" ]; then
        echo ""
        print_info "Node '$node' GPU Resources:"
        kubectl get node $node -o json | jq '.status.allocatable["nvidia.com/gpu"]'
    fi
}

# Function to check scheduler logs
check_scheduler_logs() {
    local pod_name=$1

    echo ""
    print_info "Scheduler logs for pod '$pod_name':"
    kubectl logs -n $NAMESPACE -l $SCHEDULER_LABEL --tail=50 | grep -i "$pod_name" || print_warning "No scheduler logs found for pod '$pod_name'"
}

# Function to cleanup test pods
cleanup() {
    print_info "Cleaning up test pods..."
    kubectl delete pods -n $NAMESPACE -l $TEST_LABEL --ignore-not-found=true
    print_success "Cleanup completed"
}

# Function to show node GPU resources
show_node_resources() {
    echo ""
    print_info "Cluster GPU Resources:"
    echo "------------------------------------------------"
    kubectl get nodes -o custom-columns=\
NAME:.metadata.name,\
CPU:.status.allocatable.cpu,\
MEMORY:.status.allocatable.memory,\
GPU:.status.allocatable."nvidia\.com/gpu" | head -20
    echo "------------------------------------------------"
}

# Function to run test
run_test() {
    local test_file=$1
    local pod_name=$2

    echo ""
    echo -e "${YELLOW}=========================================${NC}"
    print_info "Running test: $pod_name"
    echo -e "${YELLOW}=========================================${NC}"

    # Apply test pod
    print_info "Creating pod from $test_file..."
    kubectl apply -f $test_file

    # Wait for scheduling
    if wait_for_pod_scheduled $pod_name; then
        check_pod_status $pod_name
        check_scheduler_logs $pod_name
        return 0
    else
        print_error "Test failed: Pod not scheduled"
        check_pod_status $pod_name
        check_scheduler_logs $pod_name
        return 1
    fi
}

# Main test execution
main() {
    # Check if scheduler is running
    print_info "Checking scheduler status..."
    scheduler_pod=$(kubectl get pods -n $NAMESPACE -l $SCHEDULER_LABEL -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    if [ -z "$scheduler_pod" ]; then
        print_error "ai-storage-scheduler is not running in namespace '$NAMESPACE'"
        exit 1
    fi

    print_success "Scheduler pod found: $scheduler_pod"

    # Show current node resources
    show_node_resources

    # Cleanup any existing test pods
    cleanup

    # Test 1: GPU Pod (1 GPU)
    print_info "Test 1: Single GPU Pod"
    if run_test "gpu-test-pod.yaml" "gpu-test-pod"; then
        print_success "Test 1 passed"
    else
        print_error "Test 1 failed"
    fi

    sleep 5

    # Test 2: No GPU Pod
    print_info "Test 2: No GPU Pod"
    if run_test "no-gpu-test-pod.yaml" "no-gpu-test-pod"; then
        print_success "Test 2 passed"
    else
        print_error "Test 2 failed"
    fi

    sleep 5

    # Test 3: Multi-GPU Pod (2 GPUs)
    print_info "Test 3: Multi-GPU Pod"
    if run_test "multi-gpu-test-pod.yaml" "multi-gpu-test-pod"; then
        print_success "Test 3 passed"
    else
        print_warning "Test 3 failed (may be expected if cluster doesn't have 2 GPUs available)"
    fi

    echo ""
    echo -e "${BLUE}=========================================${NC}"
    print_info "All Tests Completed"
    echo -e "${BLUE}=========================================${NC}"

    echo ""
    print_info "Test Pods Status:"
    kubectl get pods -n $NAMESPACE -l $TEST_LABEL -o wide

    echo ""
    read -p "Do you want to cleanup test pods? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup
    else
        print_info "Test pods kept for manual inspection"
        print_info "To cleanup later, run: kubectl delete pods -n $NAMESPACE -l $TEST_LABEL"
    fi
}

# Parse command line arguments
case "${1:-}" in
    "cleanup")
        cleanup
        ;;
    "nodes")
        show_node_resources
        ;;
    "logs")
        pod_name="${2:-}"
        if [ -z "$pod_name" ]; then
            print_error "Usage: $0 logs <pod-name>"
            exit 1
        fi
        check_scheduler_logs "$pod_name"
        ;;
    "status")
        pod_name="${2:-}"
        if [ -z "$pod_name" ]; then
            kubectl get pods -n $NAMESPACE -l $TEST_LABEL -o wide
        else
            check_pod_status "$pod_name"
        fi
        ;;
    "help"|"-h"|"--help")
        echo "AI Storage Scheduler GPU Test Script"
        echo ""
        echo "Usage:"
        echo "  $0              - Run all tests"
        echo "  $0 cleanup      - Cleanup test pods"
        echo "  $0 nodes        - Show node GPU resources"
        echo "  $0 logs <pod>   - Show scheduler logs for a pod"
        echo "  $0 status [pod] - Show pod status (or all test pods)"
        echo "  $0 help         - Show this help"
        echo ""
        ;;
    *)
        main
        ;;
esac
