#!/bin/bash

# Simple test script for ai-storage-scheduler GPU scheduling examples
# Usage: ./run-examples.sh [test-name]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

NAMESPACE="keti"

print_header() {
    echo -e "${BLUE}=========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================${NC}"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check scheduler is running
check_scheduler() {
    print_info "Checking scheduler status..."
    if ! kubectl get pods -n $NAMESPACE -l app=ai-storage-scheduler | grep Running > /dev/null; then
        print_error "ai-storage-scheduler is not running!"
        exit 1
    fi
    print_success "Scheduler is running"
}

# Show cluster resources
show_resources() {
    print_header "Cluster GPU Resources"
    kubectl get nodes -o custom-columns=\
NAME:.metadata.name,\
CPU:.status.allocatable.cpu,\
MEMORY:.status.allocatable.memory,\
GPU:.status.allocatable."nvidia\.com/gpu"
    echo ""
}

# Wait for pod to be scheduled
wait_for_pod() {
    local pod_name=$1
    local timeout=30
    local elapsed=0

    print_info "Waiting for pod '$pod_name' to be scheduled..."

    while [ $elapsed -lt $timeout ]; do
        status=$(kubectl get pod $pod_name -n $NAMESPACE -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        node=$(kubectl get pod $pod_name -n $NAMESPACE -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "")

        if [ -n "$node" ]; then
            print_success "Pod scheduled to node: $node (Status: $status)"
            return 0
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    print_error "Pod not scheduled within ${timeout}s"
    return 1
}

# Show pod details
show_pod_details() {
    local pod_name=$1

    echo ""
    print_info "Pod Details:"
    kubectl get pod $pod_name -n $NAMESPACE -o wide

    echo ""
    print_info "Scheduler logs for this pod:"
    kubectl logs -n $NAMESPACE -l app=ai-storage-scheduler --tail=20 | grep "$pod_name" || echo "No logs found"
}

# Test 1: Simple GPU Pod
test_simple_gpu() {
    print_header "Test: Simple GPU Pod"

    kubectl delete pod simple-gpu-pod -n $NAMESPACE --ignore-not-found=true
    sleep 2

    print_info "Creating GPU pod..."
    kubectl apply -f example-simple-gpu.yaml

    if wait_for_pod "simple-gpu-pod"; then
        show_pod_details "simple-gpu-pod"

        node=$(kubectl get pod simple-gpu-pod -n $NAMESPACE -o jsonpath='{.spec.nodeName}')
        gpu=$(kubectl get node $node -o jsonpath='{.status.allocatable.nvidia\.com/gpu}')

        if [ "$gpu" != "null" ] && [ -n "$gpu" ]; then
            print_success "✓ Pod correctly scheduled to GPU node ($node with $gpu GPUs)"
        else
            print_error "✗ Pod scheduled to non-GPU node!"
        fi
    fi

    echo ""
    read -p "Keep this pod? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete pod simple-gpu-pod -n $NAMESPACE
        print_info "Pod deleted"
    fi
}

# Test 2: CPU-only Pod
test_cpu_only() {
    print_header "Test: CPU-only Pod"

    kubectl delete pod simple-cpu-pod -n $NAMESPACE --ignore-not-found=true
    sleep 2

    print_info "Creating CPU-only pod..."
    kubectl apply -f example-cpu-only.yaml

    if wait_for_pod "simple-cpu-pod"; then
        show_pod_details "simple-cpu-pod"
        print_success "✓ Pod scheduled successfully"
    fi

    echo ""
    read -p "Keep this pod? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete pod simple-cpu-pod -n $NAMESPACE
        print_info "Pod deleted"
    fi
}

# Test 3: GPU Deployment
test_deployment() {
    print_header "Test: GPU Deployment (2 replicas)"

    kubectl delete deployment gpu-deployment -n $NAMESPACE --ignore-not-found=true
    sleep 3

    print_info "Creating GPU deployment..."
    kubectl apply -f example-deployment.yaml

    sleep 5

    print_info "Deployment status:"
    kubectl get deployment gpu-deployment -n $NAMESPACE

    echo ""
    print_info "Pods:"
    kubectl get pods -n $NAMESPACE -l app=gpu-app -o wide

    echo ""
    print_info "Scheduler logs:"
    kubectl logs -n $NAMESPACE -l app=ai-storage-scheduler --tail=30 | grep "gpu-deployment" || echo "No logs found"

    echo ""
    read -p "Keep this deployment? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete deployment gpu-deployment -n $NAMESPACE
        print_info "Deployment deleted"
    fi
}

# Cleanup all test resources
cleanup_all() {
    print_info "Cleaning up all test resources..."
    kubectl delete pod simple-gpu-pod -n $NAMESPACE --ignore-not-found=true
    kubectl delete pod simple-cpu-pod -n $NAMESPACE --ignore-not-found=true
    kubectl delete deployment gpu-deployment -n $NAMESPACE --ignore-not-found=true
    print_success "Cleanup completed"
}

# Show all test pods
show_status() {
    print_header "Test Resources Status"
    echo ""
    print_info "Pods:"
    kubectl get pods -n $NAMESPACE -l 'app in (gpu-app)' -o wide 2>/dev/null || echo "No test pods found"
    kubectl get pod simple-gpu-pod -n $NAMESPACE -o wide 2>/dev/null || true
    kubectl get pod simple-cpu-pod -n $NAMESPACE -o wide 2>/dev/null || true
    echo ""
    print_info "Deployments:"
    kubectl get deployment gpu-deployment -n $NAMESPACE 2>/dev/null || echo "No deployments found"
}

# Main menu
show_menu() {
    print_header "AI Storage Scheduler - GPU Scheduling Examples"
    echo ""
    echo "Available tests:"
    echo "  1) Simple GPU Pod       - Single pod requesting 1 GPU"
    echo "  2) CPU-only Pod         - Pod without GPU request"
    echo "  3) GPU Deployment       - Deployment with 2 GPU pods"
    echo "  4) Run All Tests        - Execute all tests sequentially"
    echo ""
    echo "Utilities:"
    echo "  s) Show Status          - Display current test resources"
    echo "  r) Show Resources       - Display cluster GPU resources"
    echo "  c) Cleanup             - Remove all test resources"
    echo "  q) Quit"
    echo ""
}

# Main execution
main() {
    check_scheduler
    show_resources

    case "${1:-}" in
        "1"|"simple-gpu")
            test_simple_gpu
            ;;
        "2"|"cpu-only")
            test_cpu_only
            ;;
        "3"|"deployment")
            test_deployment
            ;;
        "4"|"all")
            test_simple_gpu
            echo ""
            test_cpu_only
            echo ""
            test_deployment
            ;;
        "cleanup"|"c")
            cleanup_all
            ;;
        "status"|"s")
            show_status
            ;;
        "resources"|"r")
            show_resources
            ;;
        "help"|"h")
            echo "Usage: $0 [test]"
            echo ""
            echo "Tests:"
            echo "  1, simple-gpu    - Simple GPU pod test"
            echo "  2, cpu-only      - CPU-only pod test"
            echo "  3, deployment    - GPU deployment test"
            echo "  4, all           - Run all tests"
            echo ""
            echo "Utilities:"
            echo "  s, status        - Show status"
            echo "  r, resources     - Show resources"
            echo "  c, cleanup       - Cleanup"
            echo ""
            ;;
        *)
            while true; do
                show_menu
                read -p "Select an option: " choice
                case $choice in
                    1) test_simple_gpu ;;
                    2) test_cpu_only ;;
                    3) test_deployment ;;
                    4) test_simple_gpu; test_cpu_only; test_deployment ;;
                    s|S) show_status ;;
                    r|R) show_resources ;;
                    c|C) cleanup_all ;;
                    q|Q) print_info "Exiting..."; exit 0 ;;
                    *) print_error "Invalid option" ;;
                esac
                echo ""
                read -p "Press Enter to continue..."
            done
            ;;
    esac
}

main "$@"
