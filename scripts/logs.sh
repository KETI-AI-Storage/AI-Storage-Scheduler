#!/bin/bash
# AI Storage Scheduler - 로그 조회 스크립트 (auto-reconnect)

APP_NAME="ai-storage-scheduler"
NAMESPACE="keti"
RETRY_INTERVAL=3

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${GREEN}=== $APP_NAME 로그 ===${NC}"
    echo -e "Namespace: ${YELLOW}$NAMESPACE${NC}"
    echo -e "Mode: ${YELLOW}$1${NC}"
    echo "========================"
    echo ""
}

get_pod() {
    kubectl get pods -n $NAMESPACE -l app=$APP_NAME -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

wait_for_pod() {
    echo -e "${YELLOW}Waiting for $APP_NAME pod...${NC}"
    while true; do
        POD=$(get_pod)
        if [ -n "$POD" ]; then
            # Pod가 Running 상태인지 확인
            STATUS=$(kubectl get pod -n $NAMESPACE $POD -o jsonpath='{.status.phase}' 2>/dev/null)
            if [ "$STATUS" == "Running" ]; then
                echo -e "${GREEN}Pod found: $POD${NC}"
                return 0
            fi
        fi
        sleep $RETRY_INTERVAL
    done
}

follow_logs() {
    while true; do
        POD=$(get_pod)
        if [ -z "$POD" ]; then
            wait_for_pod
            POD=$(get_pod)
        fi

        echo -e "${GREEN}Following logs from: $POD${NC}"
        echo "----------------------------------------"

        # kubectl logs -f 실행 (연결 끊기면 자동 재시도)
        kubectl logs -n $NAMESPACE $POD -f --tail=100 2>&1

        EXIT_CODE=$?
        if [ $EXIT_CODE -ne 0 ]; then
            echo ""
            echo -e "${RED}Connection lost (exit code: $EXIT_CODE)${NC}"
            echo -e "${YELLOW}Reconnecting in ${RETRY_INTERVAL}s...${NC}"
            sleep $RETRY_INTERVAL
        fi
    done
}

show_recent() {
    POD=$(get_pod)
    if [ -z "$POD" ]; then
        echo -e "${RED}Error: $APP_NAME pod not found in namespace $NAMESPACE${NC}"
        exit 1
    fi

    LINES=${1:-100}
    echo -e "${GREEN}Pod: $POD${NC}"
    echo "----------------------------------------"
    kubectl logs -n $NAMESPACE $POD --tail=$LINES
}

# 사용법 출력
usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  (없음)        실시간 로그 follow (기본값, 자동 재연결)"
    echo "  -f, --follow  실시간 로그 follow (자동 재연결)"
    echo "  -t, --tail N  최근 N줄 출력 후 종료 (기본: 100)"
    echo "  -h, --help    도움말 출력"
    echo ""
    echo "Examples:"
    echo "  $0              # 실시간 로그 (자동 재연결)"
    echo "  $0 -f           # 실시간 로그 (자동 재연결)"
    echo "  $0 -t 200       # 최근 200줄 출력"
    echo ""
    echo "Press Ctrl+C to exit"
}

# 메인 로직
case "$1" in
    -h|--help)
        usage
        exit 0
        ;;
    -t|--tail)
        print_header "tail ${2:-100} lines"
        show_recent ${2:-100}
        ;;
    -f|--follow|"")
        print_header "follow (auto-reconnect)"
        follow_logs
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        usage
        exit 1
        ;;
esac
