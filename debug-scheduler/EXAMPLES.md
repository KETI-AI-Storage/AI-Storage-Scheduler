# AI Storage Scheduler - GPU Scheduling Examples

간단하게 테스트할 수 있는 예시 파일들입니다.

## 📁 파일 목록

### YAML 예시 파일
- **example-simple-gpu.yaml** - GPU 1개 요청 Pod (간단한 busybox 이미지)
- **example-cpu-only.yaml** - GPU 없는 CPU 전용 Pod
- **example-deployment.yaml** - GPU Pod 2개 복제본 Deployment

### 스크립트
- **run-examples.sh** - 대화형 테스트 스크립트

## 🚀 빠른 시작

### 방법 1: 대화형 메뉴 사용 (추천)
```bash
cd /root/workspace/ai-storage-scheduler/debug-scheduler
./run-examples.sh
```

메뉴가 나타나면 숫자를 선택하세요:
- `1` - GPU Pod 테스트
- `2` - CPU 전용 Pod 테스트
- `3` - GPU Deployment 테스트
- `4` - 모든 테스트 실행
- `s` - 현재 상태 확인
- `r` - 클러스터 리소스 확인
- `c` - 정리
- `q` - 종료

### 방법 2: 직접 실행
```bash
# GPU Pod 테스트만 실행
./run-examples.sh 1

# 또는
./run-examples.sh simple-gpu

# 모든 테스트 실행
./run-examples.sh all

# 상태 확인
./run-examples.sh status

# 정리
./run-examples.sh cleanup
```

### 방법 3: kubectl로 직접 실행
```bash
# 1. GPU Pod 생성
kubectl apply -f example-simple-gpu.yaml

# 2. Pod 상태 확인
kubectl get pod simple-gpu-pod -n keti -o wide

# 3. 스케줄러 로그 확인
kubectl logs -n keti -l app=ai-storage-scheduler --tail=20 | grep simple-gpu-pod

# 4. 삭제
kubectl delete pod simple-gpu-pod -n keti
```

## 📝 예시 상세 설명

### 1. example-simple-gpu.yaml
```yaml
# GPU 1개를 요청하는 간단한 Pod
# 기대 결과: gpu-server-03 노드에 배치
# 실행 시간: 300초 (5분)
```

**사용법:**
```bash
kubectl apply -f example-simple-gpu.yaml
kubectl get pod simple-gpu-pod -n keti -o wide
```

**확인 사항:**
- Pod가 `gpu-server-03` 노드에 배치되었는가?
- 스케줄러 로그에 GPU 정보가 표시되는가?

### 2. example-cpu-only.yaml
```yaml
# GPU를 요청하지 않는 CPU 전용 Pod
# 기대 결과: 모든 노드 중 LeastAllocated 점수가 높은 노드에 배치
# 실행 시간: 300초 (5분)
```

**사용법:**
```bash
kubectl apply -f example-cpu-only.yaml
kubectl get pod simple-cpu-pod -n keti -o wide
```

**확인 사항:**
- Pod가 어느 노드에 배치되었는가?
- 리소스가 가장 여유로운 노드를 선택했는가?

### 3. example-deployment.yaml
```yaml
# GPU Pod 2개 복제본을 생성하는 Deployment
# 기대 결과: 모든 Pod가 gpu-server-03에 배치
# 실행: 계속 실행 (restartPolicy: Always)
```

**사용법:**
```bash
kubectl apply -f example-deployment.yaml
kubectl get pods -n keti -l app=gpu-app -o wide
```

**확인 사항:**
- 2개의 Pod가 모두 생성되었는가?
- 모두 `gpu-server-03`에 배치되었는가?
- gpu-server-03의 GPU가 2개이므로 각각 1개씩 할당되었는가?

## 🔍 테스트 시나리오

### 시나리오 1: GPU Pod 스케줄링 검증
```bash
# 1. GPU Pod 생성
./run-examples.sh 1

# 2. 배치 노드 확인
kubectl get pod simple-gpu-pod -n keti -o jsonpath='{.spec.nodeName}'
# 기대값: gpu-server-03

# 3. 노드의 GPU 리소스 확인
kubectl get node gpu-server-03 -o jsonpath='{.status.allocatable.nvidia\.com/gpu}'
# 기대값: 2

# 4. 스케줄러 로그 확인
kubectl logs -n keti -l app=ai-storage-scheduler | grep simple-gpu-pod
```

### 시나리오 2: 리소스 분산 검증
```bash
# 1. CPU Pod 여러 개 생성
for i in {1..3}; do
  kubectl run cpu-pod-$i --image=busybox -n keti \
    --restart=Never --overrides='{"spec":{"schedulerName":"ai-storage-scheduler"}}' \
    -- sh -c "sleep 300"
done

# 2. Pod 분산 확인
kubectl get pods -n keti -o wide | grep cpu-pod

# 3. 노드별 Pod 개수 확인
kubectl get pods -n keti -o wide --no-headers | awk '{print $7}' | sort | uniq -c

# 4. 정리
kubectl delete pod -n keti -l run=cpu-pod
```

### 시나리오 3: GPU 부족 상황 테스트
```bash
# gpu-server-03은 GPU 2개만 있으므로, 3개의 GPU Pod 생성 시 1개는 Pending

# 1. GPU Deployment 복제본 3개로 설정
kubectl apply -f example-deployment.yaml
kubectl scale deployment gpu-deployment -n keti --replicas=3

# 2. Pod 상태 확인
kubectl get pods -n keti -l app=gpu-app -o wide

# 3. Pending Pod 확인
kubectl get pods -n keti -l app=gpu-app --field-selector=status.phase=Pending

# 4. Pending 사유 확인
kubectl describe pod <pending-pod-name> -n keti | grep "Insufficient"

# 5. 정리
kubectl delete deployment gpu-deployment -n keti
```

## 🛠️ 디버깅 팁

### 로그 확인
```bash
# 실시간 로그
kubectl logs -n keti -l app=ai-storage-scheduler -f

# 특정 Pod 관련 로그만
kubectl logs -n keti -l app=ai-storage-scheduler | grep "pod=simple-gpu-pod"

# GPU 관련 로그만
kubectl logs -n keti -l app=ai-storage-scheduler | grep "gpu"

# 최근 50줄
kubectl logs -n keti -l app=ai-storage-scheduler --tail=50
```

### Pod 상태 확인
```bash
# Pod 이벤트 확인
kubectl describe pod simple-gpu-pod -n keti

# Pod YAML 확인
kubectl get pod simple-gpu-pod -n keti -o yaml

# Pod의 리소스 요청 확인
kubectl get pod simple-gpu-pod -n keti -o jsonpath='{.spec.containers[0].resources}'
```

### 노드 상태 확인
```bash
# 전체 노드 리소스
kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu,MEMORY:.status.allocatable.memory,GPU:.status.allocatable."nvidia\.com/gpu"

# 특정 노드 상세 정보
kubectl describe node gpu-server-03

# GPU 할당 현황
kubectl describe node gpu-server-03 | grep -A 10 "Allocated resources"
```

## 📊 예상 결과

### 정상 동작 시
```
NAME              READY   STATUS    NODE            GPU
simple-gpu-pod    1/1     Running   gpu-server-03   1
simple-cpu-pod    1/1     Running   csd-server-01   0
gpu-deployment-*  1/1     Running   gpu-server-03   1
gpu-deployment-*  1/1     Running   gpu-server-03   1
```

### 스케줄러 로그
```
level=INFO msg="[event] add pod to scheduling queue" namespace=keti pod=simple-gpu-pod gpu=1
level=INFO msg="[gpu-metrics] Fetching GPU metrics for node" node=gpu-server-03
```

## 🧹 정리

### 개별 삭제
```bash
kubectl delete pod simple-gpu-pod -n keti
kubectl delete pod simple-cpu-pod -n keti
kubectl delete deployment gpu-deployment -n keti
```

### 일괄 정리
```bash
./run-examples.sh cleanup
```

또는
```bash
kubectl delete pods,deployments -n keti -l 'app in (gpu-app)'
```

## ⚠️ 주의사항

1. **GPU 노드**: 현재 `gpu-server-03`만 GPU 2개 보유
2. **동시 실행**: GPU Pod 2개까지만 동시 실행 가능
3. **스케줄러 이름**: 모든 예시는 `schedulerName: ai-storage-scheduler` 사용
4. **네임스페이스**: 모든 리소스는 `keti` 네임스페이스에 생성

## 📚 추가 참고

- **전체 테스트**: `./test.sh` 사용 (자동화된 전체 테스트)
- **README.md**: 전체 디버깅 가이드
- **상세 문서**: `/root/workspace/ai-storage-scheduler/CLAUDE.md`
