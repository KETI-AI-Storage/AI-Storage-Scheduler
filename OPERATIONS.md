# AI Storage Scheduler 오퍼레이션 가이드

AI Storage Scheduler의 빌드, 배포, 테스트 명령어와 각 컴포넌트 설명입니다.

---

## 1. 빌드 및 배포 스크립트

### 1.1 이미지 빌드 (`scripts/1.build-image.sh`)

Go 바이너리 빌드 → Docker 이미지 생성 → 레지스트리 푸시

```bash
# 실행
./scripts/1.build-image.sh

# 수행 작업:
# 1. Go 바이너리 빌드 (CGO_ENABLED=0 GOOS=linux GOARCH=amd64)
# 2. Docker 이미지 빌드 (keti-ai-storage-scheduler:latest)
# 3. 이미지 태깅 (ketidevit2/keti-ai-storage-scheduler:latest)
# 4. Docker Hub 푸시
```

**결과물**: `ketidevit2/keti-ai-storage-scheduler:latest`

---

### 1.2 배포 적용/삭제 (`scripts/2.apply-deployment.sh`)

Kubernetes Deployment 적용 또는 삭제

```bash
# 배포 적용
./scripts/2.apply-deployment.sh apply
./scripts/2.apply-deployment.sh a     # 단축

# 배포 삭제
./scripts/2.apply-deployment.sh delete
./scripts/2.apply-deployment.sh d     # 단축
```

**배포 위치**: `keti` 네임스페이스

---

### 1.3 로그 추적 (`scripts/3.trace-log.sh`)

스케줄러 Pod 로그 실시간 추적

```bash
./scripts/3.trace-log.sh

# Pod가 Running 상태가 될 때까지 대기 후 로그 follow
```

---

### 1.4 로그 조회 (고급) (`scripts/logs.sh`)

자동 재연결 기능이 있는 로그 조회 스크립트

```bash
# 실시간 로그 (자동 재연결)
./scripts/logs.sh
./scripts/logs.sh -f
./scripts/logs.sh --follow

# 최근 N줄만 출력
./scripts/logs.sh -t 200
./scripts/logs.sh --tail 200

# 도움말
./scripts/logs.sh -h
```

---

## 2. CRD (Custom Resource Definition) 관리

### 2.1 CRD 설치

```bash
# CRD 정의 적용
kubectl apply -f config/crd/aistorageconfig.yaml

# 기본 설정 적용
kubectl apply -f config/crd/default-config.yaml
```

### 2.2 CRD 조회

```bash
# AIStorageConfig 목록 조회
kubectl get aistorageconfigs -n keti
kubectl get aisc -n keti              # 단축명

# 상세 조회
kubectl describe aisc default -n keti

# YAML 출력
kubectl get aisc default -n keti -o yaml
```

### 2.3 CRD 설정 수정

```bash
# 편집
kubectl edit aisc default -n keti

# 또는 파일 수정 후 재적용
kubectl apply -f config/crd/default-config.yaml
```

---

## 3. 스케줄러 플러그인 설명

AI Storage Scheduler는 3개의 Score 플러그인을 통해 노드 점수를 계산합니다.

### 3.1 DataLocalityAware (데이터 로컬리티 인식)

| 항목 | 설명 | 점수 범위 | 데이터 소스 |
|------|------|-----------|-------------|
| APOLLO Score | APOLLO 노드 선호도 점수 | 0-30 | APOLLO gRPC |
| PVC Locality | PVC가 바인딩된 노드 근접성 | 0-30 | K8s API |
| Cache Score | 데이터셋 캐시 위치 | 0-20 | APOLLO / Node Annotations |
| Topology Score | 네트워크 토폴로지 (Zone/Rack) | 0-20 | Pod Annotations |

**총점**: 0-100점, **가중치**: 3 (기본값)

---

### 3.2 StorageTierAware (스토리지 티어 인식)

| 항목 | 설명 | 점수 범위 | 데이터 소스 |
|------|------|-----------|-------------|
| I/O Pattern Score | I/O 패턴과 스토리지 티어 매칭 | 0-40 | APOLLO / Pod Annotations |
| Pipeline Stage Score | 파이프라인 단계별 최적 스토리지 | 0-30 | APOLLO / Pod Labels |
| Performance Score | IOPS/처리량 요구사항 충족 | 0-20 | APOLLO / Pod Annotations |
| Capacity Score | 스토리지 가용 용량 | 0-10 | Node Annotations |

**총점**: 0-100점, **가중치**: 3 (기본값)

**스토리지 티어**: NVMe > SSD > HDD > CSD

---

### 3.3 IOPatternBased (I/O 패턴 기반)

| 항목 | 설명 | 점수 범위 | 데이터 소스 |
|------|------|-----------|-------------|
| APOLLO Preference | APOLLO 노드 선호도 | 0-15 | APOLLO gRPC |
| Resource Match | 전처리 유형별 리소스 매칭 | 0-25 | Node Labels |
| I/O Optimization | I/O 패턴 최적화 | 0-20 | Node Labels |
| Expansion Score | 데이터 확장/축소 대응 | 0-20 | Node Annotations |
| CSD Score | CSD 오프로드 가능성 | 0-20 | APOLLO / Node Labels |

**총점**: 0-100점, **가중치**: 3 (기본값)

**전처리 타입**: augmentation, transformation, filtering, aggregation, sharding, normalization, featureExtraction

---

## 4. 테스트 Pod 생성

### 4.1 GPU 테스트 Pod

```bash
kubectl apply -f deployments/test-gpu-pod.yaml
```

### 4.2 전처리 워크로드 테스트 Pod

```bash
# 전처리 테스트 Pod 생성 (예시)
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-preprocess-pod
  namespace: default
  labels:
    pipeline-step: preprocessing
  annotations:
    ai-storage.keti/workload-stage: preprocessing
    ai-storage.keti/preprocessing-type: augmentation
    ai-storage.keti/io-pattern: sequential
spec:
  schedulerName: ai-storage-scheduler
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "300"]
    resources:
      requests:
        cpu: "100m"
        memory: "128Mi"
  restartPolicy: Never
EOF
```

### 4.3 스케줄링 결과 확인

```bash
# Pod 상태 확인
kubectl get pod test-preprocess-pod -o wide

# 스케줄링 이벤트 확인
kubectl describe pod test-preprocess-pod | grep -A 10 Events

# 스케줄러 로그에서 점수 확인
./scripts/logs.sh -t 100 | grep -E "\[ScoreMap\]|\[score\]"
```

---

## 5. 노드 라벨링

스케줄러가 올바르게 동작하려면 노드에 적절한 라벨이 필요합니다.

### 5.1 스토리지 티어 라벨

```bash
# NVMe 노드
kubectl label nodes <node-name> storage-tier/nvme=true

# SSD 노드
kubectl label nodes <node-name> storage-tier/ssd=true

# HDD 노드
kubectl label nodes <node-name> storage-tier/hdd=true

# CSD 노드
kubectl label nodes <node-name> storage-tier/csd=true
```

### 5.2 레이어 라벨

```bash
# 컴퓨트 노드
kubectl label nodes <node-name> layer=compute

# 스토리지 노드
kubectl label nodes <node-name> layer=storage

# 오케스트레이션 노드
kubectl label nodes <node-name> layer=orchestration
```

### 5.3 리소스 어노테이션

```bash
# IOPS 정보
kubectl annotate nodes <node-name> ai-storage.keti/available-iops=100000

# 처리량 정보 (MB/s)
kubectl annotate nodes <node-name> ai-storage.keti/available-throughput-mbps=2000

# 가용 용량 (GB)
kubectl annotate nodes <node-name> ai-storage.keti/available-capacity-gb=500

# 캐시된 데이터셋
kubectl annotate nodes <node-name> ai-storage.keti/cached-datasets=imagenet,coco

# 데이터 온도
kubectl annotate nodes <node-name> ai-storage.keti/data-temperature=hot
```

---

## 6. APOLLO 연동

### 6.1 환경 변수 설정

```bash
# 기본 엔드포인트 (변경 시)
export APOLLO_ENDPOINT="apollo-policy-server.keti.svc.cluster.local:50051"
```

### 6.2 데이터 소스 확인

스케줄러 로그에서 데이터 소스를 확인할 수 있습니다:

```
[APOLLO-DataSource] SUCCESS - Policy retrieved from APOLLO gRPC
[APOLLO-DataSource] Policy retrieved from CACHE
[APOLLO-DataSource] FALLBACK - Connection failed, will use Pod labels/annotations
```

---

## 7. 트러블슈팅

### 7.1 Pod가 Pending 상태

```bash
# 스케줄러 확인
kubectl get pods -n keti -l app=ai-storage-scheduler

# 이벤트 확인
kubectl describe pod <pod-name>

# 스케줄러 로그 확인
./scripts/logs.sh -t 200
```

### 7.2 스케줄러 재시작

```bash
# 삭제 후 재배포
./scripts/2.apply-deployment.sh d
./scripts/2.apply-deployment.sh a

# 또는 Pod 삭제 (Deployment가 자동 재생성)
kubectl delete pod -n keti -l app=ai-storage-scheduler
```

### 7.3 CRD 설정 초기화

```bash
kubectl delete aisc default -n keti
kubectl apply -f config/crd/default-config.yaml
```

---

## 8. 주요 kubectl 명령어 요약

| 작업 | 명령어 |
|------|--------|
| 스케줄러 상태 | `kubectl get pods -n keti -l app=ai-storage-scheduler` |
| 스케줄러 로그 | `kubectl logs -n keti -l app=ai-storage-scheduler -f` |
| CRD 조회 | `kubectl get aisc -n keti` |
| CRD 편집 | `kubectl edit aisc default -n keti` |
| 테스트 Pod 생성 | `kubectl apply -f deployments/test-gpu-pod.yaml` |
| Pod 삭제 | `kubectl delete pod <pod-name>` |
| 노드 라벨 조회 | `kubectl get nodes --show-labels` |
| 노드 어노테이션 | `kubectl describe node <node-name>` |

---

## 9. 파일 구조

```
ai-storage-scheduler/
├── scripts/
│   ├── 1.build-image.sh      # 이미지 빌드
│   ├── 2.apply-deployment.sh # 배포 적용/삭제
│   ├── 3.trace-log.sh        # 로그 추적
│   └── logs.sh               # 고급 로그 조회
├── deployments/
│   ├── ai-storage-scheduler.yaml  # 스케줄러 Deployment
│   └── test-gpu-pod.yaml          # GPU 테스트 Pod
├── config/crd/
│   ├── aistorageconfig.yaml  # CRD 정의
│   └── default-config.yaml   # 기본 설정
└── internal/
    ├── framework/plugin/
    │   ├── datalocalityaware.go   # DataLocalityAware 플러그인
    │   ├── storagetieraware.go    # StorageTierAware 플러그인
    │   └── iopatternbased.go      # IOPatternBased 플러그인
    ├── apollo/
    │   └── client.go              # APOLLO gRPC 클라이언트
    └── configmanager/
        └── manager.go             # CRD 설정 관리
```
