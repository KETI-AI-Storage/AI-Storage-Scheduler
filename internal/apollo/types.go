// ============================================
// APOLLO Types for AI Storage Scheduler
// gRPC 메시지 타입 정의 (APOLLO proto 호환)
// ============================================

package apollo

import (
	"context"

	"google.golang.org/grpc"
)

// ============================================
// gRPC Client Interface
// ============================================

// SchedulingPolicyServiceClient is the client API for SchedulingPolicyService service.
type SchedulingPolicyServiceClient interface {
	GetSchedulingPolicy(ctx context.Context, in *SchedulingPolicyRequest, opts ...grpc.CallOption) (*SchedulingPolicy, error)
	ReportSchedulingResult(ctx context.Context, in *SchedulingResult, opts ...grpc.CallOption) (*ReportResponse, error)
}

type schedulingPolicyServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSchedulingPolicyServiceClient(cc grpc.ClientConnInterface) SchedulingPolicyServiceClient {
	return &schedulingPolicyServiceClient{cc}
}

func (c *schedulingPolicyServiceClient) GetSchedulingPolicy(ctx context.Context, in *SchedulingPolicyRequest, opts ...grpc.CallOption) (*SchedulingPolicy, error) {
	out := new(SchedulingPolicy)
	err := c.cc.Invoke(ctx, "/apollo.SchedulingPolicyService/GetSchedulingPolicy", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulingPolicyServiceClient) ReportSchedulingResult(ctx context.Context, in *SchedulingResult, opts ...grpc.CallOption) (*ReportResponse, error) {
	out := new(ReportResponse)
	err := c.cc.Invoke(ctx, "/apollo.SchedulingPolicyService/ReportSchedulingResult", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ============================================
// Request/Response Messages
// ============================================

// SchedulingPolicyRequest is the request for GetSchedulingPolicy
type SchedulingPolicyRequest struct {
	PodName        string            `protobuf:"bytes,1,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`
	PodNamespace   string            `protobuf:"bytes,2,opt,name=pod_namespace,json=podNamespace,proto3" json:"pod_namespace,omitempty"`
	PodUid         string            `protobuf:"bytes,3,opt,name=pod_uid,json=podUid,proto3" json:"pod_uid,omitempty"`
	PodLabels      map[string]string `protobuf:"bytes,4,rep,name=pod_labels,json=podLabels,proto3" json:"pod_labels,omitempty"`
	PodAnnotations map[string]string `protobuf:"bytes,5,rep,name=pod_annotations,json=podAnnotations,proto3" json:"pod_annotations,omitempty"`
}

func (m *SchedulingPolicyRequest) Reset()         { *m = SchedulingPolicyRequest{} }
func (m *SchedulingPolicyRequest) String() string { return "" }
func (*SchedulingPolicyRequest) ProtoMessage()    {}

// SchedulingPolicy is the response from GetSchedulingPolicy
// 이 구조체가 APOLLO가 분석한 워크로드 정보를 담고 있음
type SchedulingPolicy struct {
	RequestId   string `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	PodName     string `protobuf:"bytes,2,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`
	PodNamespace string `protobuf:"bytes,3,opt,name=pod_namespace,json=podNamespace,proto3" json:"pod_namespace,omitempty"`

	// 스케줄링 결정 (ALLOW, PREFER, REQUIRE, DELAY, REJECT)
	Decision SchedulingDecision `protobuf:"varint,4,opt,name=decision,proto3,enum=apollo.SchedulingDecision" json:"decision,omitempty"`

	// 노드 선호도 (APOLLO가 분석한 최적 노드 순위)
	NodePreferences []*NodePreference `protobuf:"bytes,5,rep,name=node_preferences,json=nodePreferences,proto3" json:"node_preferences,omitempty"`

	// 리소스 요구사항 (APOLLO가 계산한 예상 사용량)
	ResourceRequirements *ComputedResourceRequirements `protobuf:"bytes,6,opt,name=resource_requirements,json=resourceRequirements,proto3" json:"resource_requirements,omitempty"`

	// 스토리지 요구사항 (핵심! insight-trace 분석 결과)
	StorageRequirements *StorageRequirements `protobuf:"bytes,7,opt,name=storage_requirements,json=storageRequirements,proto3" json:"storage_requirements,omitempty"`

	// 스케줄링 제약조건
	Constraints []*SchedulingConstraint `protobuf:"bytes,8,rep,name=constraints,proto3" json:"constraints,omitempty"`

	// 우선순위
	Priority        int32 `protobuf:"varint,9,opt,name=priority,proto3" json:"priority,omitempty"`
	AllowPreemption bool  `protobuf:"varint,10,opt,name=allow_preemption,json=allowPreemption,proto3" json:"allow_preemption,omitempty"`

	// 어피니티 규칙
	AffinityRules []*AffinityRule `protobuf:"bytes,11,rep,name=affinity_rules,json=affinityRules,proto3" json:"affinity_rules,omitempty"`

	// 결정 이유 (디버깅용)
	Reason string `protobuf:"bytes,12,opt,name=reason,proto3" json:"reason,omitempty"`

	// 워크로드 시그니처 (insight-trace에서 수집된 원본 정보)
	WorkloadSignature *WorkloadSignatureInfo `protobuf:"bytes,20,opt,name=workload_signature,json=workloadSignature,proto3" json:"workload_signature,omitempty"`
}

func (m *SchedulingPolicy) Reset()         { *m = SchedulingPolicy{} }
func (m *SchedulingPolicy) String() string { return "" }
func (*SchedulingPolicy) ProtoMessage()    {}

// WorkloadSignatureInfo contains the workload characteristics from insight-trace
// insight-trace가 분석한 워크로드 특성
type WorkloadSignatureInfo struct {
	// 워크로드 유형 (IMAGE, TEXT, TABULAR, etc.)
	WorkloadType WorkloadTypeEnum `protobuf:"varint,1,opt,name=workload_type,json=workloadType,proto3,enum=apollo.WorkloadType" json:"workload_type,omitempty"`

	// 파이프라인 단계 (PREPROCESSING, TRAINING, etc.)
	CurrentStage PipelineStageEnum `protobuf:"varint,2,opt,name=current_stage,json=currentStage,proto3,enum=apollo.PipelineStage" json:"current_stage,omitempty"`

	// I/O 패턴 (READ_HEAVY, SEQUENTIAL, BURSTY, etc.)
	IoPattern IOPatternEnum `protobuf:"varint,3,opt,name=io_pattern,json=ioPattern,proto3,enum=apollo.IOPattern" json:"io_pattern,omitempty"`

	// 신뢰도 (0.0 ~ 1.0)
	Confidence float64 `protobuf:"fixed64,4,opt,name=confidence,proto3" json:"confidence,omitempty"`

	// 프레임워크 (pytorch, tensorflow, etc.)
	Framework        string `protobuf:"bytes,5,opt,name=framework,proto3" json:"framework,omitempty"`
	FrameworkVersion string `protobuf:"bytes,6,opt,name=framework_version,json=frameworkVersion,proto3" json:"framework_version,omitempty"`

	// 리소스 프로필
	IsGpuWorkload      bool   `protobuf:"varint,7,opt,name=is_gpu_workload,json=isGpuWorkload,proto3" json:"is_gpu_workload,omitempty"`
	IsDistributed      bool   `protobuf:"varint,8,opt,name=is_distributed,json=isDistributed,proto3" json:"is_distributed,omitempty"`
	IsPipeline         bool   `protobuf:"varint,9,opt,name=is_pipeline,json=isPipeline,proto3" json:"is_pipeline,omitempty"`
	PipelineStep       string `protobuf:"bytes,10,opt,name=pipeline_step,json=pipelineStep,proto3" json:"pipeline_step,omitempty"`
	EstimatedBatchSize int32  `protobuf:"varint,11,opt,name=estimated_batch_size,json=estimatedBatchSize,proto3" json:"estimated_batch_size,omitempty"`

	// 전처리 유형 (APOLLO가 분석)
	PreprocessingType PreprocessingTypeEnum `protobuf:"varint,12,opt,name=preprocessing_type,json=preprocessingType,proto3" json:"preprocessing_type,omitempty"`

	// 데이터셋 정보
	DatasetName     string   `protobuf:"bytes,13,opt,name=dataset_name,json=datasetName,proto3" json:"dataset_name,omitempty"`
	DataLocations   []string `protobuf:"bytes,14,rep,name=data_locations,json=dataLocations,proto3" json:"data_locations,omitempty"`
	CachedOnNodes   []string `protobuf:"bytes,15,rep,name=cached_on_nodes,json=cachedOnNodes,proto3" json:"cached_on_nodes,omitempty"`
}

func (m *WorkloadSignatureInfo) Reset()         { *m = WorkloadSignatureInfo{} }
func (m *WorkloadSignatureInfo) String() string { return "" }
func (*WorkloadSignatureInfo) ProtoMessage()    {}

// ============================================
// Enum Definitions (proto 호환)
// ============================================

type SchedulingDecision int32

const (
	SchedulingDecisionUnspecified SchedulingDecision = 0
	SchedulingDecisionAllow       SchedulingDecision = 1
	SchedulingDecisionPrefer      SchedulingDecision = 2
	SchedulingDecisionRequire     SchedulingDecision = 3
	SchedulingDecisionDelay       SchedulingDecision = 4
	SchedulingDecisionReject      SchedulingDecision = 5
)

type WorkloadTypeEnum int32

const (
	WorkloadTypeEnumUnknown    WorkloadTypeEnum = 0
	WorkloadTypeEnumImage      WorkloadTypeEnum = 1
	WorkloadTypeEnumText       WorkloadTypeEnum = 2
	WorkloadTypeEnumTabular    WorkloadTypeEnum = 3
	WorkloadTypeEnumAudio      WorkloadTypeEnum = 4
	WorkloadTypeEnumVideo      WorkloadTypeEnum = 5
	WorkloadTypeEnumMultimodal WorkloadTypeEnum = 6
	WorkloadTypeEnumGeneric    WorkloadTypeEnum = 7
)

func (w WorkloadTypeEnum) String() string {
	switch w {
	case WorkloadTypeEnumImage:
		return "image"
	case WorkloadTypeEnumText:
		return "text"
	case WorkloadTypeEnumTabular:
		return "tabular"
	case WorkloadTypeEnumAudio:
		return "audio"
	case WorkloadTypeEnumVideo:
		return "video"
	case WorkloadTypeEnumMultimodal:
		return "multimodal"
	case WorkloadTypeEnumGeneric:
		return "generic"
	default:
		return "unknown"
	}
}

type PipelineStageEnum int32

const (
	PipelineStageEnumUnknown       PipelineStageEnum = 0
	PipelineStageEnumDataLoading   PipelineStageEnum = 1
	PipelineStageEnumPreprocessing PipelineStageEnum = 2
	PipelineStageEnumTraining      PipelineStageEnum = 3
	PipelineStageEnumValidation    PipelineStageEnum = 4
	PipelineStageEnumInference     PipelineStageEnum = 5
	PipelineStageEnumPostprocessing PipelineStageEnum = 6
	PipelineStageEnumCheckpointing PipelineStageEnum = 7
	PipelineStageEnumIdle          PipelineStageEnum = 8
)

func (p PipelineStageEnum) String() string {
	switch p {
	case PipelineStageEnumDataLoading:
		return "data_loading"
	case PipelineStageEnumPreprocessing:
		return "preprocessing"
	case PipelineStageEnumTraining:
		return "training"
	case PipelineStageEnumValidation:
		return "validation"
	case PipelineStageEnumInference:
		return "inference"
	case PipelineStageEnumPostprocessing:
		return "postprocessing"
	case PipelineStageEnumCheckpointing:
		return "checkpointing"
	default:
		return "unknown"
	}
}

type IOPatternEnum int32

const (
	IOPatternEnumUnknown    IOPatternEnum = 0
	IOPatternEnumReadHeavy  IOPatternEnum = 1
	IOPatternEnumWriteHeavy IOPatternEnum = 2
	IOPatternEnumBalanced   IOPatternEnum = 3
	IOPatternEnumSequential IOPatternEnum = 4
	IOPatternEnumRandom     IOPatternEnum = 5
	IOPatternEnumBursty     IOPatternEnum = 6
)

func (i IOPatternEnum) String() string {
	switch i {
	case IOPatternEnumReadHeavy:
		return "read_heavy"
	case IOPatternEnumWriteHeavy:
		return "write_heavy"
	case IOPatternEnumBalanced:
		return "balanced"
	case IOPatternEnumSequential:
		return "sequential"
	case IOPatternEnumRandom:
		return "random"
	case IOPatternEnumBursty:
		return "bursty"
	default:
		return "unknown"
	}
}

type StorageClassEnum int32

const (
	StorageClassEnumUnspecified StorageClassEnum = 0
	StorageClassEnumStandard    StorageClassEnum = 1 // HDD
	StorageClassEnumFast        StorageClassEnum = 2 // SSD
	StorageClassEnumUltraFast   StorageClassEnum = 3 // NVMe
	StorageClassEnumCSD         StorageClassEnum = 4 // Computational Storage
	StorageClassEnumMemory      StorageClassEnum = 5 // Memory-based
)

func (s StorageClassEnum) String() string {
	switch s {
	case StorageClassEnumStandard:
		return "hdd"
	case StorageClassEnumFast:
		return "ssd"
	case StorageClassEnumUltraFast:
		return "nvme"
	case StorageClassEnumCSD:
		return "csd"
	case StorageClassEnumMemory:
		return "memory"
	default:
		return "unknown"
	}
}

type PreprocessingTypeEnum int32

const (
	PreprocessingTypeEnumUnknown        PreprocessingTypeEnum = 0
	PreprocessingTypeEnumAugmentation   PreprocessingTypeEnum = 1 // 데이터 증강
	PreprocessingTypeEnumTransformation PreprocessingTypeEnum = 2 // 데이터 변환
	PreprocessingTypeEnumFiltering      PreprocessingTypeEnum = 3 // 데이터 필터링
	PreprocessingTypeEnumAggregation    PreprocessingTypeEnum = 4 // 데이터 집계
	PreprocessingTypeEnumSharding       PreprocessingTypeEnum = 5 // 데이터 샤딩
	PreprocessingTypeEnumNormalization  PreprocessingTypeEnum = 6 // 정규화
	PreprocessingTypeEnumFeatureExtract PreprocessingTypeEnum = 7 // 특성 추출
)

func (p PreprocessingTypeEnum) String() string {
	switch p {
	case PreprocessingTypeEnumAugmentation:
		return "augmentation"
	case PreprocessingTypeEnumTransformation:
		return "transformation"
	case PreprocessingTypeEnumFiltering:
		return "filtering"
	case PreprocessingTypeEnumAggregation:
		return "aggregation"
	case PreprocessingTypeEnumSharding:
		return "sharding"
	case PreprocessingTypeEnumNormalization:
		return "normalization"
	case PreprocessingTypeEnumFeatureExtract:
		return "feature_extraction"
	default:
		return "unknown"
	}
}

// ============================================
// Sub-message Definitions
// ============================================

// NodePreference represents node scoring preference from APOLLO
type NodePreference struct {
	NodeName string `protobuf:"bytes,1,opt,name=node_name,json=nodeName,proto3" json:"node_name,omitempty"`
	Score    int32  `protobuf:"varint,2,opt,name=score,proto3" json:"score,omitempty"`
	Reason   string `protobuf:"bytes,3,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (m *NodePreference) Reset()         { *m = NodePreference{} }
func (m *NodePreference) String() string { return "" }
func (*NodePreference) ProtoMessage()    {}

// ComputedResourceRequirements represents computed resource needs
type ComputedResourceRequirements struct {
	CpuCores               float64 `protobuf:"fixed64,1,opt,name=cpu_cores,json=cpuCores,proto3" json:"cpu_cores,omitempty"`
	MemoryBytes            int64   `protobuf:"varint,2,opt,name=memory_bytes,json=memoryBytes,proto3" json:"memory_bytes,omitempty"`
	StorageBytes           int64   `protobuf:"varint,3,opt,name=storage_bytes,json=storageBytes,proto3" json:"storage_bytes,omitempty"`
	GpuCount               int32   `protobuf:"varint,4,opt,name=gpu_count,json=gpuCount,proto3" json:"gpu_count,omitempty"`
	ExpectedIops           int64   `protobuf:"varint,5,opt,name=expected_iops,json=expectedIops,proto3" json:"expected_iops,omitempty"`
	ExpectedThroughputMbps int64   `protobuf:"varint,6,opt,name=expected_throughput_mbps,json=expectedThroughputMbps,proto3" json:"expected_throughput_mbps,omitempty"`
}

func (m *ComputedResourceRequirements) Reset()         { *m = ComputedResourceRequirements{} }
func (m *ComputedResourceRequirements) String() string { return "" }
func (*ComputedResourceRequirements) ProtoMessage()    {}

// StorageRequirements represents storage requirements (핵심!)
type StorageRequirements struct {
	// 권장 스토리지 클래스
	StorageClass StorageClassEnum `protobuf:"varint,1,opt,name=storage_class,json=storageClass,proto3,enum=apollo.StorageClass" json:"storage_class,omitempty"`

	// 필요 용량
	SizeBytes int64 `protobuf:"varint,2,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`

	// 최소 IOPS
	MinIops int64 `protobuf:"varint,3,opt,name=min_iops,json=minIops,proto3" json:"min_iops,omitempty"`

	// 최소 처리량 (MB/s)
	MinThroughputMbps int64 `protobuf:"varint,4,opt,name=min_throughput_mbps,json=minThroughputMbps,proto3" json:"min_throughput_mbps,omitempty"`

	// 예상 I/O 패턴
	ExpectedIoPattern IOPatternEnum `protobuf:"varint,5,opt,name=expected_io_pattern,json=expectedIoPattern,proto3,enum=apollo.IOPattern" json:"expected_io_pattern,omitempty"`

	// CSD 필요 여부
	RequiresCsd bool `protobuf:"varint,6,opt,name=requires_csd,json=requiresCsd,proto3" json:"requires_csd,omitempty"`
}

func (m *StorageRequirements) Reset()         { *m = StorageRequirements{} }
func (m *StorageRequirements) String() string { return "" }
func (*StorageRequirements) ProtoMessage()    {}

// SchedulingConstraint represents scheduling constraints
type SchedulingConstraint struct {
	ConstraintType string   `protobuf:"bytes,1,opt,name=constraint_type,json=constraintType,proto3" json:"constraint_type,omitempty"`
	Key            string   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Operator       string   `protobuf:"bytes,3,opt,name=operator,proto3" json:"operator,omitempty"`
	Values         []string `protobuf:"bytes,4,rep,name=values,proto3" json:"values,omitempty"`
}

func (m *SchedulingConstraint) Reset()         { *m = SchedulingConstraint{} }
func (m *SchedulingConstraint) String() string { return "" }
func (*SchedulingConstraint) ProtoMessage()    {}

// AffinityRule represents affinity/anti-affinity rules
type AffinityRule struct {
	RuleType     string   `protobuf:"bytes,1,opt,name=rule_type,json=ruleType,proto3" json:"rule_type,omitempty"`
	TopologyKey  string   `protobuf:"bytes,2,opt,name=topology_key,json=topologyKey,proto3" json:"topology_key,omitempty"`
	TargetLabels []string `protobuf:"bytes,3,rep,name=target_labels,json=targetLabels,proto3" json:"target_labels,omitempty"`
	IsRequired   bool     `protobuf:"varint,4,opt,name=is_required,json=isRequired,proto3" json:"is_required,omitempty"`
	Weight       int32    `protobuf:"varint,5,opt,name=weight,proto3" json:"weight,omitempty"`
}

func (m *AffinityRule) Reset()         { *m = AffinityRule{} }
func (m *AffinityRule) String() string { return "" }
func (*AffinityRule) ProtoMessage()    {}

// SchedulingResult is the feedback to APOLLO after scheduling
type SchedulingResult struct {
	RequestId            string `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	PodName              string `protobuf:"bytes,2,opt,name=pod_name,json=podName,proto3" json:"pod_name,omitempty"`
	PodNamespace         string `protobuf:"bytes,3,opt,name=pod_namespace,json=podNamespace,proto3" json:"pod_namespace,omitempty"`
	ScheduledNode        string `protobuf:"bytes,4,opt,name=scheduled_node,json=scheduledNode,proto3" json:"scheduled_node,omitempty"`
	Success              bool   `protobuf:"varint,5,opt,name=success,proto3" json:"success,omitempty"`
	FailureReason        string `protobuf:"bytes,6,opt,name=failure_reason,json=failureReason,proto3" json:"failure_reason,omitempty"`
	SchedulingDurationMs int64  `protobuf:"varint,7,opt,name=scheduling_duration_ms,json=schedulingDurationMs,proto3" json:"scheduling_duration_ms,omitempty"`
}

func (m *SchedulingResult) Reset()         { *m = SchedulingResult{} }
func (m *SchedulingResult) String() string { return "" }
func (*SchedulingResult) ProtoMessage()    {}

// ReportResponse is the response from APOLLO
type ReportResponse struct {
	Success   bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Message   string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	RequestId string `protobuf:"bytes,3,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
}

func (m *ReportResponse) Reset()         { *m = ReportResponse{} }
func (m *ReportResponse) String() string { return "" }
func (*ReportResponse) ProtoMessage()    {}
