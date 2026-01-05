// ============================================
// APOLLO Client for AI Storage Scheduler
// 스케줄러가 APOLLO로부터 워크로드 정보와 스토리지 정책을 조회
// ============================================
//
// 데이터 흐름:
// 1. insight-trace가 Pod의 syscall, 프로세스 정보를 분석하여 WorkloadSignature 생성
// 2. insight-trace → APOLLO: WorkloadSignature 전송 (gRPC)
// 3. APOLLO가 WorkloadSignature를 저장하고 분석하여 SchedulingPolicy 생성
// 4. scheduler → APOLLO: SchedulingPolicy 요청 (이 클라이언트)
// 5. scheduler가 SchedulingPolicy를 기반으로 노드 스코어링
// ============================================

package apollo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	logger "keti/ai-storage-scheduler/internal/backend/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the APOLLO gRPC client for scheduler
type Client struct {
	endpoint  string
	conn      *grpc.ClientConn
	client    SchedulingPolicyServiceClient
	connected bool
	mu        sync.RWMutex

	// Cached scheduling policies
	policyCache   map[string]*SchedulingPolicy // pod key -> policy
	cacheMu       sync.RWMutex
	cacheExpiry   time.Duration
	cacheUpdateAt map[string]time.Time
}

var (
	globalClient *Client
	clientOnce   sync.Once
)

// GetClient returns the singleton APOLLO client
func GetClient() *Client {
	clientOnce.Do(func() {
		endpoint := os.Getenv("APOLLO_ENDPOINT")
		if endpoint == "" {
			endpoint = "apollo-policy-server.keti.svc.cluster.local:50051"
		}

		globalClient = &Client{
			endpoint:      endpoint,
			policyCache:   make(map[string]*SchedulingPolicy),
			cacheUpdateAt: make(map[string]time.Time),
			cacheExpiry:   30 * time.Second,
		}

		// Try to connect on init (non-blocking)
		go func() {
			if err := globalClient.Connect(); err != nil {
				logger.Warn("[APOLLO-Client] Initial connection failed (will retry on demand)", "error", err.Error())
			}
		}()
	})
	return globalClient
}

// NewClient creates a new APOLLO client with custom endpoint
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint:      endpoint,
		policyCache:   make(map[string]*SchedulingPolicy),
		cacheUpdateAt: make(map[string]time.Time),
		cacheExpiry:   30 * time.Second,
	}
}

// Connect establishes connection to APOLLO
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	logger.Info("[APOLLO-Client] Connecting to APOLLO", "endpoint", c.endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		c.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to APOLLO: %w", err)
	}

	c.conn = conn
	c.client = NewSchedulingPolicyServiceClient(conn)
	c.connected = true

	logger.Info("[APOLLO-Client] Connected to APOLLO", "endpoint", c.endpoint)
	return nil
}

// Close closes the connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.connected = false
		return c.conn.Close()
	}
	return nil
}

// IsConnected returns connection status
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// GetSchedulingPolicy retrieves the full scheduling policy from APOLLO
// 이 함수가 핵심! APOLLO로부터 워크로드 분석 결과를 받아옴
func (c *Client) GetSchedulingPolicy(podNamespace, podName, podUID string, labels, annotations map[string]string) (*SchedulingPolicy, error) {
	cacheKey := fmt.Sprintf("%s/%s", podNamespace, podName)

	// Check cache first
	c.cacheMu.RLock()
	if policy, ok := c.policyCache[cacheKey]; ok {
		if updateAt, ok := c.cacheUpdateAt[cacheKey]; ok {
			if time.Since(updateAt) < c.cacheExpiry {
				c.cacheMu.RUnlock()
				return policy, nil
			}
		}
	}
	c.cacheMu.RUnlock()

	// Ensure connection
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			logger.Warn("[APOLLO-Client] Connection failed, returning empty policy", "error", err.Error())
			return c.createEmptyPolicy(podNamespace, podName), nil
		}
	}

	// Request from APOLLO
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &SchedulingPolicyRequest{
		PodName:        podName,
		PodNamespace:   podNamespace,
		PodUid:         podUID,
		PodLabels:      labels,
		PodAnnotations: annotations,
	}

	policy, err := c.client.GetSchedulingPolicy(ctx, req)
	if err != nil {
		logger.Warn("[APOLLO-Client] Failed to get scheduling policy", "namespace", podNamespace, "pod", podName, "error", err.Error())

		// Return cached policy if available
		c.cacheMu.RLock()
		if cached, ok := c.policyCache[cacheKey]; ok {
			c.cacheMu.RUnlock()
			return cached, nil
		}
		c.cacheMu.RUnlock()

		return c.createEmptyPolicy(podNamespace, podName), nil
	}

	// Update cache
	c.cacheMu.Lock()
	c.policyCache[cacheKey] = policy
	c.cacheUpdateAt[cacheKey] = time.Now()
	c.cacheMu.Unlock()

	logger.Info("[APOLLO-Client] Got scheduling policy",
		"namespace", podNamespace, "pod", podName,
		"stage", c.getStageName(policy),
		"io_pattern", c.getIOPatternName(policy),
		"storage_class", c.getStorageClassName(policy))

	return policy, nil
}

// createEmptyPolicy creates an empty policy for graceful degradation
func (c *Client) createEmptyPolicy(namespace, name string) *SchedulingPolicy {
	return &SchedulingPolicy{
		PodName:      name,
		PodNamespace: namespace,
		Decision:     SchedulingDecisionAllow,
	}
}

// Helper methods to extract info from policy
func (c *Client) getStageName(policy *SchedulingPolicy) string {
	if policy.WorkloadSignature != nil {
		return policy.WorkloadSignature.CurrentStage.String()
	}
	return "unknown"
}

func (c *Client) getIOPatternName(policy *SchedulingPolicy) string {
	if policy.StorageRequirements != nil {
		return policy.StorageRequirements.ExpectedIoPattern.String()
	}
	if policy.WorkloadSignature != nil {
		return policy.WorkloadSignature.IoPattern.String()
	}
	return "unknown"
}

func (c *Client) getStorageClassName(policy *SchedulingPolicy) string {
	if policy.StorageRequirements != nil {
		return policy.StorageRequirements.StorageClass.String()
	}
	return "unknown"
}

// ReportSchedulingResult sends scheduling result feedback to APOLLO
func (c *Client) ReportSchedulingResult(requestID, podNamespace, podName, scheduledNode string, success bool, failureReason string, durationMs int64) error {
	if !c.IsConnected() {
		return fmt.Errorf("not connected to APOLLO")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := &SchedulingResult{
		RequestId:            requestID,
		PodName:              podName,
		PodNamespace:         podNamespace,
		ScheduledNode:        scheduledNode,
		Success:              success,
		FailureReason:        failureReason,
		SchedulingDurationMs: durationMs,
	}

	_, err := c.client.ReportSchedulingResult(ctx, result)
	if err != nil {
		logger.Warn("[APOLLO-Client] Failed to report scheduling result", "error", err.Error())
		return err
	}

	return nil
}

// ============================================
// Policy Helper Functions for Plugins
// ============================================

// IsPreprocessingWorkload checks if the policy indicates preprocessing workload
func IsPreprocessingWorkload(policy *SchedulingPolicy) bool {
	if policy == nil {
		return false
	}

	if policy.WorkloadSignature != nil {
		stage := policy.WorkloadSignature.CurrentStage
		if stage == PipelineStageEnumPreprocessing || stage == PipelineStageEnumDataLoading {
			return true
		}

		step := policy.WorkloadSignature.PipelineStep
		if step == "preprocess" || step == "preprocessing" || step == "data-loading" {
			return true
		}
	}

	return false
}

// GetStorageClass returns the recommended storage class from policy
func GetStorageClass(policy *SchedulingPolicy) StorageClassEnum {
	if policy == nil || policy.StorageRequirements == nil {
		return StorageClassEnumUnspecified
	}
	return policy.StorageRequirements.StorageClass
}

// GetIOPattern returns the expected I/O pattern from policy
func GetIOPattern(policy *SchedulingPolicy) IOPatternEnum {
	if policy == nil {
		return IOPatternEnumUnknown
	}

	if policy.StorageRequirements != nil {
		return policy.StorageRequirements.ExpectedIoPattern
	}

	if policy.WorkloadSignature != nil {
		return policy.WorkloadSignature.IoPattern
	}

	return IOPatternEnumUnknown
}

// GetPreprocessingType returns the preprocessing type from policy
func GetPreprocessingType(policy *SchedulingPolicy) PreprocessingTypeEnum {
	if policy == nil || policy.WorkloadSignature == nil {
		return PreprocessingTypeEnumUnknown
	}
	return policy.WorkloadSignature.PreprocessingType
}

// RequiresCSD checks if the workload requires CSD
func RequiresCSD(policy *SchedulingPolicy) bool {
	if policy == nil || policy.StorageRequirements == nil {
		return false
	}
	return policy.StorageRequirements.RequiresCsd
}

// GetMinIOPS returns the minimum required IOPS
func GetMinIOPS(policy *SchedulingPolicy) int64 {
	if policy == nil || policy.StorageRequirements == nil {
		return 0
	}
	return policy.StorageRequirements.MinIops
}

// GetMinThroughput returns the minimum required throughput in MB/s
func GetMinThroughput(policy *SchedulingPolicy) int64 {
	if policy == nil || policy.StorageRequirements == nil {
		return 0
	}
	return policy.StorageRequirements.MinThroughputMbps
}

// GetDataLocations returns nodes where data is located
func GetDataLocations(policy *SchedulingPolicy) []string {
	if policy == nil || policy.WorkloadSignature == nil {
		return nil
	}
	return policy.WorkloadSignature.DataLocations
}

// GetCachedNodes returns nodes where dataset is cached
func GetCachedNodes(policy *SchedulingPolicy) []string {
	if policy == nil || policy.WorkloadSignature == nil {
		return nil
	}
	return policy.WorkloadSignature.CachedOnNodes
}

// GetNodePreferences returns APOLLO's node preferences
func GetNodePreferences(policy *SchedulingPolicy) []*NodePreference {
	if policy == nil {
		return nil
	}
	return policy.NodePreferences
}

// GetNodePreferenceScore returns the preference score for a specific node
func GetNodePreferenceScore(policy *SchedulingPolicy, nodeName string) int32 {
	if policy == nil {
		return 0
	}

	for _, pref := range policy.NodePreferences {
		if pref.NodeName == nodeName {
			return pref.Score
		}
	}

	return 0
}

// IsGPUWorkload checks if the workload requires GPU
func IsGPUWorkload(policy *SchedulingPolicy) bool {
	if policy == nil || policy.WorkloadSignature == nil {
		return false
	}
	return policy.WorkloadSignature.IsGpuWorkload
}

// IsPipelineWorkload checks if this is a pipeline/DAG workload
func IsPipelineWorkload(policy *SchedulingPolicy) bool {
	if policy == nil || policy.WorkloadSignature == nil {
		return false
	}
	return policy.WorkloadSignature.IsPipeline
}

// GetPipelineStep returns the current pipeline step
func GetPipelineStep(policy *SchedulingPolicy) string {
	if policy == nil || policy.WorkloadSignature == nil {
		return ""
	}
	return policy.WorkloadSignature.PipelineStep
}

// GetWorkloadType returns the workload type
func GetWorkloadType(policy *SchedulingPolicy) WorkloadTypeEnum {
	if policy == nil || policy.WorkloadSignature == nil {
		return WorkloadTypeEnumUnknown
	}
	return policy.WorkloadSignature.WorkloadType
}

// GetConfidence returns the analysis confidence level
func GetConfidence(policy *SchedulingPolicy) float64 {
	if policy == nil || policy.WorkloadSignature == nil {
		return 0
	}
	return policy.WorkloadSignature.Confidence
}
