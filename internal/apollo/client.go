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

// DataSource indicates where the data came from
type DataSource string

const (
	DataSourceAPOLLO   DataSource = "APOLLO"   // Data from APOLLO gRPC
	DataSourceCache    DataSource = "CACHE"    // Data from local cache
	DataSourceFallback DataSource = "FALLBACK" // Fallback (empty policy, use Pod labels)
)

// PolicyResult wraps the policy with source information
type PolicyResult struct {
	Policy *SchedulingPolicy
	Source DataSource
}

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
	result := c.GetSchedulingPolicyWithSource(podNamespace, podName, podUID, labels, annotations)
	return result.Policy, nil
}

// GetSchedulingPolicyWithSource retrieves scheduling policy with data source information
// 데이터가 어디서 왔는지 (APOLLO/CACHE/FALLBACK) 명확하게 추적
func (c *Client) GetSchedulingPolicyWithSource(podNamespace, podName, podUID string, labels, annotations map[string]string) *PolicyResult {
	cacheKey := fmt.Sprintf("%s/%s", podNamespace, podName)

	// Check cache first
	c.cacheMu.RLock()
	if policy, ok := c.policyCache[cacheKey]; ok {
		if updateAt, ok := c.cacheUpdateAt[cacheKey]; ok {
			if time.Since(updateAt) < c.cacheExpiry {
				c.cacheMu.RUnlock()
				logger.Info("[APOLLO-DataSource] Policy retrieved from CACHE",
					"namespace", podNamespace, "pod", podName,
					"cache_age_seconds", int(time.Since(updateAt).Seconds()))
				return &PolicyResult{Policy: policy, Source: DataSourceCache}
			}
		}
	}
	c.cacheMu.RUnlock()

	// Ensure connection
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			logger.Warn("[APOLLO-DataSource] FALLBACK - Connection failed, will use Pod labels/annotations",
				"namespace", podNamespace, "pod", podName, "error", err.Error())
			return &PolicyResult{Policy: c.createEmptyPolicy(podNamespace, podName), Source: DataSourceFallback}
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
		logger.Warn("[APOLLO-DataSource] FALLBACK - gRPC call failed, will use Pod labels/annotations",
			"namespace", podNamespace, "pod", podName, "error", err.Error())

		// Return cached policy if available
		c.cacheMu.RLock()
		if cached, ok := c.policyCache[cacheKey]; ok {
			c.cacheMu.RUnlock()
			logger.Info("[APOLLO-DataSource] Using stale CACHE after gRPC failure",
				"namespace", podNamespace, "pod", podName)
			return &PolicyResult{Policy: cached, Source: DataSourceCache}
		}
		c.cacheMu.RUnlock()

		return &PolicyResult{Policy: c.createEmptyPolicy(podNamespace, podName), Source: DataSourceFallback}
	}

	// Update cache
	c.cacheMu.Lock()
	c.policyCache[cacheKey] = policy
	c.cacheUpdateAt[cacheKey] = time.Now()
	c.cacheMu.Unlock()

	logger.Info("[APOLLO-DataSource] SUCCESS - Policy retrieved from APOLLO gRPC",
		"namespace", podNamespace, "pod", podName,
		"stage", c.getStageName(policy),
		"io_pattern", c.getIOPatternName(policy),
		"storage_class", c.getStorageClassName(policy),
		"preprocessing_type", c.getPreprocessingTypeName(policy),
		"has_node_preferences", len(policy.NodePreferences) > 0,
		"data_locations", policy.WorkloadSignature.GetDataLocations())

	return &PolicyResult{Policy: policy, Source: DataSourceAPOLLO}
}

// createEmptyPolicy creates an empty policy for graceful degradation
func (c *Client) createEmptyPolicy(namespace, name string) *SchedulingPolicy {
	return &SchedulingPolicy{
		PodName:      name,
		PodNamespace: namespace,
		Decision:     SchedulingDecision_SCHEDULING_DECISION_ALLOW,
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

func (c *Client) getPreprocessingTypeName(policy *SchedulingPolicy) string {
	if policy.WorkloadSignature != nil {
		return policy.WorkloadSignature.PreprocessingType.String()
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
		if stage == PipelineStage_PIPELINE_STAGE_PREPROCESSING || stage == PipelineStage_PIPELINE_STAGE_DATA_LOADING {
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
func GetStorageClass(policy *SchedulingPolicy) StorageClass {
	if policy == nil || policy.StorageRequirements == nil {
		return StorageClass_STORAGE_CLASS_UNSPECIFIED
	}
	return policy.StorageRequirements.StorageClass
}

// GetIOPattern returns the expected I/O pattern from policy
func GetIOPattern(policy *SchedulingPolicy) IOPattern {
	if policy == nil {
		return IOPattern_IO_PATTERN_UNKNOWN
	}

	if policy.StorageRequirements != nil {
		return policy.StorageRequirements.ExpectedIoPattern
	}

	if policy.WorkloadSignature != nil {
		return policy.WorkloadSignature.IoPattern
	}

	return IOPattern_IO_PATTERN_UNKNOWN
}

// GetPreprocessingType returns the preprocessing type from policy
func GetPreprocessingType(policy *SchedulingPolicy) PreprocessingType {
	if policy == nil || policy.WorkloadSignature == nil {
		return PreprocessingType_PREPROCESSING_TYPE_UNKNOWN
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
func GetWorkloadType(policy *SchedulingPolicy) WorkloadType {
	if policy == nil || policy.WorkloadSignature == nil {
		return WorkloadType_WORKLOAD_TYPE_UNKNOWN
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
