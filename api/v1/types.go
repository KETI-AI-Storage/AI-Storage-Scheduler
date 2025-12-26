// ============================================
// AIStorageConfig CRD Types
// 스케줄러 동적 설정을 위한 Kubernetes CRD 타입 정의
// ============================================

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AIStorageConfig is the Schema for the aistorageconfigs API
type AIStorageConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AIStorageConfigSpec `json:"spec,omitempty"`
}

// AIStorageConfigSpec defines the desired state of AIStorageConfig
type AIStorageConfigSpec struct {
	// Plugin configurations
	Plugins PluginsConfig `json:"plugins,omitempty"`

	// Preprocessing type characteristics
	PreprocessingTypes map[string]PreprocessingTypeConfig `json:"preprocessingTypes,omitempty"`

	// Storage tier scoring configurations
	StorageTiers StorageTiersConfig `json:"storageTiers,omitempty"`

	// APOLLO connection configuration
	Apollo ApolloConfig `json:"apollo,omitempty"`
}

// ============================================
// Plugin Configurations
// ============================================

type PluginsConfig struct {
	DataLocalityAware DataLocalityAwareConfig `json:"dataLocalityAware,omitempty"`
	StorageTierAware  StorageTierAwareConfig  `json:"storageTierAware,omitempty"`
	IOPatternBased    IOPatternBasedConfig    `json:"ioPatternBased,omitempty"`
}

type DataLocalityAwareConfig struct {
	Enabled bool                         `json:"enabled,omitempty"`
	Weight  int                          `json:"weight,omitempty"`
	Scoring DataLocalityAwareScoringConfig `json:"scoring,omitempty"`
}

type DataLocalityAwareScoringConfig struct {
	ApolloScoreMax      int `json:"apolloScoreMax,omitempty"`
	PVCLocalityScoreMax int `json:"pvcLocalityScoreMax,omitempty"`
	CacheScoreMax       int `json:"cacheScoreMax,omitempty"`
	TopologyScoreMax    int `json:"topologyScoreMax,omitempty"`
}

type StorageTierAwareConfig struct {
	Enabled bool                        `json:"enabled,omitempty"`
	Weight  int                         `json:"weight,omitempty"`
	Scoring StorageTierAwareScoringConfig `json:"scoring,omitempty"`
}

type StorageTierAwareScoringConfig struct {
	IOPatternScoreMax     int `json:"ioPatternScoreMax,omitempty"`
	PipelineStageScoreMax int `json:"pipelineStageScoreMax,omitempty"`
	IOPSScoreMax          int `json:"iopsScoreMax,omitempty"`
	CapacityScoreMax      int `json:"capacityScoreMax,omitempty"`
}

type IOPatternBasedConfig struct {
	Enabled bool                      `json:"enabled,omitempty"`
	Weight  int                       `json:"weight,omitempty"`
	Scoring IOPatternBasedScoringConfig `json:"scoring,omitempty"`
}

type IOPatternBasedScoringConfig struct {
	ApolloPreferenceScoreMax int `json:"apolloPreferenceScoreMax,omitempty"`
	ResourceMatchScoreMax    int `json:"resourceMatchScoreMax,omitempty"`
	IOOptimizationScoreMax   int `json:"ioOptimizationScoreMax,omitempty"`
	ExpansionScoreMax        int `json:"expansionScoreMax,omitempty"`
	CSDScoreMax              int `json:"csdScoreMax,omitempty"`
}

// ============================================
// Preprocessing Type Configuration
// ============================================

type PreprocessingTypeConfig struct {
	ReadIntensive    bool    `json:"readIntensive,omitempty"`
	WriteIntensive   bool    `json:"writeIntensive,omitempty"`
	CPUIntensive     bool    `json:"cpuIntensive,omitempty"`
	MemoryIntensive  bool    `json:"memoryIntensive,omitempty"`
	DataExpansion    float64 `json:"dataExpansion,omitempty"`
	SequentialAccess bool    `json:"sequentialAccess,omitempty"`
	RequiresCSD      bool    `json:"requiresCSD,omitempty"`
}

// ============================================
// Storage Tier Configurations
// ============================================

type StorageTiersConfig struct {
	NVMe NVMeTierConfig `json:"nvme,omitempty"`
	SSD  SSDTierConfig  `json:"ssd,omitempty"`
	HDD  HDDTierConfig  `json:"hdd,omitempty"`
	CSD  CSDTierConfig  `json:"csd,omitempty"`
}

type NVMeTierConfig struct {
	ReadHeavyScore  int `json:"readHeavyScore,omitempty"`
	WriteHeavyScore int `json:"writeHeavyScore,omitempty"`
	RandomScore     int `json:"randomScore,omitempty"`
	SequentialScore int `json:"sequentialScore,omitempty"`
}

type SSDTierConfig struct {
	ReadHeavyScore  int `json:"readHeavyScore,omitempty"`
	WriteHeavyScore int `json:"writeHeavyScore,omitempty"`
	RandomScore     int `json:"randomScore,omitempty"`
	SequentialScore int `json:"sequentialScore,omitempty"`
}

type HDDTierConfig struct {
	ReadHeavyScore  int `json:"readHeavyScore,omitempty"`
	WriteHeavyScore int `json:"writeHeavyScore,omitempty"`
	RandomScore     int `json:"randomScore,omitempty"`
	SequentialScore int `json:"sequentialScore,omitempty"`
}

type CSDTierConfig struct {
	FilteringBonus      int `json:"filteringBonus,omitempty"`
	AggregationBonus    int `json:"aggregationBonus,omitempty"`
	TransformationBonus int `json:"transformationBonus,omitempty"`
}

// ============================================
// APOLLO Configuration
// ============================================

type ApolloConfig struct {
	Endpoint                 string `json:"endpoint,omitempty"`
	CacheExpirySeconds       int    `json:"cacheExpirySeconds,omitempty"`
	ConnectionTimeoutSeconds int    `json:"connectionTimeoutSeconds,omitempty"`
	FallbackEnabled          bool   `json:"fallbackEnabled,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AIStorageConfigList contains a list of AIStorageConfig
type AIStorageConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIStorageConfig `json:"items"`
}

// ============================================
// Default Values
// ============================================

func DefaultAIStorageConfigSpec() AIStorageConfigSpec {
	return AIStorageConfigSpec{
		Plugins: PluginsConfig{
			DataLocalityAware: DataLocalityAwareConfig{
				Enabled: true,
				Weight:  3,
				Scoring: DataLocalityAwareScoringConfig{
					ApolloScoreMax:      30,
					PVCLocalityScoreMax: 30,
					CacheScoreMax:       20,
					TopologyScoreMax:    20,
				},
			},
			StorageTierAware: StorageTierAwareConfig{
				Enabled: true,
				Weight:  3,
				Scoring: StorageTierAwareScoringConfig{
					IOPatternScoreMax:     40,
					PipelineStageScoreMax: 30,
					IOPSScoreMax:          20,
					CapacityScoreMax:      10,
				},
			},
			IOPatternBased: IOPatternBasedConfig{
				Enabled: true,
				Weight:  3,
				Scoring: IOPatternBasedScoringConfig{
					ApolloPreferenceScoreMax: 15,
					ResourceMatchScoreMax:    25,
					IOOptimizationScoreMax:   20,
					ExpansionScoreMax:        20,
					CSDScoreMax:              20,
				},
			},
		},
		PreprocessingTypes: map[string]PreprocessingTypeConfig{
			"augmentation": {
				ReadIntensive:    true,
				WriteIntensive:   true,
				CPUIntensive:     true,
				MemoryIntensive:  true,
				DataExpansion:    5.0,
				SequentialAccess: true,
				RequiresCSD:      false,
			},
			"transformation": {
				ReadIntensive:    true,
				WriteIntensive:   true,
				CPUIntensive:     true,
				MemoryIntensive:  false,
				DataExpansion:    1.0,
				SequentialAccess: true,
				RequiresCSD:      true,
			},
			"filtering": {
				ReadIntensive:    true,
				WriteIntensive:   false,
				CPUIntensive:     false,
				MemoryIntensive:  false,
				DataExpansion:    0.3,
				SequentialAccess: true,
				RequiresCSD:      true,
			},
			"aggregation": {
				ReadIntensive:    true,
				WriteIntensive:   false,
				CPUIntensive:     true,
				MemoryIntensive:  true,
				DataExpansion:    0.01,
				SequentialAccess: true,
				RequiresCSD:      true,
			},
			"sharding": {
				ReadIntensive:    true,
				WriteIntensive:   true,
				CPUIntensive:     false,
				MemoryIntensive:  false,
				DataExpansion:    1.0,
				SequentialAccess: true,
				RequiresCSD:      false,
			},
			"normalization": {
				ReadIntensive:    true,
				WriteIntensive:   true,
				CPUIntensive:     true,
				MemoryIntensive:  false,
				DataExpansion:    1.0,
				SequentialAccess: true,
				RequiresCSD:      true,
			},
			"featureExtraction": {
				ReadIntensive:    true,
				WriteIntensive:   false,
				CPUIntensive:     true,
				MemoryIntensive:  true,
				DataExpansion:    0.1,
				SequentialAccess: true,
				RequiresCSD:      true,
			},
		},
		StorageTiers: StorageTiersConfig{
			NVMe: NVMeTierConfig{
				ReadHeavyScore:  40,
				WriteHeavyScore: 40,
				RandomScore:     40,
				SequentialScore: 35,
			},
			SSD: SSDTierConfig{
				ReadHeavyScore:  30,
				WriteHeavyScore: 25,
				RandomScore:     30,
				SequentialScore: 30,
			},
			HDD: HDDTierConfig{
				ReadHeavyScore:  20,
				WriteHeavyScore: 15,
				RandomScore:     10,
				SequentialScore: 25,
			},
			CSD: CSDTierConfig{
				FilteringBonus:      20,
				AggregationBonus:    20,
				TransformationBonus: 15,
			},
		},
		Apollo: ApolloConfig{
			Endpoint:                 "apollo-policy-server.keti.svc.cluster.local:50051",
			CacheExpirySeconds:       30,
			ConnectionTimeoutSeconds: 10,
			FallbackEnabled:          true,
		},
	}
}
