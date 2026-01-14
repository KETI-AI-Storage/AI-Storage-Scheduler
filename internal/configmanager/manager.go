// ============================================
// Dynamic Configuration Manager
// CRD를 감시하고 플러그인에 동적 설정 제공
// ============================================

package configmanager

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	v1 "keti/ai-storage-scheduler/api/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	manager     *ConfigManager
	managerOnce sync.Once
)

// ConfigManager manages dynamic scheduler configuration from CRD
type ConfigManager struct {
	mu            sync.RWMutex
	config        v1.AIStorageConfigSpec
	dynamicClient dynamic.Interface
	stopCh        chan struct{}
	namespace     string
	configName    string
	callbacks     []func(v1.AIStorageConfigSpec)
}

// GetManager returns the singleton ConfigManager instance
func GetManager() *ConfigManager {
	managerOnce.Do(func() {
		manager = &ConfigManager{
			config:     v1.DefaultAIStorageConfigSpec(),
			stopCh:     make(chan struct{}),
			namespace:  "keti",
			configName: "default",
			callbacks:  make([]func(v1.AIStorageConfigSpec), 0),
		}
	})
	return manager
}

// Initialize starts the CRD watcher
func (m *ConfigManager) Initialize(config *rest.Config) error {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Printf("[ConfigManager] Failed to create dynamic client: %v, using defaults", err)
		return err
	}
	m.dynamicClient = dynamicClient

	// Start watching CRD
	go m.watchConfig()

	// Initial load
	m.loadConfig()

	log.Printf("[ConfigManager] Initialized with namespace=%s, configName=%s", m.namespace, m.configName)
	return nil
}

// SetNamespace sets the namespace to watch
func (m *ConfigManager) SetNamespace(namespace string) {
	m.namespace = namespace
}

// SetConfigName sets the config name to watch
func (m *ConfigManager) SetConfigName(name string) {
	m.configName = name
}

// RegisterCallback registers a callback function to be called when config changes
func (m *ConfigManager) RegisterCallback(callback func(v1.AIStorageConfigSpec)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks = append(m.callbacks, callback)
}

// GetConfig returns the current configuration
func (m *ConfigManager) GetConfig() v1.AIStorageConfigSpec {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// GetPluginConfig returns plugin-specific configuration
func (m *ConfigManager) GetPluginConfig() v1.PluginsConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Plugins
}

// GetDataLocalityConfig returns DataLocalityAware plugin configuration
func (m *ConfigManager) GetDataLocalityConfig() v1.DataLocalityAwareConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Plugins.DataLocalityAware
}

// GetStorageTierConfig returns StorageTierAware plugin configuration
func (m *ConfigManager) GetStorageTierConfig() v1.StorageTierAwareConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Plugins.StorageTierAware
}

// GetIOPatternConfig returns IOPatternBased plugin configuration
func (m *ConfigManager) GetIOPatternConfig() v1.IOPatternBasedConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Plugins.IOPatternBased
}

// GetKueueAwareConfig returns KueueAware plugin configuration
func (m *ConfigManager) GetKueueAwareConfig() v1.KueueAwareConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Plugins.KueueAware
}

// GetPreprocessingTypeConfig returns configuration for a specific preprocessing type
func (m *ConfigManager) GetPreprocessingTypeConfig(typeName string) (v1.PreprocessingTypeConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	config, ok := m.config.PreprocessingTypes[typeName]
	return config, ok
}

// GetStorageTiersConfig returns storage tier configurations
func (m *ConfigManager) GetStorageTiersConfig() v1.StorageTiersConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.StorageTiers
}

// GetApolloConfig returns APOLLO configuration
func (m *ConfigManager) GetApolloConfig() v1.ApolloConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Apollo
}

// watchConfig watches for CRD changes
func (m *ConfigManager) watchConfig() {
	if m.dynamicClient == nil {
		log.Printf("[ConfigManager] No dynamic client, skipping CRD watch")
		return
	}

	gvr := schema.GroupVersionResource{
		Group:    "ai-storage.keti",
		Version:  "v1",
		Resource: "aistorageconfigs",
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		m.dynamicClient,
		30*time.Second,
		m.namespace,
		nil,
	)

	informer := factory.ForResource(gvr).Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.handleConfigChange(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			m.handleConfigChange(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			log.Printf("[ConfigManager] Config deleted, reverting to defaults")
			m.mu.Lock()
			m.config = v1.DefaultAIStorageConfigSpec()
			m.mu.Unlock()
			m.notifyCallbacks()
		},
	})

	log.Printf("[ConfigManager] Starting CRD informer")
	informer.Run(m.stopCh)
}

// handleConfigChange handles CRD add/update events
func (m *ConfigManager) handleConfigChange(obj interface{}) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Printf("[ConfigManager] Failed to convert object to Unstructured")
		return
	}

	name := unstructuredObj.GetName()
	if name != m.configName {
		return
	}

	spec, found, err := unstructured.NestedMap(unstructuredObj.Object, "spec")
	if err != nil || !found {
		log.Printf("[ConfigManager] Failed to get spec from CRD: %v", err)
		return
	}

	// Convert to JSON and back to typed struct
	specJSON, err := json.Marshal(spec)
	if err != nil {
		log.Printf("[ConfigManager] Failed to marshal spec: %v", err)
		return
	}

	var newConfig v1.AIStorageConfigSpec
	if err := json.Unmarshal(specJSON, &newConfig); err != nil {
		log.Printf("[ConfigManager] Failed to unmarshal spec: %v", err)
		return
	}

	// Apply defaults for missing fields
	newConfig = m.applyDefaults(newConfig)

	m.mu.Lock()
	m.config = newConfig
	m.mu.Unlock()

	log.Printf("[ConfigManager] Configuration updated from CRD")
	log.Printf("[ConfigManager] Plugin weights - DataLocality: %d, StorageTier: %d, IOPattern: %d",
		newConfig.Plugins.DataLocalityAware.Weight,
		newConfig.Plugins.StorageTierAware.Weight,
		newConfig.Plugins.IOPatternBased.Weight)

	m.notifyCallbacks()
}

// loadConfig loads the initial configuration from CRD
func (m *ConfigManager) loadConfig() {
	if m.dynamicClient == nil {
		log.Printf("[ConfigManager] No dynamic client, using defaults")
		return
	}

	gvr := schema.GroupVersionResource{
		Group:    "ai-storage.keti",
		Version:  "v1",
		Resource: "aistorageconfigs",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	obj, err := m.dynamicClient.Resource(gvr).Namespace(m.namespace).Get(ctx, m.configName, metav1.GetOptions{})
	if err != nil {
		log.Printf("[ConfigManager] Failed to load initial config: %v, using defaults", err)
		return
	}

	m.handleConfigChange(obj)
}

// applyDefaults applies default values to missing fields
func (m *ConfigManager) applyDefaults(config v1.AIStorageConfigSpec) v1.AIStorageConfigSpec {
	defaults := v1.DefaultAIStorageConfigSpec()

	// Apply plugin defaults
	if config.Plugins.DataLocalityAware.Weight == 0 {
		config.Plugins.DataLocalityAware.Weight = defaults.Plugins.DataLocalityAware.Weight
	}
	if config.Plugins.StorageTierAware.Weight == 0 {
		config.Plugins.StorageTierAware.Weight = defaults.Plugins.StorageTierAware.Weight
	}
	if config.Plugins.IOPatternBased.Weight == 0 {
		config.Plugins.IOPatternBased.Weight = defaults.Plugins.IOPatternBased.Weight
	}

	// Apply scoring defaults
	if config.Plugins.DataLocalityAware.Scoring.ApolloScoreMax == 0 {
		config.Plugins.DataLocalityAware.Scoring = defaults.Plugins.DataLocalityAware.Scoring
	}
	if config.Plugins.StorageTierAware.Scoring.IOPatternScoreMax == 0 {
		config.Plugins.StorageTierAware.Scoring = defaults.Plugins.StorageTierAware.Scoring
	}
	if config.Plugins.IOPatternBased.Scoring.ResourceMatchScoreMax == 0 {
		config.Plugins.IOPatternBased.Scoring = defaults.Plugins.IOPatternBased.Scoring
	}

	// Apply preprocessing type defaults
	if config.PreprocessingTypes == nil {
		config.PreprocessingTypes = defaults.PreprocessingTypes
	}

	// Apply storage tier defaults
	if config.StorageTiers.NVMe.ReadHeavyScore == 0 {
		config.StorageTiers = defaults.StorageTiers
	}

	// Apply Apollo defaults
	if config.Apollo.Endpoint == "" {
		config.Apollo = defaults.Apollo
	}

	return config
}

// notifyCallbacks notifies all registered callbacks
func (m *ConfigManager) notifyCallbacks() {
	m.mu.RLock()
	config := m.config
	callbacks := make([]func(v1.AIStorageConfigSpec), len(m.callbacks))
	copy(callbacks, m.callbacks)
	m.mu.RUnlock()

	for _, callback := range callbacks {
		go callback(config)
	}
}

// Stop stops the config manager
func (m *ConfigManager) Stop() {
	close(m.stopCh)
}
