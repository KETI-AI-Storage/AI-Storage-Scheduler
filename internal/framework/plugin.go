package framework

type Plugins struct {
	// PreEnqueue is a list of plugins that should be invoked before adding pods to the scheduling queue.
	PreEnqueue PluginSet

	// QueueSort is a list of plugins that should be invoked when sorting pods in the scheduling queue.
	QueueSort PluginSet

	// PreFilter is a list of plugins that should be invoked at "PreFilter" extension point of the scheduling framework.
	PreFilter PluginSet

	// Filter is a list of plugins that should be invoked when filtering out nodes that cannot run the Pod.
	Filter PluginSet

	// PostFilter is a list of plugins that are invoked after filtering phase, but only when no feasible nodes were found for the pod.
	PostFilter PluginSet

	// PreScore is a list of plugins that are invoked before scoring.
	PreScore PluginSet

	// Score is a list of plugins that should be invoked when ranking nodes that have passed the filtering phase.
	Score PluginSet

	// Reserve is a list of plugins invoked when reserving/unreserving resources
	// after a node is assigned to run the pod.
	Reserve PluginSet

	// Permit is a list of plugins that control binding of a Pod. These plugins can prevent or delay binding of a Pod.
	Permit PluginSet

	// PreBind is a list of plugins that should be invoked before a pod is bound.
	PreBind PluginSet

	// Bind is a list of plugins that should be invoked at "Bind" extension point of the scheduling framework.
	// The scheduler call these plugins in order. Scheduler skips the rest of these plugins as soon as one returns success.
	Bind PluginSet

	// PostBind is a list of plugins that should be invoked after a pod is successfully bound.
	PostBind PluginSet

	// MultiPoint is a simplified config field for enabling plugins for all valid extension points
	MultiPoint PluginSet
}

type PluginSet struct {
	// Enabled specifies plugins that should be enabled in addition to default plugins.
	// These are called after default plugins and in the same order specified here.
	Enabled []Plugin
	// Disabled specifies default plugins that should be disabled.
	// When all default plugins need to be disabled, an array containing only one "*" should be provided.
	Disabled []Plugin
}

// Plugin specifies a plugin name and its weight when applicable. Weight is used only for Score plugins.
type Plugin struct {
	// Name defines the name of plugin
	Name string
	// Weight defines the weight of plugin, only used for Score plugins.
	Weight int32
}
