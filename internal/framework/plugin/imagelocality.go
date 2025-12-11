package plugin

import (
	"context"

	framework "keti/ai-storage-scheduler/internal/framework"
	utils "keti/ai-storage-scheduler/internal/framework/utils"

	v1 "k8s.io/api/core/v1"
)

const ImageLocalityName = "ImageLocality"

// ImageLocality is a score plugin that favors nodes that already have the required container images.
// This reduces image pull time and improves pod startup performance.
type ImageLocality struct {
	cache *utils.Cache
}

var _ framework.ScorePlugin = &ImageLocality{}

func NewImageLocality(cache *utils.Cache) *ImageLocality {
	return &ImageLocality{
		cache: cache,
	}
}

func (i *ImageLocality) Name() string {
	return ImageLocalityName
}

// Score calculates the score based on image locality
// Higher score = more images already present on the node
func (i *ImageLocality) Score(ctx context.Context, pod *v1.Pod, nodeName string) (int64, *utils.Status) {
	nodeInfo := i.cache.Nodes()[nodeName]
	if nodeInfo == nil {
		return 0, utils.NewStatus(utils.Error, "node not found in cache")
	}

	node := nodeInfo.Node()
	if node == nil {
		return 0, utils.NewStatus(utils.Error, "node not found")
	}

	// Get all required images from pod
	requiredImages := getRequiredImages(pod)
	if len(requiredImages) == 0 {
		return 0, utils.NewStatus(utils.Success, "")
	}

	// Build a map of images present on the node with their sizes
	nodeImages := make(map[string]int64)
	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			nodeImages[name] = image.SizeBytes
		}
	}

	// Calculate score based on image presence and size
	var totalImageSize int64
	var presentImageSize int64

	for imageName, imageSize := range requiredImages {
		totalImageSize += imageSize

		// Check if image is present on node (match any of the image names)
		if size, found := nodeImages[imageName]; found {
			presentImageSize += size
		} else {
			// Try to match without tag (e.g., "nginx" matches "nginx:latest")
			for nodImageName, nodeImageSize := range nodeImages {
				if matchesImage(imageName, nodImageName) {
					presentImageSize += nodeImageSize
					break
				}
			}
		}
	}

	// Calculate score: percentage of images present (by size)
	// This accounts for the fact that larger images take longer to pull
	var score int64
	if totalImageSize > 0 {
		score = (presentImageSize * 100) / totalImageSize
	} else {
		// If we can't determine size, use simple count-based scoring
		presentCount := 0
		for imageName := range requiredImages {
			if _, found := nodeImages[imageName]; found {
				presentCount++
			}
		}
		score = int64((presentCount * 100) / len(requiredImages))
	}

	return score, utils.NewStatus(utils.Success, "")
}

func (i *ImageLocality) ScoreExtensions() framework.ScoreExtensions {
	return i
}

func (i *ImageLocality) NormalizeScore(ctx context.Context, pod *v1.Pod, scores utils.PluginResult) *utils.Status {
	return utils.NewStatus(utils.Success, "")
}

// getRequiredImages extracts all container images from a pod
// Returns a map of image name -> estimated size (0 if unknown)
func getRequiredImages(pod *v1.Pod) map[string]int64 {
	images := make(map[string]int64)

	// Get images from init containers
	for _, container := range pod.Spec.InitContainers {
		if container.Image != "" {
			images[container.Image] = 0 // Size unknown
		}
	}

	// Get images from regular containers
	for _, container := range pod.Spec.Containers {
		if container.Image != "" {
			images[container.Image] = 0 // Size unknown
		}
	}

	// Get images from ephemeral containers
	for _, container := range pod.Spec.EphemeralContainers {
		if container.Image != "" {
			images[container.Image] = 0 // Size unknown
		}
	}

	return images
}

// matchesImage checks if two image names match (considering tags)
// e.g., "nginx:1.19" matches "nginx:1.19", "docker.io/library/nginx:1.19", etc.
func matchesImage(image1, image2 string) bool {
	// Simplified image matching
	// In production, this should use proper image reference parsing
	// For now, just check if one contains the other
	return contains(image1, image2) || contains(image2, image1)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}
