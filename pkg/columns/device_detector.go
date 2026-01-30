// Package columns provides device detector caching functionality
package columns

import (
	"fmt"
	"strings"
	"time"

	"github.com/archbottle/dd2/pkg/clienthints"
	"github.com/archbottle/dd2/pkg/detector"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/dgraph-io/ristretto/v2"
)

type deviceParser interface {
	GetFullInfo(ua string, ch *clienthints.ClientHints) *detector.FullInfo
}

type cachingDeviceParser struct {
	dd    *detector.DeviceDetector
	cache *ristretto.Cache[string, *detector.FullInfo]
}

func (p *cachingDeviceParser) GetFullInfo(ua string, ch *clienthints.ClientHints) *detector.FullInfo {
	// Build cache key from UA + relevant client hint headers
	cacheKey := buildDeviceCacheKey(ua, ch)
	item, ok := p.cache.Get(cacheKey)
	if ok {
		return item
	}
	result := detector.GetInfoFromUserAgent(p.dd, ua, ch)
	p.cache.SetWithTTL(cacheKey, result, 1, time.Second*30)
	return result
}

// buildDeviceCacheKey creates a cache key from UA and client hints to ensure
// different hint combinations produce different cache entries
func buildDeviceCacheKey(ua string, ch *clienthints.ClientHints) string {
	if ch == nil {
		return ua
	}
	var parts []string
	parts = append(parts, ua)
	if ch.GetBrandList() != nil {
		parts = append(parts, fmt.Sprintf("brands:%v", ch.GetBrandList()))
	}
	if ch.IsMobile() {
		parts = append(parts, "mobile:1")
	}
	if platform := ch.GetOperatingSystem(); platform != "" {
		parts = append(parts, fmt.Sprintf("platform:%s", platform))
	}
	if platformVer := ch.GetOperatingSystemVersion(); platformVer != "" {
		parts = append(parts, fmt.Sprintf("platformVer:%s", platformVer))
	}
	if model := ch.GetModel(); model != "" {
		parts = append(parts, fmt.Sprintf("model:%s", model))
	}
	return strings.Join(parts, "|")
}

var deviceDetector = func() deviceParser {
	dd, err := detector.New()
	if err != nil {
		panic(fmt.Sprintf("Failed to create device detector: %v", err))
	}
	c, err := ristretto.NewCache(&ristretto.Config[string, *detector.FullInfo]{
		NumCounters: 100000,
		MaxCost:     10000,
		BufferItems: 64,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create device detector cache: %v", err))
	}
	return &cachingDeviceParser{
		dd:    dd,
		cache: c,
	}
}()

// DeviceFullInfo retrieves device detection FullInfo for an event, using caching
// to avoid redundant parsing. The result is cached both globally (ristretto) and
// per-event (metadata) for reuse across multiple columns.
func DeviceFullInfo(event *schema.Event) (*detector.FullInfo, error) {
	// Check cache in metadata
	cached, ok := event.Metadata["device_detector_full_info"]
	if ok {
		if result, ok := cached.(*detector.FullInfo); ok {
			return result, nil
		}
	}

	// Parse with dd2
	// Headers are already canonicalized by the receiver package
	headers := event.BoundHit.MustParsedRequest().Headers
	ua := headers.Get("User-Agent")
	ch := clienthints.New(headers)
	result := deviceDetector.GetFullInfo(ua, ch)
	event.Metadata["device_detector_full_info"] = result
	return result, nil
}
