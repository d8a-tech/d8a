package receiver

import (
	"net/netip"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
)

func maskIPByPrivacyLevel(ip string, level int) string {
	if level == 0 {
		return ip
	}

	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}

	v4BitsByLevel := map[int]int{1: 24, 2: 16, 3: 8, 4: 0}
	v6BitsByLevel := map[int]int{1: 48, 2: 32, 3: 16, 4: 0}

	bits, ok := func() (int, bool) {
		if addr.Is4() {
			bits, ok := v4BitsByLevel[level]
			return bits, ok
		}

		bits, ok := v6BitsByLevel[level]
		return bits, ok
	}()
	if !ok {
		return ip
	}

	return netip.PrefixFrom(addr, bits).Masked().Addr().String()
}

// IPMasking returns a hit processing rule that masks hit IPs per property settings.
func IPMasking(settings properties.SettingsRegistry) HitProcessingRule {
	return NewSimpleHitProcessingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
		propertySettings, err := settings.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return err
		}

		if err := properties.ValidateSettings(propertySettings); err != nil {
			return err
		}

		hit.MustParsedRequest().IP = maskIPByPrivacyLevel(hit.MustParsedRequest().IP, propertySettings.IPMaskingLevel)

		return nil
	})
}
