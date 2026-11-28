package hits

// MetadataKeyRole stores a hit's role in Hit.Metadata. Hits without it are ordinary.
const MetadataKeyRole = "hit_role"

// RoleAnnotation marks a hit that may enrich an already open session but is never an
// analytics event itself: it does not create a session, extend its inactivity deadline,
// count as an event or take part in identity resolution. Only server-side code (for
// example a receiver HitProcessingRule) sets it; no client parameter maps to it.
const RoleAnnotation = "annotation"

// MarkAnnotation gives the hit the annotation role.
func MarkAnnotation(hit *Hit) {
	if hit.Metadata == nil {
		hit.Metadata = map[string]string{}
	}
	hit.Metadata[MetadataKeyRole] = RoleAnnotation
}

// IsAnnotation reports whether the hit has the annotation role.
func IsAnnotation(hit *Hit) bool {
	return hit != nil && hit.Metadata[MetadataKeyRole] == RoleAnnotation
}
