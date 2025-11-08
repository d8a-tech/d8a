// Package ga4 provides implementation of the ga4 protocol, eventually supporting gtag, mp and other sub-protocols
package ga4

const (
	// PageViewEventType is the event type for page views
	PageViewEventType = "page_view"
	// ScrollEventType is the event type for scrolls
	ScrollEventType = "scroll"
	// ClickEventType is the event type for clicks
	ClickEventType = "click"
	// RefundEventType is the event type for refunds
	RefundEventType = "refund"
	// ViewSearchResultsEventType is the event type for view search results
	ViewSearchResultsEventType = "view_search_results"
	// SearchEventType is the event type for searches
	SearchEventType = "search"
	// FormSubmitEventType is the event type for form interactions
	FormSubmitEventType = "form_submit"
	// FormStartEventType is the event type for form starts
	FormStartEventType = "form_start"
	// VideoStartEventType is the event type for video starts
	VideoStartEventType = "video_start"
	// VideoCompleteEventType is the event type for video completes
	VideoCompleteEventType = "video_complete"
	// VideoProgressEventType is the event type for video progress
	VideoProgressEventType = "video_progress"
	// FileDownloadEventType is the event type for file downloads
	FileDownloadEventType = "file_download"
)
