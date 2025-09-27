// nolint // this is debugger code, not for prod and 100% vibe-coded
package debugger

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/sirupsen/logrus"
)

const (
	httpMethodGET = "GET"
)

// Server represents the debugger HTTP server
type Server struct {
	port       int
	server     *http.Server
	fetcher    receiver.RawLogReader
	parser     *RequestParser
	storageURL string
}

// APIResponse represents a standard API response structure
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// RequestItem represents an individual request within a rawlog file
type RequestItem struct {
	ID            string        `json:"id"`             // Unique ID: {rawlogID}_{requestIndex}
	RawlogID      string        `json:"rawlog_id"`      // Original rawlog file ID
	RequestIndex  int           `json:"request_index"`  // Index within the rawlog file (0-based)
	Timestamp     time.Time     `json:"timestamp"`      // Timestamp of the rawlog file
	FormattedTime string        `json:"formatted_time"` // Human-readable timestamp
	Request       ParsedRequest `json:"request"`        // Parsed request data
}

// NewServer creates a new debugger server instance
func NewServer(port int, storageURL string) *Server {
	fetcher := NewRemoteFetcher(storageURL)
	parser := NewRequestParser()

	return &Server{
		port:       port,
		fetcher:    fetcher,
		parser:     parser,
		storageURL: storageURL,
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API endpoints
	mux.HandleFunc("/api/rawlogs", s.corsHandler(s.handleListRawlogs))
	mux.HandleFunc("/api/rawlog/", s.corsHandler(s.handleGetRawlog))
	mux.HandleFunc("/api/requests", s.corsHandler(s.handleListRequests))
	mux.HandleFunc("/api/request/", s.corsHandler(s.handleGetRequest))

	// Static endpoints
	mux.HandleFunc("/debugger", s.handleDebuggerPage)
	mux.HandleFunc("/", s.handleRoot)

	s.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	logrus.Infof("Starting debugger server on port %d", s.port)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("server failed to start: %w", err)
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		logrus.Info("Shutting down debugger server...")
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// corsHandler adds CORS headers for local development
func (s *Server) corsHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Add CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

// handleRoot handles the root endpoint
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		http.Redirect(w, r, "/debugger", http.StatusFound)
		return
	}
	http.NotFound(w, r)
}

// handleDebuggerPage serves the main debugger interface
func (s *Server) handleDebuggerPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != httpMethodGET {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Load and parse the HTML template
	tmpl, err := s.getDebuggerTemplate()
	if err != nil {
		logrus.Errorf("Failed to load debugger template: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Template data
	data := struct {
		StorageURL string
		Port       int
	}{
		StorageURL: s.storageURL,
		Port:       s.port,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		logrus.Errorf("Failed to execute template: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleListRawlogs handles GET /api/rawlogs - returns list of available rawlogs
func (s *Server) handleListRawlogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != httpMethodGET {
		s.writeErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	items, err := s.fetcher.ListItems()
	if err != nil {
		logrus.Errorf("Failed to list rawlog items: %v", err)
		s.writeErrorResponse(w, "Failed to fetch rawlog items", http.StatusInternalServerError)
		return
	}

	s.writeSuccessResponse(w, items)
}

// handleGetRawlog handles GET /api/rawlog/{id} - returns parsed rawlog content as structured JSON
func (s *Server) handleGetRawlog(w http.ResponseWriter, r *http.Request) {
	if r.Method != httpMethodGET {
		s.writeErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from URL path
	path := strings.TrimPrefix(r.URL.Path, "/api/rawlog/")
	if path == "" {
		s.writeErrorResponse(w, "Missing rawlog ID", http.StatusBadRequest)
		return
	}

	// Remove any trailing slashes
	itemID := strings.TrimSuffix(path, "/")

	// Get raw content
	content, err := s.fetcher.GetContent(itemID)
	if err != nil {
		logrus.Errorf("Failed to get rawlog content for ID %s: %v", itemID, err)
		if strings.Contains(err.Error(), "not found") {
			s.writeErrorResponse(w, fmt.Sprintf("Rawlog %s not found", itemID), http.StatusNotFound)
		} else {
			s.writeErrorResponse(w, "Failed to fetch rawlog content", http.StatusInternalServerError)
		}
		return
	}

	// Parse content to structured JSON
	parsedRequests, err := s.parser.ParseRawlogContent(content)
	if err != nil {
		logrus.Errorf("Failed to parse rawlog content for ID %s: %v", itemID, err)
		s.writeErrorResponse(w, "Failed to parse rawlog content", http.StatusInternalServerError)
		return
	}

	// Update timestamps if we can extract them from the item ID (assuming it's a timestamp)
	if len(parsedRequests) > 0 {
		if timestamp, err := s.parseTimestampFromID(itemID); err == nil {
			for i := range parsedRequests {
				s.parser.SetTimestamp(&parsedRequests[i], timestamp)
			}
		}
	}

	// Return structured data
	s.writeSuccessResponse(w, parsedRequests)
}

// handleListRequests handles GET /api/requests - returns list of all individual requests from all rawlogs
func (s *Server) handleListRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != httpMethodGET {
		s.writeErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get all rawlog items first
	rawlogItems, err := s.fetcher.ListItems()
	if err != nil {
		logrus.Errorf("Failed to list rawlog items: %v", err)
		s.writeErrorResponse(w, "Failed to fetch rawlog items", http.StatusInternalServerError)
		return
	}

	var allRequests []RequestItem

	// Process each rawlog file to extract individual requests
	for _, rawlogItem := range rawlogItems {
		// Get raw content
		content, err := s.fetcher.GetContent(rawlogItem.ID)
		if err != nil {
			logrus.Warnf("Failed to get rawlog content for ID %s: %v", rawlogItem.ID, err)
			continue
		}

		// Parse content to structured requests
		parsedRequests, err := s.parser.ParseRawlogContent(content)
		if err != nil {
			logrus.Warnf("Failed to parse rawlog content for ID %s: %v", rawlogItem.ID, err)
			continue
		}

		// Update timestamps if we can extract them from the item ID
		if timestamp, parseErr := s.parseTimestampFromID(rawlogItem.ID); parseErr == nil {
			for i := range parsedRequests {
				s.parser.SetTimestamp(&parsedRequests[i], timestamp)
			}
		}

		// Create RequestItem for each parsed request
		for i, parsedRequest := range parsedRequests {
			requestItem := RequestItem{
				ID:            fmt.Sprintf("%s_%d", rawlogItem.ID, i),
				RawlogID:      rawlogItem.ID,
				RequestIndex:  i,
				Timestamp:     rawlogItem.Timestamp,
				FormattedTime: rawlogItem.FormattedTime,
				Request:       parsedRequest,
			}
			allRequests = append(allRequests, requestItem)
		}
	}

	s.writeSuccessResponse(w, allRequests)
}

// handleGetRequest handles GET /api/request/{id} - returns a specific request by its composite ID
func (s *Server) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != httpMethodGET {
		s.writeErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from URL path
	path := strings.TrimPrefix(r.URL.Path, "/api/request/")
	if path == "" {
		s.writeErrorResponse(w, "Missing request ID", http.StatusBadRequest)
		return
	}

	// Remove any trailing slashes
	requestID := strings.TrimSuffix(path, "/")

	// Parse composite ID: {rawlogID}_{requestIndex}
	parts := strings.Split(requestID, "_")
	if len(parts) != 2 {
		s.writeErrorResponse(w, "Invalid request ID format", http.StatusBadRequest)
		return
	}

	rawlogID := parts[0]
	requestIndex, err := strconv.Atoi(parts[1])
	if err != nil {
		s.writeErrorResponse(w, "Invalid request index in ID", http.StatusBadRequest)
		return
	}

	// Get raw content for the rawlog file
	content, err := s.fetcher.GetContent(rawlogID)
	if err != nil {
		logrus.Errorf("Failed to get rawlog content for ID %s: %v", rawlogID, err)
		if strings.Contains(err.Error(), "not found") {
			s.writeErrorResponse(w, fmt.Sprintf("Rawlog %s not found", rawlogID), http.StatusNotFound)
		} else {
			s.writeErrorResponse(w, "Failed to fetch rawlog content", http.StatusInternalServerError)
		}
		return
	}

	// Parse content to structured requests
	parsedRequests, err := s.parser.ParseRawlogContent(content)
	if err != nil {
		logrus.Errorf("Failed to parse rawlog content for ID %s: %v", rawlogID, err)
		s.writeErrorResponse(w, "Failed to parse rawlog content", http.StatusInternalServerError)
		return
	}

	// Check if request index is valid
	if requestIndex < 0 || requestIndex >= len(parsedRequests) {
		errorMsg := fmt.Sprintf("Request index %d out of range (0-%d)", requestIndex, len(parsedRequests)-1)
		s.writeErrorResponse(w, errorMsg, http.StatusNotFound)
		return
	}

	// Get the specific request
	targetRequest := parsedRequests[requestIndex]

	// Update timestamp if we can extract it from the rawlog ID
	if timestamp, parseErr := s.parseTimestampFromID(rawlogID); parseErr == nil {
		s.parser.SetTimestamp(&targetRequest, timestamp)
	}

	// Return the specific request
	s.writeSuccessResponse(w, targetRequest)
}

// writeSuccessResponse writes a successful API response
func (s *Server) writeSuccessResponse(w http.ResponseWriter, data interface{}) {
	response := APIResponse{
		Success: true,
		Data:    data,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logrus.Errorf("Failed to encode JSON response: %v", err)
	}
}

// writeErrorResponse writes an error API response
func (s *Server) writeErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	response := APIResponse{
		Success: false,
		Error:   message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logrus.Errorf("Failed to encode error JSON response: %v", err)
	}
}

// getDebuggerTemplate loads and returns the debugger HTML template
func (s *Server) getDebuggerTemplate() (*template.Template, error) {
	// Try to load from file first
	templatePath := "pkg/debugger/templates/debugger.html"
	if content, err := os.ReadFile(templatePath); err == nil {
		return template.New("debugger").Parse(string(content))
	}

	// Fallback to embedded template
	templateStr := s.getDebuggerHTML()
	return template.New("debugger").Parse(templateStr)
}

// getDebuggerHTML returns the embedded HTML template
//
//nolint:funlen // HTML template content
func (s *Server) getDebuggerHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rawlog Debugger</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/jsondiffpatch@0.6.0/dist/formatters/html.css" rel="stylesheet">
    <style>
        .request-item {
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .request-item:hover {
            background-color: #f8f9fa;
        }
        .request-item.selected-slot1 {
            background-color: #cfe2ff !important;
            border-left: 4px solid #0d6efd;
        }
        .request-item.selected-slot2 {
            background-color: #d1e7dd !important;
            border-left: 4px solid #198754;
        }
        .comparison-slot {
            min-height: 200px;
            border: 2px dashed #dee2e6;
            border-radius: 0.375rem;
        }
        .comparison-slot.has-content {
            border-style: solid;
        }
        .comparison-slot.slot1 {
            border-color: #0d6efd;
        }
        .comparison-slot.slot2 {
            border-color: #198754;
        }
        .diff-container {
            max-height: 600px;
            overflow-y: auto;
        }
        .loading {
            display: none;
        }
        .loading.show {
            display: block;
        }
        .json-content {
            background-color: #f8f9fa;
            border-radius: 0.375rem;
            padding: 1rem;
            font-family: 'Courier New', monospace;
            font-size: 0.875rem;
            white-space: pre-wrap;
            max-height: 400px;
            overflow-y: auto;
        }
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="row">
            <!-- Left Sidebar -->
            <div class="col-md-3 bg-light p-3">
                <h5>Requests</h5>
                <div class="mb-3">
                    <input type="text" class="form-control form-control-sm" id="searchFilter" placeholder="Search rawlogs...">
                </div>
                <div id="loadingItems" class="loading">
                    <div class="text-center">
                        <div class="spinner-border spinner-border-sm" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                        <div>Loading rawlogs...</div>
                    </div>
                </div>
                <div id="rawlogsList" class="list-group">
                    <!-- Rawlog items will be populated here -->
                </div>
                <div id="errorMessage" class="alert alert-danger mt-3" style="display: none;"></div>
            </div>

            <!-- Main Content -->
            <div class="col-md-9 p-3">
                <div class="d-flex justify-content-between align-items-center mb-3">
                    <h4>Rawlog Debugger</h4>
                    <div>
                        <button id="clearSelections" class="btn btn-outline-secondary btn-sm">Clear Selections</button>
                        <button id="toggleView" class="btn btn-outline-primary btn-sm">Toggle Raw View</button>
                    </div>
                </div>

                <!-- Instructions -->
                <div class="alert alert-info">
                    <strong>Instructions:</strong> Click rawlog items to expand requests. Left-click requests for Slot 1 (blue), right-click requests for Slot 2 (green). 
                    Select two requests to compare differences.
                </div>

                <!-- Comparison Slots -->
                <div class="row mb-4">
                    <div class="col-md-6">
                        <h6>Slot 1 <span class="badge bg-primary">Left Click</span></h6>
                        <div id="slot1" class="comparison-slot slot1 p-3">
                            <div class="text-muted text-center">Click a request to select for comparison</div>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <h6>Slot 2 <span class="badge bg-success">Right Click</span></h6>
                        <div id="slot2" class="comparison-slot slot2 p-3">
                            <div class="text-muted text-center">Right-click a request to select for comparison</div>
                        </div>
                    </div>
                </div>

                <!-- Diff Results -->
                <div id="diffSection" style="display: none;">
                    <h6>Comparison Results</h6>
                    <ul class="nav nav-tabs" id="diffTabs" role="tablist">
                        <li class="nav-item" role="presentation">
                            <button class="nav-link active" id="diff-tab" data-bs-toggle="tab" data-bs-target="#diff-pane" type="button" role="tab">Diff View</button>
                        </li>
                        <li class="nav-item" role="presentation">
                            <button class="nav-link" id="raw-tab" data-bs-toggle="tab" data-bs-target="#raw-pane" type="button" role="tab">Raw View</button>
                        </li>
                    </ul>
                    <div class="tab-content" id="diffTabContent">
                        <div class="tab-pane fade show active" id="diff-pane" role="tabpanel">
                            <div id="diffContainer" class="diff-container mt-3"></div>
                        </div>
                        <div class="tab-pane fade" id="raw-pane" role="tabpanel">
                            <div class="row mt-3">
                                <div class="col-md-6">
                                    <h6>Slot 1 Raw JSON</h6>
                                    <div id="raw1" class="json-content"></div>
                                </div>
                                <div class="col-md-6">
                                    <h6>Slot 2 Raw JSON</h6>
                                    <div id="raw2" class="json-content"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Scripts -->
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/jsondiffpatch@0.6.0/dist/jsondiffpatch.umd.min.js"></script>

    <script>
        class RawlogDebugger {
            constructor() {
                this.requests = [];
                this.slot1Data = null;
                this.slot2Data = null;
                this.slot1Id = null;
                this.slot2Id = null;
                this.diffInstance = jsondiffpatch.create({
                    objectHash: function(obj) {
                        return obj.id || obj.name || obj._id || JSON.stringify(obj);
                    }
                });
                this.init();
            }

            init() {
                this.bindEvents();
                this.loadRequests();
            }

            bindEvents() {
                $('#clearSelections').on('click', () => this.clearSelections());
                $('#searchFilter').on('input', (e) => this.filterRequests(e.target.value));
                
                // Prevent context menu on right-click
                $(document).on('contextmenu', '.request-item', (e) => e.preventDefault());
            }

            async loadRawlogs() {
                $('#loadingItems').addClass('show');
                $('#errorMessage').hide();

                try {
                    const response = await fetch('/api/rawlogs');
                    const result = await response.json();

                    if (!result.success) {
                        throw new Error(result.error || 'Failed to load rawlogs');
                    }

                    this.rawlogs = result.data || [];
                    this.renderRawlogsList();
                } catch (error) {
                    console.error('Failed to load rawlogs:', error);
                    $('#errorMessage').text('Failed to load rawlogs: ' + error.message).show();
                } finally {
                    $('#loadingItems').removeClass('show');
                }
            }

            renderRequestList() {
                const container = $('#requestList');
                container.empty();

                if (this.requests.length === 0) {
                    container.append('<div class="text-muted text-center">No requests found</div>');
                    return;
                }

                this.requests.forEach(item => {
                    const requestInfo = item.request;
                    const element = $('<div class="request-item list-group-item list-group-item-action" data-id="' + item.id + '">' +
                        '<div class="d-flex w-100 justify-content-between">' +
                        '<h6 class="mb-1">' + item.rawlog_id + '_' + item.request_index + '</h6>' +
                        '<small>' + item.formatted_time + '</small>' +
                        '</div>' +
                        '<div class="mb-1">' +
                        '<small class="text-muted">' + (requestInfo.metadata.method || 'N/A') + ' ' + (requestInfo.metadata.path || 'N/A') + '</small>' +
                        '</div>' +
                        '<div>' +
                        '<small class="text-muted">Params: ' + Object.keys(requestInfo.query_params || {}).length + ', Headers: ' + Object.keys(requestInfo.headers || {}).length + '</small>' +
                        '</div>' +
                        '</div>');

                    // Bind click events
                    element.on('click', (e) => {
                        e.preventDefault();
                        if (e.which === 3 || e.button === 2) {
                            // Right click - Slot 2
                            this.selectForSlot(item.id, 2);
                        } else {
                            // Left click - Slot 1
                            this.selectForSlot(item.id, 1);
                        }
                    });

                    // Bind mousedown for right-click detection
                    element.on('mousedown', (e) => {
                        if (e.which === 3 || e.button === 2) {
                            this.selectForSlot(item.id, 2);
                        }
                    });

                    container.append(element);
                });
            }

            filterRequests(query) {
                const items = $('.request-item');
                items.each(function() {
                    const text = $(this).text().toLowerCase();
                    $(this).toggle(text.includes(query.toLowerCase()));
                });
            }

            async selectForSlot(requestId, slot) {
                try {
                    const response = await fetch('/api/request/' + requestId);
                    const result = await response.json();

                    if (!result.success) {
                        throw new Error(result.error || 'Failed to load request');
                    }

                    const data = result.data;
                    
                    if (slot === 1) {
                        this.slot1Data = data;
                        this.slot1Id = requestId;
                        this.updateSlotDisplay(1, requestId, data);
                    } else {
                        this.slot2Data = data;
                        this.slot2Id = requestId;
                        this.updateSlotDisplay(2, requestId, data);
                    }

                    this.updateSelectionHighlight();
                    this.updateComparison();

                } catch (error) {
                    console.error('Failed to load request ' + requestId + ':', error);
                    alert('Failed to load request: ' + error.message);
                }
            }

            updateSlotDisplay(slot, requestId, data) {
                const slotElement = $('#slot' + slot);
                slotElement.addClass('has-content');
                
                slotElement.html(
                    '<div class="mb-2"><strong>Request ID:</strong> ' + requestId + '</div>' +
                    '<div class="mb-2"><strong>Method:</strong> ' + (data.metadata && data.metadata.method || 'N/A') + '</div>' +
                    '<div class="mb-2"><strong>Path:</strong> ' + (data.metadata && data.metadata.path || 'N/A') + '</div>' +
                    '<div class="mb-2"><strong>Query Params:</strong> ' + Object.keys(data.query_params || {}).length + '</div>' +
                    '<div><strong>Headers:</strong> ' + Object.keys(data.headers || {}).length + '</div>'
                );
            }

            updateSelectionHighlight() {
                $('.request-item').removeClass('selected-slot1 selected-slot2');
                
                if (this.slot1Id) {
                    $('.request-item[data-id="' + this.slot1Id + '"]').addClass('selected-slot1');
                }
                
                if (this.slot2Id) {
                    $('.request-item[data-id="' + this.slot2Id + '"]').addClass('selected-slot2');
                }
            }

            updateComparison() {
                if (this.slot1Data && this.slot2Data) {
                    this.showDiff();
                    $('#diffSection').show();
                } else {
                    $('#diffSection').hide();
                }
            }

            showDiff() {
                // Since we're now working with individual requests, no need to normalize
                const data1 = this.slot1Data;
                const data2 = this.slot2Data;

                // Create diff
                const delta = this.diffInstance.diff(data1, data2);
                
                // Render diff
                const diffContainer = $('#diffContainer');
                diffContainer.empty();

                if (delta) {
                    const diffHtml = jsondiffpatch.formatters.html.format(delta, data1);
                    diffContainer.html(diffHtml);
                } else {
                    diffContainer.html('<div class="text-center text-muted">No differences found</div>');
                }

                // Update raw views
                $('#raw1').text(JSON.stringify(this.slot1Data, null, 2));
                $('#raw2').text(JSON.stringify(this.slot2Data, null, 2));
            }

            clearSelections() {
                this.slot1Data = null;
                this.slot2Data = null;
                this.slot1Id = null;
                this.slot2Id = null;

                $('#slot1').removeClass('has-content').html('<div class="text-muted text-center">Click a request to select for comparison</div>');
                $('#slot2').removeClass('has-content').html('<div class="text-muted text-center">Right-click a request to select for comparison</div>');
                
                this.updateSelectionHighlight();
                $('#diffSection').hide();
            }
        }

        // Initialize debugger when page loads
        $(document).ready(() => {
            new RawlogDebugger();
        });
    </script>
</body>
</html>`
}

// parseTimestampFromID attempts to parse a timestamp from the rawlog item ID
func (s *Server) parseTimestampFromID(itemID string) (time.Time, error) {
	// Try parsing as nanoseconds timestamp
	if timestamp, err := strconv.ParseInt(itemID, 10, 64); err == nil {
		return time.Unix(0, timestamp), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp from ID: %s", itemID)
}

// Stop stops the HTTP server
func (s *Server) Stop() error {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}
