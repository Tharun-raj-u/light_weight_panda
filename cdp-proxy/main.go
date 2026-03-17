// cdp-proxy — High-performance CDP fetch proxy for Lightpanda browser pool.
//
// Architecture:
//
//	FastAPI (Python) ──HTTP POST──▶ cdp-proxy ──WebSocket/CDP──▶ Lightpanda ×N
//
// The proxy round-robins requests across N Lightpanda browser containers,
// each limited to MAX_PER_BROWSER concurrent CDP sessions.  If a browser is
// at capacity the request spills to the next one; if ALL are full the request
// queues until a slot opens (with context-based timeout).
//
// Environment variables:
//
//	PORT              HTTP listen port              (default "9333")
//	BROWSER_COUNT     number of lightpanda-N hosts  (default 10)
//	BROWSER_PREFIX    service name prefix           (default "lightpanda")
//	BROWSER_PORT      CDP port on each browser      (default "9222")
//	BROWSER_ENDPOINTS explicit comma-separated ws:// URLs (overrides above)
//	MAX_PER_BROWSER   concurrent CDP sessions/host  (default 30)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Browser pool
// ---------------------------------------------------------------------------

type pool struct {
	endpoints []string
	sems      []chan struct{}
	counter   uint64
}

func newPool(endpoints []string, maxPer int) *pool {
	sems := make([]chan struct{}, len(endpoints))
	for i := range sems {
		sems[i] = make(chan struct{}, maxPer)
	}
	return &pool{endpoints: endpoints, sems: sems}
}

// fetch picks a browser (round-robin, prefer one with capacity) and runs
// the CDP fetch.  Returns (html, browserUsed, error).
func (p *pool) fetch(ctx context.Context, pageURL string) (string, string, error) {
	n := len(p.endpoints)
	start := int(atomic.AddUint64(&p.counter, 1) - 1)

	// Phase 1: non-blocking — grab the first browser with a free slot.
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		select {
		case p.sems[idx] <- struct{}{}:
			html, err := cdpFetchHTML(ctx, p.endpoints[idx], pageURL)
			<-p.sems[idx]
			if err == nil {
				return html, p.endpoints[idx], nil
			}
			// Connection error → try next browser.
			if ctx.Err() != nil {
				return "", p.endpoints[idx], err
			}
			continue
		default:
			continue
		}
	}

	// Phase 2: every browser is at capacity — block-wait on the RR target.
	idx := start % n
	select {
	case p.sems[idx] <- struct{}{}:
		html, err := cdpFetchHTML(ctx, p.endpoints[idx], pageURL)
		<-p.sems[idx]
		return html, p.endpoints[idx], err
	case <-ctx.Done():
		return "", "", fmt.Errorf("all %d browsers busy: %w", n, ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// CDP protocol over WebSocket
// ---------------------------------------------------------------------------

func cdpFetchHTML(ctx context.Context, browserWS, pageURL string) (string, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, browserWS, nil)
	if err != nil {
		return "", fmt.Errorf("ws dial %s: %w", browserWS, err)
	}
	defer conn.Close()
	conn.SetReadLimit(10 * 1024 * 1024) // 10 MB max

	var (
		msgID     int
		ctxID     string // browser context
		targetID  string
		sessionID string
	)

	// ---- helpers ----

	send := func(method string, params map[string]interface{}, sid string) (int, error) {
		msgID++
		msg := map[string]interface{}{"id": msgID, "method": method}
		if params != nil {
			msg["params"] = params
		}
		if sid != "" {
			msg["sessionId"] = sid
		}
		return msgID, conn.WriteJSON(msg)
	}

	readDeadline := func() time.Time {
		dl := time.Now().Add(3 * time.Second)
		if d, ok := ctx.Deadline(); ok && d.Before(dl) {
			dl = d
		}
		return dl
	}

	// recvResp reads messages until we see the response with id == wantID.
	// Lifecycle events and other messages are silently skipped.
	recvResp := func(wantID int) (map[string]interface{}, error) {
		for {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			conn.SetReadDeadline(readDeadline())
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return nil, err
			}
			var data map[string]interface{}
			if json.Unmarshal(raw, &data) != nil {
				continue
			}
			if id, _ := data["id"].(float64); int(id) == wantID {
				if errObj, ok := data["error"].(map[string]interface{}); ok {
					msg, _ := errObj["message"].(string)
					return nil, fmt.Errorf("CDP %s: %s", "", msg)
				}
				return data, nil
			}
		}
	}

	str := func(resp map[string]interface{}, keys ...string) string {
		var cur interface{} = resp
		for _, k := range keys {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return ""
			}
			cur = m[k]
		}
		s, _ := cur.(string)
		return s
	}

	num := func(resp map[string]interface{}, keys ...string) float64 {
		var cur interface{} = resp
		for _, k := range keys {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return 0
			}
			cur = m[k]
		}
		f, _ := cur.(float64)
		return f
	}

	// ---- cleanup (always runs, even on ctx cancel) ----
	defer func() {
		if targetID != "" {
			send("Target.closeTarget", map[string]interface{}{"targetId": targetID}, "")
		}
		if ctxID != "" {
			send("Target.disposeBrowserContext", map[string]interface{}{"browserContextId": ctxID}, "")
		}
	}()

	// 1. Create browser context
	rid, err := send("Target.createBrowserContext", nil, "")
	if err != nil {
		return "", err
	}
	resp, err := recvResp(rid)
	if err != nil {
		return "", fmt.Errorf("createBrowserContext: %w", err)
	}
	ctxID = str(resp, "result", "browserContextId")
	if ctxID == "" {
		return "", fmt.Errorf("no browserContextId in response")
	}

	// 2. Create target (tab)
	rid, _ = send("Target.createTarget", map[string]interface{}{
		"url": "about:blank", "browserContextId": ctxID,
	}, "")
	resp, err = recvResp(rid)
	if err != nil {
		return "", fmt.Errorf("createTarget: %w", err)
	}
	targetID = str(resp, "result", "targetId")

	// 3. Attach to target
	rid, _ = send("Target.attachToTarget", map[string]interface{}{
		"targetId": targetID, "flatten": true,
	}, "")
	resp, err = recvResp(rid)
	if err != nil {
		return "", fmt.Errorf("attachToTarget: %w", err)
	}
	sessionID = str(resp, "result", "sessionId")

	// 4. Enable Page events
	rid, _ = send("Page.enable", nil, sessionID)
	if _, err = recvResp(rid); err != nil {
		return "", err
	}
	rid, _ = send("Page.setLifecycleEventsEnabled",
		map[string]interface{}{"enabled": true}, sessionID)
	if _, err = recvResp(rid); err != nil {
		return "", err
	}

	// 5. Navigate
	rid, _ = send("Page.navigate", map[string]interface{}{"url": pageURL}, sessionID)
	if _, err = recvResp(rid); err != nil {
		return "", err
	}

	// 6. Wait for a "page ready" lifecycle event
	readySet := map[string]bool{
		"DOMContentLoaded":  true,
		"load":              true,
		"networkAlmostIdle": true,
		"networkIdle":       true,
	}
	for {
		if ctx.Err() != nil {
			break
		}
		conn.SetReadDeadline(readDeadline())
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break // timeout → proceed to get HTML
		}
		var data map[string]interface{}
		json.Unmarshal(raw, &data)
		if m, _ := data["method"].(string); m == "Page.lifecycleEvent" {
			params, _ := data["params"].(map[string]interface{})
			name, _ := params["name"].(string)
			if readySet[name] {
				break
			}
		}
	}

	// 7. DOM.getDocument → root nodeId
	rid, _ = send("DOM.getDocument", map[string]interface{}{"depth": 0}, sessionID)
	resp, err = recvResp(rid)
	if err != nil {
		return "", fmt.Errorf("getDocument: %w", err)
	}
	nodeID := num(resp, "result", "root", "nodeId")

	// 8. DOM.getOuterHTML
	rid, _ = send("DOM.getOuterHTML", map[string]interface{}{"nodeId": int(nodeID)}, sessionID)
	resp, err = recvResp(rid)
	if err != nil {
		return "", fmt.Errorf("getOuterHTML: %w", err)
	}
	html := str(resp, "result", "outerHTML")
	return html, nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

type fetchReq struct {
	URL     string  `json:"url"`
	Timeout float64 `json:"timeout"`
}

type fetchResp struct {
	HTML      string `json:"html,omitempty"`
	ElapsedMs int64  `json:"elapsed_ms"`
	Error     string `json:"error,omitempty"`
	Browser   string `json:"browser,omitempty"`
}

var browserPool *pool

func handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req fetchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fetchResp{Error: "bad request: " + err.Error()})
		return
	}
	if req.URL == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fetchResp{Error: "url is required"})
		return
	}

	timeout := time.Duration(req.Timeout * float64(time.Second))
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()
	html, browser, err := browserPool.fetch(ctx, req.URL)
	elapsed := time.Since(start).Milliseconds()

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		json.NewEncoder(w).Encode(fetchResp{
			Error:     err.Error(),
			ElapsedMs: elapsed,
			Browser:   browser,
		})
		return
	}
	json.NewEncoder(w).Encode(fetchResp{
		HTML:      html,
		ElapsedMs: elapsed,
		Browser:   browser,
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	info := map[string]interface{}{
		"status":           "ok",
		"browsers":         browserPool.endpoints,
		"browser_count":    len(browserPool.endpoints),
		"max_per_browser":  cap(browserPool.sems[0]),
		"total_capacity":   len(browserPool.endpoints) * cap(browserPool.sems[0]),
	}
	json.NewEncoder(w).Encode(info)
}

// ---------------------------------------------------------------------------
// Configuration & main
// ---------------------------------------------------------------------------

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func buildEndpoints() []string {
	if ep := os.Getenv("BROWSER_ENDPOINTS"); ep != "" {
		return strings.Split(ep, ",")
	}
	count := envInt("BROWSER_COUNT", 10)
	prefix := os.Getenv("BROWSER_PREFIX")
	if prefix == "" {
		prefix = "lightpanda"
	}
	port := os.Getenv("BROWSER_PORT")
	if port == "" {
		port = "9222"
	}
	list := make([]string, count)
	for i := range list {
		list[i] = fmt.Sprintf("ws://%s-%d:%s/", prefix, i+1, port)
	}
	return list
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9333"
	}
	maxPer := envInt("MAX_PER_BROWSER", 30)
	endpoints := buildEndpoints()

	browserPool = newPool(endpoints, maxPer)

	log.Printf("cdp-proxy starting on :%s  |  %d browsers × %d slots = %d total capacity",
		port, len(endpoints), maxPer, len(endpoints)*maxPer)
	for _, ep := range endpoints {
		log.Printf("  → %s", ep)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", handleFetch)
	mux.HandleFunc("/health", handleHealth)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
