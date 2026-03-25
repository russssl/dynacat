package dynacat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

var torrentingWidgetTemplate = mustParseTemplate("torrenting.html", "widget-base.html")

var errTorrentingUnauthorized = errors.New("torrenting: unauthorized")

const (
	torrentingTypeQbit   = "qbit"
	torrentingTypeDeluge = "deluge"
)

// Universal torrent states (backend-agnostic). Each client maps its API values here.
const (
	torrentStateAllocating     = "allocating"
	torrentStateChecking       = "checking"
	torrentStateCheckingResume = "checking_resume"
	torrentStateDownloading    = "downloading"
	torrentStateSeeding        = "seeding"
	torrentStatePaused         = "paused"
	torrentStateQueued         = "queued"
	torrentStateStalled        = "stalled"
	torrentStateError          = "error"
	torrentStateMoving         = "moving"
	torrentStateUnknown        = "unknown"
)

var delugeTorrentKeys = []string{"name", "state", "progress", "total_done", "total_wanted", "total_size", "eta"}

var qbitTorrentStateMap = map[string]string{
	"allocating":         torrentStateAllocating,
	"checkingDL":         torrentStateChecking,
	"checkingUP":         torrentStateChecking,
	"checkingResumeData": torrentStateCheckingResume,
	"downloading":        torrentStateDownloading,
	"forcedDL":           torrentStateDownloading,
	"metaDL":             torrentStateDownloading,
	"stalledDL":          torrentStateStalled,
	"uploading":          torrentStateSeeding,
	"forcedUP":           torrentStateSeeding,
	"stalledUP":          torrentStateStalled,
	"pausedDL":           torrentStatePaused,
	"pausedUP":           torrentStatePaused,
	"queuedDL":           torrentStateQueued,
	"queuedUP":           torrentStateQueued,
	"error":              torrentStateError,
	"missingFiles":       torrentStateError,
}

var delugeTorrentStateMap = map[string]string{
	"Allocating":  torrentStateAllocating,
	"Checking":    torrentStateChecking,
	"Downloading": torrentStateDownloading,
	"Seeding":     torrentStateSeeding,
	"Paused":      torrentStatePaused,
	"Queued":      torrentStateQueued,
	"Error":       torrentStateError,
	"Moving":      torrentStateMoving,
}

func mapClientTorrentState(backend, raw string) string {
	raw = strings.TrimSpace(raw)
	var m map[string]string
	switch backend {
	case torrentingTypeQbit:
		m = qbitTorrentStateMap
	case torrentingTypeDeluge:
		m = delugeTorrentStateMap
	default:
		return torrentStateUnknown
	}
	if s, ok := m[raw]; ok {
		return s
	}
	return torrentStateUnknown
}

type TorrentingHostConfig struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	client   *http.Client
}

type torrentingWidget struct {
	widgetBase    `yaml:",inline"`
	Hosts         []TorrentingHostConfig `yaml:"hosts"`
	HideCompleted bool                   `yaml:"hide-completed"`
	HideInactive  bool                   `yaml:"hide-inactive"`
	HideBar       bool                   `yaml:"hide-bar"`
	WrapText      bool                   `yaml:"wrap-text"`
	CollapseAfter int                    `yaml:"collapse-after"`
	Backend       string                 `yaml:"backend"`
	mu            sync.RWMutex
	Torrents      []torrentInfo
}

type torrentInfo struct {
	Name          string
	State         string
	Progress      float64
	Downloaded    int64
	Size          int64
	ETA           int64
	IsCompleted   bool
	IsActive      bool
	Icon          string
	FmtProgress   string
	FmtETA        string
	ShortName     string
	ProgressWidth string
}

type torrentRow struct {
	Name       string  `json:"name"`
	State      string  `json:"state"`
	Progress   float64 `json:"progress"`
	Downloaded int64   `json:"downloaded"`
	Size       int64   `json:"size"`
	ETA        int64   `json:"eta"`
}

func (widget *torrentingWidget) initialize() error {
	widget.withTitle("Torrents")

	if widget.UpdateInterval == nil {
		interval := updateIntervalField(30 * time.Second)
		widget.UpdateInterval = &interval
	}

	widget.withCacheDuration(time.Duration(*widget.UpdateInterval))

	widget.Backend = strings.TrimSpace(widget.Backend)
	switch widget.Backend {
	case "":
		widget.Backend = torrentingTypeQbit
	case torrentingTypeQbit, torrentingTypeDeluge:
	default:
		return fmt.Errorf("backend must be either %q or %q, got %q", torrentingTypeQbit, torrentingTypeDeluge, widget.Backend)
	}

	if widget.CollapseAfter == 0 {
		widget.CollapseAfter = 3
	}

	if len(widget.Hosts) == 0 {
		return fmt.Errorf("at least one host must be specified")
	}

	for i := range widget.Hosts {
		host := &widget.Hosts[i]
		if host.URL == "" {
			return fmt.Errorf("host URL is required")
		}
		switch widget.Backend {
		case torrentingTypeQbit:
			if host.Username == "" {
				return fmt.Errorf("host username is required")
			}
			if host.Password == "" {
				return fmt.Errorf("host password is required")
			}
		case torrentingTypeDeluge:
			if strings.TrimSpace(host.Password) == "" {
				return fmt.Errorf("host password is required for deluge")
			}
		}

		jar, err := cookiejar.New(nil)
		if err != nil {
			return fmt.Errorf("failed to create cookie jar: %w", err)
		}
		host.client = &http.Client{
			Jar:     jar,
			Timeout: 10 * time.Second,
		}
	}

	return nil
}

func (widget *torrentingWidget) update(ctx context.Context) {
	type fetchResult struct {
		torrents []torrentInfo
		err      error
		url      string
	}

	results := make(chan fetchResult, len(widget.Hosts))
	var wg sync.WaitGroup

	for i := range widget.Hosts {
		host := &widget.Hosts[i]
		wg.Add(1)
		go func(h *TorrentingHostConfig) {
			defer wg.Done()
			torrents, err := widget.fetchFromHost(ctx, h)
			results <- fetchResult{torrents: torrents, err: err, url: h.URL}
		}(host)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allTorrents []torrentInfo
	successCount := 0
	errorCount := 0

	for result := range results {
		if result.err != nil {
			errorCount++
			slog.Error("failed to fetch torrents", "backend", widget.Backend, "url", result.url, "error", result.err)
			continue
		}
		successCount++
		allTorrents = append(allTorrents, result.torrents...)
	}

	sort.SliceStable(allTorrents, func(i, j int) bool {
		if p, q := torrentDownloadPriority(allTorrents[i]), torrentDownloadPriority(allTorrents[j]); p != q {
			return p < q
		}
		if allTorrents[i].IsCompleted != allTorrents[j].IsCompleted {
			return allTorrents[j].IsCompleted
		}
		return strings.ToLower(allTorrents[i].Name) < strings.ToLower(allTorrents[j].Name)
	})

	widget.mu.Lock()
	widget.Torrents = allTorrents
	widget.mu.Unlock()

	var err error
	if successCount == 0 {
		err = errNoContent
	} else if errorCount > 0 {
		err = errPartialContent
	}

	if !widget.canContinueUpdateAfterHandlingErr(err) {
		return
	}
}

func (widget *torrentingWidget) login(ctx context.Context, host *TorrentingHostConfig) error {
	loginURL := strings.TrimRight(host.URL, "/") + "/api/v2/auth/login"

	form := url.Values{
		"username": {host.Username},
		"password": {host.Password},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", loginURL, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := host.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(body)) != "Ok." {
		return fmt.Errorf("login failed: %s", strings.TrimSpace(string(body)))
	}

	return nil
}

func (widget *torrentingWidget) delugeLogin(ctx context.Context, host *TorrentingHostConfig) error {
	loginURL := strings.TrimRight(host.URL, "/") + "/json"

	payload, err := json.Marshal(map[string]any{
		"method": "auth.login",
		"params": []any{strings.TrimSpace(host.Password)},
		"id":     time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", loginURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := host.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var wrap struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	if wrap.Error != nil {
		return fmt.Errorf("login failed: %s", wrap.Error.Message)
	}

	var ok bool
	if err := json.Unmarshal(wrap.Result, &ok); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("login failed: invalid credentials")
	}

	return nil
}

func (widget *torrentingWidget) fetchFromHost(ctx context.Context, host *TorrentingHostConfig) ([]torrentInfo, error) {
	fetchOnce := widget.fetchTorrentsOnce
	login := widget.login
	if widget.Backend == torrentingTypeDeluge {
		fetchOnce = widget.fetchDelugeTorrentsOnce
		login = widget.delugeLogin
	}

	torrents, err := fetchOnce(ctx, host)
	if errors.Is(err, errTorrentingUnauthorized) {
		slog.Info("torrent client session expired, re-logging in", "backend", widget.Backend, "url", host.URL)
		if loginErr := login(ctx, host); loginErr != nil {
			slog.Error("torrent client re-login failed", "backend", widget.Backend, "url", host.URL, "error", loginErr)
			return nil, loginErr
		}
		torrents, err = fetchOnce(ctx, host)
	}
	return torrents, err
}

func (widget *torrentingWidget) fetchTorrentsOnce(ctx context.Context, host *TorrentingHostConfig) ([]torrentInfo, error) {
	apiURL := strings.TrimRight(host.URL, "/") + "/api/v2/torrents/info"

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := host.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, errTorrentingUnauthorized
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, host.URL)
	}

	var raw []torrentRow
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to decode torrents JSON: %w", err)
	}

	torrents := make([]torrentInfo, 0, len(raw))
	for _, t := range raw {
		t.State = mapClientTorrentState(torrentingTypeQbit, t.State)
		torrents = append(torrents, computeTorrentInfo(t))
	}
	return torrents, nil
}

func (widget *torrentingWidget) fetchDelugeTorrentsOnce(ctx context.Context, host *TorrentingHostConfig) ([]torrentInfo, error) {
	apiURL := strings.TrimRight(host.URL, "/") + "/json"

	reqBody, err := json.Marshal(map[string]any{
		"method": "core.get_torrents_status",
		"params": []any{map[string]any{}, delugeTorrentKeys},
		"id":     time.Now().UnixNano(),
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := host.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, host.URL)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var wrap struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &wrap); err != nil {
		return nil, fmt.Errorf("failed to decode torrents JSON: %w", err)
	}
	if wrap.Error != nil {
		if wrap.Error.Code == 1 {
			return nil, errTorrentingUnauthorized
		}
		return nil, fmt.Errorf("unexpected status from %s: %s", host.URL, wrap.Error.Message)
	}

	var byID map[string]json.RawMessage
	if err := json.Unmarshal(wrap.Result, &byID); err != nil {
		return nil, fmt.Errorf("failed to decode torrents JSON: %w", err)
	}

	raw := make([]torrentRow, 0, len(byID))
	for _, row := range byID {
		tr, err := delugeTorrentRow(row)
		if err != nil {
			return nil, err
		}
		raw = append(raw, tr)
	}

	torrents := make([]torrentInfo, 0, len(raw))
	for _, t := range raw {
		torrents = append(torrents, computeTorrentInfo(t))
	}
	return torrents, nil
}

func delugeTorrentRow(row json.RawMessage) (torrentRow, error) {
	var st struct {
		Name        string  `json:"name"`
		State       string  `json:"state"`
		Progress    float64 `json:"progress"`
		TotalDone   float64 `json:"total_done"`
		TotalWanted float64 `json:"total_wanted"`
		TotalSize   float64 `json:"total_size"`
		Eta         float64 `json:"eta"`
	}
	if err := json.Unmarshal(row, &st); err != nil {
		return torrentRow{}, fmt.Errorf("failed to decode torrents JSON: %w", err)
	}
	size := int64(st.TotalWanted)
	if size <= 0 {
		size = int64(st.TotalSize)
	}
	return torrentRow{
		Name: st.Name, State: mapClientTorrentState(torrentingTypeDeluge, st.State),
		Progress: st.Progress / 100, Downloaded: int64(st.TotalDone), Size: size, ETA: int64(st.Eta),
	}, nil
}

func computeTorrentInfo(t torrentRow) torrentInfo {
	info := torrentInfo{
		Name:       t.Name,
		State:      t.State,
		Progress:   t.Progress,
		Downloaded: t.Downloaded,
		Size:       t.Size,
		ETA:        t.ETA,
	}

	info.IsCompleted = t.Progress >= 1.0

	switch t.State {
	case torrentStateDownloading, torrentStateSeeding:
		info.IsActive = true
	}

	switch {
	case info.IsCompleted:
		info.Icon = "✔"
	case t.State == torrentStateDownloading:
		info.Icon = "↓"
	case t.State == torrentStateSeeding:
		info.Icon = "↑"
	case t.State == torrentStateError:
		info.Icon = "!"
	case t.State == torrentStateCheckingResume:
		info.Icon = "⟳"
	case t.State == torrentStateChecking || t.State == torrentStateAllocating || t.State == torrentStateMoving:
		info.Icon = "…"
	default:
		info.Icon = "❚❚"
	}

	if t.Size >= 1_073_741_824 {
		info.FmtProgress = fmt.Sprintf("%.2f GB / %.2f GB",
			float64(t.Downloaded)/1_073_741_824,
			float64(t.Size)/1_073_741_824,
		)
	} else {
		info.FmtProgress = fmt.Sprintf("%.2f MB / %.2f MB",
			float64(t.Downloaded)/1_048_576,
			float64(t.Size)/1_048_576,
		)
	}

	if t.ETA < 0 || t.ETA >= 8_640_000 {
		info.FmtETA = "∞"
	} else if t.ETA == 0 {
		info.FmtETA = "0m"
	} else {
		h := t.ETA / 3600
		m := (t.ETA % 3600) / 60
		s := t.ETA % 60
		if h > 0 {
			info.FmtETA = fmt.Sprintf("%dh %dm", h, m)
		} else if m > 0 {
			info.FmtETA = fmt.Sprintf("%dm", m)
		} else {
			info.FmtETA = fmt.Sprintf("%ds", s)
		}
	}

	info.ShortName = t.Name

	info.ProgressWidth = fmt.Sprintf("%.1f%%", t.Progress*100)

	return info
}

func torrentDownloadPriority(info torrentInfo) int {
	switch info.State {
	case torrentStateDownloading:
		return 0
	case torrentStateSeeding:
		return 1
	default:
		if info.IsCompleted {
			return 3
		}
		return 2
	}
}

func (widget *torrentingWidget) Render() template.HTML {
	widget.mu.RLock()
	defer widget.mu.RUnlock()
	return widget.renderTemplate(widget, torrentingWidgetTemplate)
}
