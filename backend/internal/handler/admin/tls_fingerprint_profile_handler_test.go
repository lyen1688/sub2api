//go:build unit

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type tlsFingerprintProfileHandlerRepoStub struct {
	profiles []*model.TLSFingerprintProfile
	nextID   int64
}

func (r *tlsFingerprintProfileHandlerRepoStub) List(context.Context) ([]*model.TLSFingerprintProfile, error) {
	out := make([]*model.TLSFingerprintProfile, len(r.profiles))
	copy(out, r.profiles)
	return out, nil
}

func (r *tlsFingerprintProfileHandlerRepoStub) GetByID(_ context.Context, id int64) (*model.TLSFingerprintProfile, error) {
	for _, profile := range r.profiles {
		if profile.ID == id {
			clone := *profile
			return &clone, nil
		}
	}
	return nil, nil
}

func (r *tlsFingerprintProfileHandlerRepoStub) Create(_ context.Context, profile *model.TLSFingerprintProfile) (*model.TLSFingerprintProfile, error) {
	created := *profile
	if r.nextID == 0 {
		r.nextID = 1
	}
	created.ID = r.nextID
	r.nextID++
	r.profiles = append(r.profiles, &created)
	return &created, nil
}

func (r *tlsFingerprintProfileHandlerRepoStub) Update(_ context.Context, profile *model.TLSFingerprintProfile) (*model.TLSFingerprintProfile, error) {
	updated := *profile
	for i, existing := range r.profiles {
		if existing.ID == profile.ID {
			r.profiles[i] = &updated
			return &updated, nil
		}
	}
	r.profiles = append(r.profiles, &updated)
	return &updated, nil
}

func (r *tlsFingerprintProfileHandlerRepoStub) Delete(_ context.Context, id int64) error {
	next := r.profiles[:0]
	for _, profile := range r.profiles {
		if profile.ID != id {
			next = append(next, profile)
		}
	}
	r.profiles = next
	return nil
}

func TestTLSFingerprintProfileHandlerImportCaptures(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &tlsFingerprintProfileHandlerRepoStub{}
	svc := service.NewTLSFingerprintProfileService(repo, nil)
	handler := NewTLSFingerprintProfileHandler(svc, service.NewTLSFingerprintCaptureService(newTLSFingerprintCaptureHandlerRepoStub(), svc))

	router := gin.New()
	router.POST("/api/v1/admin/tls-fingerprint-profiles/import-captures", handler.ImportCaptures)

	body := `{"profiles":[{"name":"Codex Desktop live capture","enable_grease":false,"cipher_suites":[4865,4866],"curves":[29,23],"point_formats":[0],"signature_algorithms":[1027],"alpn_protocols":["http/1.1"],"supported_versions":[772,771],"key_share_groups":[29],"psk_modes":[1],"extensions":[0,11,10,13,43,45,51]}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tls-fingerprint-profiles/import-captures", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var envelope struct {
		Code int `json:"code"`
		Data struct {
			Imported   int `json:"imported"`
			Duplicates int `json:"duplicates"`
			Profiles   []struct {
				Duplicate       bool   `json:"duplicate"`
				FingerprintHash string `json:"fingerprint_hash"`
				Profile         struct {
					ID   int64  `json:"id"`
					Name string `json:"name"`
				} `json:"profile"`
			} `json:"profiles"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	require.Equal(t, 1, envelope.Data.Imported)
	require.Equal(t, 0, envelope.Data.Duplicates)
	require.Len(t, envelope.Data.Profiles, 1)
	require.False(t, envelope.Data.Profiles[0].Duplicate)
	require.NotEmpty(t, envelope.Data.Profiles[0].FingerprintHash)
	require.Equal(t, int64(1), envelope.Data.Profiles[0].Profile.ID)
	require.Equal(t, "Codex Desktop live capture", envelope.Data.Profiles[0].Profile.Name)
}

func TestTLSFingerprintProfileHandlerCreateAndUpdatePreservesUserAgent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &tlsFingerprintProfileHandlerRepoStub{}
	svc := service.NewTLSFingerprintProfileService(repo, nil)
	handler := NewTLSFingerprintProfileHandler(svc, service.NewTLSFingerprintCaptureService(newTLSFingerprintCaptureHandlerRepoStub(), svc))

	router := gin.New()
	router.POST("/api/v1/admin/tls-fingerprint-profiles", handler.Create)
	router.PUT("/api/v1/admin/tls-fingerprint-profiles/:id", handler.Update)

	createBody := `{"platform":"openai","name":"Codex captured","user_agent":" codex_exec/0.141.0 (Ubuntu 24.4.0; x86_64) ","enable_grease":false,"cipher_suites":[4865],"curves":[29],"point_formats":[0],"signature_algorithms":[1027],"alpn_protocols":["http/1.1"],"supported_versions":[772],"key_share_groups":[29],"psk_modes":[1],"extensions":[0,11]}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tls-fingerprint-profiles", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(createRec, createReq)
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())
	require.Len(t, repo.profiles, 1)
	require.Equal(t, "codex_exec/0.141.0 (Ubuntu 24.4.0; x86_64)", repo.profiles[0].UserAgent)

	updateBody := `{"user_agent":"codex-tui/0.142.0","name":"Codex captured","platform":"openai","enable_grease":false,"cipher_suites":[4865],"curves":[29],"point_formats":[0],"signature_algorithms":[1027],"alpn_protocols":["http/1.1"],"supported_versions":[772],"key_share_groups":[29],"psk_modes":[1],"extensions":[0,11]}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/admin/tls-fingerprint-profiles/1", strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(updateRec, updateReq)
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())
	require.Len(t, repo.profiles, 1)
	require.Equal(t, "codex-tui/0.142.0", repo.profiles[0].UserAgent)
}

func TestTLSFingerprintProfileHandlerCaptureTaskLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)

	profileRepo := &tlsFingerprintProfileHandlerRepoStub{}
	profileSvc := service.NewTLSFingerprintProfileService(profileRepo, nil)
	captureRepo := newTLSFingerprintCaptureHandlerRepoStub()
	captureSvc := service.NewTLSFingerprintCaptureService(captureRepo, profileSvc)
	handler := NewTLSFingerprintProfileHandler(profileSvc, captureSvc)

	router := gin.New()
	router.POST("/api/v1/admin/tls-fingerprint-profiles/capture-tasks", handler.StartCaptureTask)
	router.GET("/api/v1/admin/tls-fingerprint-profiles/capture-tasks", handler.ListCaptureTasks)
	router.GET("/api/v1/admin/tls-fingerprint-profiles/capture-tasks/:id/samples", handler.ListCaptureSamples)
	router.POST("/api/v1/admin/tls-fingerprint-profiles/capture-tasks/:id/import", handler.ImportCaptureTaskSamples)
	router.POST("/api/v1/tls-fingerprint-captures/submit", handler.SubmitCapture)

	startBody := `{"name":"Codex live","targets":{"openai":2},"ua_keywords":["codex"]}`
	startRec := httptest.NewRecorder()
	startReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tls-fingerprint-profiles/capture-tasks", strings.NewReader(startBody))
	startReq.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(startRec, startReq)
	require.Equal(t, http.StatusOK, startRec.Code, startRec.Body.String())

	var started struct {
		Data struct {
			ID     int64  `json:"id"`
			Token  string `json:"token"`
			Status string `json:"status"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	require.NotEmpty(t, started.Data.Token)
	require.Equal(t, service.TLSFingerprintCaptureStatusRunning, started.Data.Status)

	submitBody := `{"token":` + strconv.Quote(started.Data.Token) + `,"platform":"openai","user_agent":"codex-tui/0.140.0","payload":` + strconv.Quote(`{"name":"Codex TUI live","enable_grease":false,"cipher_suites":[4865,4866],"curves":[29,23],"point_formats":[0],"signature_algorithms":[1027],"alpn_protocols":["http/1.1"],"supported_versions":[772,771],"key_share_groups":[29],"psk_modes":[1],"extensions":[0,11,10,13,43,45,51]}`) + `}`
	submitRec := httptest.NewRecorder()
	submitReq := httptest.NewRequest(http.MethodPost, "/api/v1/tls-fingerprint-captures/submit", strings.NewReader(submitBody))
	submitReq.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(submitRec, submitReq)
	require.Equal(t, http.StatusOK, submitRec.Code, submitRec.Body.String())

	samplesRec := httptest.NewRecorder()
	samplesReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tls-fingerprint-profiles/capture-tasks/1/samples", nil)
	router.ServeHTTP(samplesRec, samplesReq)
	require.Equal(t, http.StatusOK, samplesRec.Code, samplesRec.Body.String())

	var samplesEnvelope struct {
		Data []struct {
			ID              int64  `json:"id"`
			Platform        string `json:"platform"`
			FingerprintHash string `json:"fingerprint_hash"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(samplesRec.Body.Bytes(), &samplesEnvelope))
	require.Len(t, samplesEnvelope.Data, 1)
	require.Equal(t, "openai", samplesEnvelope.Data[0].Platform)
	require.NotEmpty(t, samplesEnvelope.Data[0].FingerprintHash)

	importRec := httptest.NewRecorder()
	importReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tls-fingerprint-profiles/capture-tasks/1/import", strings.NewReader(`{"sample_ids":[1]}`))
	importReq.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(importRec, importReq)
	require.Equal(t, http.StatusOK, importRec.Code, importRec.Body.String())

	var importEnvelope struct {
		Data struct {
			Imported int `json:"imported"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importEnvelope))
	require.Equal(t, 1, importEnvelope.Data.Imported)
	require.Len(t, profileRepo.profiles, 1)
	require.Equal(t, "openai", profileRepo.profiles[0].Platform)

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tls-fingerprint-profiles/capture-tasks", nil)
	router.ServeHTTP(listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())
}

type tlsFingerprintCaptureHandlerRepoStub struct {
	mu           sync.Mutex
	nextTaskID   int64
	nextSampleID int64
	tasks        []*service.TLSFingerprintCaptureTask
	samples      []*service.TLSFingerprintCaptureSample
}

func newTLSFingerprintCaptureHandlerRepoStub() *tlsFingerprintCaptureHandlerRepoStub {
	return &tlsFingerprintCaptureHandlerRepoStub{nextTaskID: 1, nextSampleID: 1}
}

func (r *tlsFingerprintCaptureHandlerRepoStub) CreateTask(_ context.Context, task *service.TLSFingerprintCaptureTask) (*service.TLSFingerprintCaptureTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	created := cloneTLSFingerprintCaptureHandlerTask(task)
	created.ID = r.nextTaskID
	r.nextTaskID++
	now := time.Now().UTC()
	created.CreatedAt = now
	created.UpdatedAt = now
	r.tasks = append(r.tasks, created)
	return cloneTLSFingerprintCaptureHandlerTask(created), nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) ListTasks(_ context.Context) ([]*service.TLSFingerprintCaptureTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*service.TLSFingerprintCaptureTask, 0, len(r.tasks))
	for _, task := range r.tasks {
		out = append(out, cloneTLSFingerprintCaptureHandlerTask(task))
	}
	return out, nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) GetTaskByID(_ context.Context, id int64) (*service.TLSFingerprintCaptureTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, task := range r.tasks {
		if task.ID == id {
			return cloneTLSFingerprintCaptureHandlerTask(task), nil
		}
	}
	return nil, nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) GetRunningTaskByToken(_ context.Context, token string) (*service.TLSFingerprintCaptureTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, task := range r.tasks {
		if task.Token == token && task.Status == service.TLSFingerprintCaptureStatusRunning {
			return cloneTLSFingerprintCaptureHandlerTask(task), nil
		}
	}
	return nil, nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) UpdateTask(_ context.Context, task *service.TLSFingerprintCaptureTask) (*service.TLSFingerprintCaptureTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	updated := cloneTLSFingerprintCaptureHandlerTask(task)
	updated.UpdatedAt = time.Now().UTC()
	for i, existing := range r.tasks {
		if existing.ID == task.ID {
			r.tasks[i] = updated
			return cloneTLSFingerprintCaptureHandlerTask(updated), nil
		}
	}
	r.tasks = append(r.tasks, updated)
	return cloneTLSFingerprintCaptureHandlerTask(updated), nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) CreateSampleIfAbsent(_ context.Context, sample *service.TLSFingerprintCaptureSample) (*service.TLSFingerprintCaptureSample, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.samples {
		if existing.TaskID == sample.TaskID && existing.FingerprintHash == sample.FingerprintHash {
			return cloneTLSFingerprintCaptureHandlerSample(existing), false, nil
		}
	}
	created := cloneTLSFingerprintCaptureHandlerSample(sample)
	created.ID = r.nextSampleID
	r.nextSampleID++
	created.CreatedAt = time.Now().UTC()
	r.samples = append(r.samples, created)
	return cloneTLSFingerprintCaptureHandlerSample(created), true, nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) GetSampleByTaskHash(_ context.Context, taskID int64, fingerprintHash string) (*service.TLSFingerprintCaptureSample, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sample := range r.samples {
		if sample.TaskID == taskID && sample.FingerprintHash == fingerprintHash {
			return cloneTLSFingerprintCaptureHandlerSample(sample), nil
		}
	}
	return nil, nil
}

func (r *tlsFingerprintCaptureHandlerRepoStub) ListSamplesByTask(_ context.Context, taskID int64) ([]*service.TLSFingerprintCaptureSample, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*service.TLSFingerprintCaptureSample, 0, len(r.samples))
	for _, sample := range r.samples {
		if sample.TaskID == taskID {
			out = append(out, cloneTLSFingerprintCaptureHandlerSample(sample))
		}
	}
	return out, nil
}

func cloneTLSFingerprintCaptureHandlerTask(task *service.TLSFingerprintCaptureTask) *service.TLSFingerprintCaptureTask {
	if task == nil {
		return nil
	}
	clone := *task
	clone.Targets = cloneStringIntMap(task.Targets)
	clone.Counts = cloneStringIntMap(task.Counts)
	clone.UAKeywords = append([]string(nil), task.UAKeywords...)
	if task.CompletedAt != nil {
		completedAt := *task.CompletedAt
		clone.CompletedAt = &completedAt
	}
	return &clone
}

func cloneTLSFingerprintCaptureHandlerSample(sample *service.TLSFingerprintCaptureSample) *service.TLSFingerprintCaptureSample {
	if sample == nil {
		return nil
	}
	clone := *sample
	if sample.Profile != nil {
		profile := *sample.Profile
		profile.CipherSuites = append([]uint16(nil), sample.Profile.CipherSuites...)
		profile.Curves = append([]uint16(nil), sample.Profile.Curves...)
		profile.PointFormats = append([]uint16(nil), sample.Profile.PointFormats...)
		profile.SignatureAlgorithms = append([]uint16(nil), sample.Profile.SignatureAlgorithms...)
		profile.ALPNProtocols = append([]string(nil), sample.Profile.ALPNProtocols...)
		profile.SupportedVersions = append([]uint16(nil), sample.Profile.SupportedVersions...)
		profile.KeyShareGroups = append([]uint16(nil), sample.Profile.KeyShareGroups...)
		profile.PSKModes = append([]uint16(nil), sample.Profile.PSKModes...)
		profile.Extensions = append([]uint16(nil), sample.Profile.Extensions...)
		clone.Profile = &profile
	}
	return &clone
}

func cloneStringIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
