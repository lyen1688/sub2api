//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestProbeOpenAIAPIKeyResponsesSupportUsesCodexUAWhenTLSProfileHasNoUserAgent(t *testing.T) {
	const configuredUA = "codex-tui/9.9.9 probe"
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"probe"}}`)),
	}}
	account := Account{
		ID:          7,
		Name:        "openai apikey",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key": "sk-test",
		},
		Extra: map[string]any{
			"enable_tls_fingerprint":     true,
			"tls_fingerprint_profile_id": float64(11),
		},
	}
	svc := &AccountTestService{
		accountRepo:  &snapshotUpdateAccountRepo{stubOpenAIAccountRepo: stubOpenAIAccountRepo{accounts: []Account{account}}},
		httpUpstream: upstream,
		cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{
			Enabled: false,
		}}},
		settingService: NewSettingService(&openAISettingRepoStub{values: map[string]string{
			SettingKeyOpenAICodexUserAgent: configuredUA,
		}}, &config.Config{}),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				11: {ID: 11, Name: "Empty UA"},
			},
		},
	}

	svc.ProbeOpenAIAPIKeyResponsesSupport(context.Background(), account.ID)

	require.NotNil(t, upstream.lastReq)
	require.Equal(t, configuredUA, upstream.lastReq.Header.Get("User-Agent"))
}

func TestForwardCCToAnthropicMessagesUsesCodexUAWhenTLSProfileHasNoUserAgent(t *testing.T) {
	setGinTestMode()
	const configuredUA = "codex-tui/9.9.9 cc-to-messages"

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
	}}
	account := &Account{
		ID:          9,
		Name:        "openai apikey messages upstream",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":  "sk-test",
			"base_url": "https://example.com",
		},
		Extra: map[string]any{
			"enable_tls_fingerprint":     true,
			"tls_fingerprint_profile_id": float64(12),
		},
	}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{},
		httpUpstream: upstream,
		settingService: NewSettingService(&openAISettingRepoStub{values: map[string]string{
			SettingKeyOpenAICodexUserAgent: configuredUA,
		}}, &config.Config{}),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				12: {ID: 12, Name: "Empty UA"},
			},
		},
	}

	_, err := svc.forwardCCToAnthropicMessages(context.Background(), c, account, body, time.Now())

	require.NoError(t, err)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, configuredUA, upstream.lastReq.Header.Get("User-Agent"))
}
