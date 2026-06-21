//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAITLSFingerprintRuntimeUsesRouterMatch(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      10,
		Name:    "openai clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "cursor",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchPrefix,
				Pattern:                 "Cursor/",
				TLSFingerprintProfileID: 7,
				UpstreamUserAgent:       "codex_cli_rs/0.125.0",
				UpstreamOriginator:      "codex_cli_rs",
			},
		},
	}
	svc := &OpenAIGatewayService{
		tlsFPRouterService: NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {
					ID:            7,
					Name:          "Chrome Routed",
					ALPNProtocols: []string{"h2", "http/1.1"},
				},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "Cursor/1.2.3")
	req.Header.Set("Originator", "codex_cli_rs")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_router_id": float64(10)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.True(t, runtime.Matched)
	require.NotNil(t, runtime.Profile)
	require.Equal(t, "Chrome Routed", runtime.Profile.Name)

	upstreamReq, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)
	upstreamReq.Header.Set("User-Agent", "original-client/1.0")
	svc.applyOpenAITLSFingerprintRuntime(context.Background(), upstreamReq, runtime, false)
	require.Equal(t, "codex_cli_rs/0.125.0", upstreamReq.Header.Get("User-Agent"))
	require.Equal(t, "codex_cli_rs", upstreamReq.Header.Get("Originator"))
}

func TestOpenAITLSFingerprintRuntimeUsesRoutedProfileUserAgent(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      12,
		Name:    "openai clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "codex",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchContains,
				Pattern:                 "codex",
				TLSFingerprintProfileID: 7,
			},
		},
	}
	const profileUA = "codex_exec/0.141.0 (Ubuntu 24.4.0; x86_64)"
	svc := &OpenAIGatewayService{
		tlsFPRouterService: NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {
					ID:        7,
					Name:      "Codex Routed",
					UserAgent: profileUA,
				},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "codex-tui/0.140.0")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_router_id": float64(12)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.True(t, runtime.Matched)
	require.NotNil(t, runtime.Profile)
	require.Equal(t, profileUA, runtime.Profile.UserAgent)

	upstreamReq, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	require.NoError(t, err)
	upstreamReq.Header.Set("User-Agent", "original-client/1.0")
	svc.applyOpenAITLSFingerprintRuntime(context.Background(), upstreamReq, runtime, false)
	require.Equal(t, profileUA, upstreamReq.Header.Get("User-Agent"))
}

func TestOpenAITLSFingerprintRuntimeFallsBackToLevelOneProfileWhenRouterMisses(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      13,
		Name:    "openai clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "cursor",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchPrefix,
				Pattern:                 "Cursor/",
				TLSFingerprintProfileID: 9,
			},
		},
	}
	const levelOneUA = "codex-tui/0.142.0 (Debian GNU/Linux 12; x86_64)"
	svc := &OpenAIGatewayService{
		tlsFPRouterService: NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Level One", UserAgent: levelOneUA},
				9: {ID: 9, Name: "Routed"},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "codex-tui/0.140.0")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra: map[string]any{
			"enable_tls_fingerprint":     true,
			"tls_fingerprint_profile_id": float64(7),
			"tls_fingerprint_router_id":  float64(13),
		},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.False(t, runtime.Matched)
	require.NotNil(t, runtime.Profile)
	require.True(t, runtime.ProfileSelected)
	require.Equal(t, "Level One", runtime.Profile.Name)

	upstreamReq, err := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)
	upstreamReq.Header.Set("User-Agent", "original-client/1.0")
	svc.applyOpenAITLSFingerprintRuntime(context.Background(), upstreamReq, runtime, false)
	require.Equal(t, levelOneUA, upstreamReq.Header.Get("User-Agent"))
}

func TestOpenAITLSFingerprintRuntimeUsesCodexUAWhenSelectedProfileHasNoUserAgentAndNotPassthrough(t *testing.T) {
	setGinTestMode()
	const configuredUA = "codex-tui/9.9.9 configured"
	svc := &OpenAIGatewayService{
		settingService: NewSettingService(&openAISettingRepoStub{values: map[string]string{
			SettingKeyOpenAICodexUserAgent: configuredUA,
		}}, &config.Config{}),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Empty UA"},
			},
		},
	}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_profile_id": float64(7)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), nil, account)

	require.NotNil(t, runtime.Profile)
	require.True(t, runtime.ProfileSelected)
	require.Empty(t, runtime.Profile.UserAgent)

	upstreamReq, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	require.NoError(t, err)
	upstreamReq.Header.Set("User-Agent", "Go-http-client/1.1")
	svc.applyOpenAITLSFingerprintRuntime(context.Background(), upstreamReq, runtime, false)
	require.Equal(t, configuredUA, upstreamReq.Header.Get("User-Agent"))
}

func TestOpenAITLSFingerprintRuntimeKeepsClientUAWhenSelectedProfileHasNoUserAgentAndPassthrough(t *testing.T) {
	setGinTestMode()
	svc := &OpenAIGatewayService{
		settingService: NewSettingService(&openAISettingRepoStub{values: map[string]string{
			SettingKeyOpenAICodexUserAgent: "codex-tui/9.9.9 configured",
		}}, &config.Config{}),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Empty UA"},
			},
		},
	}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_profile_id": float64(7)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), nil, account)

	upstreamReq, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	require.NoError(t, err)
	upstreamReq.Header.Set("User-Agent", "real-client/1.0")
	svc.applyOpenAITLSFingerprintRuntime(context.Background(), upstreamReq, runtime, true)
	require.Equal(t, "real-client/1.0", upstreamReq.Header.Get("User-Agent"))
}

func TestOpenAIWSFingerprintRuntimeHeadersUseCodexUAFallback(t *testing.T) {
	setGinTestMode()
	const configuredUA = "codex-tui/9.9.9 ws"
	svc := &OpenAIGatewayService{
		settingService: NewSettingService(&openAISettingRepoStub{values: map[string]string{
			SettingKeyOpenAICodexUserAgent: configuredUA,
		}}, &config.Config{}),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Empty UA"},
			},
		},
	}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_profile_id": float64(7)},
	}
	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), nil, account)
	headers := http.Header{}
	headers.Set("user-agent", "Go-http-client/1.1")

	svc.applyOpenAIWSFingerprintRuntimeHeaders(context.Background(), headers, runtime, false)

	require.Equal(t, configuredUA, headers.Get("user-agent"))
}

func TestOpenAITLSFingerprintRuntimeDoesNotOverrideOriginatorFromUserAgentOnly(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      11,
		Name:    "openai guarded clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "cursor",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchPrefix,
				Pattern:                 "Cursor/",
				TLSFingerprintProfileID: 7,
				UpstreamUserAgent:       "codex_cli_rs/0.125.0",
				UpstreamOriginator:      "codex_cli_rs",
			},
		},
	}
	svc := &OpenAIGatewayService{
		tlsFPRouterService: NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Chrome Routed"},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "Cursor/1.2.3")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_router_id": float64(11)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.False(t, runtime.Matched)
	require.Empty(t, runtime.UpstreamOriginator)
}

func TestOpenAITLSFingerprintRuntimeSkipsRouterWhenTLSFingerprintDisabled(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      10,
		Name:    "openai clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "cursor",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchPrefix,
				Pattern:                 "Cursor/",
				TLSFingerprintProfileID: 7,
				UpstreamUserAgent:       "codex_cli_rs/0.125.0",
				UpstreamOriginator:      "codex_cli_rs",
			},
		},
	}
	svc := &OpenAIGatewayService{
		tlsFPRouterService: NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{
			localCache: map[int64]*model.TLSFingerprintProfile{
				7: {ID: 7, Name: "Chrome Routed"},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "Cursor/1.2.3")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{"tls_fingerprint_router_id": float64(10)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.False(t, runtime.Matched)
	require.Nil(t, runtime.Profile)
	require.Empty(t, runtime.UpstreamUserAgent)
	require.Empty(t, runtime.UpstreamOriginator)
}

func TestOpenAITLSFingerprintRuntimeIgnoresRouterMatchWithMissingProfile(t *testing.T) {
	setGinTestMode()
	router := &model.TLSFingerprintRouter{
		ID:      10,
		Name:    "openai clients",
		Enabled: true,
		Rules: []model.TLSFingerprintRouterRule{
			{
				Name:                    "cursor",
				Enabled:                 true,
				MatchType:               model.TLSFingerprintRouterMatchPrefix,
				Pattern:                 "Cursor/",
				TLSFingerprintProfileID: 404,
				UpstreamUserAgent:       "codex_cli_rs/0.125.0",
				UpstreamOriginator:      "codex_cli_rs",
			},
		},
	}
	svc := &OpenAIGatewayService{
		tlsFPRouterService:  NewTLSFingerprintRouterService(&tlsFingerprintRouterRepoStub{routers: []*model.TLSFingerprintRouter{router}}, nil),
		tlsFPProfileService: &TLSFingerprintProfileService{localCache: map[int64]*model.TLSFingerprintProfile{}},
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/responses", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "Cursor/1.2.3")
	c := &gin.Context{Request: req}
	account := &Account{
		ID:       1,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{"enable_tls_fingerprint": true, "tls_fingerprint_router_id": float64(10)},
	}

	runtime := svc.resolveOpenAITLSFingerprintRuntime(context.Background(), c, account)

	require.False(t, runtime.Matched)
	require.NotNil(t, runtime.Profile)
	require.Equal(t, "Built-in Default (Node.js 24.x)", runtime.Profile.Name)
	require.Empty(t, runtime.UpstreamUserAgent)
	require.Empty(t, runtime.UpstreamOriginator)
}
