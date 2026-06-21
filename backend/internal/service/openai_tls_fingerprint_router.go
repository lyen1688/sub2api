package service

import (
	"context"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
)

type openAITLSFingerprintRuntime struct {
	Profile            *tlsfingerprint.Profile
	ProfileSelected    bool
	UpstreamUserAgent  string
	UpstreamOriginator string
	Matched            bool
}

func (s *OpenAIGatewayService) SetTLSFingerprintRouterService(routerService *TLSFingerprintRouterService) {
	if s == nil {
		return
	}
	s.tlsFPRouterService = routerService
}

func (s *OpenAIGatewayService) SetTLSFingerprintProfileService(profileService *TLSFingerprintProfileService) {
	if s == nil {
		return
	}
	s.tlsFPProfileService = profileService
}

func (s *OpenAIGatewayService) resolveOpenAITLSProfile(account *Account) (*tlsfingerprint.Profile, bool) {
	if s == nil || s.tlsFPProfileService == nil {
		return nil, false
	}
	profile := s.tlsFPProfileService.ResolveTLSProfile(account)
	return profile, account != nil && account.IsOpenAITLSFingerprintEnabled() && profile != nil
}

func (s *OpenAIGatewayService) resolveOpenAITLSFingerprintRuntime(ctx context.Context, c *gin.Context, account *Account) openAITLSFingerprintRuntime {
	runtime := openAITLSFingerprintRuntime{}
	if s != nil {
		runtime.Profile, runtime.ProfileSelected = s.resolveOpenAITLSProfile(account)
	}
	if s == nil || s.tlsFPRouterService == nil || account == nil {
		return runtime
	}
	if !account.IsOpenAITLSFingerprintEnabled() {
		return runtime
	}
	routerID := account.GetTLSFingerprintRouterID()
	if routerID <= 0 {
		return runtime
	}
	inboundUA := ""
	inboundOriginator := ""
	if c != nil && c.Request != nil {
		inboundUA = c.Request.Header.Get("User-Agent")
		inboundOriginator = c.Request.Header.Get("Originator")
	}
	match, ok := s.tlsFPRouterService.MatchRequest(ctx, routerID, inboundUA, inboundOriginator)
	if !ok {
		return runtime
	}
	if s.tlsFPProfileService == nil || match.ProfileID <= 0 {
		return runtime
	}
	profile := s.tlsFPProfileService.ResolveTLSProfileByID(match.ProfileID)
	if profile == nil {
		return runtime
	}
	runtime.Profile = profile
	runtime.ProfileSelected = true
	runtime.UpstreamUserAgent = strings.TrimSpace(match.UpstreamUserAgent)
	runtime.UpstreamOriginator = strings.TrimSpace(match.UpstreamOriginator)
	runtime.Matched = true
	return runtime
}

func (s *OpenAIGatewayService) resolveOpenAITLSFingerprintUserAgent(ctx context.Context, runtime openAITLSFingerprintRuntime, passthrough bool) string {
	if userAgent := strings.TrimSpace(runtime.UpstreamUserAgent); userAgent != "" {
		return userAgent
	}
	if runtime.Profile == nil {
		return ""
	}
	if userAgent := strings.TrimSpace(runtime.Profile.UserAgent); userAgent != "" {
		return userAgent
	}
	if !runtime.ProfileSelected {
		return ""
	}
	if passthrough {
		return ""
	}
	if s != nil && s.settingService != nil {
		if userAgent := strings.TrimSpace(s.settingService.GetOpenAICodexUserAgent(ctx)); userAgent != "" {
			return userAgent
		}
	}
	return DefaultOpenAICodexUserAgent
}

func (s *OpenAIGatewayService) applyOpenAITLSFingerprintRuntime(ctx context.Context, req *http.Request, runtime openAITLSFingerprintRuntime, passthrough bool) {
	if req == nil {
		return
	}
	if userAgent := s.resolveOpenAITLSFingerprintUserAgent(ctx, runtime, passthrough); userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	if runtime.UpstreamOriginator != "" {
		req.Header.Set("Originator", runtime.UpstreamOriginator)
	}
}

func (s *OpenAIGatewayService) applyOpenAIWSFingerprintRuntimeHeaders(ctx context.Context, headers http.Header, runtime openAITLSFingerprintRuntime, passthrough bool) {
	if headers == nil {
		return
	}
	if userAgent := s.resolveOpenAITLSFingerprintUserAgent(ctx, runtime, passthrough); userAgent != "" {
		headers.Set("user-agent", userAgent)
	}
	if runtime.UpstreamOriginator != "" {
		headers.Set("originator", runtime.UpstreamOriginator)
	}
}

func (s *AccountTestService) resolveOpenAITLSProfile(account *Account) (*tlsfingerprint.Profile, bool) {
	if s == nil || s.tlsFPProfileService == nil {
		return nil, false
	}
	profile := s.tlsFPProfileService.ResolveTLSProfile(account)
	return profile, account != nil && account.IsOpenAITLSFingerprintEnabled() && profile != nil
}

func (s *AccountTestService) resolveOpenAITLSFingerprintRuntime(account *Account) openAITLSFingerprintRuntime {
	runtime := openAITLSFingerprintRuntime{}
	if s != nil {
		runtime.Profile, runtime.ProfileSelected = s.resolveOpenAITLSProfile(account)
	}
	return runtime
}

func (s *AccountTestService) resolveOpenAITLSFingerprintUserAgent(ctx context.Context, runtime openAITLSFingerprintRuntime, passthrough bool) string {
	if userAgent := strings.TrimSpace(runtime.UpstreamUserAgent); userAgent != "" {
		return userAgent
	}
	if runtime.Profile == nil {
		return ""
	}
	if userAgent := strings.TrimSpace(runtime.Profile.UserAgent); userAgent != "" {
		return userAgent
	}
	if !runtime.ProfileSelected {
		return ""
	}
	if passthrough {
		return ""
	}
	if s != nil && s.settingService != nil {
		if userAgent := strings.TrimSpace(s.settingService.GetOpenAICodexUserAgent(ctx)); userAgent != "" {
			return userAgent
		}
	}
	return DefaultOpenAICodexUserAgent
}

func (s *AccountTestService) applyOpenAITLSFingerprintRuntime(ctx context.Context, req *http.Request, runtime openAITLSFingerprintRuntime, passthrough bool) {
	if req == nil {
		return
	}
	if userAgent := s.resolveOpenAITLSFingerprintUserAgent(ctx, runtime, passthrough); userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	if runtime.UpstreamOriginator != "" {
		req.Header.Set("Originator", runtime.UpstreamOriginator)
	}
}
