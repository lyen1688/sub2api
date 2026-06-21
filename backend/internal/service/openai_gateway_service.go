package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/Wei-Shaw/sub2api/internal/util/urlvalidator"
	"github.com/cespare/xxhash/v2"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

const (
	// ChatGPT internal API for OAuth accounts
	chatgptCodexURL = "https://chatgpt.com/backend-api/codex/responses"
	// OpenAI Platform API for API Key accounts (fallback)
	openaiPlatformAPIURL   = "https://api.openai.com/v1/responses"
	openaiStickySessionTTL = time.Hour // 粘性会话TTL
	// 与真实 Codex CLI 的 User-Agent 结构对齐：
	// {originator}/{version} ({OS} {OS_version}; {arch}) {terminal}
	// 旧值 "codex_cli_rs/0.125.0" 缺少 OS/架构/终端后缀，易被上游指纹识别为非官方客户端。
	codexCLIUserAgent = "codex_cli_rs/0.125.0 (Ubuntu 22.4.0; x86_64) xterm-256color"
	// codex_cli_only 拒绝时单个请求头日志长度上限（字符）
	codexCLIOnlyHeaderValueMaxBytes = 256

	// OpenAIParsedRequestBodyKey 缓存 handler 侧已解析的请求体，避免重复解析。
	OpenAIParsedRequestBodyKey = "openai_parsed_request_body"
	// OpenAI WS Mode 失败后的重连次数上限（不含首次尝试）。
	// 与 Codex 客户端保持一致：失败后最多重连 5 次。
	openAIWSReconnectRetryLimit = 5
	// 上游错误体只需要提取错误 JSON/日志摘要，默认 512KiB 避免错误风暴叠加大请求体。
	openAIUpstreamErrorBodyReadLimit int64 = 512 << 10
	// OpenAI WS Mode 重连退避默认值（可由配置覆盖）。
	openAIWSRetryBackoffInitialDefault   = 120 * time.Millisecond
	openAIWSRetryBackoffMaxDefault       = 2 * time.Second
	openAIWSRetryJitterRatioDefault      = 0.2
	openAICompactSessionSeedKey          = "openai_compact_session_seed"
	openAICodexTransformObsKey           = "openai_codex_transform_observability"
	openAICodexCompatFallbackKey         = "openai_codex_compat_fallback"
	openAICodexCompatFallbackReasonKey   = "openai_codex_compat_fallback_reason"
	openAIRoutingPromptCacheKeyKey       = "openai_routing_prompt_cache_key"
	openAIMessagesDispatchForcedModelKey = "openai_messages_dispatch_forced_model"
	openAIFailoverRequestBodyKey         = "openai_failover_request_body"
	openAITTFTWatchdogBypassKey          = "openai_ttft_watchdog_bypass"
	codexCLIVersion                      = "0.125.0"
	// Codex 限额快照仅用于后台展示/诊断，不需要每个成功请求都立即落库。
	openAICodexSnapshotPersistMinInterval = 30 * time.Second
	// 配额自动暂停时，超过该时长仍未刷新的 used% 快照视为陈旧，不再据此暂停账号。
	openAICodexAutoPauseStaleAfter = 2 * time.Hour
	openAICacheProbePrefix4KBytes  = 4 * 1024
	openAICacheProbePrefix16KBytes = 16 * 1024
)

var (
	openAITTFTWatchdogTimeout  = 0 * time.Second
	openAITTFTCooldownDuration = time.Minute
)

func openAIUpstreamErrorBodyReadLimitForConfig(cfg *config.Config) int64 {
	limit := openAIUpstreamErrorBodyReadLimit
	if cfg != nil && cfg.Gateway.LogUpstreamErrorBody && cfg.Gateway.LogUpstreamErrorBodyMaxBytes > int(limit) {
		limit = int64(cfg.Gateway.LogUpstreamErrorBodyMaxBytes)
	}
	return limit
}

func (s *OpenAIGatewayService) readUpstreamErrorBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	cfg := (*config.Config)(nil)
	if s != nil {
		cfg = s.cfg
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, openAIUpstreamErrorBodyReadLimitForConfig(cfg)))
	return body
}

var openAIResponsesUnsupportedFields = []string{
	"prompt_cache_retention",
	"reasoningSummary",
	"safety_identifier",
	"metadata",
	"stream_options",
	"temperature",
	"verbosity",
	"enable_thinking",
	"stop_sequences",
	"promptCacheKey",
}

type openAICodexCompatFallbackState struct {
	Triggered    bool
	Reason       string
	BodyModified bool
}

func SetOpenAIMessagesDispatchForcedModel(c *gin.Context, model string) {
	if c == nil {
		return
	}
	if model = strings.TrimSpace(model); model != "" {
		c.Set(openAIMessagesDispatchForcedModelKey, model)
	}
}

func getOpenAIMessagesDispatchForcedModel(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, ok := c.Get(openAIMessagesDispatchForcedModelKey)
	if !ok {
		return ""
	}
	return strings.TrimSpace(firstNonEmptyString(value))
}

func setOpenAIFailoverRequestBody(c *gin.Context, body []byte) {
	if c == nil || len(body) == 0 {
		return
	}
	c.Set(openAIFailoverRequestBodyKey, append([]byte(nil), body...))
}

func getOpenAIFailoverRequestBody(c *gin.Context, fallback []byte) ([]byte, bool) {
	if c == nil {
		return fallback, false
	}
	value, ok := c.Get(openAIFailoverRequestBodyKey)
	if !ok {
		return fallback, false
	}
	body, ok := value.([]byte)
	if !ok || len(body) == 0 {
		return fallback, false
	}
	return append([]byte(nil), body...), true
}

func setOpenAIRoutingPromptCacheKey(c *gin.Context, promptCacheKey string) {
	if c == nil {
		return
	}
	if promptCacheKey = strings.TrimSpace(promptCacheKey); promptCacheKey != "" {
		c.Set(openAIRoutingPromptCacheKeyKey, promptCacheKey)
	}
}

func getOpenAIRoutingPromptCacheKey(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, ok := c.Get(openAIRoutingPromptCacheKeyKey)
	if !ok {
		return ""
	}
	return strings.TrimSpace(firstNonEmptyString(value))
}

func setOpenAITTFTWatchdogBypass(c *gin.Context, bypass bool) {
	if c == nil {
		return
	}
	if bypass {
		c.Set(openAITTFTWatchdogBypassKey, true)
		return
	}
	if c.Keys != nil {
		delete(c.Keys, openAITTFTWatchdogBypassKey)
	}
}

func shouldBypassOpenAITTFTWatchdog(c *gin.Context) bool {
	if c == nil {
		return false
	}
	value, ok := c.Get(openAITTFTWatchdogBypassKey)
	if !ok {
		return false
	}
	bypass, ok := value.(bool)
	return ok && bypass
}

func clearOpenAIRequestBodyCache(c *gin.Context) {
	if c == nil || c.Keys == nil {
		return
	}
	delete(c.Keys, OpenAIParsedRequestBodyKey)
}

// ClearOpenAICompatRequestState clears per-request OpenAI compat replay/cache state.
func ClearOpenAICompatRequestState(c *gin.Context) {
	clearOpenAIRequestBodyCache(c)
	ClearOpenAIStreamRetryReplayState(c)
}

// OpenAI allowed headers whitelist (for non-passthrough).
var openaiAllowedHeaders = map[string]bool{
	"accept":                   true,
	"accept-language":          true,
	"content-type":             true,
	"conversation_id":          true,
	"user-agent":               true,
	"originator":               true,
	"session_id":               true,
	"x-client-request-id":      true,
	"x-codex-beta-features":    true,
	"x-codex-installation-id":  true,
	"x-codex-turn-state":       true,
	"x-codex-turn-metadata":    true,
	"x-codex-window-id":        true,
	"x-codex-parent-thread-id": true,
	"x-openai-subagent":        true,
}

// OpenAI passthrough allowed headers whitelist.
// 透传模式下仅放行这些低风险请求头，避免将非标准/环境噪声头传给上游触发风控。
var openaiPassthroughAllowedHeaders = map[string]bool{
	"accept":                   true,
	"accept-language":          true,
	"content-type":             true,
	"conversation_id":          true,
	"openai-beta":              true,
	"user-agent":               true,
	"originator":               true,
	"session_id":               true,
	"x-client-request-id":      true,
	"x-codex-beta-features":    true,
	"x-codex-installation-id":  true,
	"x-codex-turn-state":       true,
	"x-codex-turn-metadata":    true,
	"x-codex-window-id":        true,
	"x-codex-parent-thread-id": true,
	"x-openai-subagent":        true,
}

var openaiOAuthOnlyHeaders = map[string]bool{
	"x-client-request-id":      true,
	"x-codex-beta-features":    true,
	"x-codex-installation-id":  true,
	"x-codex-turn-state":       true,
	"x-codex-turn-metadata":    true,
	"x-codex-window-id":        true,
	"x-codex-parent-thread-id": true,
	"x-openai-subagent":        true,
}

// codex_cli_only 拒绝时记录的请求头白名单（仅用于诊断日志，不参与上游透传）
var codexCLIOnlyDebugHeaderWhitelist = []string{
	"User-Agent",
	"Content-Type",
	"Accept",
	"Accept-Language",
	"OpenAI-Beta",
	"Originator",
	"Session_ID",
	"Conversation_ID",
	"X-Request-ID",
	"X-Client-Request-ID",
	"X-Forwarded-For",
	"X-Real-IP",
}

// OpenAICodexUsageSnapshot represents Codex API usage limits from response headers
type OpenAICodexUsageSnapshot struct {
	PrimaryUsedPercent          *float64 `json:"primary_used_percent,omitempty"`
	PrimaryResetAfterSeconds    *int     `json:"primary_reset_after_seconds,omitempty"`
	PrimaryWindowMinutes        *int     `json:"primary_window_minutes,omitempty"`
	SecondaryUsedPercent        *float64 `json:"secondary_used_percent,omitempty"`
	SecondaryResetAfterSeconds  *int     `json:"secondary_reset_after_seconds,omitempty"`
	SecondaryWindowMinutes      *int     `json:"secondary_window_minutes,omitempty"`
	PrimaryOverSecondaryPercent *float64 `json:"primary_over_secondary_percent,omitempty"`
	UpdatedAt                   string   `json:"updated_at,omitempty"`
}

// NormalizedCodexLimits contains normalized 5h/7d rate limit data
type NormalizedCodexLimits struct {
	Used5hPercent   *float64
	Reset5hSeconds  *int
	Window5hMinutes *int
	Used7dPercent   *float64
	Reset7dSeconds  *int
	Window7dMinutes *int
}

// Normalize converts primary/secondary fields to canonical 5h/7d fields.
// Strategy: Compare window_minutes to determine which is 5h vs 7d.
// Returns nil if snapshot is nil or has no useful data.
func (s *OpenAICodexUsageSnapshot) Normalize() *NormalizedCodexLimits {
	if s == nil {
		return nil
	}

	result := &NormalizedCodexLimits{}

	primaryMins := 0
	secondaryMins := 0
	hasPrimaryWindow := false
	hasSecondaryWindow := false

	if s.PrimaryWindowMinutes != nil {
		primaryMins = *s.PrimaryWindowMinutes
		hasPrimaryWindow = true
	}
	if s.SecondaryWindowMinutes != nil {
		secondaryMins = *s.SecondaryWindowMinutes
		hasSecondaryWindow = true
	}

	// Determine mapping based on window_minutes
	use5hFromPrimary := false
	use7dFromPrimary := false

	if hasPrimaryWindow && hasSecondaryWindow {
		// Both known: smaller window is 5h, larger is 7d
		if primaryMins < secondaryMins {
			use5hFromPrimary = true
		} else {
			use7dFromPrimary = true
		}
	} else if hasPrimaryWindow {
		// Only primary known: classify by threshold (<=360 min = 6h -> 5h window)
		if primaryMins <= 360 {
			use5hFromPrimary = true
		} else {
			use7dFromPrimary = true
		}
	} else if hasSecondaryWindow {
		// Only secondary known: classify by threshold
		if secondaryMins <= 360 {
			// 5h from secondary, so primary (if any data) is 7d
			use7dFromPrimary = true
		} else {
			// 7d from secondary, so primary (if any data) is 5h
			use5hFromPrimary = true
		}
	} else {
		// No window_minutes: fall back to legacy assumption (primary=7d, secondary=5h)
		use7dFromPrimary = true
	}

	// Assign values
	if use5hFromPrimary {
		result.Used5hPercent = s.PrimaryUsedPercent
		result.Reset5hSeconds = s.PrimaryResetAfterSeconds
		result.Window5hMinutes = s.PrimaryWindowMinutes
		result.Used7dPercent = s.SecondaryUsedPercent
		result.Reset7dSeconds = s.SecondaryResetAfterSeconds
		result.Window7dMinutes = s.SecondaryWindowMinutes
	} else if use7dFromPrimary {
		result.Used7dPercent = s.PrimaryUsedPercent
		result.Reset7dSeconds = s.PrimaryResetAfterSeconds
		result.Window7dMinutes = s.PrimaryWindowMinutes
		result.Used5hPercent = s.SecondaryUsedPercent
		result.Reset5hSeconds = s.SecondaryResetAfterSeconds
		result.Window5hMinutes = s.SecondaryWindowMinutes
	}

	return result
}

// OpenAIUsage represents OpenAI API response usage
type OpenAIUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
	ImageOutputTokens        int `json:"image_output_tokens,omitempty"`
}

// OpenAIForwardResult represents the result of forwarding
type OpenAIForwardResult struct {
	RequestID  string
	ResponseID string
	Usage      OpenAIUsage
	Model      string // 原始模型（用于响应和日志显示）
	// BillingModel is the model used for cost calculation.
	// When non-empty, CalculateCost uses this instead of Model.
	// This is set by the Anthropic Messages conversion path where
	// the mapped upstream model differs from the client-facing model.
	BillingModel string
	// TokenBillingModel overrides token cost calculation when an image request
	// also produces response-side tokens billed against a different model.
	TokenBillingModel string
	// ImageUsageTokenBilling means image generation usage contains billable
	// image API tokens; when true, image_count is used only as metadata.
	ImageUsageTokenBilling bool
	// UpstreamModel is the actual model sent to the upstream provider after mapping.
	// Empty when no mapping was applied (requested model was used as-is).
	UpstreamModel string
	// ServiceTier records the OpenAI Responses API service tier, e.g. "priority" / "flex".
	// Nil means the request did not specify a recognized tier.
	ServiceTier *string
	// ReasoningEffort is extracted from request body (reasoning.effort) or derived from model suffix.
	// Stored for usage records display; nil means not provided / not applicable.
	ReasoningEffort      *string
	Stream               bool
	OpenAIWSMode         bool
	OpenAIWSProfile      string
	OpenAIWSConnReused   bool
	EffectiveRequestType RequestType
	ResponseHeaders      http.Header
	Duration             time.Duration
	FirstTokenMs         *int
	ClientDisconnected   bool
	ClientDisconnect     bool
	ImageCount           int
	ImageSize            string
	ImageInputSize       string
	ImageOutputSize      string
	ImageOutputSizes     []string
	ImageSizeSource      string
	ImageSizeBreakdown   map[string]int
	wsReplayInput        []json.RawMessage
	wsReplayInputExists  bool

	// strict-delta shadow 载体（TEMP_DIAG openai_ws_delta_shadow remove_after_debug=true）。
	// 仅 shadow 度量用，承载本轮 raw upstream output 的 canonical 哈希和结构签名，不含原文。
	DeltaShadowOutputHashes   [][32]byte
	DeltaShadowOutputShapes   []string
	DeltaShadowOutputCaptured bool
	DeltaShadowRawClientEquiv bool
}

// ResolveUsageRequestID returns the stable request identifier shared by usage
// logs and AI center traces. Unlike billing-time fallback logic, it never
// generates a synthetic ID when no request-scoped identifier exists.
func ResolveUsageRequestID(ctx context.Context, upstreamRequestID string) string {
	if ctx != nil {
		if clientRequestID, _ := ctx.Value(ctxkey.ClientRequestID).(string); strings.TrimSpace(clientRequestID) != "" {
			return "client:" + strings.TrimSpace(clientRequestID)
		}
		if requestID, _ := ctx.Value(ctxkey.RequestID).(string); strings.TrimSpace(requestID) != "" {
			return "local:" + strings.TrimSpace(requestID)
		}
	}
	return strings.TrimSpace(upstreamRequestID)
}

type OpenAIWSRetryMetricsSnapshot struct {
	RetryAttemptsTotal            int64 `json:"retry_attempts_total"`
	RetryBackoffMsTotal           int64 `json:"retry_backoff_ms_total"`
	RetryExhaustedTotal           int64 `json:"retry_exhausted_total"`
	NonRetryableFastFallbackTotal int64 `json:"non_retryable_fast_fallback_total"`
}

type OpenAICompatibilityFallbackMetricsSnapshot struct {
	SessionHashLegacyReadFallbackTotal int64   `json:"session_hash_legacy_read_fallback_total"`
	SessionHashLegacyReadFallbackHit   int64   `json:"session_hash_legacy_read_fallback_hit"`
	SessionHashLegacyDualWriteTotal    int64   `json:"session_hash_legacy_dual_write_total"`
	SessionHashLegacyReadHitRate       float64 `json:"session_hash_legacy_read_hit_rate"`

	MetadataLegacyFallbackIsMaxTokensOneHaikuTotal int64 `json:"metadata_legacy_fallback_is_max_tokens_one_haiku_total"`
	MetadataLegacyFallbackThinkingEnabledTotal     int64 `json:"metadata_legacy_fallback_thinking_enabled_total"`
	MetadataLegacyFallbackPrefetchedStickyAccount  int64 `json:"metadata_legacy_fallback_prefetched_sticky_account_total"`
	MetadataLegacyFallbackPrefetchedStickyGroup    int64 `json:"metadata_legacy_fallback_prefetched_sticky_group_total"`
	MetadataLegacyFallbackSingleAccountRetryTotal  int64 `json:"metadata_legacy_fallback_single_account_retry_total"`
	MetadataLegacyFallbackAccountSwitchCountTotal  int64 `json:"metadata_legacy_fallback_account_switch_count_total"`
	MetadataLegacyFallbackTotal                    int64 `json:"metadata_legacy_fallback_total"`
}

type openAIWSRetryMetrics struct {
	retryAttempts            atomic.Int64
	retryBackoffMs           atomic.Int64
	retryExhausted           atomic.Int64
	nonRetryableFastFallback atomic.Int64
}

type accountWriteThrottle struct {
	minInterval time.Duration
	mu          sync.Mutex
	lastByID    map[int64]time.Time
}

func newAccountWriteThrottle(minInterval time.Duration) *accountWriteThrottle {
	return &accountWriteThrottle{
		minInterval: minInterval,
		lastByID:    make(map[int64]time.Time),
	}
}

func (t *accountWriteThrottle) Allow(id int64, now time.Time) bool {
	if t == nil || id <= 0 || t.minInterval <= 0 {
		return true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if last, ok := t.lastByID[id]; ok && now.Sub(last) < t.minInterval {
		return false
	}
	t.lastByID[id] = now

	if len(t.lastByID) > 4096 {
		cutoff := now.Add(-4 * t.minInterval)
		for accountID, writtenAt := range t.lastByID {
			if writtenAt.Before(cutoff) {
				delete(t.lastByID, accountID)
			}
		}
	}

	return true
}

var defaultOpenAICodexSnapshotPersistThrottle = newAccountWriteThrottle(openAICodexSnapshotPersistMinInterval)

// ErrNoAvailableCompactAccounts indicates the request needs /responses/compact
// support but no compatible account is available.
var ErrNoAvailableCompactAccounts = errors.New("no available OpenAI accounts support /responses/compact")

// OpenAIGatewayService handles OpenAI API gateway operations
type OpenAIGatewayService struct {
	accountRepo           AccountRepository
	usageLogRepo          UsageLogRepository
	usageBillingRepo      UsageBillingRepository
	userRepo              UserRepository
	userSubRepo           UserSubscriptionRepository
	cache                 GatewayCache
	cfg                   *config.Config
	codexDetector         CodexClientRestrictionDetector
	schedulerSnapshot     *SchedulerSnapshotService
	concurrencyService    *ConcurrencyService
	billingService        *BillingService
	rateLimitService      *RateLimitService
	billingCacheService   *BillingCacheService
	userGroupRateResolver *userGroupRateResolver
	httpUpstream          HTTPUpstream
	deferredService       *DeferredService
	openAITokenProvider   *OpenAITokenProvider
	toolCorrector         *CodexToolCorrector
	openaiWSResolver      OpenAIWSProtocolResolver
	resolver              *ModelPricingResolver
	channelService        *ChannelService
	balanceNotifyService  *BalanceNotifyService
	settingService        *SettingService
	tlsFPProfileService   *TLSFingerprintProfileService
	tlsFPRouterService    *TLSFingerprintRouterService
	userPlatformQuotaRepo UserPlatformQuotaRepository

	openaiWSPoolOnce              sync.Once
	openaiWSStateStoreOnce        sync.Once
	openaiSchedulerOnce           sync.Once
	openaiWSPassthroughDialerOnce sync.Once
	openaiWSPool                  *openAIWSConnPool
	openaiWSStateStore            OpenAIWSStateStore
	openaiScheduler               OpenAIAccountScheduler
	openaiWSPassthroughDialer     openAIWSClientDialer
	openaiWSURLBuilder            func(*Account) (string, error)
	openaiAccountStats            *openAIAccountRuntimeStats

	openaiWSFallbackUntil             sync.Map // key: int64(accountID), value: time.Time
	openaiWSRetryMetrics              openAIWSRetryMetrics
	openaiWSSessionPreemptions        openAIWSSessionPreemptRegistry
	responseHeaderFilter              *responseheaders.CompiledHeaderFilter
	codexSnapshotThrottle             *accountWriteThrottle
	openaiCompatSessionResponses      sync.Map
	openaiAccountRuntimeBlockUntil    sync.Map // key: int64(accountID), value: time.Time
	openaiOAuth429WindowStartUnixNano atomic.Int64
	openaiOAuth429WindowCount         atomic.Int64
}

// NewOpenAIGatewayService creates a new OpenAIGatewayService
func NewOpenAIGatewayService(
	accountRepo AccountRepository,
	usageLogRepo UsageLogRepository,
	usageBillingRepo UsageBillingRepository,
	userRepo UserRepository,
	userSubRepo UserSubscriptionRepository,
	userGroupRateRepo UserGroupRateRepository,
	cache GatewayCache,
	cfg *config.Config,
	schedulerSnapshot *SchedulerSnapshotService,
	concurrencyService *ConcurrencyService,
	billingService *BillingService,
	rateLimitService *RateLimitService,
	billingCacheService *BillingCacheService,
	httpUpstream HTTPUpstream,
	deferredService *DeferredService,
	openAITokenProvider *OpenAITokenProvider,
	resolver *ModelPricingResolver,
	channelService *ChannelService,
	balanceNotifyService *BalanceNotifyService,
	tlsFPProfileService *TLSFingerprintProfileService,
	settingService *SettingService,
	userPlatformQuotaRepo UserPlatformQuotaRepository,
) *OpenAIGatewayService {
	svc := &OpenAIGatewayService{
		accountRepo:         accountRepo,
		usageLogRepo:        usageLogRepo,
		usageBillingRepo:    usageBillingRepo,
		userRepo:            userRepo,
		userSubRepo:         userSubRepo,
		cache:               cache,
		cfg:                 cfg,
		codexDetector:       NewOpenAICodexClientRestrictionDetector(cfg),
		schedulerSnapshot:   schedulerSnapshot,
		concurrencyService:  concurrencyService,
		billingService:      billingService,
		rateLimitService:    rateLimitService,
		billingCacheService: billingCacheService,
		userGroupRateResolver: newUserGroupRateResolver(
			userGroupRateRepo,
			nil,
			resolveUserGroupRateCacheTTL(cfg),
			nil,
			"service.openai_gateway",
		),
		httpUpstream:          httpUpstream,
		deferredService:       deferredService,
		openAITokenProvider:   openAITokenProvider,
		toolCorrector:         NewCodexToolCorrector(),
		openaiWSResolver:      NewOpenAIWSProtocolResolver(cfg),
		resolver:              resolver,
		channelService:        channelService,
		balanceNotifyService:  balanceNotifyService,
		settingService:        settingService,
		tlsFPProfileService:   tlsFPProfileService,
		userPlatformQuotaRepo: userPlatformQuotaRepo,
		responseHeaderFilter:  compileResponseHeaderFilter(cfg),
		codexSnapshotThrottle: newAccountWriteThrottle(openAICodexSnapshotPersistMinInterval),
	}
	if rateLimitService != nil {
		rateLimitService.SetAccountRuntimeBlocker(svc)
	}
	if openAITokenProvider != nil {
		openAITokenProvider.SetAccountRuntimeBlocker(svc)
	}
	svc.registerOpenAIWSPoolReconcileHook()
	svc.logOpenAIWSModeBootstrap()
	return svc
}

// ResolveChannelMapping 解析渠道级模型映射（代理到 ChannelService）
func (s *OpenAIGatewayService) ResolveChannelMapping(ctx context.Context, groupID int64, model string) ChannelMappingResult {
	if s.channelService == nil {
		return ChannelMappingResult{MappedModel: model}
	}
	return s.channelService.ResolveChannelMapping(ctx, groupID, model)
}

// IsModelRestricted 检查模型是否被渠道限制（代理到 ChannelService）
func (s *OpenAIGatewayService) IsModelRestricted(ctx context.Context, groupID int64, model string) bool {
	if s.channelService == nil {
		return false
	}
	return s.channelService.IsModelRestricted(ctx, groupID, model)
}

// ResolveChannelMappingAndRestrict 解析渠道映射。
// 模型限制检查已移至调度阶段，restricted 始终返回 false。
func (s *OpenAIGatewayService) ResolveChannelMappingAndRestrict(ctx context.Context, groupID *int64, model string) (ChannelMappingResult, bool) {
	if s.channelService == nil {
		return ChannelMappingResult{MappedModel: model}, false
	}
	return s.channelService.ResolveChannelMappingAndRestrict(ctx, groupID, model)
}

func (s *OpenAIGatewayService) isCodexImageGenerationBridgeEnabled(ctx context.Context, account *Account, apiKey *APIKey) bool {
	if override := account.CodexImageGenerationBridgeOverride(); override != nil {
		return *override
	}
	if s != nil && s.channelService != nil && apiKey != nil && apiKey.GroupID != nil {
		ch, err := s.channelService.GetChannelForGroup(ctx, *apiKey.GroupID)
		if err != nil {
			slog.Warn("failed to resolve codex image generation bridge channel override", "group_id", *apiKey.GroupID, "error", err)
		} else if override := ch.CodexImageGenerationBridgeOverride(PlatformOpenAI); override != nil {
			return *override
		}
	}
	return s != nil && s.cfg != nil && s.cfg.Gateway.CodexImageGenerationBridgeEnabled
}

func (s *OpenAIGatewayService) checkChannelPricingRestriction(ctx context.Context, groupID *int64, requestedModel string) bool {
	if groupID == nil || s.channelService == nil || requestedModel == "" {
		return false
	}
	mapping := s.channelService.ResolveChannelMapping(ctx, *groupID, requestedModel)
	billingModel := billingModelForRestriction(mapping.BillingModelSource, requestedModel, mapping.MappedModel)
	if billingModel == "" {
		return false
	}
	return s.channelService.IsModelRestricted(ctx, *groupID, billingModel)
}

func (s *OpenAIGatewayService) isUpstreamModelRestrictedByChannel(ctx context.Context, groupID int64, account *Account, requestedModel string, requireCompact bool) bool {
	if s.channelService == nil {
		return false
	}
	upstreamModel := resolveOpenAIAccountUpstreamModelForRequest(ctx, s.settingService, account, requestedModel, requireCompact)
	if upstreamModel == "" {
		return false
	}
	return s.channelService.IsModelRestricted(ctx, groupID, upstreamModel)
}

func (s *OpenAIGatewayService) needsUpstreamChannelRestrictionCheck(ctx context.Context, groupID *int64) bool {
	if groupID == nil || s.channelService == nil {
		return false
	}
	ch, err := s.channelService.GetChannelForGroup(ctx, *groupID)
	if err != nil {
		slog.Warn("failed to check openai channel upstream restriction", "group_id", *groupID, "error", err)
		return false
	}
	if ch == nil || !ch.RestrictModels {
		return false
	}
	return ch.BillingModelSource == BillingModelSourceUpstream
}

// ReplaceModelInBody 替换请求体中的 JSON model 字段（通用 gjson/sjson 实现）。
func (s *OpenAIGatewayService) ReplaceModelInBody(body []byte, newModel string) []byte {
	return ReplaceModelInBody(body, newModel)
}

func (s *OpenAIGatewayService) getCodexSnapshotThrottle() *accountWriteThrottle {
	if s != nil && s.codexSnapshotThrottle != nil {
		return s.codexSnapshotThrottle
	}
	return defaultOpenAICodexSnapshotPersistThrottle
}

func (s *OpenAIGatewayService) billingDeps() *billingDeps {
	return &billingDeps{
		accountRepo:           s.accountRepo,
		userRepo:              s.userRepo,
		userSubRepo:           s.userSubRepo,
		billingCacheService:   s.billingCacheService,
		deferredService:       s.deferredService,
		balanceNotifyService:  s.balanceNotifyService,
		userPlatformQuotaRepo: s.userPlatformQuotaRepo,
		cfg:                   s.cfg,
	}
}

// CloseOpenAIWSPool 关闭 OpenAI WebSocket 连接池的后台 worker 和空闲连接。
// 应在应用优雅关闭时调用。
func (s *OpenAIGatewayService) CloseOpenAIWSPool() {
	if s != nil && s.openaiWSPool != nil {
		s.openaiWSPool.Close()
	}
}

func ResolveOpenAIWSOutboundPayloadHTTPFallbackThresholdBytes(cfg *config.Config) int64 {
	if cfg != nil && cfg.Gateway.OpenAIWS.OutboundPayloadHTTPFallbackThresholdBytes > 0 {
		return cfg.Gateway.OpenAIWS.OutboundPayloadHTTPFallbackThresholdBytes
	}
	return ResolveOpenAIWSClientReadLimitBytes(cfg)
}

func (s *OpenAIGatewayService) shouldPreflightFallbackOpenAIWSPayloadToHTTP(body []byte) (bool, int64) {
	threshold := ResolveOpenAIWSOutboundPayloadHTTPFallbackThresholdBytes(nil)
	if s != nil {
		threshold = ResolveOpenAIWSOutboundPayloadHTTPFallbackThresholdBytes(s.cfg)
	}
	return threshold > 0 && int64(len(body)) > threshold, threshold
}

func (s *OpenAIGatewayService) logOpenAIWSModeBootstrap() {
	if s == nil || s.cfg == nil {
		return
	}
	wsCfg := s.cfg.Gateway.OpenAIWS
	logOpenAIWSModeInfo(
		"bootstrap enabled=%v oauth_enabled=%v apikey_enabled=%v force_http=%v responses_websockets_v2=%v responses_websockets=%v payload_log_sample_rate=%.3f event_flush_batch_size=%d event_flush_interval_ms=%d prewarm_cooldown_ms=%d retry_backoff_initial_ms=%d retry_backoff_max_ms=%d retry_jitter_ratio=%.3f retry_total_budget_ms=%d ws_read_limit_bytes=%d outbound_payload_http_fallback_threshold_bytes=%d",
		wsCfg.Enabled,
		wsCfg.OAuthEnabled,
		wsCfg.APIKeyEnabled,
		wsCfg.ForceHTTP,
		wsCfg.ResponsesWebsocketsV2,
		wsCfg.ResponsesWebsockets,
		wsCfg.PayloadLogSampleRate,
		wsCfg.EventFlushBatchSize,
		wsCfg.EventFlushIntervalMS,
		wsCfg.PrewarmCooldownMS,
		wsCfg.RetryBackoffInitialMS,
		wsCfg.RetryBackoffMaxMS,
		wsCfg.RetryJitterRatio,
		wsCfg.RetryTotalBudgetMS,
		openAIWSMessageReadLimitBytes,
		ResolveOpenAIWSOutboundPayloadHTTPFallbackThresholdBytes(s.cfg),
	)
}

func (s *OpenAIGatewayService) getCodexClientRestrictionDetector() CodexClientRestrictionDetector {
	if s != nil && s.codexDetector != nil {
		return s.codexDetector
	}
	var cfg *config.Config
	if s != nil {
		cfg = s.cfg
	}
	return NewOpenAICodexClientRestrictionDetector(cfg)
}

func (s *OpenAIGatewayService) getOpenAIWSProtocolResolver() OpenAIWSProtocolResolver {
	if s != nil && s.openaiWSResolver != nil {
		return s.openaiWSResolver
	}
	var cfg *config.Config
	if s != nil {
		cfg = s.cfg
	}
	return NewOpenAIWSProtocolResolver(cfg)
}

func classifyOpenAIWSReconnectReason(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(err, &fallbackErr) || fallbackErr == nil {
		return "", false
	}
	reason := strings.TrimSpace(fallbackErr.Reason)
	if reason == "" {
		return "", false
	}

	baseReason := strings.TrimPrefix(reason, "prewarm_")

	if baseReason == "response_failed" {
		return reason, false
	}

	switch baseReason {
	case "policy_violation",
		"message_too_big",
		"upgrade_required",
		"ws_unsupported",
		"auth_failed",
		"model_unavailable",
		"invalid_encrypted_content",
		"previous_response_not_found",
		"unsafe_tool_continuation",
		"session_preempted",
		// 账户级并发上限：同账号盲目重试只会反复撞满，应快速 failover 到其他账号。
		"ws_connection_limit_reached":
		return reason, false
	}

	switch baseReason {
	case "read_event",
		"write_request",
		"write",
		"acquire_timeout",
		"acquire_conn",
		"conn_queue_full",
		"dial_failed",
		"upstream_5xx",
		"event_error",
		"error_event",
		"upstream_error_event",
		// 60min 单连接 TTL 到期：evict 旧连接后同账号重拨一条新连接即可恢复。
		"ws_conn_ttl_evict",
		"missing_final_response":
		return reason, true
	default:
		return reason, false
	}
}

func resolveOpenAIWSFallbackErrorResponse(err error) (statusCode int, errType string, clientMessage string, upstreamMessage string, ok bool) {
	if err == nil {
		return 0, "", "", "", false
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(err, &fallbackErr) || fallbackErr == nil {
		return 0, "", "", "", false
	}

	reason := strings.TrimSpace(fallbackErr.Reason)
	reason = strings.TrimPrefix(reason, "prewarm_")
	if reason == "" {
		return 0, "", "", "", false
	}

	var dialErr *openAIWSDialError
	if fallbackErr.Err != nil && errors.As(fallbackErr.Err, &dialErr) && dialErr != nil {
		if dialErr.StatusCode > 0 {
			statusCode = dialErr.StatusCode
		}
		if dialErr.Err != nil {
			upstreamMessage = sanitizeUpstreamErrorMessage(strings.TrimSpace(dialErr.Err.Error()))
		}
	}

	switch reason {
	case "invalid_encrypted_content":
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		errType = "invalid_request_error"
		if upstreamMessage == "" {
			upstreamMessage = "encrypted content could not be verified"
		}
	case "previous_response_not_found":
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		errType = "invalid_request_error"
		if upstreamMessage == "" {
			upstreamMessage = "previous response not found"
		}
	case "unsafe_tool_continuation":
		if statusCode == 0 {
			statusCode = http.StatusConflict
		}
		errType = "invalid_request_error"
		if upstreamMessage == "" {
			upstreamMessage = "previous response binding unavailable for tool continuation"
		}
	case "call_id", "item_reference", "tool_context", "system_role", "input_schema":
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		errType = "invalid_request_error"
	case "upgrade_required":
		if statusCode == 0 {
			statusCode = http.StatusUpgradeRequired
		}
	case "ws_unsupported":
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
	case "auth_failed":
		if statusCode == 0 {
			statusCode = http.StatusUnauthorized
		}
	case "upstream_rate_limited", "ws_connection_limit_reached":
		if statusCode == 0 {
			statusCode = http.StatusTooManyRequests
		}
	case "response_failed":
		var failedErr *openAIWSResponseFailedError
		if !errors.As(fallbackErr.Err, &failedErr) || failedErr == nil || failedErr.retryable {
			return 0, "", "", "", false
		}
		statusCode = openAIWSErrorHTTPStatusFromRaw(failedErr.code, failedErr.errType)
		errType = "upstream_error"
		upstreamMessage = sanitizeUpstreamErrorMessage(strings.TrimSpace(failedErr.message))
	case "session_preempted":
		if statusCode == 0 {
			statusCode = 499
		}
		errType = "request_canceled"
		if upstreamMessage == "" {
			upstreamMessage = "Superseded by a newer request in the same session"
		}
	default:
		if statusCode == 0 {
			return 0, "", "", "", false
		}
	}

	if upstreamMessage == "" && fallbackErr.Err != nil {
		upstreamMessage = sanitizeUpstreamErrorMessage(strings.TrimSpace(fallbackErr.Err.Error()))
	}
	if upstreamMessage == "" {
		switch reason {
		case "upgrade_required":
			upstreamMessage = "upstream websocket upgrade required"
		case "ws_unsupported":
			upstreamMessage = "upstream websocket not supported"
		case "auth_failed":
			upstreamMessage = "upstream authentication failed"
		case "upstream_rate_limited":
			upstreamMessage = "upstream rate limit exceeded, please retry later"
		default:
			upstreamMessage = "Upstream request failed"
		}
	}

	if errType == "" {
		if statusCode == http.StatusTooManyRequests {
			errType = "rate_limit_error"
		} else {
			errType = "upstream_error"
		}
	}
	clientMessage = upstreamMessage
	return statusCode, errType, clientMessage, upstreamMessage, true
}

func (s *OpenAIGatewayService) newOpenAIWSFailoverError(c *gin.Context, account *Account, wsErr error) *UpstreamFailoverError {
	if c != nil && c.Writer != nil && c.Writer.Written() {
		return nil
	}
	statusCode, errType, clientMessage, upstreamMessage, ok := resolveOpenAIWSFallbackErrorResponse(wsErr)
	if !ok {
		return nil
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(wsErr, &fallbackErr) || fallbackErr == nil {
		return nil
	}
	reason := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(fallbackErr.Reason), "prewarm_"))
	if reason != "upstream_rate_limited" && reason != "ws_connection_limit_reached" {
		return nil
	}
	if strings.TrimSpace(clientMessage) == "" {
		clientMessage = "Upstream request failed"
	}
	if strings.TrimSpace(upstreamMessage) == "" {
		upstreamMessage = clientMessage
	}

	setOpsUpstreamError(c, statusCode, upstreamMessage, "")
	if account != nil {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: statusCode,
			Kind:               "failover",
			Message:            upstreamMessage,
		})
	}
	body, _ := json.Marshal(gin.H{
		"error": gin.H{
			"type":    errType,
			"message": clientMessage,
		},
	})
	return &UpstreamFailoverError{
		StatusCode:   statusCode,
		ResponseBody: body,
	}
}

func shouldFallbackOpenAIWSToHTTP(wsErr error) bool {
	var fallbackErr *openAIWSFallbackError
	if !errors.As(wsErr, &fallbackErr) || fallbackErr == nil {
		return false
	}
	reason, _ := classifyOpenAIWSReconnectReason(wsErr)
	reason = strings.TrimSpace(strings.TrimPrefix(reason, "prewarm_"))
	switch reason {
	case "":
		return false
	case "upstream_rate_limited",
		"call_id",
		"invalid_encrypted_content",
		"model_unavailable",
		"previous_response_not_found",
		"response_failed",
		"session_preempted",
		"unsafe_tool_continuation":
		return false
	default:
		return true
	}
}

func openAIWSActiveDeltaPreviousResponseID(wsErr error) string {
	var fallbackErr *openAIWSFallbackError
	if !errors.As(wsErr, &fallbackErr) || fallbackErr == nil || !fallbackErr.ActiveDelta {
		return ""
	}
	return strings.TrimSpace(fallbackErr.PreviousResponseID)
}

func (s *OpenAIGatewayService) prepareOpenAIWSContinuationFailoverBody(c *gin.Context, account *Account, wsErr error, wsReqBody map[string]any) bool {
	reason, _ := classifyOpenAIWSReconnectReason(wsErr)
	if strings.TrimPrefix(strings.TrimSpace(reason), "prewarm_") != "ws_connection_limit_reached" {
		return false
	}
	if account == nil || account.Type != AccountTypeOAuth || len(wsReqBody) == 0 {
		return false
	}
	previousResponseID := openAIWSPayloadString(wsReqBody, "previous_response_id")
	if previousResponseID == "" {
		return false
	}
	if HasFunctionCallOutput(wsReqBody) {
		logOpenAIWSModeInfo(
			"reconnect_ws_connection_limit_failover_skip account_id=%d reason=tool_continuation previous_response_id=%s",
			account.ID,
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
		)
		return true
	}
	failoverReqBody := make(map[string]any, len(wsReqBody))
	for k, v := range wsReqBody {
		failoverReqBody[k] = v
	}
	delete(failoverReqBody, "previous_response_id")
	failoverReqBody["store"] = false
	trimOpenAIStoreFalseReasoningItems(failoverReqBody)
	failoverBody, marshalErr := marshalOpenAIResponsesRequestBodyOrdered(failoverReqBody)
	if marshalErr != nil {
		logOpenAIWSModeInfo(
			"reconnect_ws_connection_limit_failover_skip account_id=%d reason=serialize previous_response_id=%s cause=%s",
			account.ID,
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
			truncateOpenAIWSLogValue(marshalErr.Error(), openAIWSLogValueMaxLen),
		)
		return true
	}
	setOpenAIFailoverRequestBody(c, failoverBody)
	logOpenAIWSModeInfo(
		"reconnect_ws_connection_limit_failover_body account_id=%d action=drop_previous_response_id_full_create previous_response_id=%s",
		account.ID,
		truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
	)
	return false
}

func (s *OpenAIGatewayService) writeOpenAIWSFallbackErrorResponse(c *gin.Context, account *Account, wsErr error) bool {
	if c == nil || c.Writer == nil || c.Writer.Written() {
		return false
	}
	statusCode, errType, clientMessage, upstreamMessage, ok := resolveOpenAIWSFallbackErrorResponse(wsErr)
	if !ok {
		return false
	}
	if strings.TrimSpace(clientMessage) == "" {
		clientMessage = "Upstream request failed"
	}
	if strings.TrimSpace(upstreamMessage) == "" {
		upstreamMessage = clientMessage
	}

	isSessionPreempted := IsOpenAIWSSessionPreemptedError(wsErr)
	if !isSessionPreempted {
		setOpsUpstreamError(c, statusCode, upstreamMessage, "")
		if account != nil {
			appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
				Platform:           account.Platform,
				AccountID:          account.ID,
				AccountName:        account.Name,
				UpstreamStatusCode: statusCode,
				Kind:               "ws_error",
				Message:            upstreamMessage,
			})
		}
	}
	c.JSON(statusCode, gin.H{
		"error": gin.H{
			"type":    errType,
			"message": clientMessage,
		},
	})
	return true
}

func (s *OpenAIGatewayService) openAIWSRetryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	initial := openAIWSRetryBackoffInitialDefault
	maxBackoff := openAIWSRetryBackoffMaxDefault
	jitterRatio := openAIWSRetryJitterRatioDefault
	if s != nil && s.cfg != nil {
		wsCfg := s.cfg.Gateway.OpenAIWS
		if wsCfg.RetryBackoffInitialMS > 0 {
			initial = time.Duration(wsCfg.RetryBackoffInitialMS) * time.Millisecond
		}
		if wsCfg.RetryBackoffMaxMS > 0 {
			maxBackoff = time.Duration(wsCfg.RetryBackoffMaxMS) * time.Millisecond
		}
		if wsCfg.RetryJitterRatio >= 0 {
			jitterRatio = wsCfg.RetryJitterRatio
		}
	}
	if initial <= 0 {
		return 0
	}
	if maxBackoff <= 0 {
		maxBackoff = initial
	}
	if maxBackoff < initial {
		maxBackoff = initial
	}
	if jitterRatio < 0 {
		jitterRatio = 0
	}
	if jitterRatio > 1 {
		jitterRatio = 1
	}

	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	backoff := initial
	if shift > 0 {
		backoff = initial * time.Duration(1<<shift)
	}
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	if jitterRatio <= 0 {
		return backoff
	}
	jitter := time.Duration(float64(backoff) * jitterRatio)
	if jitter <= 0 {
		return backoff
	}
	delta := time.Duration(rand.Int63n(int64(jitter)*2+1)) - jitter
	withJitter := backoff + delta
	if withJitter < 0 {
		return 0
	}
	return withJitter
}

func (s *OpenAIGatewayService) openAIWSRetryTotalBudget() time.Duration {
	if s != nil && s.cfg != nil {
		ms := s.cfg.Gateway.OpenAIWS.RetryTotalBudgetMS
		if ms <= 0 {
			return 0
		}
		return time.Duration(ms) * time.Millisecond
	}
	return 0
}

func (s *OpenAIGatewayService) recordOpenAIWSRetryAttempt(backoff time.Duration) {
	if s == nil {
		return
	}
	s.openaiWSRetryMetrics.retryAttempts.Add(1)
	if backoff > 0 {
		s.openaiWSRetryMetrics.retryBackoffMs.Add(backoff.Milliseconds())
	}
}

func (s *OpenAIGatewayService) recordOpenAIWSRetryExhausted() {
	if s == nil {
		return
	}
	s.openaiWSRetryMetrics.retryExhausted.Add(1)
}

func (s *OpenAIGatewayService) recordOpenAIWSNonRetryableFastFallback() {
	if s == nil {
		return
	}
	s.openaiWSRetryMetrics.nonRetryableFastFallback.Add(1)
}

func (s *OpenAIGatewayService) SnapshotOpenAIWSRetryMetrics() OpenAIWSRetryMetricsSnapshot {
	if s == nil {
		return OpenAIWSRetryMetricsSnapshot{}
	}
	return OpenAIWSRetryMetricsSnapshot{
		RetryAttemptsTotal:            s.openaiWSRetryMetrics.retryAttempts.Load(),
		RetryBackoffMsTotal:           s.openaiWSRetryMetrics.retryBackoffMs.Load(),
		RetryExhaustedTotal:           s.openaiWSRetryMetrics.retryExhausted.Load(),
		NonRetryableFastFallbackTotal: s.openaiWSRetryMetrics.nonRetryableFastFallback.Load(),
	}
}

func SnapshotOpenAICompatibilityFallbackMetrics() OpenAICompatibilityFallbackMetricsSnapshot {
	legacyReadFallbackTotal, legacyReadFallbackHit, legacyDualWriteTotal := openAIStickyCompatStats()
	isMaxTokensOneHaiku, thinkingEnabled, prefetchedStickyAccount, prefetchedStickyGroup, singleAccountRetry, accountSwitchCount := RequestMetadataFallbackStats()

	readHitRate := float64(0)
	if legacyReadFallbackTotal > 0 {
		readHitRate = float64(legacyReadFallbackHit) / float64(legacyReadFallbackTotal)
	}
	metadataFallbackTotal := isMaxTokensOneHaiku + thinkingEnabled + prefetchedStickyAccount + prefetchedStickyGroup + singleAccountRetry + accountSwitchCount

	return OpenAICompatibilityFallbackMetricsSnapshot{
		SessionHashLegacyReadFallbackTotal: legacyReadFallbackTotal,
		SessionHashLegacyReadFallbackHit:   legacyReadFallbackHit,
		SessionHashLegacyDualWriteTotal:    legacyDualWriteTotal,
		SessionHashLegacyReadHitRate:       readHitRate,

		MetadataLegacyFallbackIsMaxTokensOneHaikuTotal: isMaxTokensOneHaiku,
		MetadataLegacyFallbackThinkingEnabledTotal:     thinkingEnabled,
		MetadataLegacyFallbackPrefetchedStickyAccount:  prefetchedStickyAccount,
		MetadataLegacyFallbackPrefetchedStickyGroup:    prefetchedStickyGroup,
		MetadataLegacyFallbackSingleAccountRetryTotal:  singleAccountRetry,
		MetadataLegacyFallbackAccountSwitchCountTotal:  accountSwitchCount,
		MetadataLegacyFallbackTotal:                    metadataFallbackTotal,
	}
}

func (s *OpenAIGatewayService) detectCodexClientRestriction(c *gin.Context, account *Account) CodexClientRestrictionDetectionResult {
	var globalAllowedClients []string
	if account != nil && account.IsCodexCLIOnlyEnabled() && s != nil && s.settingService != nil {
		ctx := context.Background()
		if c != nil && c.Request != nil {
			ctx = c.Request.Context()
		}
		if s.settingService.IsOpenAIAllowClaudeCodeCodexPluginEnabled(ctx) {
			globalAllowedClients = []string{openai.AllowedClientClaudeCode}
		}
	}
	return s.getCodexClientRestrictionDetector().Detect(c, account, globalAllowedClients)
}

func getAPIKeyIDFromContext(c *gin.Context) int64 {
	if c == nil {
		return 0
	}
	v, exists := c.Get("api_key")
	if !exists {
		return 0
	}
	apiKey, ok := v.(*APIKey)
	if !ok || apiKey == nil {
		return 0
	}
	return apiKey.ID
}

func isOpenAICodexOfficialClientRequest(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	return openai.IsCodexOfficialClientByHeaders(c.GetHeader("User-Agent"), c.GetHeader("originator"))
}

func isOpenAICodexOfficialOrForcedClientRequest(c *gin.Context, cfg *config.Config) bool {
	return isOpenAICodexOfficialClientRequest(c) || (cfg != nil && cfg.Gateway.ForceCodexCLI)
}

func shouldForwardOpenAIRequestHeader(account *Account, lowerKey string) bool {
	if !openaiAllowedHeaders[lowerKey] {
		return false
	}
	if account != nil && account.Type != AccountTypeOAuth && openaiOAuthOnlyHeaders[lowerKey] {
		return false
	}
	return true
}

func shouldForwardOpenAIPassthroughHeader(account *Account, lowerKey string) bool {
	if !openaiPassthroughAllowedHeaders[lowerKey] {
		return false
	}
	if account != nil && account.Type != AccountTypeOAuth && openaiOAuthOnlyHeaders[lowerKey] {
		return false
	}
	return true
}

// isolateOpenAISessionID 将 apiKeyID 混入 session 标识符，
// 确保不同 API Key 的用户即使使用相同的原始 session_id/conversation_id，
// 到达上游的标识符也不同，防止跨用户会话碰撞。
func isolateOpenAISessionID(apiKeyID int64, raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	h := xxhash.New()
	_, _ = fmt.Fprintf(h, "k%d:", apiKeyID)
	_, _ = h.WriteString(raw)
	return fmt.Sprintf("%016x", h.Sum64())
}

func logCodexCLIOnlyDetection(ctx context.Context, c *gin.Context, account *Account, apiKeyID int64, result CodexClientRestrictionDetectionResult, body []byte) {
	if !result.Enabled {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
	}
	fields := []zap.Field{
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", accountID),
		zap.Bool("codex_cli_only_enabled", result.Enabled),
		zap.Bool("codex_official_client_match", result.Matched),
		zap.String("reject_reason", result.Reason),
	}
	if apiKeyID > 0 {
		fields = append(fields, zap.Int64("api_key_id", apiKeyID))
	}
	if !result.Matched {
		fields = appendCodexCLIOnlyRejectedRequestFields(fields, c, body)
	}
	log := logger.FromContext(ctx).With(fields...)
	if result.Matched {
		log.Info("OpenAI codex_cli_only 放行请求")
		return
	}
	log.Warn("OpenAI codex_cli_only 拒绝非官方客户端请求")
}

func appendCodexCLIOnlyRejectedRequestFields(fields []zap.Field, c *gin.Context, body []byte) []zap.Field {
	if c == nil || c.Request == nil {
		return fields
	}

	req := c.Request
	requestModel, requestStream, promptCacheKey := extractOpenAIRequestMetaFromBody(body)
	fields = append(fields,
		zap.String("request_method", strings.TrimSpace(req.Method)),
		zap.String("request_path", strings.TrimSpace(req.URL.Path)),
		zap.String("request_query", strings.TrimSpace(req.URL.RawQuery)),
		zap.String("request_host", strings.TrimSpace(req.Host)),
		zap.String("request_client_ip", strings.TrimSpace(ip.GetClientIP(c))),
		zap.String("request_remote_addr", strings.TrimSpace(req.RemoteAddr)),
		zap.String("request_user_agent", strings.TrimSpace(req.Header.Get("User-Agent"))),
		zap.String("request_content_type", strings.TrimSpace(req.Header.Get("Content-Type"))),
		zap.Int64("request_content_length", req.ContentLength),
		zap.Bool("request_stream", requestStream),
	)
	if requestModel != "" {
		fields = append(fields, zap.String("request_model", requestModel))
	}
	if promptCacheKey != "" {
		fields = append(fields, zap.String("request_prompt_cache_key_sha256", hashSensitiveValueForLog(promptCacheKey)))
	}

	if headers := snapshotCodexCLIOnlyHeaders(req.Header); len(headers) > 0 {
		fields = append(fields, zap.Any("request_headers", headers))
	}
	fields = append(fields, zap.Int("request_body_size", len(body)))
	return fields
}

func snapshotCodexCLIOnlyHeaders(header http.Header) map[string]string {
	if len(header) == 0 {
		return nil
	}
	result := make(map[string]string, len(codexCLIOnlyDebugHeaderWhitelist))
	for _, key := range codexCLIOnlyDebugHeaderWhitelist {
		value := strings.TrimSpace(header.Get(key))
		if value == "" {
			continue
		}
		result[strings.ToLower(key)] = truncateString(value, codexCLIOnlyHeaderValueMaxBytes)
	}
	return result
}

func hashSensitiveValueForLog(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func hashBytesForLog(raw []byte) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	sum := sha256.Sum256(trimmed)
	return hex.EncodeToString(sum[:8])
}

func hashBytesPrefixForLog(raw []byte, prefixBytes int) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if prefixBytes > 0 && len(trimmed) > prefixBytes {
		trimmed = trimmed[:prefixBytes]
	}
	sum := sha256.Sum256(trimmed)
	return hex.EncodeToString(sum[:8])
}

func openAIBodyFieldRaw(body []byte, field string) []byte {
	if len(body) == 0 || strings.TrimSpace(field) == "" {
		return nil
	}
	value := gjson.GetBytes(body, field)
	if !value.Exists() {
		return nil
	}
	return []byte(value.Raw)
}

func hashOpenAIBodyFieldForLog(body []byte, field string) string {
	return hashBytesForLog(openAIBodyFieldRaw(body, field))
}

func hashOpenAIBodyFieldPrefixForLog(body []byte, field string, prefixBytes int) string {
	return hashBytesPrefixForLog(openAIBodyFieldRaw(body, field), prefixBytes)
}

func countOpenAIInputItems(body []byte) int {
	value := gjson.GetBytes(body, "input")
	if !value.Exists() {
		return 0
	}
	if value.IsArray() {
		return len(value.Array())
	}
	return 1
}

func describeOpenAIUpstreamSessionSource(c *gin.Context, promptCacheKey string, compactPath bool, body []byte) (source string, sessionID string) {
	if c != nil {
		if sessionID = strings.TrimSpace(c.GetHeader("session_id")); sessionID != "" {
			return "session_id", sessionID
		}
		if sessionID = strings.TrimSpace(c.GetHeader("conversation_id")); sessionID != "" {
			return "conversation_id", sessionID
		}
	}
	if sessionID = strings.TrimSpace(promptCacheKey); sessionID != "" {
		return "prompt_cache_key", sessionID
	}
	if compactPath {
		if c != nil {
			if seed, ok := c.Get(openAICompactSessionSeedKey); ok {
				if seedStr, ok := seed.(string); ok && strings.TrimSpace(seedStr) != "" {
					return "compact_seed", strings.TrimSpace(seedStr)
				}
			}
		}
		if sessionID = strings.TrimSpace(deriveOpenAIContentSessionSeed(body)); sessionID != "" {
			return "compact_content_seed", sessionID
		}
	}
	if compactPath {
		return "compact_generated", ""
	}
	return "", ""
}

func generateOpenAISessionHashForLog(c *gin.Context, body []byte) string {
	if c == nil {
		return ""
	}
	sessionID := strings.TrimSpace(c.GetHeader("session_id"))
	if sessionID == "" {
		sessionID = strings.TrimSpace(c.GetHeader("conversation_id"))
	}
	if sessionID == "" && len(body) > 0 {
		sessionID = strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String())
	}
	if sessionID == "" {
		return ""
	}
	currentHash, _ := deriveOpenAIRequestScopedSessionHashes(c, sessionID)
	return currentHash
}

func boolToOptionalAny(ok bool, value any) any {
	if !ok {
		return nil
	}
	return value
}

func emitOpenAICacheProbeEvent(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	originalBody []byte,
	finalBody []byte,
	result *OpenAIForwardResult,
	promptCacheKeyForUpstream string,
	passthrough bool,
) {
	if result == nil || account == nil {
		return
	}

	inboundModel, inboundStream, inboundPromptCacheKey := extractOpenAIRequestMetaFromBody(originalBody)
	upstreamModel, upstreamStream, upstreamPromptCacheKey := extractOpenAIRequestMetaFromBody(finalBody)
	inboundPrevResponseID := strings.TrimSpace(gjson.GetBytes(originalBody, "previous_response_id").String())
	upstreamPrevResponseID := strings.TrimSpace(gjson.GetBytes(finalBody, "previous_response_id").String())

	if inboundPromptCacheKey == "" &&
		strings.TrimSpace(promptCacheKeyForUpstream) == "" &&
		upstreamPromptCacheKey == "" &&
		result.Usage.CacheReadInputTokens == 0 &&
		result.Usage.CacheCreationInputTokens == 0 {
		return
	}

	compactPath := isOpenAIResponsesCompactPath(c)
	upstreamSessionSource, upstreamSessionID := describeOpenAIUpstreamSessionSource(c, promptCacheKeyForUpstream, compactPath, finalBody)
	stickySessionHash := ""
	if c != nil {
		stickySessionHash = shortSessionHash(generateOpenAISessionHashForLog(c, originalBody))
	}
	requestID := resolveUsageBillingRequestID(ctx, result.RequestID)
	billableInputTokens := result.Usage.InputTokens - result.Usage.CacheReadInputTokens
	if billableInputTokens < 0 {
		billableInputTokens = 0
	}
	requestStore := gjson.GetBytes(originalBody, "store")
	upstreamStore := gjson.GetBytes(finalBody, "store")
	requestUserAgent := ""
	requestPath := ""
	if c != nil {
		requestUserAgent = strings.TrimSpace(c.GetHeader("User-Agent"))
		if c.Request != nil && c.Request.URL != nil {
			requestPath = strings.TrimSpace(c.Request.URL.Path)
		}
	}

	fields := map[string]any{
		"request_id":                            requestID,
		"upstream_request_id":                   strings.TrimSpace(result.RequestID),
		"component":                             "audit.openai_cache_probe",
		"account_id":                            account.ID,
		"api_key_id":                            getAPIKeyIDFromContext(c),
		"platform":                              strings.TrimSpace(string(account.Platform)),
		"model":                                 strings.TrimSpace(result.Model),
		"requested_model":                       strings.TrimSpace(inboundModel),
		"upstream_model":                        firstNonEmptyString(result.UpstreamModel, upstreamModel),
		"account_type":                          strings.TrimSpace(string(account.Type)),
		"request_user_agent":                    requestUserAgent,
		"inbound_endpoint":                      requestPath,
		"openai_passthrough":                    passthrough,
		"openai_compact_path":                   compactPath,
		"codex_official_client":                 isOpenAICodexOfficialClientRequest(c),
		"sticky_session_hash":                   stickySessionHash,
		"upstream_session_source":               upstreamSessionSource,
		"upstream_session_id_sha256":            hashSensitiveValueForLog(upstreamSessionID),
		"request_prompt_cache_key_sha256":       hashSensitiveValueForLog(inboundPromptCacheKey),
		"routing_prompt_cache_key_sha256":       hashSensitiveValueForLog(promptCacheKeyForUpstream),
		"upstream_prompt_cache_key_sha256":      hashSensitiveValueForLog(upstreamPromptCacheKey),
		"prompt_cache_key_dropped":              inboundPromptCacheKey != "" && upstreamPromptCacheKey == "",
		"previous_response_id_present":          inboundPrevResponseID != "",
		"upstream_previous_response_id_present": upstreamPrevResponseID != "",
		"previous_response_id_dropped":          inboundPrevResponseID != "" && upstreamPrevResponseID == "",
		"request_body_bytes":                    len(originalBody),
		"upstream_body_bytes":                   len(finalBody),
		"request_body_sha256":                   hashBytesForLog(originalBody),
		"upstream_body_sha256":                  hashBytesForLog(finalBody),
		"request_body_prefix_4k_sha256":         hashBytesPrefixForLog(originalBody, openAICacheProbePrefix4KBytes),
		"request_body_prefix_16k_sha256":        hashBytesPrefixForLog(originalBody, openAICacheProbePrefix16KBytes),
		"upstream_body_prefix_4k_sha256":        hashBytesPrefixForLog(finalBody, openAICacheProbePrefix4KBytes),
		"upstream_body_prefix_16k_sha256":       hashBytesPrefixForLog(finalBody, openAICacheProbePrefix16KBytes),
		"request_input_sha256":                  hashOpenAIBodyFieldForLog(originalBody, "input"),
		"upstream_input_sha256":                 hashOpenAIBodyFieldForLog(finalBody, "input"),
		"request_input_prefix_4k_sha256":        hashOpenAIBodyFieldPrefixForLog(originalBody, "input", openAICacheProbePrefix4KBytes),
		"request_input_prefix_16k_sha256":       hashOpenAIBodyFieldPrefixForLog(originalBody, "input", openAICacheProbePrefix16KBytes),
		"upstream_input_prefix_4k_sha256":       hashOpenAIBodyFieldPrefixForLog(finalBody, "input", openAICacheProbePrefix4KBytes),
		"upstream_input_prefix_16k_sha256":      hashOpenAIBodyFieldPrefixForLog(finalBody, "input", openAICacheProbePrefix16KBytes),
		"request_input_items":                   countOpenAIInputItems(originalBody),
		"upstream_input_items":                  countOpenAIInputItems(finalBody),
		"body_modified":                         !bytes.Equal(bytes.TrimSpace(originalBody), bytes.TrimSpace(finalBody)),
		"input_modified":                        hashOpenAIBodyFieldForLog(originalBody, "input") != hashOpenAIBodyFieldForLog(finalBody, "input"),
		"request_stream":                        inboundStream,
		"upstream_stream":                       upstreamStream,
		"stream_changed":                        inboundStream != upstreamStream,
		"request_store_present":                 requestStore.Exists(),
		"upstream_store_present":                upstreamStore.Exists(),
		"store_changed":                         requestStore.Raw != upstreamStore.Raw,
		"request_store_value":                   boolToOptionalAny(requestStore.Exists(), requestStore.Bool()),
		"upstream_store_value":                  boolToOptionalAny(upstreamStore.Exists(), upstreamStore.Bool()),
		"usage_input_tokens":                    result.Usage.InputTokens,
		"usage_billable_input_tokens":           billableInputTokens,
		"usage_cache_read_tokens":               result.Usage.CacheReadInputTokens,
		"usage_cache_creation_tokens":           result.Usage.CacheCreationInputTokens,
		"usage_output_tokens":                   result.Usage.OutputTokens,
		"usage_image_output_tokens":             result.Usage.ImageOutputTokens,
		"service_tier":                          strings.TrimSpace(firstNonEmptyString(result.ServiceTier)),
		"reasoning_effort":                      strings.TrimSpace(firstNonEmptyString(result.ReasoningEffort)),
	}
	if c != nil {
		if value, ok := c.Get(openAICodexTransformObsKey); ok {
			if obs, ok := value.(codexTransformObservability); ok {
				fields["codex_transform_needs_tool_continuation"] = obs.NeedsToolContinuation
				fields["codex_transform_model_normalized"] = obs.ModelNormalized
				fields["codex_transform_compact_store_removed"] = obs.CompactStoreRemoved
				fields["codex_transform_compact_stream_removed"] = obs.CompactStreamRemoved
				fields["codex_transform_compact_deferred_tool_search"] = obs.CompactDeferredToolSearch
				fields["codex_transform_store_forced_false"] = obs.StoreForcedFalse
				fields["codex_transform_stream_forced_true"] = obs.StreamForcedTrue
				fields["codex_transform_unsupported_fields_stripped_count"] = len(obs.UnsupportedFieldsStripped)
				fields["codex_transform_unsupported_fields_stripped"] = strings.Join(obs.UnsupportedFieldsStripped, ",")
				fields["codex_transform_functions_converted"] = obs.FunctionsConverted
				fields["codex_transform_function_call_converted"] = obs.FunctionCallConverted
				fields["codex_transform_tools_normalized"] = obs.ToolsNormalized
				fields["codex_transform_tool_choice_normalized"] = obs.ToolChoiceNormalized
				fields["codex_transform_system_messages_extracted"] = obs.SystemMessagesExtracted
				fields["codex_transform_default_instructions_applied"] = obs.DefaultInstructionsApplied
				fields["codex_transform_spark_instructions_applied"] = obs.SparkInstructionsApplied
				fields["codex_transform_input_tool_role_normalized"] = obs.InputToolRoleNormalized
				fields["codex_transform_input_message_content_normalized"] = obs.InputMessageContentNormalized
				fields["codex_transform_input_filtered"] = obs.InputFiltered
				fields["codex_transform_input_string_wrapped"] = obs.InputStringWrapped
				fields["codex_transform_input_items_before"] = obs.InputItemsBefore
				fields["codex_transform_input_items_after"] = obs.InputItemsAfter
			}
		}
		if fallbackValue, ok := c.Get(openAICodexCompatFallbackKey); ok {
			if fallbackTriggered, ok := fallbackValue.(bool); ok && fallbackTriggered {
				fields["codex_compat_fallback_triggered"] = true
			}
		}
		if fallbackReason, ok := c.Get(openAICodexCompatFallbackReasonKey); ok {
			if reason := strings.TrimSpace(firstNonEmptyString(fallbackReason)); reason != "" {
				fields["codex_compat_fallback_reason"] = reason
			}
		}
	}

	logger.WriteSinkEvent("info", "audit.openai_cache_probe", "OpenAI cache probe", fields)
}

func clearOpenAICodexCompatContext(c *gin.Context) {
	if c == nil {
		return
	}
	c.Set(openAICodexTransformObsKey, nil)
	c.Set(openAICodexCompatFallbackKey, nil)
	c.Set(openAICodexCompatFallbackReasonKey, nil)
}

func emitOpenAICodexCompatFallbackEvent(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	originalBody []byte,
	finalBody []byte,
	state openAICodexCompatFallbackState,
	outcome string,
	upstreamStatusCode int,
	upstreamCode string,
	upstreamMsg string,
	result *OpenAIForwardResult,
) {
	if account == nil || !state.Triggered {
		return
	}

	requestID := ""
	upstreamRequestID := ""
	if result != nil {
		requestID = resolveUsageBillingRequestID(ctx, result.RequestID)
		upstreamRequestID = strings.TrimSpace(result.RequestID)
	}
	if requestID == "" {
		requestID = resolveUsageBillingRequestID(ctx, "")
	}

	requestPath := ""
	requestUserAgent := ""
	if c != nil {
		requestUserAgent = strings.TrimSpace(c.GetHeader("User-Agent"))
		if c.Request != nil && c.Request.URL != nil {
			requestPath = strings.TrimSpace(c.Request.URL.Path)
		}
	}

	fields := map[string]any{
		"request_id":                        requestID,
		"upstream_request_id":               upstreamRequestID,
		"component":                         "audit.openai_codex_compat_fallback",
		"account_id":                        account.ID,
		"api_key_id":                        getAPIKeyIDFromContext(c),
		"platform":                          strings.TrimSpace(string(account.Platform)),
		"account_type":                      strings.TrimSpace(string(account.Type)),
		"request_user_agent":                requestUserAgent,
		"inbound_endpoint":                  requestPath,
		"fallback_reason":                   strings.TrimSpace(state.Reason),
		"fallback_body_modified":            state.BodyModified,
		"fallback_outcome":                  strings.TrimSpace(outcome),
		"upstream_status_code":              upstreamStatusCode,
		"upstream_error_code":               strings.TrimSpace(upstreamCode),
		"upstream_error_message":            sanitizeUpstreamErrorMessage(strings.TrimSpace(upstreamMsg)),
		"request_body_sha256":               hashBytesForLog(originalBody),
		"final_upstream_body_sha256":        hashBytesForLog(finalBody),
		"request_input_prefix_16k_sha256":   hashOpenAIBodyFieldPrefixForLog(originalBody, "input", openAICacheProbePrefix16KBytes),
		"final_input_prefix_16k_sha256":     hashOpenAIBodyFieldPrefixForLog(finalBody, "input", openAICacheProbePrefix16KBytes),
		"request_prompt_cache_key_sha256":   hashSensitiveValueForLog(gjson.GetBytes(originalBody, "prompt_cache_key").String()),
		"final_prompt_cache_key_sha256":     hashSensitiveValueForLog(gjson.GetBytes(finalBody, "prompt_cache_key").String()),
		"request_previous_response_present": strings.TrimSpace(gjson.GetBytes(originalBody, "previous_response_id").String()) != "",
		"final_previous_response_present":   strings.TrimSpace(gjson.GetBytes(finalBody, "previous_response_id").String()) != "",
	}
	if result != nil {
		fields["model"] = strings.TrimSpace(result.Model)
		fields["upstream_model"] = strings.TrimSpace(firstNonEmptyString(result.UpstreamModel, gjson.GetBytes(finalBody, "model").String()))
		fields["usage_input_tokens"] = result.Usage.InputTokens
		fields["usage_cache_read_tokens"] = result.Usage.CacheReadInputTokens
		fields["usage_output_tokens"] = result.Usage.OutputTokens
	} else {
		fields["model"] = strings.TrimSpace(gjson.GetBytes(originalBody, "model").String())
		fields["upstream_model"] = strings.TrimSpace(gjson.GetBytes(finalBody, "model").String())
	}

	logger.WriteSinkEvent("info", "audit.openai_codex_compat_fallback", "OpenAI Codex compat fallback", fields)
}

func classifyOpenAICodexCompatFallback(statusCode int, upstreamCode, upstreamMsg string, upstreamBody []byte) string {
	if statusCode != http.StatusBadRequest {
		return ""
	}
	if isOpenAIInvalidEncryptedContentError(upstreamCode, upstreamMsg, upstreamBody) {
		return ""
	}

	msg := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody)))
	}
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(string(upstreamBody)))
	}
	if msg == "" {
		return ""
	}
	return classifyOpenAICodexCompatFallbackMessage(msg)
}

func classifyOpenAICodexCompatFallbackMessage(msg string) string {
	hasSchemaSignal := strings.Contains(msg, "invalid schema") ||
		strings.Contains(msg, "invalid field") ||
		strings.Contains(msg, "unknown field") ||
		strings.Contains(msg, "unknown parameter") ||
		strings.Contains(msg, "unsupported field") ||
		strings.Contains(msg, "unsupported parameter") ||
		strings.Contains(msg, "must be set")

	switch {
	case strings.Contains(msg, "item_reference"):
		if hasSchemaSignal || strings.Contains(msg, "missing") {
			return "item_reference"
		}
	case strings.Contains(msg, "tool_call_id"), strings.Contains(msg, "call_id"), strings.Contains(msg, "function_call_output"):
		if hasSchemaSignal || strings.Contains(msg, "missing") || strings.Contains(msg, "invalid") {
			return "call_id"
		}
		if strings.Contains(msg, "no tool call found") &&
			(strings.Contains(msg, "function call output") || strings.Contains(msg, "function_call_output")) {
			return "call_id"
		}
	case strings.Contains(msg, "tool context"):
		return "tool_context"
	case strings.Contains(msg, "role") && strings.Contains(msg, "system"):
		if hasSchemaSignal || strings.Contains(msg, "unsupported") || strings.Contains(msg, "invalid") {
			return "system_role"
		}
	case strings.Contains(msg, "input") && hasSchemaSignal:
		return "input_schema"
	}

	return ""
}

func isOpenAIInvalidEncryptedContentError(upstreamCode, upstreamMsg string, upstreamBody []byte) bool {
	code := strings.ToLower(strings.TrimSpace(upstreamCode))
	if code == "invalid_encrypted_content" {
		return true
	}

	msg := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody)))
	}
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(string(upstreamBody)))
	}

	hasEncryptedContentSignal := strings.Contains(msg, "encrypted content") &&
		(strings.Contains(msg, "could not be decrypted") ||
			strings.Contains(msg, "could not be verified") ||
			strings.Contains(msg, "decrypted or parsed"))
	return code == "thinking_signature_invalid" && hasEncryptedContentSignal
}

// isOpenAIUnsupportedPreviousResponseIDError reports whether the upstream 400
// response is the explicit "Unsupported parameter: previous_response_id" error
// thrown by models that do not allow continuation via previous_response_id.
// In that case the gateway can safely drop previous_response_id and retry,
// because the request will then go through as an independent turn.
func isOpenAIUnsupportedPreviousResponseIDError(upstreamCode, upstreamMsg string) bool {
	msg := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if msg == "" {
		return false
	}
	if !strings.Contains(msg, "previous_response_id") {
		return false
	}
	if strings.Contains(msg, "unsupported parameter") ||
		strings.Contains(msg, "unsupported field") ||
		strings.Contains(msg, "unknown parameter") ||
		strings.Contains(msg, "not supported") {
		return true
	}
	code := strings.ToLower(strings.TrimSpace(upstreamCode))
	return strings.Contains(code, "unsupported_parameter") || strings.Contains(code, "unknown_parameter")
}

func isOpenAIUnsupportedReasoningEnabledError(upstreamCode, upstreamMsg string, upstreamBody []byte) bool {
	msg := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody)))
	}
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(string(upstreamBody)))
	}
	if !strings.Contains(msg, "reasoning.enabled") {
		return false
	}
	if strings.Contains(msg, "unsupported parameter") ||
		strings.Contains(msg, "unsupported field") ||
		strings.Contains(msg, "unknown parameter") ||
		strings.Contains(msg, "not supported") {
		return true
	}
	code := strings.ToLower(strings.TrimSpace(upstreamCode))
	return strings.Contains(code, "unsupported_parameter") || strings.Contains(code, "unknown_parameter")
}

func dropOpenAIReasoningEnabled(reqBody map[string]any) bool {
	if reqBody == nil {
		return false
	}
	reasoning, ok := reqBody["reasoning"].(map[string]any)
	if !ok {
		return false
	}
	if _, ok := reasoning["enabled"]; !ok {
		return false
	}
	delete(reasoning, "enabled")
	if len(reasoning) == 0 {
		delete(reqBody, "reasoning")
	}
	return true
}

func isOpenAILargeRequestUpstreamError(statusCode int, responseBody []byte) bool {
	if statusCode == http.StatusRequestEntityTooLarge {
		return true
	}
	msg := strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(responseBody)))
	body := strings.ToLower(strings.TrimSpace(string(responseBody)))
	combined := msg + " " + body
	return strings.Contains(combined, "message too big") ||
		strings.Contains(combined, "websocket: close 1009") ||
		strings.Contains(combined, "request entity too large") ||
		strings.Contains(combined, "payload too large")
}

func isOpenAICodexCompatFallbackReason(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "call_id", "item_reference", "tool_context", "system_role", "input_schema":
		return true
	default:
		return false
	}
}

func remarshalOpenAIOAuthCompatFallbackBody(
	reqBody map[string]any,
	promptCacheKey string,
	fallbackReason string,
) ([]byte, codexTransformResult, string, error) {
	codexResult := applyCodexOAuthTransformWithInputModeAndFallbackReason(
		reqBody,
		false,
		false,
		codexTransformInputModeStrict,
		fallbackReason,
	)
	if strings.TrimSpace(fallbackReason) == "call_id" && !HasFunctionCallOutput(reqBody) {
		if _, present := reqBody["previous_response_id"]; present {
			delete(reqBody, "previous_response_id")
			codexResult.Modified = true
		}
	}
	trimmedPromptCacheKey := strings.TrimSpace(promptCacheKey)
	if codexResult.PromptCacheKey != "" {
		trimmedPromptCacheKey = codexResult.PromptCacheKey
	}
	body, err := marshalOpenAIResponsesRequestBodyOrdered(reqBody)
	if err != nil {
		return nil, codexResult, trimmedPromptCacheKey, err
	}
	return body, codexResult, trimmedPromptCacheKey, nil
}

func applyOpenAIWSCodexCompatFallback(
	reqBody map[string]any,
	c *gin.Context,
	isCodexCLI bool,
	isCompact bool,
	fallbackReason string,
) codexTransformResult {
	codexResult := applyCodexOAuthTransformWithInputModeAndFallbackReason(
		reqBody,
		isCodexCLI,
		isCompact,
		codexTransformInputModeStrict,
		fallbackReason,
	)
	if strings.TrimSpace(fallbackReason) == "call_id" && !HasFunctionCallOutput(reqBody) {
		if _, present := reqBody["previous_response_id"]; present {
			delete(reqBody, "previous_response_id")
			codexResult.Modified = true
		}
	}
	if c != nil {
		c.Set(openAICodexTransformObsKey, codexResult.Observability)
		if codexResult.Modified {
			c.Set(openAICodexCompatFallbackKey, true)
			c.Set(openAICodexCompatFallbackReasonKey, fallbackReason)
		}
	}
	return codexResult
}

func logOpenAIInstructionsRequiredDebug(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	upstreamStatusCode int,
	upstreamMsg string,
	requestBody []byte,
	upstreamBody []byte,
) {
	msg := strings.TrimSpace(upstreamMsg)
	if !isOpenAIInstructionsRequiredError(upstreamStatusCode, msg, upstreamBody) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	accountID := int64(0)
	accountName := ""
	if account != nil {
		accountID = account.ID
		accountName = strings.TrimSpace(account.Name)
	}

	userAgent := ""
	originator := ""
	if c != nil {
		userAgent = strings.TrimSpace(c.GetHeader("User-Agent"))
		originator = strings.TrimSpace(c.GetHeader("originator"))
	}

	fields := []zap.Field{
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", accountID),
		zap.String("account_name", accountName),
		zap.Int("upstream_status_code", upstreamStatusCode),
		zap.String("upstream_error_message", msg),
		zap.String("request_user_agent", userAgent),
		zap.Bool("codex_official_client_match", openai.IsCodexOfficialClientByHeaders(userAgent, originator)),
	}
	fields = appendCodexCLIOnlyRejectedRequestFields(fields, c, requestBody)

	logger.FromContext(ctx).With(fields...).Warn("OpenAI 上游返回 Instructions are required，已记录请求详情用于排查")
}

func isOpenAIInstructionsRequiredError(upstreamStatusCode int, upstreamMsg string, upstreamBody []byte) bool {
	if upstreamStatusCode != http.StatusBadRequest {
		return false
	}

	hasInstructionRequired := func(text string) bool {
		lower := strings.ToLower(strings.TrimSpace(text))
		if lower == "" {
			return false
		}
		if strings.Contains(lower, "instructions are required") {
			return true
		}
		if strings.Contains(lower, "required parameter: 'instructions'") {
			return true
		}
		if strings.Contains(lower, "required parameter: instructions") {
			return true
		}
		if strings.Contains(lower, "missing required parameter") && strings.Contains(lower, "instructions") {
			return true
		}
		return strings.Contains(lower, "instruction") && strings.Contains(lower, "required")
	}

	if hasInstructionRequired(upstreamMsg) {
		return true
	}
	if len(upstreamBody) == 0 {
		return false
	}

	errMsg := gjson.GetBytes(upstreamBody, "error.message").String()
	errMsgLower := strings.ToLower(strings.TrimSpace(errMsg))
	errCode := strings.ToLower(strings.TrimSpace(gjson.GetBytes(upstreamBody, "error.code").String()))
	errParam := strings.ToLower(strings.TrimSpace(gjson.GetBytes(upstreamBody, "error.param").String()))
	errType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(upstreamBody, "error.type").String()))

	if errParam == "instructions" {
		return true
	}
	if hasInstructionRequired(errMsg) {
		return true
	}
	if strings.Contains(errCode, "missing_required_parameter") && strings.Contains(errMsgLower, "instructions") {
		return true
	}
	if strings.Contains(errType, "invalid_request") && strings.Contains(errMsgLower, "instructions") && strings.Contains(errMsgLower, "required") {
		return true
	}

	return false
}

func isOpenAITransientProcessingError(upstreamStatusCode int, upstreamMsg string, upstreamBody []byte) bool {
	if upstreamStatusCode != http.StatusBadRequest {
		return false
	}

	match := func(text string) bool {
		lower := strings.ToLower(strings.TrimSpace(text))
		if lower == "" {
			return false
		}
		if strings.Contains(lower, "an error occurred while processing your request") {
			return true
		}
		if strings.Contains(lower, "selected model is at capacity") {
			return true
		}
		return strings.Contains(lower, "you can retry your request") &&
			strings.Contains(lower, "help.openai.com") &&
			strings.Contains(lower, "request id")
	}

	if match(upstreamMsg) {
		return true
	}
	if len(upstreamBody) == 0 {
		return false
	}
	if match(gjson.GetBytes(upstreamBody, "error.message").String()) {
		return true
	}
	return match(string(upstreamBody))
}

// ExtractSessionID extracts the raw session ID from headers or body without hashing.
// Used by ForwardAsAnthropic to pass as prompt_cache_key for upstream cache.
func (s *OpenAIGatewayService) ExtractSessionID(c *gin.Context, body []byte) string {
	if c == nil {
		return ""
	}
	sessionID := strings.TrimSpace(c.GetHeader("session_id"))
	if sessionID == "" {
		sessionID = strings.TrimSpace(c.GetHeader("conversation_id"))
	}
	if sessionID == "" && len(body) > 0 {
		sessionID = strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String())
	}
	return sessionID
}

// GenerateExplicitSessionHash generates a sticky-session hash only from explicit
// client session signals. It intentionally skips content-derived fallback and is
// used by stateless endpoints such as /v1/images.
func (s *OpenAIGatewayService) GenerateExplicitSessionHash(c *gin.Context, body []byte) string {
	sessionID := s.ExtractSessionID(c, body)
	if sessionID == "" {
		return ""
	}

	currentHash, legacyHash := deriveOpenAIRequestScopedSessionHashes(c, sessionID)
	attachOpenAILegacySessionHashToGin(c, legacyHash)
	return currentHash
}

// GenerateSessionHash generates a sticky-session hash for OpenAI requests.
//
// Priority:
//  1. Header: session_id
//  2. Header: conversation_id
//  3. Body:   prompt_cache_key (opencode)
//
// Requests without an explicit session signal are intentionally left unstuck.
// Content-derived hashes are not a reliable session boundary and can merge
// unrelated conversations that happen to share the same first prompt.
func (s *OpenAIGatewayService) GenerateSessionHash(c *gin.Context, body []byte) string {
	if c == nil {
		return ""
	}

	sessionID := strings.TrimSpace(c.GetHeader("session_id"))
	if sessionID == "" {
		sessionID = strings.TrimSpace(c.GetHeader("conversation_id"))
	}
	if sessionID == "" && len(body) > 0 {
		sessionID = strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String())
	}
	if sessionID == "" {
		return ""
	}

	currentHash, legacyHash := deriveOpenAIRequestScopedSessionHashes(c, sessionID)
	attachOpenAILegacySessionHashToGin(c, legacyHash)
	return currentHash
}

// GenerateSessionHashWithFallback 先按常规信号生成会话哈希；
// 当未携带 session_id/conversation_id/prompt_cache_key 时，使用 fallbackSeed 生成稳定哈希。
// 该方法用于 WS ingress，避免会话信号缺失时发生跨账号漂移。
func (s *OpenAIGatewayService) GenerateSessionHashWithFallback(c *gin.Context, body []byte, fallbackSeed string) string {
	sessionHash := s.GenerateSessionHash(c, body)
	if sessionHash != "" {
		return sessionHash
	}

	seed := strings.TrimSpace(fallbackSeed)
	if seed == "" {
		return ""
	}

	currentHash, legacyHash := deriveOpenAISessionHashes(seed)
	attachOpenAILegacySessionHashToGin(c, legacyHash)
	return currentHash
}

func deriveOpenAIRequestScopedSessionHashes(c *gin.Context, sessionID string) (string, string) {
	return deriveOpenAISessionHashes(openAIRequestScopedSessionSeed(getAPIKeyIDFromContext(c), sessionID))
}

func openAIRequestScopedSessionSeed(apiKeyID int64, sessionID string) string {
	normalized := strings.TrimSpace(sessionID)
	if normalized == "" {
		return ""
	}
	if apiKeyID <= 0 {
		return normalized
	}
	return fmt.Sprintf("api_key:%d:%s", apiKeyID, normalized)
}

func resolveOpenAIUpstreamOriginator(c *gin.Context, isOfficialClient bool) string {
	if c != nil {
		if originator := strings.TrimSpace(c.GetHeader("originator")); originator != "" {
			return originator
		}
	}
	if isOfficialClient {
		return "codex_cli_rs"
	}
	return "opencode"
}

func resolveOpenAIUpstreamSessionID(c *gin.Context, promptCacheKey string) string {
	if c != nil {
		if sessionID := strings.TrimSpace(c.GetHeader("session_id")); sessionID != "" {
			return sessionID
		}
		if conversationID := strings.TrimSpace(c.GetHeader("conversation_id")); conversationID != "" {
			return conversationID
		}
	}
	if cacheKey := strings.TrimSpace(promptCacheKey); cacheKey != "" {
		return cacheKey
	}
	return ""
}

func shouldUseOpenAIMessagesBridgeHeaders(c *gin.Context, body []byte) bool {
	if isOpenAICompatMessagesBridgeContext(c) {
		return true
	}
	return isOpenAICompatMessagesBridgeBody(body)
}

// BindStickySession sets session -> account binding with standard TTL.
func (s *OpenAIGatewayService) BindStickySession(ctx context.Context, groupID *int64, sessionHash string, accountID int64) error {
	if sessionHash == "" || accountID <= 0 {
		return nil
	}
	ttl := openaiStickySessionTTL
	if s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.StickySessionTTLSeconds > 0 {
		ttl = time.Duration(s.cfg.Gateway.OpenAIWS.StickySessionTTLSeconds) * time.Second
	}
	return s.setStickySessionAccountID(ctx, groupID, sessionHash, accountID, ttl)
}

// ClearStickySession removes the current session -> account binding.
func (s *OpenAIGatewayService) ClearStickySession(ctx context.Context, groupID *int64, sessionHash string) error {
	if sessionHash == "" {
		return nil
	}
	return s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
}

// ClearPreviousResponseBinding removes the previous_response_id -> account binding.
func (s *OpenAIGatewayService) ClearPreviousResponseBinding(ctx context.Context, groupID *int64, apiKeyID int64, previousResponseID string) error {
	responseID := strings.TrimSpace(previousResponseID)
	if responseID == "" {
		return nil
	}
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return nil
	}
	return store.DeleteResponseAccount(ctx, derefGroupID(groupID), apiKeyID, responseID)
}

// SelectAccount selects an OpenAI account with sticky session support
func (s *OpenAIGatewayService) SelectAccount(ctx context.Context, groupID *int64, sessionHash string) (*Account, error) {
	return s.SelectAccountForModel(ctx, groupID, sessionHash, "")
}

// SelectAccountForModel selects an account supporting the requested model
func (s *OpenAIGatewayService) SelectAccountForModel(ctx context.Context, groupID *int64, sessionHash string, requestedModel string) (*Account, error) {
	return s.SelectAccountForModelWithExclusions(ctx, groupID, sessionHash, requestedModel, nil)
}

// SelectAccountForModelWithExclusions selects an account supporting the requested model while excluding specified accounts.
// SelectAccountForModelWithExclusions 选择支持指定模型的账号，同时排除指定的账号。
func (s *OpenAIGatewayService) SelectAccountForModelWithExclusions(ctx context.Context, groupID *int64, sessionHash string, requestedModel string, excludedIDs map[int64]struct{}) (*Account, error) {
	return s.selectAccountForModelWithExclusions(ctx, groupID, sessionHash, requestedModel, excludedIDs, false, 0, "")
}

func shouldUseOpenAIGroupModelUnsupportedError(ctx context.Context, settingService *SettingService, accounts []Account, requestedModel string, requireCompact bool) bool {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" || len(accounts) == 0 {
		return false
	}
	hasRelevantAccount := false
	for i := range accounts {
		acc := &accounts[i]
		if !acc.IsOpenAI() {
			continue
		}
		if ResolveEffectiveModelRouting(ctx, settingService, acc, requestedModel, requireCompact).Supported {
			return false
		}
		if !acc.IsSchedulable() {
			continue
		}
		hasRelevantAccount = true
	}
	return hasRelevantAccount
}

func (s *OpenAIGatewayService) openAISelectionErrorAccounts(ctx context.Context, accounts []Account, requestedModel string, requireCompact bool, excludedIDs map[int64]struct{}, isRelevant func(*Account) bool) []Account {
	if len(accounts) == 0 {
		return nil
	}
	requestedModel = strings.TrimSpace(requestedModel)
	filtered := make([]Account, 0, len(accounts))
	for i := range accounts {
		acc := &accounts[i]
		latest := acc
		if s != nil && s.schedulerSnapshot != nil && s.accountRepo != nil {
			fresh, err := s.accountRepo.GetByID(ctx, acc.ID)
			if err != nil || fresh == nil {
				continue
			}
			latest = fresh
		}
		if requestedModel != "" && latest.IsOpenAI() && ResolveEffectiveModelRouting(ctx, s.settingService, latest, requestedModel, requireCompact).Supported {
			filtered = append(filtered, *latest)
			continue
		}
		if _, excluded := excludedIDs[latest.ID]; excluded {
			continue
		}
		if s.isOpenAIAccountRuntimeBlocked(latest) {
			continue
		}
		if isRelevant != nil && !isRelevant(latest) {
			continue
		}
		filtered = append(filtered, *latest)
	}
	return filtered
}

// noAvailableOpenAISelectionError builds the standard "no account available" error
// while preserving compact and group-model-unsupported semantics when applicable.
func noAvailableOpenAISelectionError(requestedModel string, compactBlocked bool, accounts ...[]Account) error {
	return noAvailableOpenAISelectionErrorWithRouting(context.Background(), nil, requestedModel, compactBlocked, false, accounts...)
}

func noAvailableOpenAISelectionErrorWithRouting(ctx context.Context, settingService *SettingService, requestedModel string, compactBlocked bool, requireCompact bool, accounts ...[]Account) error {
	if compactBlocked {
		return ErrNoAvailableCompactAccounts
	}
	if len(accounts) > 0 && shouldUseOpenAIGroupModelUnsupportedError(ctx, settingService, accounts[0], requestedModel, requireCompact) {
		if err := newGroupModelUnsupportedErrorWithRouting(ctx, settingService, PlatformOpenAI, requestedModel, accounts[0]); err != nil {
			return err
		}
	}
	if requestedModel != "" {
		return fmt.Errorf("no available OpenAI accounts supporting model: %s", requestedModel)
	}
	return errors.New("no available OpenAI accounts")
}

// openAICompactSupportTier classifies an OpenAI account by compact capability.
// 0 = explicitly unsupported, 1 = unknown / not yet probed, 2 = explicitly supported.
func openAICompactSupportTier(account *Account) int {
	if account == nil || !account.IsOpenAI() {
		return 0
	}
	supported, known := account.OpenAICompactSupportKnown()
	if !known {
		return 1
	}
	if supported {
		return 2
	}
	return 0
}

func allowRateLimitedOpenAIImageRouteScheduling(route string) bool {
	normalized := strings.TrimSpace(route)
	if normalized == "" {
		return false
	}
	switch NormalizeGroupImageGenerationRoute(normalized) {
	case GroupImageGenerationRouteCodex, GroupImageGenerationRouteWeb2API:
		return true
	default:
		return false
	}
}

func openAIImageRouteSchedulingResetAt(account *Account, route string) *time.Time {
	if account == nil || !allowRateLimitedOpenAIImageRouteScheduling(route) {
		return nil
	}
	resetAt := account.openAIImageRouteResetAt(route)
	if resetAt == nil || !time.Now().Before(*resetAt) {
		return nil
	}
	return resetAt
}

func reorderOpenAIImageRouteRateLimitedAccounts(accounts []*Account, route string) []*Account {
	if len(accounts) == 0 || !allowRateLimitedOpenAIImageRouteScheduling(route) {
		return accounts
	}
	limited := make([]*Account, 0, len(accounts))
	normal := make([]*Account, 0, len(accounts))
	for _, account := range accounts {
		if openAIImageRouteSchedulingResetAt(account, route) != nil {
			limited = append(limited, account)
			continue
		}
		normal = append(normal, account)
	}
	out := make([]*Account, 0, len(accounts))
	out = append(out, limited...)
	out = append(out, normal...)
	return out
}

func reorderOpenAIImageRouteRateLimitedAccountLoads(items []accountWithLoad, route string) []accountWithLoad {
	if len(items) == 0 || !allowRateLimitedOpenAIImageRouteScheduling(route) {
		return items
	}
	limited := make([]accountWithLoad, 0, len(items))
	normal := make([]accountWithLoad, 0, len(items))
	for _, item := range items {
		if openAIImageRouteSchedulingResetAt(item.account, route) != nil {
			limited = append(limited, item)
			continue
		}
		normal = append(normal, item)
	}
	out := make([]accountWithLoad, 0, len(items))
	out = append(out, limited...)
	out = append(out, normal...)
	return out
}

func reorderOpenAIImageRouteRateLimitedCandidates(items []openAIAccountCandidateScore, route string) []openAIAccountCandidateScore {
	if len(items) == 0 || !allowRateLimitedOpenAIImageRouteScheduling(route) {
		return items
	}
	limited := make([]openAIAccountCandidateScore, 0, len(items))
	normal := make([]openAIAccountCandidateScore, 0, len(items))
	for _, item := range items {
		if openAIImageRouteSchedulingResetAt(item.account, route) != nil {
			limited = append(limited, item)
			continue
		}
		normal = append(normal, item)
	}
	out := make([]openAIAccountCandidateScore, 0, len(items))
	out = append(out, limited...)
	out = append(out, normal...)
	return out
}

func openAIAccountTemporaryRecoveryAt(account *Account, requestedModel string, requiredImageRoute string) *time.Time {
	if account == nil {
		return nil
	}
	now := time.Now()
	var recoverAt *time.Time
	updateRecoverAt := func(candidate *time.Time) {
		if candidate == nil || !now.Before(*candidate) {
			return
		}
		if recoverAt == nil || recoverAt.Before(*candidate) {
			value := *candidate
			recoverAt = &value
		}
	}

	updateRecoverAt(account.OverloadUntil)
	updateRecoverAt(account.RateLimitResetAt)
	updateRecoverAt(account.TempUnschedulableUntil)
	updateRecoverAt(openAIImageRouteSchedulingResetAt(account, requiredImageRoute))
	if remaining := account.GetRateLimitRemainingTimeWithContext(context.Background(), requestedModel); remaining > 0 {
		value := now.Add(remaining)
		updateRecoverAt(&value)
	}
	return recoverAt
}

func buildOpenAIAccountWaitPlan(account *Account, requestedModel string, requiredImageRoute string, maxConcurrency int, timeout time.Duration, maxWaiting int) *AccountWaitPlan {
	recoverAt := openAIAccountTemporaryRecoveryAt(account, requestedModel, requiredImageRoute)
	if recoverAt == nil || timeout <= 0 {
		return nil
	}
	if time.Until(*recoverAt) > timeout {
		return nil
	}
	return &AccountWaitPlan{
		AccountID:      account.ID,
		MaxConcurrency: maxConcurrency,
		Timeout:        timeout,
		MaxWaiting:     maxWaiting,
		NotBefore:      recoverAt,
	}
}

func hasOpenAIAccountTemporaryRecoveryPending(account *Account, requestedModel string, requiredImageRoute string) bool {
	return openAIAccountTemporaryRecoveryAt(account, requestedModel, requiredImageRoute) != nil
}

func shouldClearOpenAIStickyAccount(account *Account, requestedModel string, requiredImageRoute string, timeout time.Duration) bool {
	if account == nil {
		return false
	}
	if !account.IsActive() || !account.Schedulable {
		return true
	}
	now := time.Now()
	if account.AutoPauseOnExpired && account.ExpiresAt != nil && !now.Before(*account.ExpiresAt) {
		return true
	}
	if account.IsAPIKeyOrBedrock() && account.IsQuotaExceeded() {
		return true
	}
	recoverAt := openAIAccountTemporaryRecoveryAt(account, requestedModel, requiredImageRoute)
	if recoverAt == nil {
		return false
	}
	return timeout <= 0 || time.Until(*recoverAt) > timeout
}

func isOpenAIStickyCandidateCompatible(ctx context.Context, settingService *SettingService, account *Account, requestedModel string, requireCompact bool, requiredImageRoute string, requireOAuthAccount bool, requireImageEnabled bool) bool {
	if account == nil || !account.IsOpenAI() {
		return false
	}
	if requireImageEnabled && !account.OpenAIImageGenerationAllowed() {
		return false
	}
	if requireOAuthAccount && !account.IsOpenAIOAuth() {
		return false
	}
	if requiredImageRoute != "" {
		if !account.SupportsOpenAIImageRoute(requiredImageRoute) {
			return false
		}
		if NormalizeGroupImageGenerationRoute(requiredImageRoute) == GroupImageGenerationRouteWeb2API && !account.HasOpenAIImageWeb2APIProfile() {
			return false
		}
	}
	if requestedModel != "" && !ResolveEffectiveModelRouting(ctx, settingService, account, requestedModel, requireCompact).Supported {
		return false
	}
	if requireCompact && openAICompactSupportTier(account) == 0 {
		return false
	}
	return true
}

func (s *OpenAIGatewayService) openAIStickyAccountWithinGroupScope(ctx context.Context, account *Account, groupID *int64) bool {
	if account == nil {
		return false
	}
	if openAIStickyAccountMatchesGroup(account, groupID) {
		return true
	}
	if groupID == nil || s == nil || s.accountRepo == nil {
		return false
	}
	accounts, err := s.accountRepo.ListByGroup(ctx, *groupID)
	if err != nil {
		return false
	}
	for i := range accounts {
		if accounts[i].ID == account.ID && accounts[i].Platform == account.Platform {
			return true
		}
	}
	return false
}

// isOpenAIAccountEligibleForRequest centralises the schedulable / OpenAI / model /
// compact-support checks used during account selection.
func isOpenAIAccountEligibleForRequest(ctx context.Context, settingService *SettingService, account *Account, requestedModel string, requireCompact bool, requiredImageRoute string, requireOAuthAccount bool) bool {
	if account == nil || !account.IsOpenAI() {
		return false
	}
	if requireOAuthAccount && !account.IsOpenAIOAuth() {
		return false
	}
	if requiredImageRoute != "" {
		if !account.IsSelectableForOpenAIImageRoute(requiredImageRoute, allowRateLimitedOpenAIImageRouteScheduling(requiredImageRoute)) {
			return false
		}
		if NormalizeGroupImageGenerationRoute(requiredImageRoute) == GroupImageGenerationRouteWeb2API && !account.HasOpenAIImageWeb2APIProfile() {
			return false
		}
	} else if !account.IsSchedulableForModelWithContext(ctx, requestedModel) {
		return false
	}
	if paused, reason := shouldAutoPauseOpenAIAccountByQuota(ctx, account); paused {
		// Debug level: this fires per-candidate on the scheduling hot path, so Info
		// would amplify into log spam once several accounts cross the threshold.
		slog.Debug("account_auto_paused_by_quota",
			"account_id", account.ID,
			"window", reason.window,
			"threshold", reason.threshold,
			"utilization", reason.utilization,
		)
		return false
	}
	if requestedModel != "" && !ResolveEffectiveModelRouting(ctx, settingService, account, requestedModel, requireCompact).Supported {
		return false
	}
	if requireCompact && openAICompactSupportTier(account) == 0 {
		return false
	}
	return true
}

type openAIQuotaAutoPauseDecision struct {
	window      string
	threshold   float64
	utilization float64
}

func shouldAutoPauseOpenAIAccountByQuota(ctx context.Context, account *Account) (bool, openAIQuotaAutoPauseDecision) {
	if account == nil || !account.IsOpenAI() {
		return false, openAIQuotaAutoPauseDecision{}
	}
	// Per-account explicit-disable flags must take precedence over the global default.
	// Without these, leaving the account threshold blank means "use global default",
	// so an admin has no way to exempt a single account from auto-pause once a global
	// default exists. The disable flag is per-window so an account can opt out of
	// only 5h or only 7d auto-pause.
	disabled5h := resolveAccountExtraBool(account.Extra, "auto_pause_5h_disabled")
	disabled7d := resolveAccountExtraBool(account.Extra, "auto_pause_7d_disabled")
	threshold5h, threshold7d := resolveOpenAIQuotaAutoPauseThresholds(ctx, account)
	now := time.Now()
	if !disabled5h && threshold5h > 0 {
		if utilization, ok := resolveOpenAIQuotaUtilization(account.Extra, "5h", now); ok && utilization >= threshold5h {
			return true, openAIQuotaAutoPauseDecision{window: "5h", threshold: threshold5h, utilization: utilization}
		}
	}
	if !disabled7d && threshold7d > 0 {
		if utilization, ok := resolveOpenAIQuotaUtilization(account.Extra, "7d", now); ok && utilization >= threshold7d {
			return true, openAIQuotaAutoPauseDecision{window: "7d", threshold: threshold7d, utilization: utilization}
		}
	}
	return false, openAIQuotaAutoPauseDecision{}
}

// resolveAccountExtraBool reads a bool-like value from account extra, tolerating
// the few shapes JSON unmarshalling may produce (real bool, "true"/"false"
// strings, 0/1 numbers).
func resolveAccountExtraBool(extra map[string]any, key string) bool {
	if len(extra) == 0 {
		return false
	}
	value, ok := extra[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		return err == nil && parsed
	case float64:
		return v != 0
	case float32:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i != 0
		}
	}
	return false
}

func resolveOpenAIQuotaAutoPauseThresholds(ctx context.Context, account *Account) (float64, float64) {
	threshold5h, _ := resolveAccountExtraNumber(account.Extra, "auto_pause_5h_threshold")
	threshold7d, _ := resolveAccountExtraNumber(account.Extra, "auto_pause_7d_threshold")
	threshold5h = clamp01(threshold5h)
	threshold7d = clamp01(threshold7d)
	if threshold5h > 0 && threshold7d > 0 {
		return threshold5h, threshold7d
	}
	settings := openAIQuotaAutoPauseSettingsFromContext(ctx)
	if threshold5h <= 0 {
		threshold5h = clamp01(settings.DefaultThreshold5h)
	}
	if threshold7d <= 0 {
		threshold7d = clamp01(settings.DefaultThreshold7d)
	}
	return threshold5h, threshold7d
}

func resolveAccountExtraNumber(extra map[string]any, keys ...string) (float64, bool) {
	if len(extra) == 0 {
		return 0, false
	}
	for _, key := range keys {
		value, ok := extra[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int:
			return float64(v), true
		case int64:
			return float64(v), true
		case json.Number:
			parsed, err := v.Float64()
			if err == nil {
				return parsed, true
			}
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

// resolveOpenAIQuotaUtilization returns the current utilization ratio (0..1) for the
// given Codex usage window. ok=false means there is no usable signal to pause on:
// either no snapshot exists, or the window has already rolled over so the cached
// percentage is stale. The stale guard matters because a paused account stops
// receiving requests, so its snapshot is never refreshed from upstream headers —
// without this check an old used_percent would keep the account paused forever even
// after the real window reset.
func resolveOpenAIQuotaUtilization(extra map[string]any, window string, now time.Time) (float64, bool) {
	usedPercent := readOpenAIQuotaUsedPercent(extra, window)
	if usedPercent <= 0 {
		return 0, false
	}
	if openAIQuotaWindowReset(extra, window, now) {
		return 0, false
	}
	// 快照过于陈旧（账号长期未收到流量刷新）时，不再据此暂停。放行后下一次响应头
	// 会刷新快照实现自愈，避免账号在错误/过期的 used% 上被永久跳过（issue #2994）。
	if openAICodexSnapshotStaleForPause(extra, now) {
		return 0, false
	}
	return usedPercent / 100, true
}

// openAICodexSnapshotStaleForPause reports whether the Codex usage snapshot is stale
// enough that it should no longer keep an account auto-paused. It anchors on
// codex_usage_updated_at (always written by buildCodexUsageExtraUpdates). A missing or
// unparseable timestamp returns false (treated as fresh, so the account stays paused) —
// this is deliberate: it prevents any snapshot without a write time from silently escaping
// auto-pause, and a genuinely-exhausted account that is actively served refreshes the
// timestamp on every response so it never crosses the staleness bound.
func openAICodexSnapshotStaleForPause(extra map[string]any, now time.Time) bool {
	if len(extra) == 0 {
		return false
	}
	updatedRaw, ok := extra["codex_usage_updated_at"]
	if !ok {
		return false
	}
	updatedAt, err := parseTime(fmt.Sprint(updatedRaw))
	if err != nil {
		return false
	}
	return now.Sub(updatedAt) >= openAICodexAutoPauseStaleAfter
}

// openAIQuotaWindowReset reports whether the Codex usage window's reset time has
// already passed relative to now. It prefers the absolute codex_<window>_reset_at
// timestamp and falls back to codex_<window>_reset_after_seconds anchored at
// codex_usage_updated_at, mirroring AccountUsageService's window-progress logic.
func openAIQuotaWindowReset(extra map[string]any, window string, now time.Time) bool {
	if len(extra) == 0 {
		return false
	}
	if resetAtRaw, ok := extra["codex_"+window+"_reset_at"]; ok {
		if resetAt, err := parseTime(fmt.Sprint(resetAtRaw)); err == nil {
			return !now.Before(resetAt)
		}
	}
	resetAfter := parseExtraInt(extra["codex_"+window+"_reset_after_seconds"])
	if resetAfter <= 0 {
		return false
	}
	base := now
	if updatedRaw, ok := extra["codex_usage_updated_at"]; ok {
		if updatedAt, err := parseTime(fmt.Sprint(updatedRaw)); err == nil {
			base = updatedAt
		}
	}
	resetAt := base.Add(time.Duration(resetAfter) * time.Second)
	return !now.Before(resetAt)
}

func readOpenAIQuotaUsedPercent(extra map[string]any, window string) float64 {
	if len(extra) == 0 {
		return 0
	}
	if value, ok := resolveAccountExtraNumber(extra, "codex_"+window+"_used_percent"); ok {
		return value
	}
	return 0
}

type openAIQuotaAutoPauseCtxKey struct{}

func withOpenAIQuotaAutoPauseSettings(ctx context.Context, settings OpsOpenAIAccountQuotaAutoPauseSettings) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAIQuotaAutoPauseCtxKey{}, settings)
}

func openAIQuotaAutoPauseSettingsFromContext(ctx context.Context) OpsOpenAIAccountQuotaAutoPauseSettings {
	if ctx == nil {
		return OpsOpenAIAccountQuotaAutoPauseSettings{}
	}
	settings, _ := ctx.Value(openAIQuotaAutoPauseCtxKey{}).(OpsOpenAIAccountQuotaAutoPauseSettings)
	return settings
}

func (s *OpenAIGatewayService) withOpenAIQuotaAutoPauseContext(ctx context.Context) context.Context {
	if s == nil || s.settingService == nil {
		return ctx
	}
	return withOpenAIQuotaAutoPauseSettings(ctx, s.settingService.GetOpenAIQuotaAutoPauseSettings(ctx))
}

// prioritizeOpenAICompactAccounts re-orders a slice so that accounts with known
// compact support are tried first, followed by unknown, then explicitly unsupported.
// The relative order within each tier is preserved.
func prioritizeOpenAICompactAccounts(accounts []*Account) []*Account {
	if len(accounts) == 0 {
		return nil
	}
	supported := make([]*Account, 0, len(accounts))
	unknown := make([]*Account, 0, len(accounts))
	unsupported := make([]*Account, 0, len(accounts))
	for _, account := range accounts {
		switch openAICompactSupportTier(account) {
		case 2:
			supported = append(supported, account)
		case 1:
			unknown = append(unknown, account)
		default:
			unsupported = append(unsupported, account)
		}
	}
	out := make([]*Account, 0, len(accounts))
	out = append(out, supported...)
	out = append(out, unknown...)
	out = append(out, unsupported...)
	return out
}

// resolveOpenAIAccountUpstreamModelForRequest resolves the upstream model that
// would be sent for a given request, honouring compact-only mappings when the
// caller is on the /responses/compact path.
func resolveOpenAIAccountUpstreamModelForRequest(ctx context.Context, settingService *SettingService, account *Account, requestedModel string, requireCompact bool) string {
	routing := ResolveEffectiveModelRouting(ctx, settingService, account, requestedModel, requireCompact)
	if strings.TrimSpace(routing.Model) == "" {
		return ""
	}
	return strings.TrimSpace(routing.Model)
}

func (s *OpenAIGatewayService) selectAccountForModelWithExclusions(ctx context.Context, groupID *int64, sessionHash string, requestedModel string, excludedIDs map[int64]struct{}, requireCompact bool, stickyAccountID int64, requiredImageRoute string) (*Account, error) {
	if s.checkChannelPricingRestriction(ctx, groupID, requestedModel) {
		slog.Warn("channel pricing restriction blocked request",
			"group_id", derefGroupID(groupID),
			"model", requestedModel)
		return nil, fmt.Errorf("%w supporting model: %s (channel pricing restriction)", ErrNoAvailableAccounts, requestedModel)
	}

	// 1. 尝试粘性会话命中
	// Try sticky session hit
	if account := s.tryStickySessionHit(ctx, groupID, sessionHash, requestedModel, excludedIDs, requireCompact, stickyAccountID, requiredImageRoute); account != nil {
		return account, nil
	}

	// 2. 获取可调度的 OpenAI 账号
	// Get schedulable OpenAI accounts
	accounts, err := s.listSchedulableAccounts(ctx, groupID, requiredImageRoute)
	if err != nil {
		return nil, fmt.Errorf("query accounts failed: %w", err)
	}

	// 3. 按优先级 + LRU 选择最佳账号
	// Select by priority + LRU
	selected, compactBlocked := s.selectBestAccount(ctx, groupID, accounts, requestedModel, excludedIDs, requireCompact, requiredImageRoute)

	if selected == nil {
		errorAccounts := s.openAISelectionErrorAccounts(ctx, accounts, requestedModel, requireCompact, excludedIDs, func(acc *Account) bool {
			if !isOpenAIAccountEligibleForRequest(ctx, s.settingService, acc, "", requireCompact, requiredImageRoute, false) {
				return false
			}
			return groupID == nil || !s.needsUpstreamChannelRestrictionCheck(ctx, groupID) ||
				!s.isUpstreamModelRestrictedByChannel(ctx, *groupID, acc, requestedModel, requireCompact)
		})
		return nil, noAvailableOpenAISelectionErrorWithRouting(ctx, s.settingService, requestedModel, compactBlocked, requireCompact, errorAccounts)
	}

	hydrated, err := s.hydrateSelectedAccount(ctx, selected)
	if err != nil {
		return nil, err
	}

	// 4. 设置粘性会话绑定
	// Set sticky session binding
	if sessionHash != "" {
		_ = s.setStickySessionAccountID(ctx, groupID, sessionHash, selected.ID, openaiStickySessionTTL)
	}

	return hydrated, nil
}

// tryStickySessionHit 尝试从粘性会话获取账号。
// 如果命中且账号可用则返回账号；如果账号不可用则清理会话并返回 nil。
//
// tryStickySessionHit attempts to get account from sticky session.
// Returns account if hit and usable; clears session and returns nil if account is unavailable.
func (s *OpenAIGatewayService) tryStickySessionHit(ctx context.Context, groupID *int64, sessionHash, requestedModel string, excludedIDs map[int64]struct{}, requireCompact bool, stickyAccountID int64, requiredImageRoute string) *Account {
	if sessionHash == "" {
		return nil
	}

	accountID := stickyAccountID
	if accountID <= 0 {
		var err error
		accountID, err = s.getStickySessionAccountID(ctx, groupID, sessionHash)
		if err != nil || accountID <= 0 {
			return nil
		}
	}

	if _, excluded := excludedIDs[accountID]; excluded {
		return nil
	}

	account, err := s.getSchedulableAccount(ctx, accountID)
	if err != nil {
		return nil
	}
	if !s.openAIStickyAccountWithinGroupScope(ctx, account, groupID) {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}

	waitTimeout := s.openAIStickyWaitTimeout(ctx)
	if shouldClearOpenAIStickyAccount(account, requestedModel, requiredImageRoute, waitTimeout) {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}

	if !isOpenAIStickyCandidateCompatible(ctx, s.settingService, account, requestedModel, requireCompact, requiredImageRoute, false, requiredImageRoute != "") {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}
	if s.isOpenAIAccountRuntimeBlocked(account) {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}
	account = s.recheckSelectedStickyOpenAIAccountFromDB(ctx, account, requestedModel, requireCompact, requiredImageRoute, false, requiredImageRoute != "")
	if account == nil {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}
	if groupID != nil && s.needsUpstreamChannelRestrictionCheck(ctx, groupID) &&
		s.isUpstreamModelRestrictedByChannel(ctx, *groupID, account, requestedModel, requireCompact) {
		_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
		return nil
	}

	// 刷新会话 TTL 并返回账号
	// Refresh session TTL and return account
	_ = s.refreshStickySessionTTL(ctx, groupID, sessionHash, openaiStickySessionTTL)
	return account
}

// selectBestAccount 从候选账号中选择最佳账号（优先级 + LRU）。
// 返回 nil 表示无可用账号。
//
// selectBestAccount selects the best account from candidates (priority + LRU).
// Returns nil if no available account. The second return reports whether at
// least one candidate was filtered out solely because it lacks compact support
// (only meaningful when requireCompact=true).
func (s *OpenAIGatewayService) selectBestAccount(ctx context.Context, groupID *int64, accounts []Account, requestedModel string, excludedIDs map[int64]struct{}, requireCompact bool, requiredImageRoute string) (*Account, bool) {
	var selected *Account
	selectedCompactTier := -1
	compactBlocked := false
	needsUpstreamCheck := s.needsUpstreamChannelRestrictionCheck(ctx, groupID)

	for i := range accounts {
		acc := &accounts[i]

		// 跳过被排除的账号
		// Skip excluded accounts
		if _, excluded := excludedIDs[acc.ID]; excluded {
			continue
		}

		fresh := s.resolveFreshSchedulableOpenAIAccount(ctx, acc, requestedModel, false, requiredImageRoute)
		if fresh == nil {
			continue
		}
		fresh = s.recheckSelectedOpenAIAccountFromDB(ctx, fresh, requestedModel, false, requiredImageRoute)
		if fresh == nil {
			continue
		}
		if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
			continue
		}
		compactTier := 0
		if requireCompact {
			compactTier = openAICompactSupportTier(fresh)
			if compactTier == 0 {
				compactBlocked = true
				continue
			}
		}

		// 选择优先级最高且最久未使用的账号
		// Select highest priority and least recently used
		if selected == nil {
			selected = fresh
			selectedCompactTier = compactTier
			continue
		}

		candidateRateLimited := openAIImageRouteSchedulingResetAt(fresh, requiredImageRoute) != nil
		selectedRateLimited := openAIImageRouteSchedulingResetAt(selected, requiredImageRoute) != nil
		if candidateRateLimited != selectedRateLimited {
			if candidateRateLimited {
				selected = fresh
				selectedCompactTier = compactTier
			}
			continue
		}

		// compact 模式下高 tier 优先；同 tier 内才比较 priority/LRU。
		if requireCompact && compactTier != selectedCompactTier {
			if compactTier > selectedCompactTier {
				selected = fresh
				selectedCompactTier = compactTier
			}
			continue
		}

		if s.isBetterAccount(fresh, selected) {
			selected = fresh
			selectedCompactTier = compactTier
		}
	}

	return selected, compactBlocked
}

// isBetterAccount 判断 candidate 是否比 current 更优。
// 规则：优先级更高（数值更小）优先；同优先级时，未使用过的优先，其次是最久未使用的。
//
// isBetterAccount checks if candidate is better than current.
// Rules: higher priority (lower value) wins; same priority: never used > least recently used.
func (s *OpenAIGatewayService) isBetterAccount(candidate, current *Account) bool {
	// 优先级更高（数值更小）
	// Higher priority (lower value)
	if candidate.Priority < current.Priority {
		return true
	}
	if candidate.Priority > current.Priority {
		return false
	}

	// 同优先级，比较最后使用时间
	// Same priority, compare last used time
	switch {
	case candidate.LastUsedAt == nil && current.LastUsedAt != nil:
		// candidate 从未使用，优先
		return true
	case candidate.LastUsedAt != nil && current.LastUsedAt == nil:
		// current 从未使用，保持
		return false
	case candidate.LastUsedAt == nil && current.LastUsedAt == nil:
		if !candidate.CreatedAt.Equal(current.CreatedAt) {
			return candidate.CreatedAt.After(current.CreatedAt)
		}
		// 都未使用且创建时间相同，保持
		return false
	default:
		// 都使用过，先比最后使用时间，再比创建时间
		if candidate.LastUsedAt.Before(*current.LastUsedAt) {
			return true
		}
		if current.LastUsedAt.Before(*candidate.LastUsedAt) {
			return false
		}
		if !candidate.CreatedAt.Equal(current.CreatedAt) {
			return candidate.CreatedAt.After(current.CreatedAt)
		}
		return false
	}
}

// SelectAccountWithLoadAwareness selects an account with load-awareness and wait plan.
func (s *OpenAIGatewayService) SelectAccountWithLoadAwareness(ctx context.Context, groupID *int64, sessionHash string, requestedModel string, excludedIDs map[int64]struct{}) (*AccountSelectionResult, error) {
	return s.selectAccountWithLoadAwareness(ctx, groupID, sessionHash, requestedModel, excludedIDs, false, "", false)
}

func (s *OpenAIGatewayService) selectAccountWithLoadAwarenessForImageRoute(ctx context.Context, groupID *int64, sessionHash string, requestedModel string, excludedIDs map[int64]struct{}, requireCompact bool, imageRoute string, requireOAuthAccount bool) (*AccountSelectionResult, error) {
	return s.selectAccountWithLoadAwareness(ctx, groupID, sessionHash, requestedModel, excludedIDs, requireCompact, imageRoute, requireOAuthAccount)
}

func (s *OpenAIGatewayService) selectAccountWithLoadAwareness(ctx context.Context, groupID *int64, sessionHash string, requestedModel string, excludedIDs map[int64]struct{}, requireCompact bool, requiredImageRoute string, requireOAuthAccount bool) (*AccountSelectionResult, error) {
	if s.checkChannelPricingRestriction(ctx, groupID, requestedModel) {
		slog.Warn("channel pricing restriction blocked request",
			"group_id", derefGroupID(groupID),
			"model", requestedModel)
		return nil, fmt.Errorf("%w supporting model: %s (channel pricing restriction)", ErrNoAvailableAccounts, requestedModel)
	}

	cfg := s.schedulingConfig()
	stickyWaitTimeout := s.openAIStickyWaitTimeout(ctx)
	needsUpstreamCheck := s.needsUpstreamChannelRestrictionCheck(ctx, groupID)
	var stickyAccountID int64
	if sessionHash != "" && s.cache != nil {
		if accountID, err := s.getStickySessionAccountID(ctx, groupID, sessionHash); err == nil {
			stickyAccountID = accountID
		}
	}
	if s.concurrencyService == nil || !cfg.LoadBatchEnabled {
		account, err := s.selectAccountForModelWithExclusions(ctx, groupID, sessionHash, requestedModel, excludedIDs, requireCompact, stickyAccountID, requiredImageRoute)
		if err != nil {
			return nil, err
		}
		acquireLimit := concurrencyForOpenAIAccountSelection(account, requiredImageRoute)
		if stickyAccountID <= 0 || stickyAccountID != account.ID {
			acquireLimit = s.freshSessionAdmissionLimit(ctx, account, requiredImageRoute)
		}
		waitTimeout := cfg.FallbackWaitTimeout
		maxWaiting := cfg.FallbackMaxWaiting
		if stickyAccountID > 0 && stickyAccountID == account.ID {
			waitTimeout = stickyWaitTimeout
			maxWaiting = cfg.StickySessionMaxWaiting
		}
		if waitPlan := buildOpenAIAccountWaitPlan(account, requestedModel, requiredImageRoute, acquireLimit, waitTimeout, maxWaiting); waitPlan != nil {
			return s.newSelectionResult(ctx, account, false, nil, waitPlan)
		}
		result, err := s.tryAcquireAccountSlot(ctx, account.ID, groupID, acquireLimit)
		if err == nil && result != nil && result.Acquired {
			return s.newAcquiredSelectionResult(ctx, account, result.ReleaseFunc)
		}
		if stickyAccountID > 0 && stickyAccountID == account.ID && s.concurrencyService != nil {
			waitingCount, _ := s.concurrencyService.GetAccountWaitingCount(ctx, account.ID)
			if waitingCount < cfg.StickySessionMaxWaiting {
				return s.newSelectionResult(ctx, account, false, nil, &AccountWaitPlan{
					AccountID:      account.ID,
					MaxConcurrency: concurrencyForOpenAIAccountSelection(account, requiredImageRoute),
					Timeout:        stickyWaitTimeout,
					MaxWaiting:     cfg.StickySessionMaxWaiting,
				})
			}
		}
		return s.newSelectionResult(ctx, account, false, nil, &AccountWaitPlan{
			AccountID:      account.ID,
			MaxConcurrency: acquireLimit,
			Timeout:        cfg.FallbackWaitTimeout,
			MaxWaiting:     cfg.FallbackMaxWaiting,
		})
	}

	accounts, err := s.listSchedulableAccounts(ctx, groupID, requiredImageRoute)
	if err != nil {
		return nil, err
	}
	if len(accounts) == 0 {
		return nil, ErrNoAvailableAccounts
	}

	isExcluded := func(accountID int64) bool {
		if excludedIDs == nil {
			return false
		}
		_, excluded := excludedIDs[accountID]
		return excluded
	}

	// ============ Layer 1: Sticky session ============
	if sessionHash != "" {
		accountID := stickyAccountID
		if accountID > 0 && !isExcluded(accountID) {
			account, err := s.getSchedulableAccount(ctx, accountID)
			if err == nil {
				clearSticky := shouldClearOpenAIStickyAccount(account, requestedModel, requiredImageRoute, stickyWaitTimeout)
				if clearSticky {
					_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
				}
				if !clearSticky && isOpenAIStickyCandidateCompatible(ctx, s.settingService, account, requestedModel, requireCompact, requiredImageRoute, requireOAuthAccount, requiredImageRoute != "") {
					account = s.recheckSelectedStickyOpenAIAccountFromDB(ctx, account, requestedModel, requireCompact, requiredImageRoute, requireOAuthAccount, requiredImageRoute != "")
					if account == nil {
						_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
					} else if !s.openAIStickyAccountWithinGroupScope(ctx, account, groupID) {
						_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
					} else if s.isOpenAIAccountRuntimeBlocked(account) {
						_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
					} else if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, account, requestedModel, requireCompact) {
						_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
					} else {
						if waitPlan := buildOpenAIAccountWaitPlan(
							account,
							requestedModel,
							requiredImageRoute,
							concurrencyForOpenAIAccountSelection(account, requiredImageRoute),
							stickyWaitTimeout,
							cfg.StickySessionMaxWaiting,
						); waitPlan != nil {
							return s.newSelectionResult(ctx, account, false, nil, waitPlan)
						}
						result, err := s.tryAcquireAccountSlot(ctx, accountID, groupID, concurrencyForOpenAIAccountSelection(account, requiredImageRoute))
						if err == nil && result != nil && result.Acquired {
							_ = s.refreshStickySessionTTL(ctx, groupID, sessionHash, openaiStickySessionTTL)
							return s.newAcquiredSelectionResult(ctx, account, result.ReleaseFunc)
						}

						waitingCount, _ := s.concurrencyService.GetAccountWaitingCount(ctx, accountID)
						if waitingCount < cfg.StickySessionMaxWaiting {
							return s.newSelectionResult(ctx, account, false, nil, &AccountWaitPlan{
								AccountID:      accountID,
								MaxConcurrency: concurrencyForOpenAIAccountSelection(account, requiredImageRoute),
								Timeout:        stickyWaitTimeout,
								MaxWaiting:     cfg.StickySessionMaxWaiting,
							})
						}
					}
				} else if !clearSticky {
					_ = s.deleteStickySessionAccountID(ctx, groupID, sessionHash)
				}
			}
		}
	}

	// ============ Layer 2: Load-aware selection ============
	baseCandidateCount := 0
	candidates := make([]*Account, 0, len(accounts))
	for i := range accounts {
		acc := &accounts[i]
		if isExcluded(acc.ID) {
			continue
		}
		// Scheduler snapshots can be temporarily stale (bucket rebuild is throttled);
		// re-check schedulability here so recently rate-limited/overloaded accounts
		// are not selected again before the bucket is rebuilt.
		if requiredImageRoute != "" {
			if !acc.IsSelectableForOpenAIImageRoute(requiredImageRoute, allowRateLimitedOpenAIImageRouteScheduling(requiredImageRoute)) {
				continue
			}
		} else if !acc.IsSchedulable() {
			continue
		}
		if s.isOpenAIAccountRuntimeBlocked(acc) {
			continue
		}
		if requestedModel != "" && !ResolveEffectiveModelRouting(ctx, s.settingService, acc, requestedModel, requireCompact).Supported {
			continue
		}
		if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, acc, requestedModel, requireCompact) {
			continue
		}
		baseCandidateCount++
		candidates = append(candidates, acc)
	}

	if len(candidates) == 0 {
		errorAccounts := s.openAISelectionErrorAccounts(ctx, accounts, requestedModel, requireCompact, excludedIDs, func(acc *Account) bool {
			if !isOpenAIAccountEligibleForRequest(ctx, s.settingService, acc, "", requireCompact, requiredImageRoute, requireOAuthAccount) {
				return false
			}
			return groupID == nil || !needsUpstreamCheck ||
				!s.isUpstreamModelRestrictedByChannel(ctx, *groupID, acc, requestedModel, requireCompact)
		})
		return nil, noAvailableOpenAISelectionErrorWithRouting(ctx, s.settingService, requestedModel, false, requireCompact, errorAccounts)
	}

	accountLoads := make([]AccountWithConcurrency, 0, len(candidates))
	for _, acc := range candidates {
		accountLoads = append(accountLoads, AccountWithConcurrency{
			ID:             acc.ID,
			MaxConcurrency: acc.EffectiveLoadFactor(),
		})
	}

	loadMap, err := s.concurrencyService.GetAccountsLoadBatch(ctx, accountLoads)
	if err != nil {
		ordered := append([]*Account(nil), candidates...)
		sortAccountsByPriorityAndLastUsed(ordered, false)
		ordered = reorderOpenAIImageRouteRateLimitedAccounts(ordered, requiredImageRoute)
		if requireCompact {
			ordered = prioritizeOpenAICompactAccounts(ordered)
		}
		for _, acc := range ordered {
			fresh := s.resolveFreshSchedulableOpenAIAccount(ctx, acc, requestedModel, false, requiredImageRoute)
			if fresh == nil {
				continue
			}
			fresh = s.recheckSelectedOpenAIAccountFromDB(ctx, fresh, requestedModel, requireCompact, requiredImageRoute)
			if fresh == nil {
				continue
			}
			if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
				continue
			}
			if waitPlan := buildOpenAIAccountWaitPlan(
				fresh,
				requestedModel,
				requiredImageRoute,
				s.freshSessionAdmissionLimit(ctx, fresh, requiredImageRoute),
				s.schedulingConfig().FallbackWaitTimeout,
				s.schedulingConfig().FallbackMaxWaiting,
			); waitPlan != nil {
				return s.newSelectionResult(ctx, fresh, false, nil, waitPlan)
			}
			if hasOpenAIAccountTemporaryRecoveryPending(fresh, requestedModel, requiredImageRoute) {
				continue
			}
			result, err := s.tryAcquireAccountSlot(ctx, fresh.ID, groupID, s.freshSessionAdmissionLimit(ctx, fresh, requiredImageRoute))
			if err == nil && result != nil && result.Acquired {
				if sessionHash != "" {
					_ = s.setStickySessionAccountID(ctx, groupID, sessionHash, fresh.ID, openaiStickySessionTTL)
				}
				return s.newAcquiredSelectionResult(ctx, fresh, result.ReleaseFunc)
			}
		}
	} else {
		var available []accountWithLoad
		for _, acc := range candidates {
			loadInfo := loadMap[acc.ID]
			if loadInfo == nil {
				loadInfo = &AccountLoadInfo{AccountID: acc.ID}
			}
			freshLimit := s.freshSessionAdmissionLimit(ctx, acc, requiredImageRoute)
			if freshLimit <= 0 {
				continue
			}
			if loadInfo.CurrentConcurrency < freshLimit {
				available = append(available, accountWithLoad{
					account:  acc,
					loadInfo: loadInfo,
				})
			}
		}

		if len(available) > 0 {
			sort.SliceStable(available, func(i, j int) bool {
				a, b := available[i], available[j]
				return lessOpenAINewSessionAccountWithLoad(
					a,
					b,
					s.freshSessionAdmissionLimit(ctx, a.account, requiredImageRoute),
					s.freshSessionAdmissionLimit(ctx, b.account, requiredImageRoute),
				)
			})
			available = reorderOpenAIImageRouteRateLimitedAccountLoads(available, requiredImageRoute)

			selectionOrder := make([]accountWithLoad, 0, len(available))
			if requireCompact {
				appendTier := func(out []accountWithLoad, tier int) []accountWithLoad {
					for _, item := range available {
						if openAICompactSupportTier(item.account) == tier {
							out = append(out, item)
						}
					}
					return out
				}
				selectionOrder = appendTier(selectionOrder, 2)
				selectionOrder = appendTier(selectionOrder, 1)
				// tier 0 候选作为兜底追加：DB recheck 时若发现 cache tier 0 实际
				// 已升级为 1/2（探测刚跑完，cache 尚未刷新），仍可正常命中。
				selectionOrder = appendTier(selectionOrder, 0)
			} else {
				selectionOrder = append(selectionOrder, available...)
			}

			for _, item := range selectionOrder {
				fresh := s.resolveFreshSchedulableOpenAIAccount(ctx, item.account, requestedModel, false, requiredImageRoute)
				if fresh == nil {
					continue
				}
				fresh = s.recheckSelectedOpenAIAccountFromDB(ctx, fresh, requestedModel, requireCompact, requiredImageRoute)
				if fresh == nil {
					continue
				}
				if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
					continue
				}
				if waitPlan := buildOpenAIAccountWaitPlan(
					fresh,
					requestedModel,
					requiredImageRoute,
					s.freshSessionAdmissionLimit(ctx, fresh, requiredImageRoute),
					s.schedulingConfig().FallbackWaitTimeout,
					s.schedulingConfig().FallbackMaxWaiting,
				); waitPlan != nil {
					if sessionHash != "" {
						_ = s.setStickySessionAccountID(ctx, groupID, sessionHash, fresh.ID, openaiStickySessionTTL)
					}
					return s.newSelectionResult(ctx, fresh, false, nil, waitPlan)
				}
				if hasOpenAIAccountTemporaryRecoveryPending(fresh, requestedModel, requiredImageRoute) {
					continue
				}
				result, err := s.tryAcquireAccountSlot(ctx, fresh.ID, groupID, s.freshSessionAdmissionLimit(ctx, fresh, requiredImageRoute))
				if err == nil && result != nil && result.Acquired {
					if sessionHash != "" {
						_ = s.setStickySessionAccountID(ctx, groupID, sessionHash, fresh.ID, openaiStickySessionTTL)
					}
					return s.newAcquiredSelectionResult(ctx, fresh, result.ReleaseFunc)
				}
			}
		}
	}

	// ============ Layer 3: Fallback wait ============
	waitCandidates := make([]accountWithLoad, 0, len(candidates))
	for _, acc := range candidates {
		loadInfo := loadMap[acc.ID]
		if loadInfo == nil {
			loadInfo = &AccountLoadInfo{AccountID: acc.ID}
		}
		waitCandidates = append(waitCandidates, accountWithLoad{account: acc, loadInfo: loadInfo})
	}
	sort.SliceStable(waitCandidates, func(i, j int) bool {
		a, b := waitCandidates[i], waitCandidates[j]
		return lessOpenAINewSessionAccountWithLoad(
			a,
			b,
			s.freshSessionAdmissionLimit(ctx, a.account, requiredImageRoute),
			s.freshSessionAdmissionLimit(ctx, b.account, requiredImageRoute),
		)
	})
	waitCandidates = reorderOpenAIImageRouteRateLimitedAccountLoads(waitCandidates, requiredImageRoute)
	if requireCompact {
		sorted := make([]accountWithLoad, 0, len(waitCandidates))
		appendTier := func(tier int) {
			for _, item := range waitCandidates {
				if openAICompactSupportTier(item.account) == tier {
					sorted = append(sorted, item)
				}
			}
		}
		appendTier(2)
		appendTier(1)
		appendTier(0)
		waitCandidates = sorted
	}
	for _, item := range waitCandidates {
		fresh := s.resolveFreshSchedulableOpenAIAccount(ctx, item.account, requestedModel, false, requiredImageRoute)
		if fresh == nil {
			continue
		}
		fresh = s.recheckSelectedOpenAIAccountFromDB(ctx, fresh, requestedModel, requireCompact, requiredImageRoute)
		if fresh == nil {
			continue
		}
		if needsUpstreamCheck && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
			continue
		}
		waitLimit := s.freshSessionAdmissionLimit(ctx, fresh, requiredImageRoute)
		if waitPlan := buildOpenAIAccountWaitPlan(
			fresh,
			requestedModel,
			requiredImageRoute,
			waitLimit,
			cfg.FallbackWaitTimeout,
			cfg.FallbackMaxWaiting,
		); waitPlan != nil {
			return s.newSelectionResult(ctx, fresh, false, nil, waitPlan)
		}
		if hasOpenAIAccountTemporaryRecoveryPending(fresh, requestedModel, requiredImageRoute) {
			continue
		}
		return s.newSelectionResult(ctx, fresh, false, nil, &AccountWaitPlan{
			AccountID:      fresh.ID,
			MaxConcurrency: waitLimit,
			Timeout:        cfg.FallbackWaitTimeout,
			MaxWaiting:     cfg.FallbackMaxWaiting,
		})
	}

	if requireCompact && baseCandidateCount > 0 {
		return nil, ErrNoAvailableCompactAccounts
	}
	return nil, ErrNoAvailableAccounts
}

func (s *OpenAIGatewayService) listSchedulableAccounts(ctx context.Context, groupID *int64, requiredImageRoute string) ([]Account, error) {
	if requiredImageRoute == "" && s.schedulerSnapshot != nil {
		accounts, _, err := s.schedulerSnapshot.ListSchedulableAccounts(ctx, groupID, PlatformOpenAI, false)
		return s.filterOpenAIAccountsBySchedulingThreshold(ctx, accounts), err
	}

	if requiredImageRoute != "" {
		accounts, err := s.listOpenAIImageCandidateAccounts(ctx, groupID)
		if err != nil {
			return nil, err
		}
		return s.filterOpenAIAccountsBySchedulingThreshold(ctx, accounts), nil
	}

	var accounts []Account
	var err error
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		accounts, err = s.accountRepo.ListSchedulableByPlatform(ctx, PlatformOpenAI)
	} else if groupID != nil {
		accounts, err = s.accountRepo.ListSchedulableByGroupIDAndPlatform(ctx, *groupID, PlatformOpenAI)
	} else {
		accounts, err = s.accountRepo.ListSchedulableUngroupedByPlatform(ctx, PlatformOpenAI)
	}
	if err != nil {
		return nil, fmt.Errorf("query accounts failed: %w", err)
	}
	return s.filterOpenAIAccountsBySchedulingThreshold(ctx, accounts), nil
}

func (s *OpenAIGatewayService) listOpenAIImageCandidateAccounts(ctx context.Context, groupID *int64) ([]Account, error) {
	if s == nil || s.accountRepo == nil {
		return nil, fmt.Errorf("account repository is not available")
	}

	var (
		accounts []Account
		err      error
	)
	switch {
	case s.cfg != nil && s.cfg.RunMode == config.RunModeSimple:
		accounts, err = s.accountRepo.ListByPlatform(ctx, PlatformOpenAI)
	case groupID != nil:
		accounts, err = s.accountRepo.ListByGroup(ctx, *groupID)
	default:
		accounts, err = s.accountRepo.ListByPlatform(ctx, PlatformOpenAI)
	}
	if err != nil {
		return nil, fmt.Errorf("query image candidate accounts failed: %w", err)
	}

	filtered := make([]Account, 0, len(accounts))
	for _, acc := range accounts {
		if acc.Platform != PlatformOpenAI {
			continue
		}
		if groupID == nil && s.cfg != nil && s.cfg.RunMode != config.RunModeSimple && len(acc.AccountGroups) > 0 {
			continue
		}
		filtered = append(filtered, acc)
	}
	if s.schedulerSnapshot != nil {
		s.schedulerSnapshot.overlayLastUsedFromCache(ctx, filtered)
	}
	return filtered, nil
}

func (s *OpenAIGatewayService) tryAcquireAccountSlot(ctx context.Context, accountID int64, groupID *int64, maxConcurrency int) (*AcquireResult, error) {
	if s.concurrencyService == nil {
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {}}, nil
	}
	return s.concurrencyService.AcquireAccountSlotForGroup(ctx, accountID, groupID, maxConcurrency)
}

func concurrencyForOpenAIAccountSelection(account *Account, requiredImageRoute string) int {
	requiredImageRoute = openAIImageRouteForAccountScheduling(requiredImageRoute)
	if account == nil || account.Concurrency <= 0 {
		return 1
	}
	return account.Concurrency
}

func compareOptionalTimeAsc(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	case a.Before(*b):
		return -1
	case b.Before(*a):
		return 1
	default:
		return 0
	}
}

func compareConcurrencyRatioAsc(currentA, limitA, currentB, limitB int) int {
	if limitA <= 0 {
		limitA = 1
	}
	if limitB <= 0 {
		limitB = 1
	}
	left := int64(currentA) * int64(limitB)
	right := int64(currentB) * int64(limitA)
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func stickyReserveSlots(maxConcurrency int, reservePercent int) int {
	if maxConcurrency <= 1 || reservePercent <= 0 {
		return 0
	}
	if reservePercent > 100 {
		reservePercent = 100
	}
	reserved := (maxConcurrency * reservePercent) / 100
	if reserved >= maxConcurrency {
		return maxConcurrency - 1
	}
	if reserved < 0 {
		return 0
	}
	return reserved
}

func (s *OpenAIGatewayService) freshSessionAdmissionLimit(ctx context.Context, account *Account, requiredImageRoute string) int {
	maxConcurrency := concurrencyForOpenAIAccountSelection(account, requiredImageRoute)
	if maxConcurrency <= 1 {
		return maxConcurrency
	}
	reserved := stickyReserveSlots(maxConcurrency, s.openAIStickyReservePercent(ctx))
	limit := maxConcurrency - reserved
	if limit < 1 {
		return 1
	}
	return limit
}

func lessOpenAINewSessionAccountWithLoad(a, b accountWithLoad, limitA, limitB int) bool {
	if a.account.Priority != b.account.Priority {
		return a.account.Priority < b.account.Priority
	}
	if cmp := compareOptionalTimeAsc(a.account.LastUsedAt, b.account.LastUsedAt); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareConcurrencyRatioAsc(a.loadInfo.CurrentConcurrency, limitA, b.loadInfo.CurrentConcurrency, limitB); cmp != 0 {
		return cmp < 0
	}
	if a.loadInfo.CurrentConcurrency != b.loadInfo.CurrentConcurrency {
		return a.loadInfo.CurrentConcurrency < b.loadInfo.CurrentConcurrency
	}
	if a.loadInfo.WaitingCount != b.loadInfo.WaitingCount {
		return a.loadInfo.WaitingCount < b.loadInfo.WaitingCount
	}
	return a.account.ID < b.account.ID
}

func lessOpenAINewSessionCandidate(a, b openAIAccountCandidateScore, limitA, limitB int) bool {
	if a.account.Priority != b.account.Priority {
		return a.account.Priority < b.account.Priority
	}
	if cmp := compareOptionalTimeAsc(a.account.LastUsedAt, b.account.LastUsedAt); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareConcurrencyRatioAsc(a.loadInfo.CurrentConcurrency, limitA, b.loadInfo.CurrentConcurrency, limitB); cmp != 0 {
		return cmp < 0
	}
	if a.loadInfo.CurrentConcurrency != b.loadInfo.CurrentConcurrency {
		return a.loadInfo.CurrentConcurrency < b.loadInfo.CurrentConcurrency
	}
	if a.loadInfo.WaitingCount != b.loadInfo.WaitingCount {
		return a.loadInfo.WaitingCount < b.loadInfo.WaitingCount
	}
	return a.account.ID < b.account.ID
}

func (s *OpenAIGatewayService) resolveFreshSchedulableOpenAIAccount(ctx context.Context, account *Account, requestedModel string, requireCompact bool, requiredImageRoute string) *Account {
	if account == nil {
		return nil
	}

	fresh := account
	if s.schedulerSnapshot != nil {
		current, err := s.getSchedulableAccount(ctx, account.ID)
		if err != nil || current == nil {
			return nil
		}
		fresh = current
	}

	if !isOpenAIAccountEligibleForRequest(ctx, s.settingService, fresh, requestedModel, requireCompact, requiredImageRoute, false) {
		return nil
	}
	if s.isOpenAIAccountRuntimeBlocked(fresh) {
		return nil
	}
	return fresh
}

func (s *OpenAIGatewayService) recheckSelectedOpenAIAccountFromDB(ctx context.Context, account *Account, requestedModel string, requireCompact bool, requiredImageRoute string) *Account {
	if account == nil {
		return nil
	}
	if s.schedulerSnapshot == nil || s.accountRepo == nil {
		if !isOpenAIAccountEligibleForRequest(ctx, s.settingService, account, requestedModel, requireCompact, requiredImageRoute, false) {
			return nil
		}
		if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, account) {
			return nil
		}
		return account
	}

	latest, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || latest == nil {
		return nil
	}
	if !isOpenAIAccountEligibleForRequest(ctx, s.settingService, latest, requestedModel, requireCompact, requiredImageRoute, false) {
		return nil
	}
	if s.isOpenAIAccountRuntimeBlocked(latest) {
		return nil
	}
	if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, latest) {
		return nil
	}
	return latest
}

func (s *OpenAIGatewayService) recheckSelectedStickyOpenAIAccountFromDB(ctx context.Context, account *Account, requestedModel string, requireCompact bool, requiredImageRoute string, requireOAuthAccount bool, requireImageEnabled bool) *Account {
	if account == nil {
		return nil
	}
	waitTimeout := s.openAIStickyWaitTimeout(ctx)
	if s.schedulerSnapshot == nil || s.accountRepo == nil {
		if shouldClearOpenAIStickyAccount(account, requestedModel, requiredImageRoute, waitTimeout) {
			return nil
		}
		if !isOpenAIStickyCandidateCompatible(ctx, s.settingService, account, requestedModel, requireCompact, requiredImageRoute, requireOAuthAccount, requireImageEnabled) {
			return nil
		}
		if s.isOpenAIAccountRuntimeBlocked(account) {
			return nil
		}
		if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, account) {
			return nil
		}
		return account
	}

	latest, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || latest == nil {
		return nil
	}
	if shouldClearOpenAIStickyAccount(latest, requestedModel, requiredImageRoute, waitTimeout) {
		return nil
	}
	if !isOpenAIStickyCandidateCompatible(ctx, s.settingService, latest, requestedModel, requireCompact, requiredImageRoute, requireOAuthAccount, requireImageEnabled) {
		return nil
	}
	if s.isOpenAIAccountRuntimeBlocked(latest) {
		return nil
	}
	if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, latest) {
		return nil
	}
	return latest
}

func (s *OpenAIGatewayService) RecheckSelectedOpenAIAccountForResponses(ctx context.Context, account *Account, requestedModel string) *Account {
	return s.recheckSelectedOpenAIAccountFromDB(ctx, account, requestedModel, false, "")
}

func (s *OpenAIGatewayService) getSchedulableAccount(ctx context.Context, accountID int64) (*Account, error) {
	var (
		account *Account
		err     error
	)
	if s.schedulerSnapshot != nil {
		account, err = s.schedulerSnapshot.GetAccount(ctx, accountID)
	} else {
		account, err = s.accountRepo.GetByID(ctx, accountID)
	}
	if err != nil || account == nil {
		return account, err
	}
	if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, account) {
		return nil, nil
	}
	return account, nil
}

func (s *OpenAIGatewayService) filterOpenAIAccountsBySchedulingThreshold(ctx context.Context, accounts []Account) []Account {
	if len(accounts) == 0 {
		return accounts
	}

	filtered := make([]Account, 0, len(accounts))
	for i := range accounts {
		if s.isOpenAIAccountBlockedBySchedulingThreshold(ctx, &accounts[i]) {
			continue
		}
		filtered = append(filtered, accounts[i])
	}
	return filtered
}

func (s *OpenAIGatewayService) isOpenAIAccountBlockedBySchedulingThreshold(ctx context.Context, account *Account) bool {
	if s == nil || s.rateLimitService == nil || account == nil {
		return false
	}
	return s.rateLimitService.ApplyAccountSchedulingThreshold(ctx, account)
}

func (s *OpenAIGatewayService) hydrateSelectedAccount(ctx context.Context, account *Account) (*Account, error) {
	if account == nil || s.schedulerSnapshot == nil {
		return account, nil
	}
	hydrated, err := s.schedulerSnapshot.GetAccount(ctx, account.ID)
	if err != nil {
		return nil, err
	}
	if hydrated == nil {
		return nil, fmt.Errorf("selected openai account %d not found during hydration", account.ID)
	}
	return hydrated, nil
}

func (s *OpenAIGatewayService) hotUpdateSelectedAccountLastUsed(account *Account) {
	if account == nil || account.ID <= 0 {
		return
	}
	now := time.Now()
	account.LastUsedAt = &now
	if s == nil || s.schedulerSnapshot == nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.schedulerSnapshot.UpdateLastUsedInCache(cacheCtx, account.ID, now)
}

func (s *OpenAIGatewayService) newSelectionResult(ctx context.Context, account *Account, acquired bool, release func(), waitPlan *AccountWaitPlan) (*AccountSelectionResult, error) {
	hydrated, err := s.hydrateSelectedAccount(ctx, account)
	if err != nil {
		return nil, err
	}
	if acquired {
		s.hotUpdateSelectedAccountLastUsed(hydrated)
	}
	return &AccountSelectionResult{
		Account:     hydrated,
		Acquired:    acquired,
		ReleaseFunc: release,
		WaitPlan:    waitPlan,
	}, nil
}

func (s *OpenAIGatewayService) newAcquiredSelectionResult(ctx context.Context, account *Account, release func()) (*AccountSelectionResult, error) {
	selection, err := s.newSelectionResult(ctx, account, true, release, nil)
	if err != nil && release != nil {
		release()
	}
	return selection, err
}

func (s *OpenAIGatewayService) schedulingConfig() config.GatewaySchedulingConfig {
	cfg := config.GatewaySchedulingConfig{
		StickySessionMaxWaiting:  3,
		StickySessionWaitTimeout: 45 * time.Second,
		FallbackWaitTimeout:      30 * time.Second,
		FallbackMaxWaiting:       100,
		LoadBatchEnabled:         true,
		SlotCleanupInterval:      30 * time.Second,
	}
	if s.cfg == nil {
		return cfg
	}
	runtimeCfg := s.cfg.Gateway.Scheduling
	if runtimeCfg.StickySessionMaxWaiting > 0 {
		cfg.StickySessionMaxWaiting = runtimeCfg.StickySessionMaxWaiting
	}
	if runtimeCfg.StickySessionWaitTimeout > 0 {
		cfg.StickySessionWaitTimeout = runtimeCfg.StickySessionWaitTimeout
	}
	if runtimeCfg.FallbackWaitTimeout > 0 {
		cfg.FallbackWaitTimeout = runtimeCfg.FallbackWaitTimeout
	}
	if runtimeCfg.FallbackMaxWaiting > 0 {
		cfg.FallbackMaxWaiting = runtimeCfg.FallbackMaxWaiting
	}
	cfg.FallbackSelectionMode = runtimeCfg.FallbackSelectionMode
	cfg.LoadBatchEnabled = runtimeCfg.LoadBatchEnabled
	cfg.LoadBatchCacheTTLMS = runtimeCfg.LoadBatchCacheTTLMS
	cfg.SnapshotMGetChunkSize = runtimeCfg.SnapshotMGetChunkSize
	cfg.SnapshotWriteChunkSize = runtimeCfg.SnapshotWriteChunkSize
	cfg.SlotCleanupInterval = runtimeCfg.SlotCleanupInterval
	if cfg.SlotCleanupInterval <= 0 {
		cfg.SlotCleanupInterval = 30 * time.Second
	}
	return cfg
}

func (s *OpenAIGatewayService) openAIHTTPIngressUpstreamWSEnabled() bool {
	return s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIWS.HttpIngressUpstreamWSEnabled
}

// GetAccessToken gets the access token for an OpenAI account
func (s *OpenAIGatewayService) GetAccessToken(ctx context.Context, account *Account) (string, string, error) {
	switch account.Type {
	case AccountTypeOAuth:
		// 使用 TokenProvider 获取缓存的 token
		if s.openAITokenProvider != nil {
			accessToken, err := s.openAITokenProvider.GetAccessToken(ctx, account)
			if err != nil {
				return "", "", err
			}
			return accessToken, "oauth", nil
		}
		// 降级：TokenProvider 未配置时直接从账号读取
		accessToken := account.GetOpenAIAccessToken()
		if accessToken == "" {
			return "", "", errors.New("access_token not found in credentials")
		}
		return accessToken, "oauth", nil
	case AccountTypeAPIKey:
		apiKey := account.GetOpenAIApiKey()
		if apiKey == "" {
			return "", "", errors.New("api_key not found in credentials")
		}
		return apiKey, "apikey", nil
	default:
		return "", "", fmt.Errorf("unsupported account type: %s", account.Type)
	}
}

func (s *OpenAIGatewayService) shouldFailoverUpstreamError(statusCode int) bool {
	switch statusCode {
	case 401, 402, 403, 429, 529:
		return true
	default:
		return statusCode >= 500
	}
}

func (s *OpenAIGatewayService) shouldFailoverOpenAIUpstreamResponse(statusCode int, upstreamMsg string, upstreamBody []byte) bool {
	if s.shouldFailoverUpstreamError(statusCode) {
		return true
	}
	if isOpenAIImageGenerationToolUnsupportedError(statusCode, upstreamMsg, upstreamBody) {
		return true
	}
	return isOpenAITransientProcessingError(statusCode, upstreamMsg, upstreamBody)
}

func isOpenAIImageGenerationToolUnsupportedError(statusCode int, upstreamMsg string, upstreamBody []byte) bool {
	if statusCode != http.StatusBadRequest {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(upstreamBody)))
	}
	if msg == "" {
		msg = strings.ToLower(strings.TrimSpace(string(upstreamBody)))
	}
	return strings.Contains(msg, "tool choice 'image_generation' not found") &&
		strings.Contains(msg, "'tools' parameter")
}

func (s *OpenAIGatewayService) handleFailoverSideEffects(ctx context.Context, resp *http.Response, account *Account, requestedModel ...string) {
	body := s.readUpstreamErrorBody(resp)
	if len(requestedModel) > 0 {
		s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body, requestedModel[0])
		return
	}
	s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body)
}

// Forward forwards request to OpenAI API
func (s *OpenAIGatewayService) Forward(ctx context.Context, c *gin.Context, account *Account, body []byte) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	clearOpenAICodexCompatContext(c)
	codexCompatFallbackState := openAICodexCompatFallbackState{}
	if failoverBody, ok := getOpenAIFailoverRequestBody(c, body); ok {
		body = failoverBody
		clearOpenAIRequestBodyCache(c)
	}

	restrictionResult := s.detectCodexClientRestriction(c, account)
	apiKeyID := getAPIKeyIDFromContext(c)
	logCodexCLIOnlyDetection(ctx, c, account, apiKeyID, restrictionResult, body)
	if restrictionResult.Enabled && !restrictionResult.Matched {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"type":    "forbidden_error",
				"message": "This account only allows Codex official clients",
			},
		})
		return nil, errors.New("codex_cli_only restriction: only codex official clients are allowed")
	}

	originalBody := body
	requestView := newOpenAIRequestView(body)
	reqModel, reqStream, promptCacheKey := requestView.Model, requestView.Stream, requestView.PromptCacheKey
	setOpenAIRoutingPromptCacheKey(c, promptCacheKey)
	originalModel := reqModel
	allowImageGeneration := true
	if apiKey := getAPIKeyFromContext(c); apiKey != nil {
		allowImageGeneration = GroupAllowsImageGeneration(apiKeyGroup(apiKey))
	}
	if !allowImageGeneration &&
		IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body) {
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"type":    "permission_error",
				"message": ImageGenerationPermissionMessage(),
			},
		})
		return nil, errors.New(ImageGenerationPermissionMessage())
	}

	isCodexCLI := isOpenAICodexOfficialOrForcedClientRequest(c, s.cfg)
	wsDecision := s.getOpenAIWSProtocolResolver().Resolve(account)
	clientTransport := GetOpenAIClientTransport(c)
	// 默认仅允许 WS 入站请求走 WS 上游；显式开启时 HTTP /v1/responses
	// 也可使用上游 WSv2，由 forwarder 负责无会话请求的一次性隔离。
	httpIngressUpstreamWSEnabled := s.openAIHTTPIngressUpstreamWSEnabled()
	wsDecision = resolveOpenAIWSDecisionByClientTransport(wsDecision, clientTransport, httpIngressUpstreamWSEnabled)
	if c != nil {
		c.Set("openai_ws_transport_decision", string(wsDecision.Transport))
		c.Set("openai_ws_transport_reason", wsDecision.Reason)
	}
	if wsDecision.Transport == OpenAIUpstreamTransportResponsesWebsocketV2 {
		logOpenAIWSModeDebug(
			"selected account_id=%d account_type=%s transport=%s reason=%s model=%s stream=%v",
			account.ID,
			account.Type,
			normalizeOpenAIWSLogValue(string(wsDecision.Transport)),
			normalizeOpenAIWSLogValue(wsDecision.Reason),
			reqModel,
			reqStream,
		)
	}
	// 当前仅支持 WSv2；WSv1 命中时直接返回错误，避免出现“配置可开但行为不确定”。
	if wsDecision.Transport == OpenAIUpstreamTransportResponsesWebsocket {
		if c != nil {
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalFeatureGate)
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"type":    "invalid_request_error",
					"message": "OpenAI WSv1 is temporarily unsupported. Please enable responses_websockets_v2.",
				},
			})
		}
		return nil, errors.New("openai ws v1 is temporarily unsupported; use ws v2")
	}
	passthroughEnabled := account.IsOpenAIPassthroughEnabled()
	if passthroughEnabled {
		// 透传分支只需要轻量提取字段，避免热路径全量 Unmarshal。
		reasoningEffort := extractOpenAIReasoningEffortFromBody(body, reqModel)
		return s.forwardOpenAIPassthrough(ctx, c, account, originalBody, reqModel, reasoningEffort, startTime)
	}

	bodyModified := false
	var reqBody map[string]any
	resetRawBodyView := func(nextBody []byte) {
		body = nextBody
		requestView = newOpenAIRequestView(body)
		reqBody = nil
		bodyModified = false
	}
	ensureReqBody := func() (map[string]any, error) {
		if requestView.HasPatches() {
			patchedBody, patchErr := requestView.ApplyPatches()
			if patchErr != nil {
				return nil, patchErr
			}
			resetRawBodyView(patchedBody)
		}
		if reqBody != nil {
			return reqBody, nil
		}
		decoded, decodeErr := requestView.Decode(c)
		if decodeErr != nil {
			return nil, decodeErr
		}
		reqBody = decoded
		return reqBody, nil
	}
	markPatchSet := func(path string, value any) {
		bodyModified = true
		if reqBody != nil {
			setOpenAIRequestMapPath(reqBody, path, value)
		}
		if requestView.patchesDisabled {
			return
		}
		requestView.MarkPatchSet(path, value)
	}
	markPatchDelete := func(path string) {
		bodyModified = true
		if reqBody != nil {
			deleteOpenAIRequestMapPath(reqBody, path)
		}
		if requestView.patchesDisabled {
			return
		}
		requestView.MarkPatchDelete(path)
	}
	disablePatch := func() {
		requestView.DisablePatches()
	}
	markDecodedModified := func() {
		bodyModified = true
		disablePatch()
	}
	if ShouldForwardOpenAITextResponsesViaAnthropicMessages(account) {
		return s.forwardResponsesToAnthropicMessages(ctx, c, account, body)
	}
	if account.Platform == PlatformOpenAI &&
		account.Type == AccountTypeAPIKey &&
		openai_compat.ResolveResponsesSupport(account.Extra) == openai_compat.ResponsesSupportNo {
		return s.forwardResponsesViaRawChatCompletions(ctx, c, account, body)
	}

	apiKey := getAPIKeyFromContext(c)
	clientStream := reqStream
	var upstreamStream bool
	imageGenerationAllowed := allowImageGeneration
	codexImageGenerationBridgeEnabled := isCodexCLI && imageGenerationAllowed && s.isCodexImageGenerationBridgeEnabled(ctx, account, apiKey)
	imageIntent := IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body)
	if imageIntent && !imageGenerationAllowed {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalFeatureGate)
		c.JSON(http.StatusForbidden, gin.H{"error": gin.H{"type": "permission_error", "message": ImageGenerationPermissionMessage()}})
		return nil, errors.New("image generation disabled for group")
	}

	instructions := gjson.GetBytes(body, "instructions")
	instructionsEmpty := !instructions.Exists() || instructions.Type != gjson.String || strings.TrimSpace(instructions.String()) == ""
	isMessagesBridgeRequest := isOpenAICompatMessagesBridgeBody(body)
	if instructionsEmpty && shouldPatchDefaultCodexSynthInstructions(c, isCodexCLI, isMessagesBridgeRequest) {
		markPatchSet("instructions", defaultCodexSynthInstructions(reqModel))
	}
	if shouldMarkOpenAICompatMessagesBridgeContext(c, reqBody) {
		setOpenAICompatMessagesBridgeContext(c, true)
	}
	isCompactRequest := isOpenAIResponsesCompactPath(c)
	if account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth {
		if _, err := ensureReqBody(); err != nil {
			return nil, err
		}
		isMessagesBridgeRequest = isMessagesBridgeRequest || isOpenAICompatMessagesBridgeRequestBody(reqBody)
		if shouldMarkOpenAICompatMessagesBridgeContext(c, reqBody) {
			setOpenAICompatMessagesBridgeContext(c, true)
		}
	}
	if shouldDecodeOpenAIResponsesImageToolMutationBody(body, codexImageGenerationBridgeEnabled, isMessagesBridgeRequest, allowImageGeneration, imageIntent, account) {
		if _, err := ensureReqBody(); err != nil {
			return nil, err
		}
	}

	// 非透传模式下，仅对 OpenAI OAuth /responses 请求补默认 instructions，
	// 避免影响 API Key、自定义兼容上游或其他平台对“空 instructions”的原始语义。
	if shouldInjectDefaultInstructionsForOpenAIResponses(c, account, isMessagesBridgeRequest, isCompactRequest) && isInstructionsEmpty(reqBody) {
		reqBody["instructions"] = "You are a helpful coding assistant."
		bodyModified = true
		markPatchSet("instructions", "You are a helpful coding assistant.")
	}

	if codexImageGenerationBridgeEnabled && !isMessagesBridgeRequest && ensureOpenAIResponsesImageGenerationTool(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Injected /responses image_generation tool for Codex client")
	}

	if normalizeOpenAIResponsesImageGenerationTools(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized /responses image_generation tool payload")
	}
	if normalizeOpenAIStrictFunctionToolSchemas(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized strict function tool schemas for /responses request")
	}
	if normalizeOpenAIResponseFormatSchemas(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized response_format JSON schemas for /responses request")
	}
	if codexImageGenerationBridgeEnabled && !isMessagesBridgeRequest && applyCodexImageGenerationBridgeInstructions(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Added Codex image_generation bridge instructions")
	}

	if !allowImageGeneration && stripOpenAIImageGenerationTools(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Stripped image_generation tool capability for disabled group")
	} else if allowImageGeneration &&
		accountShouldStripDeclaredImageGenerationTool(account, IsImageGenerationIntentMap(openAIResponsesEndpoint, reqModel, reqBody)) &&
		stripOpenAIImageGenerationTools(reqBody) {
		bodyModified = true
		disablePatch()
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Stripped declared image_generation tool for image-disabled account: %s", account.Name)
	}

	if account.Type == AccountTypeOAuth && account.Platform == PlatformOpenAI && !isCompactRequest && !isMessagesBridgeRequest {
		if store, ok := reqBody["store"].(bool); !ok || store {
			reqBody["store"] = false
			bodyModified = true
			disablePatch()
		}
	}
	if account.Type == AccountTypeOAuth && !isCompactRequest && !isMessagesBridgeRequest {
		if stream, ok := reqBody["stream"].(bool); !ok || !stream {
			reqBody["stream"] = true
			bodyModified = true
			disablePatch()
		}
		if inputStr, ok := reqBody["input"].(string); ok {
			if strings.TrimSpace(inputStr) != "" {
				reqBody["input"] = []any{
					map[string]any{
						"type":    "message",
						"role":    "user",
						"content": inputStr,
					},
				}
			} else {
				reqBody["input"] = []any{}
			}
			bodyModified = true
			disablePatch()
		}
	}

	// 对所有请求执行模型映射（包含 Codex CLI）。
	routingCompact := isOpenAIResponsesCompactPath(c)
	modelRouting := ResolveEffectiveModelRouting(ctx, s.settingService, account, reqModel, routingCompact)
	billingModel := modelRouting.Model
	compactRoutingApplied := false
	if routingCompact {
		nonCompactModel := ResolveEffectiveMappedModel(ctx, s.settingService, account, reqModel, false)
		compactRoutingApplied = nonCompactModel != "" && billingModel != "" && !strings.EqualFold(nonCompactModel, billingModel)
	}
	if billingModel != reqModel {
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Model mapping applied: %s -> %s (account: %s, isCodexCLI: %v)", reqModel, billingModel, account.Name, isCodexCLI)
		reqModel = billingModel
		markPatchSet("model", billingModel)
	}
	upstreamModel := billingModel
	if normalizeOpenAIResponsesImageOnlyModel(reqBody, resolveOpenAIResponsesImageMainModel(c.Request.Context())) {
		bodyModified = true
		disablePatch()
		if model, ok := reqBody["model"].(string); ok {
			upstreamModel = strings.TrimSpace(model)
		}
		logger.LegacyPrintf(
			"service.openai_gateway",
			"[OpenAI] Normalized /responses image-only model request inbound_model=%s image_model=%s upstream_model=%s",
			reqModel,
			billingModel,
			upstreamModel,
		)
	}
	if err := validateOpenAIResponsesImageModel(reqBody, upstreamModel); err != nil {
		setOpsUpstreamError(c, http.StatusBadRequest, err.Error(), "")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"type":    "invalid_request_error",
				"message": err.Error(),
				"param":   "model",
			},
		})
		return nil, err
	}
	if hasOpenAIImageGenerationTool(reqBody) {
		logger.LegacyPrintf(
			"service.openai_gateway",
			"[OpenAI] /responses image_generation tool declared inbound_model=%s mapped_model=%s account_type=%s",
			reqModel,
			upstreamModel,
			account.Type,
		)
	}
	if err := validateCodexSparkInput(reqBody, upstreamModel); err != nil {
		setOpsUpstreamError(c, http.StatusBadRequest, err.Error(), "")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"type":    "invalid_request_error",
				"message": err.Error(),
				"param":   "input",
			},
		})
		return nil, err
	}

	// Compact-only model 映射：仅在 /responses/compact 路径生效，且优先级高于
	// OAuth 模型规范化（避免 OAuth 规范化覆盖 compact-only 自定义模型）。
	compactMappedModel := ""
	compactMapped := compactRoutingApplied
	if compactRoutingApplied {
		compactMappedModel = billingModel
	} else if isCompactRequest {
		compactMappedModel = resolveOpenAICompactForwardModel(account, billingModel)
		if compactMappedModel != "" && compactMappedModel != billingModel {
			compactMapped = true
			upstreamModel = compactMappedModel
			reqModel = compactMappedModel
			markPatchSet("model", compactMappedModel)
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Compact model mapping applied: %s -> %s (account: %s, isCodexCLI: %v)", billingModel, compactMappedModel, account.Name, isCodexCLI)
		}
	}

	// OpenAI OAuth 账号走 ChatGPT internal Codex endpoint，需要将模型名规范化为
	// 上游可识别的 Codex/GPT 系列。API Key 账号则应保留原始/映射后的模型名，
	// 以兼容自定义 base_url 的 OpenAI-compatible 上游。
	if model, ok := reqBody["model"].(string); ok {
		shouldNormalizeOAuthUpstreamModel := account.Type == AccountTypeOAuth &&
			account.Platform == PlatformOpenAI &&
			isOpenAIResponsesInboundPath(c)
		if !compactMapped && shouldNormalizeOAuthUpstreamModel {
			upstreamModel = normalizeOpenAIModelForUpstream(account, model)
			if upstreamModel != "" && upstreamModel != model {
				logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Upstream model resolved: %s -> %s (account: %s, type: %s, isCodexCLI: %v)",
					model, upstreamModel, account.Name, account.Type, isCodexCLI)
				reqBody["model"] = upstreamModel
				bodyModified = true
				markPatchSet("model", upstreamModel)
			}
		}

		// 移除 gpt-5.2-codex 以下的版本 verbosity 参数
		// 确保高版本模型向低版本模型映射不报错
		if !ResolveOpenAIModelCapabilities(upstreamModel).SupportsVerbosity {
			if text, ok := reqBody["text"].(map[string]any); ok {
				if _, exists := text["verbosity"]; exists {
					delete(text, "verbosity")
					recordOpenAICompatStrippedField("verbosity")
				}
			}
		}
	}
	if strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String()) == "minimal" {
		markPatchSet("reasoning.effort", "none")
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized reasoning.effort: minimal -> none (account: %s)", account.Name)
	}

	imageIntent = imageIntent || IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, nil) || isOpenAIImageGenerationModel(upstreamModel)
	if imageIntent && !imageGenerationAllowed {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalFeatureGate)
		c.JSON(http.StatusForbidden, gin.H{"error": gin.H{"type": "permission_error", "message": ImageGenerationPermissionMessage()}})
		return nil, errors.New("image generation disabled for group")
	}

	if imageGenerationAllowed && (codexImageGenerationBridgeEnabled || isOpenAIImageGenerationModel(requestView.Model) || openAIRequestBodyImageGenerationToolNeedsNormalization(body) || isOpenAIImageGenerationModel(upstreamModel)) {
		decoded, decodeErr := ensureReqBody()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if codexImageGenerationBridgeEnabled && ensureOpenAIResponsesImageGenerationTool(decoded) {
			markDecodedModified()
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Injected /responses image_generation tool for Codex client")
		}
		if normalizeOpenAIResponsesImageGenerationTools(decoded) {
			markDecodedModified()
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized /responses image_generation tool payload")
		}
		if normalizeOpenAIResponsesImageOnlyModel(decoded, resolveOpenAIResponsesImageMainModel(c.Request.Context())) {
			markDecodedModified()
			if model, ok := decoded["model"].(string); ok {
				upstreamModel = strings.TrimSpace(model)
			}
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Normalized /responses image-only model request inbound_model=%s image_model=%s upstream_model=%s", requestView.Model, billingModel, upstreamModel)
		}
		if err := validateOpenAIResponsesImageModel(decoded, upstreamModel); err != nil {
			setOpsUpstreamError(c, http.StatusBadRequest, err.Error(), "")
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"type": "invalid_request_error", "message": err.Error(), "param": "model"}})
			return nil, err
		}
		if hasOpenAIImageGenerationTool(decoded) {
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] /responses image_generation request inbound_model=%s mapped_model=%s account_type=%s", requestView.Model, upstreamModel, account.Type)
		}
		if codexImageGenerationBridgeEnabled && applyCodexImageGenerationBridgeInstructions(decoded) {
			markDecodedModified()
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Added Codex image_generation bridge instructions")
		}
	} else if imageGenerationAllowed && imageIntent && openAIRequestBodyHasImageGenerationTool(body) {
		// 完整 image_generation tool 只做 raw 计费读取，校验/桥接/旧字段迁移命中时才展开大 input map。
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] /responses image_generation request inbound_model=%s mapped_model=%s account_type=%s", requestView.Model, upstreamModel, account.Type)
	}

	if isCodexSparkModel(upstreamModel) && openAIRequestBodyMayContainImageInput(body) {
		decoded, decodeErr := ensureReqBody()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if err := validateCodexSparkInput(decoded, upstreamModel); err != nil {
			setOpsUpstreamError(c, http.StatusBadRequest, err.Error(), "")
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"type": "invalid_request_error", "message": err.Error(), "param": "input"}})
			return nil, err
		}
	}
	if reqBody != nil {
		if trimOpenAIStoreFalseReasoningItems(reqBody) {
			bodyModified = true
			disablePatch()
		}
	} else if openAIRequestBodyStoreFalseMayContainReasoningInputItem(body) {
		decoded, decodeErr := ensureReqBody()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if trimOpenAIStoreFalseReasoningItems(decoded) {
			markDecodedModified()
		}
	}

	if account.Type == AccountTypeOAuth {
		shouldApplyOAuthCodexTransform := isCodexCLI || isMessagesBridgeRequest
		if !shouldApplyOAuthCodexTransform {
			goto oauthTransformDone
		}
		codexInputMode := codexTransformInputModeStrict
		if wsDecision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 || NeedsToolContinuation(reqBody) {
			codexInputMode = codexTransformInputModePreservePrefix
		}
		codexResult := applyCodexOAuthTransformWithInputModeAndOptions(
			reqBody,
			isCodexCLI,
			isCompactRequest,
			codexInputMode,
			codexOAuthTransformOptions{
				SkipDefaultInstructions: isMessagesBridgeRequest && !shouldInjectDefaultInstructionsForOpenAIMessagesBridge(c),
			},
		)
		if c != nil {
			c.Set(openAICodexTransformObsKey, codexResult.Observability)
		}
		if codexResult.Modified {
			markDecodedModified()
		}
		// 带真实 device_id 时补齐 client_metadata 安装标识，与真实 Codex 对齐（compact 形态不同，跳过）。
		if !isCompactRequest && applyCodexClientMetadata(reqBody, account) {
			markDecodedModified()
		}
		if compactMapped && compactMappedModel != "" {
			if currentModel, ok := reqBody["model"].(string); !ok || strings.TrimSpace(currentModel) != compactMappedModel {
				reqBody["model"] = compactMappedModel
				bodyModified = true
				disablePatch()
			}
			upstreamModel = compactMappedModel
		} else if codexResult.NormalizedModel != "" {
			upstreamModel = codexResult.NormalizedModel
		}
		if codexResult.PromptCacheKey != "" {
			promptCacheKey = codexResult.PromptCacheKey
			setOpenAIRoutingPromptCacheKey(c, promptCacheKey)
		}
		if isMessagesBridgeRequest {
			if _, hasPromptCacheKey := reqBody["prompt_cache_key"]; hasPromptCacheKey {
				delete(reqBody, "prompt_cache_key")
				bodyModified = true
				markPatchDelete("prompt_cache_key")
			}
		}
		if input, ok := reqBody["input"].([]any); ok && sanitizeOpenAIResponsesOrphanToolOutputs(reqBody, input, strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"])) != "") {
			bodyModified = true
			disablePatch()
		}
		if streamValue, ok := reqBody["stream"].(bool); ok {
			reqStream = streamValue
		}
	}
oauthTransformDone:

	// OpenAI /responses upstream rejects max_output_tokens and max_completion_tokens
	// regardless of whether the request looks like Codex CLI traffic, so strip them
	// unconditionally for OpenAI OAuth/API Key accounts.
	if account.Platform == PlatformOpenAI {
		stripTokenLimitField := func(field string) error {
			if account.Type != AccountTypeAPIKey && account.Type != AccountTypeOAuth {
				return nil
			}
			if reqBody != nil {
				if _, has := reqBody[field]; !has {
					return nil
				}
				delete(reqBody, field)
				bodyModified = true
				markPatchDelete(field)
				return nil
			}
			if !gjson.GetBytes(body, field).Exists() {
				return nil
			}
			bodyModified = true
			if requestView.patchesDisabled {
				decoded, decodeErr := ensureReqBody()
				if decodeErr != nil {
					return decodeErr
				}
				delete(decoded, field)
				return nil
			}
			markPatchDelete(field)
			return nil
		}
		if err := stripTokenLimitField("max_output_tokens"); err != nil {
			return nil, err
		}
		if err := stripTokenLimitField("max_completion_tokens"); err != nil {
			return nil, err
		}
	}

	// Handle provider-specific token limits and unsupported fields for non-Codex CLI compatibility paths.
	if !isCodexCLI {
		if maxOutputTokens, hasMaxOutputTokens := reqBody["max_output_tokens"]; hasMaxOutputTokens && account.Platform != PlatformOpenAI {
			switch account.Platform {
			case PlatformAnthropic:
				decoded, decodeErr := ensureReqBody()
				if decodeErr != nil {
					return nil, decodeErr
				}
				delete(decoded, "max_output_tokens")
				if _, hasMaxTokens := decoded["max_tokens"]; !hasMaxTokens {
					decoded["max_tokens"] = maxOutputTokens
				}
				markDecodedModified()
			case PlatformGemini:
				markPatchDelete("max_output_tokens")
			default:
				markPatchDelete("max_output_tokens")
			}
		}

		// Keep this list semantically aligned with cursorResponsesUnsupportedFields
		// in openai_gateway_chat_completions.go so direct /v1/responses and
		// responses-shaped /v1/chat/completions short-circuit apply the same
		// unsupported-parameter stripping behavior.
		unsupportedFields := append([]string(nil), openAIResponsesUnsupportedFields...)
		if shouldStripTopPForResponsesUpstream(account) {
			unsupportedFields = append(unsupportedFields, "top_p")
		}
		for _, unsupportedField := range unsupportedFields {
			hasUnsupportedField := false
			if reqBody != nil {
				_, hasUnsupportedField = reqBody[unsupportedField]
			} else {
				hasUnsupportedField = gjson.GetBytes(body, unsupportedField).Exists()
			}
			if hasUnsupportedField {
				if reqBody != nil {
					delete(reqBody, unsupportedField)
				}
				bodyModified = true
				markPatchDelete(unsupportedField)
				if unsupportedField == "temperature" || unsupportedField == "top_p" || unsupportedField == "verbosity" {
					recordOpenAICompatStrippedField(unsupportedField)
				}
			}
		}
	}
	if wsDecision.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 && gjson.GetBytes(body, "previous_response_id").Exists() {
		markPatchDelete("previous_response_id")
	}
	if openAIRequestBodyMayContainEmptyBase64InputImage(body) {
		decoded, decodeErr := ensureReqBody()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if sanitizeEmptyBase64InputImagesInOpenAIRequestBodyMap(decoded) {
			markDecodedModified()
		}
	}

	if sanitizeEmptyBase64InputImagesInOpenAIRequestBodyMap(reqBody) {
		bodyModified = true
		disablePatch()
	}
	if account.Platform == PlatformOpenAI {
		if promptCacheKeyValue, ok := reqBody["prompt_cache_key"].(string); ok && strings.TrimSpace(promptCacheKeyValue) != "" {
			// Preserve prompt-cache-friendly field ordering after any body rewrite.
			disablePatch()
		}
	}

	if bodyModified {
		if requestView.HasPatches() {
			if patchedBody, patchErr := requestView.ApplyPatches(); patchErr == nil {
				resetRawBodyView(patchedBody)
			}
		}
		if bodyModified {
			decoded, decodeErr := ensureReqBody()
			if decodeErr != nil {
				return nil, decodeErr
			}
			var marshalErr error
			body, marshalErr = marshalOpenAIResponsesRequestBodyOrdered(decoded)
			if marshalErr != nil {
				return nil, fmt.Errorf("serialize request body: %w", marshalErr)
			}
			resetRawBodyView(body)
			reqBody = decoded
		}
	}
	if IsImageGenerationIntentMap(openAIResponsesEndpoint, reqModel, reqBody) {
		var imageCfgErr error
		_, imageCfgErr = resolveOpenAIResponsesImageBillingConfigDetailed(reqBody, billingModel)
		if imageCfgErr != nil {
			setOpsUpstreamError(c, http.StatusBadRequest, imageCfgErr.Error(), "")
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"type": "invalid_request_error", "message": imageCfgErr.Error(), "param": "size"}})
			return nil, imageCfgErr
		}
	}
	if account.Platform == PlatformOpenAI {
		filteredBody, policyErr := s.applyOpenAIFastPolicyToBody(ctx, account, upstreamModel, body)
		if policyErr != nil {
			var blocked *OpenAIFastBlockedError
			if errors.As(policyErr, &blocked) {
				writeOpenAIFastPolicyBlockedResponse(c, blocked)
			}
			return nil, policyErr
		}
		if !bytes.Equal(filteredBody, body) {
			resetRawBodyView(filteredBody)
			decoded, decodeErr := ensureReqBody()
			if decodeErr != nil {
				return nil, fmt.Errorf("parse request body after fast policy: %w", decodeErr)
			}
			if v, ok := decoded["stream"].(bool); ok {
				reqStream = v
			}
		}
	}

	if account.Type == AccountTypeOAuth && isOpenAIResponsesCompactPath(c) {
		normalizedBody, normalized, err := normalizeOpenAICompactRequestBody(body)
		if err != nil {
			return nil, err
		}
		if normalized {
			resetRawBodyView(normalizedBody)
		}
		reqStream = gjson.GetBytes(body, "stream").Bool()
	} else if account.Type == AccountTypeOAuth {
		reqStream = clientStream
	}
	finalizedBody, finalized, finalizeErr := finalizeOpenAIResponsesOAuthUpstreamBody(c, account, reqModel, body)
	if finalizeErr != nil {
		return nil, finalizeErr
	}
	if finalized {
		resetRawBodyView(finalizedBody)
	} else {
		body = finalizedBody
	}
	if shouldInjectDefaultInstructionsForOpenAIResponses(c, account, isMessagesBridgeRequest, isCompactRequest) {
		bodyWithInstructions, injected, injectErr := ensureOpenAIPassthroughInstructions(c, reqModel, body)
		if injectErr != nil {
			return nil, injectErr
		}
		if injected {
			resetRawBodyView(bodyWithInstructions)
		}
	}
	upstreamStream = gjson.GetBytes(body, "stream").Bool()
	// Get access token
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}

	// Capture upstream request body for ops retry of this attempt.
	setOpsUpstreamRequestBody(c, body)

	wsSkippedByPayloadPreflight := false
	if wsDecision.Transport == OpenAIUpstreamTransportResponsesWebsocketV2 {
		if skipWS, threshold := s.shouldPreflightFallbackOpenAIWSPayloadToHTTP(body); skipWS {
			wsSkippedByPayloadPreflight = true
			logOpenAIWSModeInfo(
				"preflight_fallback_to_http account_id=%d reason=payload_too_large payload_bytes=%d threshold_bytes=%d action=replay_full_payload",
				account.ID,
				len(body),
				threshold,
			)
		}
	}

	// 命中 WS 时优先走 WebSocket Mode；WS 传输异常且未写下游时可回退同账号 HTTP。
	if wsDecision.Transport == OpenAIUpstreamTransportResponsesWebsocketV2 && !wsSkippedByPayloadPreflight {
		wsHTTPFallbackBody := append([]byte(nil), body...)
		wsHTTPFallbackPromptCacheKey := promptCacheKey
		// WS 分支需要结构化 payload 与重连恢复，命中后再触发 full-map decode。
		wsReqBody, err := ensureReqBody()
		if err != nil {
			return nil, err
		}
		_, hasPreviousResponseID := wsReqBody["previous_response_id"]
		logOpenAIWSModeDebug(
			"forward_start account_id=%d account_type=%s model=%s stream=%v has_previous_response_id=%v",
			account.ID,
			account.Type,
			upstreamModel,
			reqStream,
			hasPreviousResponseID,
		)
		maxAttempts := openAIWSReconnectRetryLimit + 1
		wsAttempts := 0
		var wsResult *OpenAIForwardResult
		var wsErr error
		wsLastFailureReason := ""
		wsPrevResponseRecoveryTried := false
		wsInvalidEncryptedContentRecoveryTried := false
		wsCodexCompatRecoveryTried := false
		wsModelFallbackRecoveryTried := false
		syncWSRecoveredBody := func(stage string) bool {
			nextBody, marshalErr := marshalOpenAIResponsesRequestBodyOrdered(wsReqBody)
			if marshalErr != nil {
				wsErr = wrapOpenAIWSFallback(stage+"_serialize", marshalErr)
				logOpenAIWSModeInfo(
					"reconnect_%s_skip account_id=%d reason=serialize cause=%s",
					normalizeOpenAIWSLogValue(stage),
					account.ID,
					truncateOpenAIWSLogValue(marshalErr.Error(), openAIWSLogValueMaxLen),
				)
				return false
			}
			if account.Type == AccountTypeOAuth && isCompactRequest {
				normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(nextBody)
				if normErr != nil {
					wsErr = wrapOpenAIWSFallback(stage+"_normalize", normErr)
					logOpenAIWSModeInfo(
						"reconnect_%s_skip account_id=%d reason=normalize cause=%s",
						normalizeOpenAIWSLogValue(stage),
						account.ID,
						truncateOpenAIWSLogValue(normErr.Error(), openAIWSLogValueMaxLen),
					)
					return false
				}
				if normalized {
					nextBody = normalizedBody
					nextReqBody := map[string]any{}
					if unmarshalErr := json.Unmarshal(nextBody, &nextReqBody); unmarshalErr != nil {
						wsErr = wrapOpenAIWSFallback(stage+"_parse", unmarshalErr)
						logOpenAIWSModeInfo(
							"reconnect_%s_skip account_id=%d reason=parse cause=%s",
							normalizeOpenAIWSLogValue(stage),
							account.ID,
							truncateOpenAIWSLogValue(unmarshalErr.Error(), openAIWSLogValueMaxLen),
						)
						return false
					}
					wsReqBody = nextReqBody
				}
			}
			body = nextBody
			reqStream = gjson.GetBytes(body, "stream").Bool()
			if bodyPromptCacheKey := strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String()); bodyPromptCacheKey != "" {
				promptCacheKey = bodyPromptCacheKey
				setOpenAIRoutingPromptCacheKey(c, promptCacheKey)
			} else if promptCacheKey == "" {
				promptCacheKey = getOpenAIRoutingPromptCacheKey(c)
			}
			clearOpenAIRequestBodyCache(c)
			setOpsUpstreamRequestBody(c, body)
			return true
		}
		recoverPrevResponseNotFound := func(attempt int) bool {
			if wsPrevResponseRecoveryTried {
				return false
			}
			previousResponseID := openAIWSPayloadString(wsReqBody, "previous_response_id")
			activeDeltaRecovery := false
			if previousResponseID == "" {
				previousResponseID = openAIWSActiveDeltaPreviousResponseID(wsErr)
				activeDeltaRecovery = previousResponseID != ""
			}
			if previousResponseID == "" {
				logOpenAIWSModeInfo(
					"reconnect_prev_response_recovery_skip account_id=%d attempt=%d reason=missing_previous_response_id previous_response_id_present=false",
					account.ID,
					attempt,
				)
				return false
			}
			if HasFunctionCallOutput(wsReqBody) {
				replayReqBody := map[string]any{}
				if err := json.Unmarshal(wsHTTPFallbackBody, &replayReqBody); err != nil {
					wsErr = wrapOpenAIWSFallback("previous_response_recovery_parse", err)
					logOpenAIWSModeInfo(
						"reconnect_prev_response_recovery_skip account_id=%d attempt=%d reason=parse_full_payload previous_response_id_present=true active_delta=%v cause=%s",
						account.ID,
						attempt,
						activeDeltaRecovery,
						truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
					)
					return false
				}
				replayInputItems, replayInputExists, replayInputErr := openAIWSExtractNormalizedInputSequence(wsHTTPFallbackBody)
				if replayInputErr != nil {
					wsErr = wrapOpenAIWSFallback("previous_response_recovery_input_parse", replayInputErr)
					logOpenAIWSModeInfo(
						"reconnect_prev_response_recovery_skip account_id=%d attempt=%d reason=parse_full_input previous_response_id_present=true active_delta=%v cause=%s",
						account.ID,
						attempt,
						activeDeltaRecovery,
						truncateOpenAIWSLogValue(replayInputErr.Error(), openAIWSLogValueMaxLen),
					)
					return false
				}
				if !replayInputExists || !openAIWSRawItemsHaveToolCallContextForOutputs(replayInputItems) {
					logOpenAIWSModeInfo(
						"reconnect_prev_response_recovery_skip account_id=%d attempt=%d reason=missing_tool_call_context previous_response_id_present=true active_delta=%v",
						account.ID,
						attempt,
						activeDeltaRecovery,
					)
					return false
				}
				wsReqBody = replayReqBody
			}
			delete(wsReqBody, "previous_response_id")
			wsReqBody["store"] = false
			trimOpenAIStoreFalseReasoningItems(wsReqBody)
			if !syncWSRecoveredBody("prev_response_recovery") {
				return false
			}
			wsPrevResponseRecoveryTried = true
			s.RecordOpenAIAccountRecoveryReason(account.ID, "previous_response_not_found")
			logOpenAIWSModeInfo(
				"reconnect_prev_response_recovery account_id=%d attempt=%d action=drop_previous_response_id_full_replay retry=1 previous_response_id=%s previous_response_id_kind=%s active_delta=%v has_function_call_output=%v",
				account.ID,
				attempt,
				truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(ClassifyOpenAIPreviousResponseIDKind(previousResponseID)),
				activeDeltaRecovery,
				HasFunctionCallOutput(wsReqBody),
			)
			return true
		}
		recoverInvalidEncryptedContent := func(attempt int) bool {
			if wsInvalidEncryptedContentRecoveryTried {
				return false
			}
			removedReasoningItems := trimOpenAIEncryptedReasoningItems(wsReqBody)
			if !removedReasoningItems {
				logOpenAIWSModeInfo(
					"reconnect_invalid_encrypted_content_recovery_skip account_id=%d attempt=%d reason=missing_encrypted_reasoning_items",
					account.ID,
					attempt,
				)
				return false
			}
			previousResponseID := openAIWSPayloadString(wsReqBody, "previous_response_id")
			hasFunctionCallOutput := HasFunctionCallOutput(wsReqBody)
			if previousResponseID != "" && !hasFunctionCallOutput {
				delete(wsReqBody, "previous_response_id")
			}
			if !syncWSRecoveredBody("invalid_encrypted_content_recovery") {
				return false
			}
			wsInvalidEncryptedContentRecoveryTried = true
			s.RecordOpenAIAccountRecoveryReason(account.ID, "invalid_encrypted_content")
			logOpenAIWSModeInfo(
				"reconnect_invalid_encrypted_content_recovery account_id=%d attempt=%d action=drop_encrypted_reasoning_items retry=1 previous_response_id_present=%v previous_response_id=%s previous_response_id_kind=%s has_function_call_output=%v dropped_previous_response_id=%v",
				account.ID,
				attempt,
				previousResponseID != "",
				truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
				normalizeOpenAIWSLogValue(ClassifyOpenAIPreviousResponseIDKind(previousResponseID)),
				hasFunctionCallOutput,
				previousResponseID != "" && !hasFunctionCallOutput,
			)
			return true
		}
		recoverModelUnavailable := func(attempt int) bool {
			if wsModelFallbackRecoveryTried {
				return false
			}
			currentModel := strings.TrimSpace(upstreamModel)
			if currentModel == "" {
				currentModel = openAIWSPayloadString(wsReqBody, "model")
			}
			fallbackModel := resolveConfiguredFallbackModel(ctx, s.settingService, PlatformOpenAI, currentModel)
			if fallbackModel == "" {
				logOpenAIWSModeInfo(
					"reconnect_model_fallback_skip account_id=%d attempt=%d reason=no_configured_fallback current_model=%s",
					account.ID,
					attempt,
					normalizeOpenAIWSLogValue(currentModel),
				)
				return false
			}
			var fallbackUpstreamModel string
			if isCompactRequest {
				fallbackUpstreamModel = resolveOpenAICompactFallbackUpstreamModel(ctx, s.settingService, account, fallbackModel)
			} else {
				fallbackUpstreamModel = ResolveEffectiveMappedModel(ctx, s.settingService, account, fallbackModel, false)
				if normalized := normalizeOpenAIModelForUpstream(account, fallbackUpstreamModel); normalized != "" {
					fallbackUpstreamModel = normalized
				}
			}
			if fallbackUpstreamModel == "" || strings.EqualFold(fallbackUpstreamModel, currentModel) {
				logOpenAIWSModeInfo(
					"reconnect_model_fallback_skip account_id=%d attempt=%d reason=same_or_empty current_model=%s fallback_model=%s",
					account.ID,
					attempt,
					normalizeOpenAIWSLogValue(currentModel),
					normalizeOpenAIWSLogValue(fallbackUpstreamModel),
				)
				return false
			}

			wsReqBody["model"] = fallbackUpstreamModel
			nextBody, marshalErr := marshalOpenAIResponsesRequestBodyOrdered(wsReqBody)
			if marshalErr != nil {
				wsErr = wrapOpenAIWSFallback("model_fallback_serialize", marshalErr)
				logOpenAIWSModeInfo(
					"reconnect_model_fallback_fail account_id=%d attempt=%d reason=serialize current_model=%s fallback_model=%s cause=%s",
					account.ID,
					attempt,
					normalizeOpenAIWSLogValue(currentModel),
					normalizeOpenAIWSLogValue(fallbackUpstreamModel),
					truncateOpenAIWSLogValue(marshalErr.Error(), openAIWSLogValueMaxLen),
				)
				return false
			}
			if account.Type == AccountTypeOAuth && isCompactRequest {
				normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(nextBody)
				if normErr != nil {
					wsErr = wrapOpenAIWSFallback("model_fallback_normalize", normErr)
					logOpenAIWSModeInfo(
						"reconnect_model_fallback_fail account_id=%d attempt=%d reason=normalize current_model=%s fallback_model=%s cause=%s",
						account.ID,
						attempt,
						normalizeOpenAIWSLogValue(currentModel),
						normalizeOpenAIWSLogValue(fallbackUpstreamModel),
						truncateOpenAIWSLogValue(normErr.Error(), openAIWSLogValueMaxLen),
					)
					return false
				}
				if normalized {
					nextBody = normalizedBody
					nextReqBody := map[string]any{}
					if unmarshalErr := json.Unmarshal(nextBody, &nextReqBody); unmarshalErr != nil {
						wsErr = wrapOpenAIWSFallback("model_fallback_parse", unmarshalErr)
						logOpenAIWSModeInfo(
							"reconnect_model_fallback_fail account_id=%d attempt=%d reason=parse current_model=%s fallback_model=%s cause=%s",
							account.ID,
							attempt,
							normalizeOpenAIWSLogValue(currentModel),
							normalizeOpenAIWSLogValue(fallbackUpstreamModel),
							truncateOpenAIWSLogValue(unmarshalErr.Error(), openAIWSLogValueMaxLen),
						)
						return false
					}
					wsReqBody = nextReqBody
				}
				reqStream = gjson.GetBytes(nextBody, "stream").Bool()
			}

			body = nextBody
			upstreamModel = fallbackUpstreamModel
			wsModelFallbackRecoveryTried = true
			s.RecordOpenAIAccountRecoveryReason(account.ID, "model_unavailable")
			clearOpenAIRequestBodyCache(c)
			setOpsUpstreamRequestBody(c, body)
			logOpenAIWSModeInfo(
				"reconnect_model_fallback account_id=%d attempt=%d retry=1 current_model=%s fallback_model=%s",
				account.ID,
				attempt,
				normalizeOpenAIWSLogValue(currentModel),
				normalizeOpenAIWSLogValue(fallbackUpstreamModel),
			)
			return true
		}
		recoverCodexCompat := func(attempt int, reason string) bool {
			if wsCodexCompatRecoveryTried {
				return false
			}
			if account.Type != AccountTypeOAuth {
				return false
			}
			reason = strings.TrimSpace(reason)
			if reason == "" {
				return false
			}
			if !isOpenAICodexCompatFallbackReason(reason) {
				return false
			}
			codexResult := applyOpenAIWSCodexCompatFallback(wsReqBody, c, isCodexCLI, isCompactRequest, reason)
			if !codexResult.Modified {
				logOpenAIWSModeInfo(
					"reconnect_codex_compat_recovery_skip account_id=%d attempt=%d reason=%s modified=false",
					account.ID,
					attempt,
					normalizeOpenAIWSLogValue(reason),
				)
				return false
			}
			if !syncWSRecoveredBody("codex_compat_recovery") {
				return false
			}
			if strings.TrimSpace(codexResult.PromptCacheKey) != "" {
				promptCacheKey = strings.TrimSpace(codexResult.PromptCacheKey)
				setOpenAIRoutingPromptCacheKey(c, promptCacheKey)
			}
			wsCodexCompatRecoveryTried = true
			s.RecordOpenAIAccountRecoveryReason(account.ID, reason)
			logOpenAIWSModeInfo(
				"reconnect_codex_compat_recovery account_id=%d attempt=%d reason=%s retry=1",
				account.ID,
				attempt,
				normalizeOpenAIWSLogValue(reason),
			)
			return true
		}
		retryBudget := s.openAIWSRetryTotalBudget()
		retryStartedAt := time.Now()
	wsRetryLoop:
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			wsAttempts = attempt
			wsResult, wsErr = s.forwardOpenAIWSV2(
				ctx,
				c,
				account,
				wsReqBody,
				token,
				wsDecision,
				isCodexCLI,
				reqStream,
				originalModel,
				upstreamModel,
				startTime,
				attempt,
				wsLastFailureReason,
			)
			if wsErr == nil {
				break
			}
			if c != nil && c.Writer != nil && c.Writer.Written() {
				break
			}

			reason, retryable := classifyOpenAIWSReconnectReason(wsErr)
			if reason != "" {
				wsLastFailureReason = reason
			}
			// previous_response_not_found 说明续链锚点不可用：
			// 对非 function_call_output 场景，允许一次“去掉 previous_response_id 后重放”。
			if reason == "previous_response_not_found" && recoverPrevResponseNotFound(attempt) {
				continue
			}
			if reason == "invalid_encrypted_content" && recoverInvalidEncryptedContent(attempt) {
				continue
			}
			if strings.TrimPrefix(reason, "prewarm_") == "model_unavailable" && recoverModelUnavailable(attempt) {
				continue
			}
			if recoverCodexCompat(attempt, reason) {
				continue
			}
			if retryable && attempt < maxAttempts {
				backoff := s.openAIWSRetryBackoff(attempt)
				if retryBudget > 0 && time.Since(retryStartedAt)+backoff > retryBudget {
					s.recordOpenAIWSRetryExhausted()
					logOpenAIWSModeInfo(
						"reconnect_budget_exhausted account_id=%d attempts=%d max_retries=%d reason=%s elapsed_ms=%d budget_ms=%d",
						account.ID,
						attempt,
						openAIWSReconnectRetryLimit,
						normalizeOpenAIWSLogValue(reason),
						time.Since(retryStartedAt).Milliseconds(),
						retryBudget.Milliseconds(),
					)
					break
				}
				s.recordOpenAIWSRetryAttempt(backoff)
				logOpenAIWSModeInfo(
					"reconnect_retry account_id=%d retry=%d max_retries=%d reason=%s backoff_ms=%d",
					account.ID,
					attempt,
					openAIWSReconnectRetryLimit,
					normalizeOpenAIWSLogValue(reason),
					backoff.Milliseconds(),
				)
				if backoff > 0 {
					timer := time.NewTimer(backoff)
					select {
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						wsErr = wrapOpenAIWSFallback("retry_backoff_canceled", ctx.Err())
						break wsRetryLoop
					case <-timer.C:
					}
				}
				continue
			}
			if retryable {
				s.recordOpenAIWSRetryExhausted()
				logOpenAIWSModeInfo(
					"reconnect_exhausted account_id=%d attempts=%d max_retries=%d reason=%s",
					account.ID,
					attempt,
					openAIWSReconnectRetryLimit,
					normalizeOpenAIWSLogValue(reason),
				)
			} else if reason != "" {
				s.recordOpenAIWSNonRetryableFastFallback()
				logOpenAIWSModeInfo(
					"reconnect_stop account_id=%d attempt=%d reason=%s",
					account.ID,
					attempt,
					normalizeOpenAIWSLogValue(reason),
				)
			}
			break
		}
		if wsErr == nil {
			firstTokenMs := int64(0)
			hasFirstTokenMs := wsResult != nil && wsResult.FirstTokenMs != nil
			if hasFirstTokenMs {
				firstTokenMs = int64(*wsResult.FirstTokenMs)
			}
			requestID := ""
			if wsResult != nil {
				requestID = strings.TrimSpace(wsResult.RequestID)
			}
			logOpenAIWSModeDebug(
				"forward_succeeded account_id=%d request_id=%s stream=%v has_first_token_ms=%v first_token_ms=%d ws_attempts=%d",
				account.ID,
				requestID,
				reqStream,
				hasFirstTokenMs,
				firstTokenMs,
				wsAttempts,
			)
			wsResult.UpstreamModel = upstreamModel
			return wsResult, nil
		}
		if suppressFailover := s.prepareOpenAIWSContinuationFailoverBody(c, account, wsErr, wsReqBody); suppressFailover {
			s.writeOpenAIWSFallbackErrorResponse(c, account, wsErr)
			return nil, wsErr
		}
		if failoverErr := s.newOpenAIWSFailoverError(c, account, wsErr); failoverErr != nil {
			return nil, failoverErr
		}
		if c != nil && c.Writer != nil && !c.Writer.Written() &&
			shouldFallbackOpenAIWSToHTTP(wsErr) &&
			!HasToolContinuationOutputInRawPayload(wsHTTPFallbackBody) {
			reason, _ := classifyOpenAIWSReconnectReason(wsErr)
			body = append([]byte(nil), wsHTTPFallbackBody...)
			reqStream = gjson.GetBytes(body, "stream").Bool()
			upstreamStream = reqStream
			promptCacheKey = wsHTTPFallbackPromptCacheKey
			clearOpenAIRequestBodyCache(c)
			setOpsUpstreamRequestBody(c, body)
			logOpenAIWSModeInfo(
				"fallback_to_http account_id=%d reason=%s action=replay_full_payload bytes=%d",
				account.ID,
				normalizeOpenAIWSLogValue(reason),
				len(body),
			)
		} else {
			s.writeOpenAIWSFallbackErrorResponse(c, account, wsErr)
			return nil, wsErr
		}
	}

	httpInvalidEncryptedContentRetryTried := false
	httpCodexCompatRetryTried := false
	httpModelFallbackRetryTried := false
	httpInstructionsRetryTried := false
	httpUnsupportedPreviousResponseIDRetryTried := false
	httpReasoningEnabledRetryTried := false
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	for {
		// Build upstream request
		upstreamCtx, releaseUpstreamCtx := detachStreamUpstreamContext(ctx, upstreamStream)
		upstreamReq, err := s.buildUpstreamRequest(upstreamCtx, c, account, body, token, upstreamStream, promptCacheKey, isCodexCLI)
		releaseUpstreamCtx()
		if err != nil {
			return nil, err
		}
		s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, account.IsOpenAIPassthroughEnabled())

		// Get proxy URL
		proxyURL := ""
		if account.ProxyID != nil && account.Proxy != nil {
			proxyURL = account.Proxy.URL()
		}

		// Send request
		SetOpsLatencyMs(c, OpsOpenAIForwardPrepareLatencyMsKey, time.Since(startTime).Milliseconds())
		upstreamStart := time.Now()
		resp, err := s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
		SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
		if err != nil {
			safeErr := sanitizeUpstreamErrorMessage(err.Error())
			emitOpenAICodexCompatFallbackEvent(
				ctx,
				c,
				account,
				originalBody,
				body,
				codexCompatFallbackState,
				"request_error",
				0,
				"",
				safeErr,
				nil,
			)
			return nil, s.handleOpenAIUpstreamTransportError(ctx, c, account, err, false)
		}

		// Handle error response
		if resp.StatusCode >= 400 {
			respBody := s.readUpstreamErrorBody(resp)
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(respBody))

			upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
			upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
			upstreamCode := extractUpstreamErrorCode(respBody)
			if !httpModelFallbackRetryTried {
				currentModel := strings.TrimSpace(upstreamModel)
				if currentModel == "" {
					currentModel = strings.TrimSpace(gjson.GetBytes(body, "model").String())
				}
				if fallbackModel := resolveConfiguredFallbackModel(ctx, s.settingService, PlatformOpenAI, currentModel); fallbackModel != "" &&
					isUpstreamModelUnavailableForFallback(resp.StatusCode, respBody) {
					httpModelFallbackRetryTried = true
					var fallbackUpstreamModel string
					if isCompactRequest {
						fallbackUpstreamModel = resolveOpenAICompactFallbackUpstreamModel(ctx, s.settingService, account, fallbackModel)
					} else {
						fallbackUpstreamModel = ResolveEffectiveMappedModel(ctx, s.settingService, account, fallbackModel, false)
						if normalized := normalizeOpenAIModelForUpstream(account, fallbackUpstreamModel); normalized != "" {
							fallbackUpstreamModel = normalized
						}
					}
					if fallbackUpstreamModel != "" && !strings.EqualFold(fallbackUpstreamModel, currentModel) {
						reqBody, err = ensureReqBody()
						if err != nil {
							return nil, fmt.Errorf("parse openai model fallback retry body: %w", err)
						}
						reqBody["model"] = fallbackUpstreamModel
						body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
						if err != nil {
							return nil, fmt.Errorf("serialize openai model fallback body: %w", err)
						}
						if account.Type == AccountTypeOAuth && isCompactRequest {
							normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(body)
							if normErr != nil {
								return nil, normErr
							}
							if normalized {
								body = normalizedBody
							}
							reqStream = gjson.GetBytes(body, "stream").Bool()
						}
						resetRawBodyView(body)
						upstreamModel = fallbackUpstreamModel
						setOpsUpstreamRequestBody(c, body)
						logger.LegacyPrintf(
							"service.openai_gateway",
							"[OpenAI] Retrying once with fallback model %s -> %s (account: %s, status: %d)",
							currentModel,
							fallbackUpstreamModel,
							account.Name,
							resp.StatusCode,
						)
						continue
					}
				}
			}
			if !httpInvalidEncryptedContentRetryTried && resp.StatusCode == http.StatusBadRequest &&
				isOpenAIInvalidEncryptedContentError(upstreamCode, upstreamMsg, respBody) {
				reqBody, err = ensureReqBody()
				if err != nil {
					return nil, fmt.Errorf("parse invalid_encrypted_content retry body: %w", err)
				}
				removedReasoningItems := trimOpenAIEncryptedReasoningItems(reqBody)
				previousResponseID := openAIWSPayloadString(reqBody, "previous_response_id")
				hasFunctionCallOutput := HasFunctionCallOutput(reqBody)
				droppedPreviousResponseID := false
				if previousResponseID != "" && !hasFunctionCallOutput {
					delete(reqBody, "previous_response_id")
					droppedPreviousResponseID = true
				}
				if removedReasoningItems || droppedPreviousResponseID {
					body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
					if err != nil {
						return nil, fmt.Errorf("serialize invalid_encrypted_content retry body: %w", err)
					}
					if account.Type == AccountTypeOAuth && isOpenAIResponsesCompactPath(c) {
						normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(body)
						if normErr != nil {
							return nil, normErr
						}
						if normalized {
							body = normalizedBody
						}
						reqStream = gjson.GetBytes(body, "stream").Bool()
					}
					resetRawBodyView(body)
					setOpsUpstreamRequestBody(c, body)
					httpInvalidEncryptedContentRetryTried = true
					s.RecordOpenAIAccountRecoveryReason(account.ID, "invalid_encrypted_content")
					logger.LegacyPrintf(
						"service.openai_gateway",
						"[OpenAI] Retrying non-WSv2 request once after invalid_encrypted_content (account: %s, dropped_reasoning_items=%v, dropped_previous_response_id=%v, has_function_call_output=%v)",
						account.Name,
						removedReasoningItems,
						droppedPreviousResponseID,
						hasFunctionCallOutput,
					)
					continue
				}
				logger.LegacyPrintf(
					"service.openai_gateway",
					"[OpenAI] Skip non-WSv2 invalid_encrypted_content retry because nothing can be dropped (account: %s, previous_response_id_present=%v, has_function_call_output=%v)",
					account.Name,
					previousResponseID != "",
					hasFunctionCallOutput,
				)
			}
			if !httpUnsupportedPreviousResponseIDRetryTried &&
				resp.StatusCode == http.StatusBadRequest &&
				isOpenAIUnsupportedPreviousResponseIDError(upstreamCode, upstreamMsg) {
				reqBody, err = ensureReqBody()
				if err != nil {
					return nil, fmt.Errorf("parse unsupported previous_response_id retry body: %w", err)
				}
				if _, present := reqBody["previous_response_id"]; present && !HasFunctionCallOutput(reqBody) {
					delete(reqBody, "previous_response_id")
					body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
					if err != nil {
						return nil, fmt.Errorf("serialize unsupported previous_response_id retry body: %w", err)
					}
					if account.Type == AccountTypeOAuth && isOpenAIResponsesCompactPath(c) {
						normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(body)
						if normErr != nil {
							return nil, normErr
						}
						if normalized {
							body = normalizedBody
						}
						reqStream = gjson.GetBytes(body, "stream").Bool()
					}
					resetRawBodyView(body)
					setOpsUpstreamRequestBody(c, body)
					httpUnsupportedPreviousResponseIDRetryTried = true
					s.RecordOpenAIAccountRecoveryReason(account.ID, "unsupported_previous_response_id")
					logger.LegacyPrintf(
						"service.openai_gateway",
						"[OpenAI] Retrying non-WSv2 request once after dropping unsupported previous_response_id (account: %s)",
						account.Name,
					)
					continue
				}
			}
			if !httpReasoningEnabledRetryTried &&
				resp.StatusCode == http.StatusBadRequest &&
				isOpenAIUnsupportedReasoningEnabledError(upstreamCode, upstreamMsg, respBody) {
				reqBody, err = ensureReqBody()
				if err != nil {
					return nil, fmt.Errorf("parse unsupported reasoning.enabled retry body: %w", err)
				}
				if dropOpenAIReasoningEnabled(reqBody) {
					body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
					if err != nil {
						return nil, fmt.Errorf("serialize unsupported reasoning.enabled retry body: %w", err)
					}
					if account.Type == AccountTypeOAuth && isOpenAIResponsesCompactPath(c) {
						normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(body)
						if normErr != nil {
							return nil, normErr
						}
						if normalized {
							body = normalizedBody
						}
						reqStream = gjson.GetBytes(body, "stream").Bool()
					}
					resetRawBodyView(body)
					setOpsUpstreamRequestBody(c, body)
					httpReasoningEnabledRetryTried = true
					s.RecordOpenAIAccountRecoveryReason(account.ID, "unsupported_reasoning_enabled")
					logger.LegacyPrintf(
						"service.openai_gateway",
						"[OpenAI] Retrying non-WSv2 request once after dropping unsupported reasoning.enabled (account: %s)",
						account.Name,
					)
					continue
				}
			}
			if !httpInstructionsRetryTried &&
				account.Type == AccountTypeOAuth &&
				shouldInjectDefaultInstructionsForOpenAIResponses(c, account, isMessagesBridgeRequest, isCompactRequest) &&
				isOpenAIInstructionsRequiredError(resp.StatusCode, upstreamMsg, respBody) {
				var injected bool
				body, injected, err = ensureOpenAIPassthroughInstructions(c, reqModel, body)
				if err != nil {
					return nil, fmt.Errorf("serialize instructions retry body: %w", err)
				}
				if injected {
					resetRawBodyView(body)
					setOpsUpstreamRequestBody(c, body)
					httpInstructionsRetryTried = true
					logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Retrying non-WSv2 request once after injecting missing instructions (account: %s)", account.Name)
					continue
				}
			}
			if !httpCodexCompatRetryTried && account.Type == AccountTypeOAuth {
				if fallbackReason := classifyOpenAICodexCompatFallback(resp.StatusCode, upstreamCode, upstreamMsg, respBody); fallbackReason != "" {
					httpCodexCompatRetryTried = true
					codexCompatFallbackState.Triggered = true
					codexCompatFallbackState.Reason = fallbackReason
					if c != nil {
						c.Set(openAICodexCompatFallbackKey, true)
						c.Set(openAICodexCompatFallbackReasonKey, fallbackReason)
					}
					var codexResult codexTransformResult
					reqBody, err = ensureReqBody()
					if err != nil {
						return nil, fmt.Errorf("parse codex compat fallback retry body: %w", err)
					}
					body, codexResult, promptCacheKey, err = remarshalOpenAIOAuthCompatFallbackBody(reqBody, promptCacheKey, fallbackReason)
					if err != nil {
						return nil, fmt.Errorf("serialize codex compat fallback body: %w", err)
					}
					setOpenAIRoutingPromptCacheKey(c, promptCacheKey)
					if c != nil {
						c.Set(openAICodexTransformObsKey, codexResult.Observability)
					}
					codexCompatFallbackState.BodyModified = codexResult.Modified
					if codexResult.Modified {
						if account.Type == AccountTypeOAuth && isOpenAIResponsesCompactPath(c) {
							normalizedBody, normalized, normErr := normalizeOpenAICompactRequestBody(body)
							if normErr != nil {
								return nil, normErr
							}
							if normalized {
								body = normalizedBody
							}
							reqStream = gjson.GetBytes(body, "stream").Bool()
						}
						resetRawBodyView(body)
						setOpsUpstreamRequestBody(c, body)
						logger.LegacyPrintf(
							"service.openai_gateway",
							"[OpenAI] Retrying non-WSv2 request once with Codex compat fallback (account: %s, reason: %s)",
							account.Name,
							fallbackReason,
						)
						continue
					}
					emitOpenAICodexCompatFallbackEvent(
						ctx,
						c,
						account,
						originalBody,
						body,
						codexCompatFallbackState,
						"noop",
						resp.StatusCode,
						upstreamCode,
						upstreamMsg,
						nil,
					)
					codexCompatFallbackState = openAICodexCompatFallbackState{}
				}
			}
			if s.shouldFailoverOpenAIUpstreamResponse(resp.StatusCode, upstreamMsg, respBody) {
				if httpModelFallbackRetryTried && !bytes.Equal(body, originalBody) {
					setOpenAIFailoverRequestBody(c, body)
				}
				emitOpenAICodexCompatFallbackEvent(
					ctx,
					c,
					account,
					originalBody,
					body,
					codexCompatFallbackState,
					"failover",
					resp.StatusCode,
					upstreamCode,
					upstreamMsg,
					nil,
				)
				upstreamDetail := ""
				if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
					maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
					if maxBytes <= 0 {
						maxBytes = 2048
					}
					upstreamDetail = truncateString(string(respBody), maxBytes)
				}
				appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
					Platform:           account.Platform,
					AccountID:          account.ID,
					AccountName:        account.Name,
					UpstreamStatusCode: resp.StatusCode,
					UpstreamRequestID:  resp.Header.Get("x-request-id"),
					Kind:               "failover",
					Message:            upstreamMsg,
					Detail:             upstreamDetail,
				})

				s.handleFailoverSideEffects(ctx, resp, account, upstreamModel)
				return nil, &UpstreamFailoverError{
					StatusCode:             resp.StatusCode,
					ResponseBody:           respBody,
					RetryableOnSameAccount: account.IsPoolMode() && (account.IsPoolModeRetryableStatus(resp.StatusCode) || isOpenAITransientProcessingError(resp.StatusCode, upstreamMsg, respBody)),
				}
			}
			result, handleErr := s.handleErrorResponse(ctx, resp, c, account, body, billingModel)
			emitOpenAICodexCompatFallbackEvent(
				ctx,
				c,
				account,
				originalBody,
				body,
				codexCompatFallbackState,
				"http_error",
				resp.StatusCode,
				upstreamCode,
				upstreamMsg,
				result,
			)
			return result, handleErr
		}
		defer func() { _ = resp.Body.Close() }()

		// Handle normal response
		var usage *OpenAIUsage
		var firstTokenMs *int
		responseID := ""
		imageCount := 0
		if reqStream {
			streamResult, err := s.handleStreamingResponse(ctx, resp, c, account, startTime, originalModel, upstreamModel)
			if err != nil {
				return nil, err
			}
			usage = streamResult.usage
			firstTokenMs = streamResult.firstTokenMs
			responseID = strings.TrimSpace(streamResult.responseID)
			imageCount = streamResult.imageCount
		} else {
			result, err := s.handleNonStreamingResponse(ctx, resp, c, account, originalModel, upstreamModel)
			if err != nil {
				return nil, err
			}
			usage = result.usage
			imageCount = result.imageCount
		}
		s.bindHTTPResponseAccount(ctx, c, account, responseID)

		// Extract and save Codex usage snapshot from response headers (for OAuth accounts)
		if account.Type == AccountTypeOAuth {
			if snapshot := ParseCodexRateLimitHeaders(resp.Header); snapshot != nil {
				s.updateCodexUsageSnapshot(ctx, account.ID, snapshot)
			}
		}

		if usage == nil {
			usage = &OpenAIUsage{}
		}

		reasoningEffort := extractOpenAIReasoningEffortFromBody(body, originalModel)
		serviceTier := extractOpenAIServiceTierFromBody(body)

		result := &OpenAIForwardResult{
			RequestID:       resp.Header.Get("x-request-id"),
			ResponseID:      responseID,
			Usage:           *usage,
			Model:           originalModel,
			UpstreamModel:   upstreamModel,
			ServiceTier:     serviceTier,
			ReasoningEffort: reasoningEffort,
			Stream:          reqStream,
			OpenAIWSMode:    false,
			Duration:        time.Since(startTime),
			FirstTokenMs:    firstTokenMs,
			ImageCount:      imageCount,
		}
		applyOpenAIResponsesImageBillingMeta(result, body, upstreamModel)
		emitOpenAICacheProbeEvent(ctx, c, account, originalBody, body, result, promptCacheKey, false)
		emitOpenAICodexCompatFallbackEvent(
			ctx,
			c,
			account,
			originalBody,
			body,
			codexCompatFallbackState,
			"success",
			http.StatusOK,
			"",
			"",
			result,
		)
		return result, nil
	}
}

func (s *OpenAIGatewayService) forwardOpenAIPassthrough(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	reqModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	originalBody := body
	promptCacheKey := strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String())
	allowImageGeneration := GroupAllowsImageGeneration(apiKeyGroup(getAPIKeyFromContext(c)))
	if isOpenAIResponsesCompactPath(c) {
		compactMappedModel := resolveOpenAICompactUpstreamModel(account, reqModel)
		if compactMappedModel != "" && compactMappedModel != reqModel {
			nextBody, setErr := sjson.SetBytes(body, "model", compactMappedModel)
			if setErr != nil {
				return nil, fmt.Errorf("set compact passthrough model: %w", setErr)
			}
			body = nextBody
		}
	}

	normalizedBaseBody, _, err := normalizeOpenAIPassthroughBaseBody(body, isOpenAIResponsesCompactPath(c), shouldStripTopPForResponsesUpstream(account))
	if err != nil {
		return nil, err
	}
	body = normalizedBaseBody
	if !allowImageGeneration {
		if IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body) {
			c.JSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"type":    "permission_error",
					"message": ImageGenerationPermissionMessage(),
				},
			})
			return nil, errors.New(ImageGenerationPermissionMessage())
		}
		strippedBody, stripped, stripErr := stripOpenAIImageGenerationToolsBytes(body)
		if stripErr != nil {
			return nil, fmt.Errorf("strip image_generation tool capability: %w", stripErr)
		}
		if stripped {
			body = strippedBody
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI passthrough] Stripped image_generation tool capability for disabled group")
		}
	} else if accountShouldStripDeclaredImageGenerationTool(account, IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body)) {
		strippedBody, stripped, stripErr := stripOpenAIImageGenerationToolsBytes(body)
		if stripErr != nil {
			return nil, fmt.Errorf("strip image_generation tool capability: %w", stripErr)
		}
		if stripped {
			body = strippedBody
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI passthrough] Stripped declared image_generation tool for image-disabled account: %s", account.Name)
		}
	}

	if account != nil && account.Type == AccountTypeOAuth {
		bodyWithInstructions, _, err := ensureOpenAIPassthroughInstructions(c, reqModel, body)
		if err != nil {
			return nil, err
		}
		body = bodyWithInstructions
		setOpenAITTFTWatchdogBypass(c, IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body))
		isCompact := isOpenAIResponsesCompactPath(c)
		if rejectReason := detectOpenAIPassthroughInstructionsRejectReason(c, reqModel, body, s != nil && s.cfg != nil && s.cfg.Gateway.ForceCodexCLI); rejectReason != "" {
			rejectMsg := "OpenAI codex passthrough requires a non-empty instructions field"
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
			logOpenAIPassthroughInstructionsRejected(ctx, c, account, reqModel, rejectReason, body)
			c.JSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"type":    "forbidden_error",
					"message": rejectMsg,
				},
			})
			return nil, fmt.Errorf("openai passthrough rejected before upstream: %s", rejectReason)
		}

		normalizedBody, normalized, err := normalizeOpenAIPassthroughOAuthBody(body, isCompact)
		if err != nil {
			return nil, err
		}
		if normalized {
			body = normalizedBody
		}
		if isCompact {
			compactBody, compactNormalized, err := normalizeOpenAICompactRequestBody(body)
			if err != nil {
				return nil, err
			}
			if compactNormalized {
				body = compactBody
			}
		}
		bodyWithInstructions, _, err = ensureOpenAIPassthroughInstructions(c, reqModel, body)
		if err != nil {
			return nil, err
		}
		body = bodyWithInstructions
	}

	sanitizedBody, sanitized, err := sanitizeEmptyBase64InputImagesInOpenAIBody(body)
	if err != nil {
		return nil, err
	}
	if sanitized {
		body = sanitizedBody
	}
	body, _, err = finalizeOpenAIResponsesOAuthUpstreamBody(c, account, reqModel, body)
	if err != nil {
		return nil, err
	}
	policyModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if policyModel == "" {
		policyModel = reqModel
	}
	updatedBody, policyErr := s.applyOpenAIFastPolicyToBody(ctx, account, policyModel, body)
	if policyErr != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(policyErr, &blocked) {
			writeOpenAIFastPolicyBlockedResponse(c, blocked)
		}
		return nil, policyErr
	}
	body = updatedBody
	reqStream := gjson.GetBytes(body, "stream").Bool()
	apiKey := getAPIKeyFromContext(c)
	if upstreamModel := resolveOpenAIAccountUpstreamModelForRequest(c.Request.Context(), s.settingService, account, reqModel, isOpenAIResponsesCompactPath(c)); upstreamModel != "" {
		currentModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
		if !strings.EqualFold(currentModel, upstreamModel) {
			body = ReplaceModelInBody(body, upstreamModel)
		}
	}
	if IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body) && !GroupAllowsImageGeneration(apiKeyGroup(apiKey)) {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalFeatureGate)
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"type":    "permission_error",
				"message": ImageGenerationPermissionMessage(),
			},
		})
		return nil, errors.New("image generation disabled for group")
	}
	if IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body) {
		var imageCfgErr error
		_, imageCfgErr = resolveOpenAIResponsesImageBillingConfigDetailedFromBody(body, reqModel)
		if imageCfgErr != nil {
			setOpsUpstreamError(c, http.StatusBadRequest, imageCfgErr.Error(), "")
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"type":    "invalid_request_error",
					"message": imageCfgErr.Error(),
					"param":   "size",
				},
			})
			return nil, imageCfgErr
		}
	}

	logger.LegacyPrintf("service.openai_gateway",
		"[OpenAI 自动透传] 命中自动透传分支: account=%d name=%s type=%s model=%s stream=%v",
		account.ID,
		account.Name,
		account.Type,
		reqModel,
		reqStream,
	)
	if reqStream && c != nil && c.Request != nil {
		if timeoutHeaders := collectOpenAIPassthroughTimeoutHeaders(c.Request.Header); len(timeoutHeaders) > 0 {
			streamWarnLogger := logger.FromContext(ctx).With(
				zap.String("component", "service.openai_gateway"),
				zap.Int64("account_id", account.ID),
				zap.Strings("timeout_headers", timeoutHeaders),
			)
			if s.isOpenAIPassthroughTimeoutHeadersAllowed() {
				streamWarnLogger.Warn("OpenAI passthrough 透传请求包含超时相关请求头，且当前配置为放行，可能导致上游提前断流")
			} else {
				streamWarnLogger.Warn("OpenAI passthrough 检测到超时相关请求头，将按配置过滤以降低断流风险")
			}
		}
	}

	// Get access token
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}

	setOpsUpstreamRequestBody(c, body)
	if c != nil {
		c.Set("openai_passthrough", true)
	}

	fallbackModelRetried := false
	instructionsRetryTried := false
	invalidEncryptedContentRetryTried := false
	previousResponseIDRetryTried := false
	reasoningEnabledRetryTried := false
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	var upstreamReq *http.Request
	var resp *http.Response
	for {
		upstreamCtx, releaseUpstreamCtx := detachStreamUpstreamContext(ctx, shouldDetachLegacyOAuthPassthroughContext(account, reqStream, body))
		upstreamReq, err = s.buildUpstreamRequestOpenAIPassthrough(upstreamCtx, c, account, body, token, promptCacheKey)
		releaseUpstreamCtx()
		if err != nil {
			return nil, err
		}
		s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, true)

		SetOpsLatencyMs(c, OpsOpenAIForwardPrepareLatencyMsKey, time.Since(startTime).Milliseconds())
		upstreamStart := time.Now()
		resp, err = s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
		SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
		if err != nil {
			return nil, s.handleOpenAIUpstreamTransportError(ctx, c, account, err, true)
		}

		if resp.StatusCode < 400 {
			break
		}

		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
		currentModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
		if !fallbackModelRetried {
			if fallbackModel := resolveConfiguredFallbackModel(ctx, s.settingService, PlatformOpenAI, currentModel); fallbackModel != "" &&
				isUpstreamModelUnavailableForFallback(resp.StatusCode, respBody) {
				var fallbackUpstreamModel string
				if isOpenAIResponsesCompactPath(c) {
					fallbackUpstreamModel = resolveOpenAICompactFallbackUpstreamModel(ctx, s.settingService, account, fallbackModel)
				} else {
					fallbackUpstreamModel = ResolveEffectiveMappedModel(ctx, s.settingService, account, fallbackModel, false)
					if normalized := normalizeOpenAIModelForUpstream(account, fallbackUpstreamModel); normalized != "" {
						fallbackUpstreamModel = normalized
					}
				}
				if fallbackUpstreamModel != "" && !strings.EqualFold(fallbackUpstreamModel, currentModel) {
					fallbackBody, setErr := sjson.SetBytes(body, "model", fallbackUpstreamModel)
					if setErr != nil {
						return nil, fmt.Errorf("set openai passthrough fallback model: %w", setErr)
					}
					body = fallbackBody
					setOpsUpstreamRequestBody(c, body)
					fallbackModelRetried = true
					logger.LegacyPrintf(
						"service.openai_gateway",
						"[OpenAI 自动透传] retry once with fallback model account=%d from=%s to=%s",
						account.ID,
						currentModel,
						fallbackUpstreamModel,
					)
					continue
				}
			}
		}
		upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
		upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
		upstreamCode := extractUpstreamErrorCode(respBody)
		isCompact := isOpenAIResponsesCompactPath(c)
		isMessagesBridge := isOpenAICompatMessagesBridgeContext(c) ||
			shouldUseOpenAIMessagesBridgeHeaders(c, body) ||
			isOpenAICompatMessagesBridgePromptCacheKey(strings.TrimSpace(promptCacheKey))
		if !previousResponseIDRetryTried &&
			resp.StatusCode == http.StatusBadRequest &&
			isOpenAIUnsupportedPreviousResponseIDError(upstreamCode, upstreamMsg) {
			var reqBody map[string]any
			if err := json.Unmarshal(body, &reqBody); err != nil {
				return nil, fmt.Errorf("unmarshal passthrough previous_response_id retry body: %w", err)
			}
			previousResponseID := openAIWSPayloadString(reqBody, "previous_response_id")
			hasFunctionCallOutput := HasFunctionCallOutput(reqBody)
			if previousResponseID != "" && !hasFunctionCallOutput {
				delete(reqBody, "previous_response_id")
				body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
				if err != nil {
					return nil, fmt.Errorf("serialize passthrough previous_response_id retry body: %w", err)
				}
				setOpsUpstreamRequestBody(c, body)
				previousResponseIDRetryTried = true
				s.RecordOpenAIAccountRecoveryReason(account.ID, "unsupported_previous_response_id")
				logger.LegacyPrintf(
					"service.openai_gateway",
					"[OpenAI 自动透传] retry once after unsupported previous_response_id account=%d has_function_call_output=%v",
					account.ID,
					hasFunctionCallOutput,
				)
				continue
			}
			logger.LegacyPrintf(
				"service.openai_gateway",
				"[OpenAI 自动透传] skip previous_response_id retry account=%d previous_response_id_present=%v has_function_call_output=%v",
				account.ID,
				previousResponseID != "",
				hasFunctionCallOutput,
			)
		}
		if !reasoningEnabledRetryTried &&
			resp.StatusCode == http.StatusBadRequest &&
			isOpenAIUnsupportedReasoningEnabledError(upstreamCode, upstreamMsg, respBody) {
			var reqBody map[string]any
			if err := json.Unmarshal(body, &reqBody); err != nil {
				return nil, fmt.Errorf("unmarshal passthrough reasoning.enabled retry body: %w", err)
			}
			if dropOpenAIReasoningEnabled(reqBody) {
				body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
				if err != nil {
					return nil, fmt.Errorf("serialize passthrough reasoning.enabled retry body: %w", err)
				}
				setOpsUpstreamRequestBody(c, body)
				reasoningEnabledRetryTried = true
				s.RecordOpenAIAccountRecoveryReason(account.ID, "unsupported_reasoning_enabled")
				logger.LegacyPrintf(
					"service.openai_gateway",
					"[OpenAI 自动透传] retry once after unsupported reasoning.enabled account=%d",
					account.ID,
				)
				continue
			}
			logger.LegacyPrintf(
				"service.openai_gateway",
				"[OpenAI 自动透传] skip reasoning.enabled retry because field is absent account=%d",
				account.ID,
			)
		}
		if !invalidEncryptedContentRetryTried && resp.StatusCode == http.StatusBadRequest &&
			isOpenAIInvalidEncryptedContentError(upstreamCode, upstreamMsg, respBody) {
			var reqBody map[string]any
			if err := json.Unmarshal(body, &reqBody); err != nil {
				return nil, fmt.Errorf("unmarshal passthrough invalid_encrypted_content retry body: %w", err)
			}
			removedReasoningItems := trimOpenAIEncryptedReasoningItems(reqBody)
			previousResponseID := openAIWSPayloadString(reqBody, "previous_response_id")
			hasFunctionCallOutput := HasFunctionCallOutput(reqBody)
			droppedPreviousResponseID := false
			if previousResponseID != "" && !hasFunctionCallOutput {
				delete(reqBody, "previous_response_id")
				droppedPreviousResponseID = true
			}
			if removedReasoningItems || droppedPreviousResponseID {
				body, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
				if err != nil {
					return nil, fmt.Errorf("serialize passthrough invalid_encrypted_content retry body: %w", err)
				}
				setOpsUpstreamRequestBody(c, body)
				invalidEncryptedContentRetryTried = true
				s.RecordOpenAIAccountRecoveryReason(account.ID, "invalid_encrypted_content")
				logger.LegacyPrintf(
					"service.openai_gateway",
					"[OpenAI 自动透传] retry once after invalid_encrypted_content account=%d dropped_reasoning_items=%v dropped_previous_response_id=%v has_function_call_output=%v",
					account.ID,
					removedReasoningItems,
					droppedPreviousResponseID,
					hasFunctionCallOutput,
				)
				continue
			}
			logger.LegacyPrintf(
				"service.openai_gateway",
				"[OpenAI 自动透传] skip invalid_encrypted_content retry because nothing can be dropped account=%d previous_response_id_present=%v has_function_call_output=%v",
				account.ID,
				previousResponseID != "",
				hasFunctionCallOutput,
			)
		}
		if !instructionsRetryTried &&
			account.Type == AccountTypeOAuth &&
			shouldInjectDefaultInstructionsForOpenAIResponses(c, account, isMessagesBridge, isCompact) &&
			isOpenAIInstructionsRequiredError(resp.StatusCode, upstreamMsg, respBody) {
			body, _, err = ensureOpenAIPassthroughInstructions(c, reqModel, body)
			if err != nil {
				return nil, err
			}
			setOpsUpstreamRequestBody(c, body)
			instructionsRetryTried = true
			logger.LegacyPrintf(
				"service.openai_gateway",
				"[OpenAI 自动透传] retry once after upstream instructions_required account=%d model=%s",
				account.ID,
				currentModel,
			)
			continue
		}

		// 透传模式默认保持原样代理；但容量、网关和 Cloudflare transient
		// 错误应先触发多账号 failover 以维持基础 SLA。大请求类错误留给
		// 错误改写规则返回明确 413，不做无效重试。
		if shouldFailoverOpenAIPassthroughResponse(resp.StatusCode, respBody) {
			if fallbackModelRetried {
				setOpenAIFailoverRequestBody(c, body)
			}
			return nil, s.handleFailoverErrorResponsePassthrough(ctx, resp, c, account, body)
		}
		return nil, s.handleErrorResponsePassthrough(ctx, resp, c, account, body)
	}
	defer func() { _ = resp.Body.Close() }()

	var usage *OpenAIUsage
	var firstTokenMs *int
	responseID := ""
	imageCount := 0
	upstreamPassthroughModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if reqStream {
		result, err := s.handleStreamingResponsePassthrough(ctx, resp, c, account, startTime, reqModel, upstreamPassthroughModel)
		if err != nil {
			return nil, err
		}
		usage = result.usage
		firstTokenMs = result.firstTokenMs
		responseID = strings.TrimSpace(result.responseID)
		imageCount = result.imageCount
	} else {
		result, err := s.handleNonStreamingResponsePassthrough(ctx, resp, c, account, reqModel, upstreamPassthroughModel)
		if err != nil {
			return nil, err
		}
		usage = result.usage
		responseID = strings.TrimSpace(result.responseID)
		imageCount = result.imageCount
	}
	s.bindHTTPResponseAccount(ctx, c, account, responseID)

	if snapshot := ParseCodexRateLimitHeaders(resp.Header); snapshot != nil {
		s.updateCodexUsageSnapshot(ctx, account.ID, snapshot)
	}

	if usage == nil {
		usage = &OpenAIUsage{}
	}

	result := &OpenAIForwardResult{
		RequestID:       resp.Header.Get("x-request-id"),
		ResponseID:      responseID,
		Usage:           *usage,
		Model:           reqModel,
		UpstreamModel:   upstreamPassthroughModel,
		ServiceTier:     extractOpenAIServiceTierFromBody(body),
		ReasoningEffort: reasoningEffort,
		Stream:          reqStream,
		OpenAIWSMode:    false,
		Duration:        time.Since(startTime),
		FirstTokenMs:    firstTokenMs,
		ImageCount:      imageCount,
	}
	applyOpenAIResponsesImageBillingMeta(result, body, upstreamPassthroughModel)
	emitOpenAICacheProbeEvent(ctx, c, account, originalBody, body, result, promptCacheKey, true)
	return result, nil
}

func logOpenAIPassthroughInstructionsRejected(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	reqModel string,
	rejectReason string,
	body []byte,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	accountID := int64(0)
	accountName := ""
	accountType := ""
	if account != nil {
		accountID = account.ID
		accountName = strings.TrimSpace(account.Name)
		accountType = strings.TrimSpace(string(account.Type))
	}
	fields := []zap.Field{
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", accountID),
		zap.String("account_name", accountName),
		zap.String("account_type", accountType),
		zap.String("request_model", strings.TrimSpace(reqModel)),
		zap.String("reject_reason", strings.TrimSpace(rejectReason)),
	}
	fields = appendCodexCLIOnlyRejectedRequestFields(fields, c, body)
	logger.FromContext(ctx).With(fields...).Warn("OpenAI passthrough 本地拦截：Codex 请求缺少有效 instructions")
}

func (s *OpenAIGatewayService) buildUpstreamRequestOpenAIPassthrough(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	token string,
	promptCacheKey string,
) (*http.Request, error) {
	targetURL := openaiPlatformAPIURL
	switch account.Type {
	case AccountTypeOAuth:
		targetURL = chatgptCodexURL
	case AccountTypeAPIKey:
		baseURL := account.GetOpenAIBaseURL()
		if baseURL != "" {
			validatedURL, err := s.validateUpstreamBaseURL(baseURL)
			if err != nil {
				return nil, err
			}
			targetURL = buildOpenAIResponsesURL(validatedURL)
		}
	}
	targetURL = appendOpenAIResponsesRequestPathSuffix(targetURL, openAIResponsesRequestPathSuffix(c))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))

	// 透传客户端请求头（安全白名单）。
	allowTimeoutHeaders := s.isOpenAIPassthroughTimeoutHeadersAllowed()
	if c != nil && c.Request != nil {
		for key, values := range c.Request.Header {
			lower := strings.ToLower(strings.TrimSpace(key))
			if !isOpenAIPassthroughAllowedRequestHeader(account, lower, allowTimeoutHeaders) {
				continue
			}
			for _, v := range values {
				req.Header.Add(key, v)
			}
		}
	}

	// 覆盖入站鉴权残留，并注入上游认证
	req.Header.Del("authorization")
	req.Header.Del("x-api-key")
	req.Header.Del("x-goog-api-key")
	req.Header.Set("authorization", "Bearer "+token)

	// OAuth 透传到 ChatGPT internal API 时补齐必要头。
	if account.Type == AccountTypeOAuth {
		req.Host = "chatgpt.com"
		if chatgptAccountID := account.GetChatGPTAccountID(); chatgptAccountID != "" {
			req.Header.Set("chatgpt-account-id", chatgptAccountID)
		}
		compactPath := isOpenAIResponsesCompactPath(c)
		officialClient := isOpenAICodexOfficialOrForcedClientRequest(c, s.cfg)
		isMessagesBridge := shouldUseOpenAIMessagesBridgeHeaders(c, body) ||
			isOpenAICompatMessagesBridgePromptCacheKey(strings.TrimSpace(promptCacheKey))
		clientSessionID := strings.TrimSpace(req.Header.Get("session_id"))
		if clientSessionID == "" {
			clientSessionID = strings.TrimSpace(req.Header.Get("conversation_id"))
		}
		if clientSessionID == "" && (compactPath || officialClient || isMessagesBridge) {
			clientSessionID = strings.TrimSpace(promptCacheKey)
		}
		if compactPath {
			req.Header.Del("conversation_id")
			if strings.TrimSpace(req.Header.Get("accept")) == "" {
				req.Header.Set("accept", "*/*")
			}
			if clientSessionID == "" {
				clientSessionID = resolveOpenAICompactSessionID(c, body)
			}
		} else if req.Header.Get("accept") == "" {
			req.Header.Set("accept", "text/event-stream")
		}
		if compactPath || officialClient || isMessagesBridge {
			req.Header.Del("conversation_id")
			req.Header.Del("OpenAI-Beta")
		} else if req.Header.Get("OpenAI-Beta") == "" {
			req.Header.Set("OpenAI-Beta", "responses=experimental")
		}
		if req.Header.Get("originator") == "" {
			req.Header.Set("originator", resolveOpenAIUpstreamOriginator(c, officialClient))
		}
		if clientSessionID != "" {
			req.Header.Set("session_id", clientSessionID)
		}
		if !compactPath && !officialClient && !isMessagesBridge {
			apiKeyID := getAPIKeyIDFromContext(c)
			req.Header.Del("session_id")
			req.Header.Del("conversation_id")
			if clientSessionID != "" {
				req.Header.Set("session_id", isolateOpenAISessionID(apiKeyID, clientSessionID))
			}
			if promptCacheKey != "" {
				isolated := isolateOpenAISessionID(apiKeyID, promptCacheKey)
				req.Header.Set("conversation_id", isolated)
				if clientSessionID == "" {
					req.Header.Set("session_id", isolated)
				}
			}
		}
	}

	// 透传模式也支持账户自定义 User-Agent 与 ForceCodexCLI 兜底。
	customUA := account.GetOpenAIUserAgent()
	if customUA != "" {
		req.Header.Set("user-agent", customUA)
	}
	if s.cfg != nil && s.cfg.Gateway.ForceCodexCLI {
		req.Header.Set("user-agent", codexCLIUserAgent)
	}

	if req.Header.Get("content-type") == "" {
		req.Header.Set("content-type", "application/json")
	}

	return req, nil
}

func shouldFailoverOpenAIPassthroughResponse(statusCode int, responseBody []byte) bool {
	switch statusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, 520, 524, 529:
		return true
	case http.StatusInternalServerError:
		return !isOpenAILargeRequestUpstreamError(statusCode, responseBody)
	default:
		return false
	}
}

func (s *OpenAIGatewayService) handleFailoverErrorResponsePassthrough(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	requestBody []byte,
) error {
	body := s.readUpstreamErrorBody(resp)

	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
	upstreamDetail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		upstreamDetail = truncateString(string(body), maxBytes)
	}
	setOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)
	logOpenAIInstructionsRequiredDebug(ctx, c, account, resp.StatusCode, upstreamMsg, requestBody, body)
	reqModel, _, _ := extractOpenAIRequestMetaFromBody(requestBody)
	_ = s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body, reqModel)
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:             account.Platform,
		AccountID:            account.ID,
		AccountName:          account.Name,
		UpstreamStatusCode:   resp.StatusCode,
		UpstreamRequestID:    resp.Header.Get("x-request-id"),
		Passthrough:          true,
		Kind:                 "failover",
		Message:              upstreamMsg,
		Detail:               upstreamDetail,
		UpstreamResponseBody: upstreamDetail,
	})
	return &UpstreamFailoverError{
		StatusCode:      resp.StatusCode,
		ResponseBody:    body,
		ResponseHeaders: resp.Header.Clone(),
	}
}

func (s *OpenAIGatewayService) handleErrorResponsePassthrough(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	requestBody []byte,
) error {
	MarkResponseCommitted(c)
	body := s.readUpstreamErrorBody(resp)
	_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)

	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
	upstreamDetail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		upstreamDetail = truncateString(string(body), maxBytes)
	}
	setOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)
	logOpenAIInstructionsRequiredDebug(ctx, c, account, resp.StatusCode, upstreamMsg, requestBody, body)
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:             account.Platform,
		AccountID:            account.ID,
		AccountName:          account.Name,
		UpstreamStatusCode:   resp.StatusCode,
		UpstreamRequestID:    resp.Header.Get("x-request-id"),
		Passthrough:          true,
		Kind:                 "http_error",
		Message:              upstreamMsg,
		Detail:               upstreamDetail,
		UpstreamResponseBody: upstreamDetail,
	})

	// 错误改写规则用于把已知客户端问题（例如大请求）稳定返回给客户端；
	// 命中时不应再触发账号错误策略或冷却。
	if status, errType, errMsg, matched := applyErrorPassthroughRule(
		c,
		PlatformOpenAI,
		resp.StatusCode,
		body,
		http.StatusBadGateway,
		"upstream_error",
		"Upstream request failed",
	); matched {
		c.JSON(status, gin.H{
			"error": gin.H{
				"type":    errType,
				"message": errMsg,
			},
		})
		if upstreamMsg == "" {
			upstreamMsg = errMsg
		}
		if upstreamMsg == "" {
			return fmt.Errorf("upstream error: %d (passthrough rule matched)", resp.StatusCode)
		}
		return fmt.Errorf("upstream error: %d (passthrough rule matched) message=%s", resp.StatusCode, upstreamMsg)
	}

	// 透传模式保留原始上游错误响应，但运行态账号状态仍需更新，
	// 避免粘性路由继续复用刚被限流的账号。
	reqModel, _, _ := extractOpenAIRequestMetaFromBody(requestBody)
	_ = s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body, reqModel)

	writeOpenAIPassthroughResponseHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	c.Data(resp.StatusCode, contentType, body)

	if upstreamMsg == "" {
		return fmt.Errorf("upstream error: %d", resp.StatusCode)
	}
	return fmt.Errorf("upstream error: %d message=%s", resp.StatusCode, upstreamMsg)
}

func isOpenAIPassthroughAllowedRequestHeader(account *Account, lowerKey string, allowTimeoutHeaders bool) bool {
	if lowerKey == "" {
		return false
	}
	if isOpenAIPassthroughTimeoutHeader(lowerKey) {
		return allowTimeoutHeaders
	}
	return shouldForwardOpenAIPassthroughHeader(account, lowerKey)
}

func isOpenAIPassthroughTimeoutHeader(lowerKey string) bool {
	switch lowerKey {
	case "x-stainless-timeout", "x-stainless-read-timeout", "x-stainless-connect-timeout", "x-request-timeout", "request-timeout", "grpc-timeout":
		return true
	default:
		return false
	}
}

func (s *OpenAIGatewayService) isOpenAIPassthroughTimeoutHeadersAllowed() bool {
	return s != nil && s.cfg != nil && s.cfg.Gateway.OpenAIPassthroughAllowTimeoutHeaders
}

func collectOpenAIPassthroughTimeoutHeaders(h http.Header) []string {
	if h == nil {
		return nil
	}
	var matched []string
	for key, values := range h {
		lowerKey := strings.ToLower(strings.TrimSpace(key))
		if isOpenAIPassthroughTimeoutHeader(lowerKey) {
			entry := lowerKey
			if len(values) > 0 {
				entry = fmt.Sprintf("%s=%s", lowerKey, strings.Join(values, "|"))
			}
			matched = append(matched, entry)
		}
	}
	sort.Strings(matched)
	return matched
}

type openaiStreamingResultPassthrough struct {
	usage        *OpenAIUsage
	firstTokenMs *int
	responseID   string
	imageCount   int
}

type openaiNonStreamingResultPassthrough struct {
	usage      *OpenAIUsage
	responseID string
	imageCount int
}

func openAIStreamClientOutputStarted(c *gin.Context, localStarted bool) bool {
	if localStarted {
		return true
	}
	return c != nil && c.Writer != nil && c.Writer.Written()
}

func openAIStreamEventIsPreamble(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "response.created", "response.in_progress":
		return true
	default:
		return false
	}
}

func openAIStreamDataStartsClientOutput(data, eventType string) bool {
	trimmed := strings.TrimSpace(data)
	if trimmed == "" {
		return false
	}
	if strings.TrimSpace(eventType) == "response.failed" {
		return false
	}
	return !openAIStreamEventIsPreamble(eventType)
}

func openAIStreamFailedEventShouldFailover(payload []byte, message string) bool {
	if _, matched := classifyOpenAIRetryableOverload(payload, message); matched {
		return true
	}
	code := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String()))
	if code == "" {
		code = strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.code").String()))
	}
	errType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.error.type").String()))
	if errType == "" {
		errType = strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.type").String()))
	}
	combined := strings.ToLower(strings.TrimSpace(message + " " + code + " " + errType))
	if combined == "" {
		return true
	}
	nonRetryableMarkers := []string{
		"invalid_request",
		"content_policy",
		"policy",
		"safety",
		"high-risk cyber",
		"not allowed",
		"violat",
		"context window",
		"context_length_exceeded",
		"model_context_window_exceeded",
		"exceeds the context",
	}
	for _, marker := range nonRetryableMarkers {
		if strings.Contains(combined, marker) {
			return false
		}
	}
	return true
}

const openAIRetryableOverloadClientMessage = "Upstream service overloaded, please retry later"

func classifyOpenAIRetryableOverload(payload []byte, message string) (string, bool) {
	if len(payload) == 0 && strings.TrimSpace(message) == "" {
		return "", false
	}
	code := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String()))
	if code == "" {
		code = strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.code").String()))
	}
	errType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.error.type").String()))
	if errType == "" {
		errType = strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "error.type").String()))
	}
	errMsg := strings.ToLower(strings.TrimSpace(extractOpenAISSEErrorMessage(payload)))
	combined := strings.ToLower(strings.TrimSpace(strings.Join([]string{message, errMsg, code, errType}, " ")))
	if combined == "" {
		return "", false
	}
	markers := []string{
		"server_is_overloaded",
		"service_unavailable_error",
		"selected model is at capacity",
		"our servers are currently overloaded",
		"please try a different model",
	}
	for _, marker := range markers {
		if strings.Contains(combined, marker) {
			if message = strings.TrimSpace(message); message != "" {
				return sanitizeUpstreamErrorMessage(message), true
			}
			if errMsg != "" {
				return sanitizeUpstreamErrorMessage(errMsg), true
			}
			return openAIRetryableOverloadClientMessage, true
		}
	}
	return "", false
}

func (s *OpenAIGatewayService) shouldEnableOpenAITTFTWatchdog(c *gin.Context, account *Account) bool {
	if account == nil {
		return false
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return false
	}
	if !isOpenAIResponsesInboundPath(c) {
		return false
	}
	if shouldBypassOpenAITTFTWatchdog(c) {
		return false
	}
	return openAITTFTWatchdogTimeout > 0
}

func (s *OpenAIGatewayService) applyOpenAITTFTCooldown(ctx context.Context, account *Account, model string) {
	if s == nil || s.accountRepo == nil || account == nil || openAITTFTCooldownDuration <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	until := now.Add(openAITTFTCooldownDuration)
	state := &TempUnschedState{
		UntilUnix:       until.Unix(),
		TriggeredAtUnix: now.Unix(),
		StatusCode:      http.StatusGatewayTimeout,
		MatchedKeyword:  "ttft_timeout",
		RuleIndex:       -1,
		ErrorMessage: fmt.Sprintf(
			"TTFT watchdog timeout after %s for model: %s",
			openAITTFTWatchdogTimeout,
			strings.TrimSpace(model),
		),
	}

	reason := state.ErrorMessage
	if raw, err := json.Marshal(state); err == nil {
		reason = string(raw)
	}
	if err := s.accountRepo.SetTempUnschedulable(ctx, account.ID, until, reason); err != nil {
		logger.FromContext(ctx).Warn(
			"openai.ttft_watchdog_set_temp_unsched_failed",
			zap.Int64("account_id", account.ID),
			zap.Error(err),
		)
		return
	}
	if s.rateLimitService != nil && s.rateLimitService.tempUnschedCache != nil {
		if err := s.rateLimitService.tempUnschedCache.SetTempUnsched(ctx, account.ID, state); err != nil {
			logger.FromContext(ctx).Warn(
				"openai.ttft_watchdog_set_temp_unsched_cache_failed",
				zap.Int64("account_id", account.ID),
				zap.Error(err),
			)
		}
	}
}

func (s *OpenAIGatewayService) newOpenAITTFTTimeoutFailoverError(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	passthrough bool,
	upstreamRequestID string,
	model string,
) *UpstreamFailoverError {
	if ctx == nil {
		ctx = context.Background()
	}
	s.applyOpenAITTFTCooldown(ctx, account, model)
	message := fmt.Sprintf(
		"No forwardable event received within %s before first token; switching account",
		openAITTFTWatchdogTimeout,
	)
	if c != nil {
		setOpsUpstreamError(c, http.StatusGatewayTimeout, message, "")
		event := OpsUpstreamErrorEvent{
			Platform:           PlatformOpenAI,
			UpstreamStatusCode: http.StatusGatewayTimeout,
			UpstreamRequestID:  strings.TrimSpace(upstreamRequestID),
			Passthrough:        passthrough,
			Kind:               "failover",
			Message:            message,
		}
		if account != nil {
			event.Platform = account.Platform
			event.AccountID = account.ID
			event.AccountName = account.Name
		}
		appendOpsUpstreamError(c, event)
	}
	logger.FromContext(ctx).Warn(
		"openai.ttft_watchdog_failover",
		zap.Int64("account_id", func() int64 {
			if account == nil {
				return 0
			}
			return account.ID
		}()),
		zap.String("model", strings.TrimSpace(model)),
		zap.Duration("ttft_timeout", openAITTFTWatchdogTimeout),
		zap.Duration("cooldown", openAITTFTCooldownDuration),
		zap.Bool("passthrough", passthrough),
	)
	body, _ := json.Marshal(gin.H{
		"error": gin.H{
			"type":    "upstream_timeout",
			"message": message,
		},
	})
	return &UpstreamFailoverError{
		StatusCode:   http.StatusGatewayTimeout,
		ResponseBody: body,
	}
}

func (s *OpenAIGatewayService) newOpenAIStreamFailoverError(
	c *gin.Context,
	account *Account,
	passthrough bool,
	upstreamRequestID string,
	payload []byte,
	message string,
) *UpstreamFailoverError {
	message = sanitizeUpstreamErrorMessage(strings.TrimSpace(message))
	if message == "" {
		message = "OpenAI stream disconnected before completion"
	}
	detail := ""
	if len(payload) > 0 && s != nil && s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = truncateString(string(payload), maxBytes)
	}
	if c != nil {
		setOpsUpstreamError(c, http.StatusBadGateway, message, detail)
		event := OpsUpstreamErrorEvent{
			Platform:           PlatformOpenAI,
			UpstreamStatusCode: http.StatusBadGateway,
			UpstreamRequestID:  strings.TrimSpace(upstreamRequestID),
			Passthrough:        passthrough,
			Kind:               "failover",
			Message:            message,
			Detail:             detail,
		}
		if account != nil {
			event.Platform = account.Platform
			event.AccountID = account.ID
			event.AccountName = account.Name
		}
		appendOpsUpstreamError(c, event)
	}
	body, _ := json.Marshal(gin.H{
		"error": gin.H{
			"type":    "upstream_error",
			"message": message,
		},
	})
	return &UpstreamFailoverError{
		StatusCode:   http.StatusBadGateway,
		ResponseBody: body,
	}
}

func (s *OpenAIGatewayService) newOpenAIRetryableOverloadFailoverError(
	_ context.Context,
	c *gin.Context,
	account *Account,
	passthrough bool,
	upstreamRequestID string,
	payload []byte,
	message string,
) *UpstreamFailoverError {
	message, matched := classifyOpenAIRetryableOverload(payload, message)
	if !matched {
		message = openAIRetryableOverloadClientMessage
	} else if message == "" {
		message = openAIRetryableOverloadClientMessage
	}
	detail := ""
	if len(payload) > 0 && s != nil && s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = truncateString(string(payload), maxBytes)
	}
	if c != nil {
		setOpsUpstreamError(c, http.StatusServiceUnavailable, message, detail)
		event := OpsUpstreamErrorEvent{
			Platform:           PlatformOpenAI,
			UpstreamStatusCode: http.StatusServiceUnavailable,
			UpstreamRequestID:  strings.TrimSpace(upstreamRequestID),
			Passthrough:        passthrough,
			Kind:               "failover",
			Message:            message,
			Detail:             detail,
		}
		if account != nil {
			event.Platform = account.Platform
			event.AccountID = account.ID
			event.AccountName = account.Name
		}
		appendOpsUpstreamError(c, event)
	}
	body, _ := json.Marshal(gin.H{
		"error": gin.H{
			"type":    "upstream_error",
			"message": openAIRetryableOverloadClientMessage,
		},
	})
	return &UpstreamFailoverError{
		StatusCode:             http.StatusServiceUnavailable,
		ResponseBody:           body,
		RetryableOnSameAccount: true,
	}
}

func (s *OpenAIGatewayService) newOpenAISoftRateLimitFailoverError(
	_ context.Context,
	c *gin.Context,
	account *Account,
	passthrough bool,
	upstreamRequestID string,
	payload []byte,
	message string,
) *UpstreamFailoverError {
	message = sanitizeUpstreamErrorMessage(strings.TrimSpace(message))
	if message == "" {
		message = "Approaching upstream rate limits; switch account and retry"
	}
	detail := ""
	if len(payload) > 0 && s != nil && s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = truncateString(string(payload), maxBytes)
	}
	if c != nil {
		setOpsUpstreamError(c, http.StatusTooManyRequests, message, detail)
		event := OpsUpstreamErrorEvent{
			Platform:           PlatformOpenAI,
			UpstreamStatusCode: http.StatusTooManyRequests,
			UpstreamRequestID:  strings.TrimSpace(upstreamRequestID),
			Passthrough:        passthrough,
			Kind:               "failover",
			Message:            message,
			Detail:             detail,
		}
		if account != nil {
			event.Platform = account.Platform
			event.AccountID = account.ID
			event.AccountName = account.Name
		}
		appendOpsUpstreamError(c, event)
	}
	body, _ := json.Marshal(gin.H{
		"error": gin.H{
			"type":    "rate_limit_error",
			"message": message,
		},
	})
	return &UpstreamFailoverError{
		StatusCode:   http.StatusTooManyRequests,
		ResponseBody: body,
	}
}

func (s *OpenAIGatewayService) handleStreamingResponsePassthrough(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	startTime time.Time,
	originalModel string,
	mappedModel string,
) (*openaiStreamingResultPassthrough, error) {
	writeOpenAIPassthroughResponseHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)

	// SSE headers
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	if v := resp.Header.Get("x-request-id"); v != "" {
		c.Header("x-request-id", v)
	}

	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming not supported")
	}

	usage := &OpenAIUsage{}
	var firstTokenMs *int
	responseID := ""
	clientDisconnected := false
	sawDone := false
	sawTerminalEvent := false
	sawSuccessfulTerminal := false
	sawFailedEvent := false
	failedMessage := ""
	clientOutputStarted := false
	upstreamRequestID := strings.TrimSpace(resp.Header.Get("x-request-id"))
	imageCounter := newOpenAIImageOutputCounter()
	resultWithUsage := func() *openaiStreamingResultPassthrough {
		return &openaiStreamingResultPassthrough{
			usage:        usage,
			firstTokenMs: firstTokenMs,
			responseID:   responseID,
			imageCount:   imageCounter.Count(),
		}
	}
	ttftWatchdogEnabled := s.shouldEnableOpenAITTFTWatchdog(c, account)
	var ttftTimer *time.Timer
	var ttftCh <-chan time.Time
	stopTTFTWatchdog := func() {
		if ttftTimer == nil {
			return
		}
		if !ttftTimer.Stop() {
			select {
			case <-ttftTimer.C:
			default:
			}
		}
		ttftTimer = nil
		ttftCh = nil
	}
	if ttftWatchdogEnabled {
		ttftTimer = time.NewTimer(openAITTFTWatchdogTimeout)
		ttftCh = ttftTimer.C
		defer stopTTFTWatchdog()
	}
	replayAttempt := beginOpenAIStreamRetryReplayAttempt(c, account.ID)
	pendingFrames := make([]openAICompatSSEFrame, 0, 8)

	writeFrame := func(frame openAICompatSSEFrame) bool {
		frame, emit := replayAttempt.filterFrame(frame)
		if !emit {
			return true
		}
		if _, err := fmt.Fprint(w, openAIStreamFrameString(frame)); err != nil {
			clientDisconnected = true
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI passthrough] Client disconnected during streaming, continue draining upstream for usage: account=%d", account.ID)
			return false
		}
		replayAttempt.recordEmittedFrame(frame)
		clientOutputStarted = true
		flusher.Flush()
		return true
	}
	flushPendingFrames := func() bool {
		for _, pending := range pendingFrames {
			if !writeFrame(pending) {
				return false
			}
		}
		pendingFrames = pendingFrames[:0]
		return true
	}

	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanBuf := getSSEScannerBuf64K()
	scanner.Buffer(scanBuf[:0], maxLineSize)

	needModelReplace := strings.TrimSpace(originalModel) != "" && strings.TrimSpace(mappedModel) != "" && strings.TrimSpace(originalModel) != strings.TrimSpace(mappedModel)
	var parser openAICompatSSEFrameParser
	processFrame := func(frame openAICompatSSEFrame) error {
		eventType, data := openAIStreamFrameEventTypeAndData(frame)
		hasData := strings.TrimSpace(data) != ""
		if needModelReplace && hasData && mappedModel != "" && strings.Contains(data, mappedModel) {
			replacedLine := s.replaceModelInSSELine("data: "+data, mappedModel, originalModel)
			if replacedData, ok := extractOpenAISSEDataLine(replacedLine); ok {
				openAICompatSetSSEFrameData(&frame, replacedData)
				eventType, data = openAIStreamFrameEventTypeAndData(frame)
				hasData = strings.TrimSpace(data) != ""
			}
		}

		if hasData {
			dataBytes := []byte(data)
			_ = s.markOpenAICyberPolicyIfDetected(ctx, account, dataBytes)
			if strings.TrimSpace(data) == "[DONE]" {
				sawDone = true
				if !sawFailedEvent {
					sawSuccessfulTerminal = true
				}
			}
			if openAIStreamEventIsTerminal(data) {
				sawTerminalEvent = true
			}
			if openAIStreamFrameIsSuccessfulTerminal(frame) {
				sawSuccessfulTerminal = true
			}
			if responseID == "" {
				responseID = extractOpenAIResponseIDFromJSONBytes(dataBytes)
			}
			if overloadMsg, matched := classifyOpenAIRetryableOverload(dataBytes, ""); matched {
				if sawSuccessfulTerminal {
					return nil
				}
				return s.newOpenAIRetryableOverloadFailoverError(ctx, c, account, true, upstreamRequestID, dataBytes, overloadMsg)
			}
			if advisoryMsg, matched := classifyOpenAIWSSoftRateLimitAdvisory(dataBytes); matched {
				if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
					return s.newOpenAISoftRateLimitFailoverError(ctx, c, account, true, upstreamRequestID, dataBytes, advisoryMsg)
				}
				return fmt.Errorf("openai passthrough soft rate limit advisory: %s", advisoryMsg)
			}

			forceFlushFailedEvent := false
			if eventType == "response.failed" {
				failedMessage = extractOpenAISSEErrorMessage(dataBytes)
				if !openAIStreamClientOutputStarted(c, clientOutputStarted) && openAIStreamFailedEventShouldFailover(dataBytes, failedMessage) {
					sawFailedEvent = true
					return s.newOpenAIStreamFailoverError(c, account, true, upstreamRequestID, dataBytes, failedMessage)
				}
				forceFlushFailedEvent = true
				sawFailedEvent = true
			}

			lineStartsClientOutput := forceFlushFailedEvent || openAIStreamFrameStartsClientOutput(frame)
			if firstTokenMs == nil && lineStartsClientOutput && strings.TrimSpace(data) != "[DONE]" {
				ms := int(time.Since(startTime).Milliseconds())
				firstTokenMs = &ms
				stopTTFTWatchdog()
			}
			imageCounter.AddSSEData(dataBytes)
			s.parseSSEUsageBytes(dataBytes, usage)
			if clientDisconnected {
				return nil
			}
			if !clientOutputStarted && !lineStartsClientOutput {
				pendingFrames = append(pendingFrames, frame)
				return nil
			}
			if !clientOutputStarted && len(pendingFrames) > 0 {
				if !flushPendingFrames() {
					return nil
				}
			}
			_ = writeFrame(frame)
			return nil
		}

		if clientDisconnected {
			return nil
		}
		if !clientOutputStarted {
			pendingFrames = append(pendingFrames, frame)
			return nil
		}
		_ = writeFrame(frame)
		return nil
	}

	processScannedLine := func(line string) error {
		if frame, ok := parser.AddLine(line); ok {
			if err := processFrame(frame); err != nil {
				return err
			}
		}
		return nil
	}
	finalizeParser := func() error {
		if frame, ok := parser.Finish(); ok {
			if err := processFrame(frame); err != nil {
				return err
			}
		}
		return nil
	}
	handleScanErr := func(err error) error {
		if err == nil {
			return nil
		}
		if sawTerminalEvent && !sawFailedEvent {
			return nil
		}
		if sawFailedEvent {
			return fmt.Errorf("upstream response failed: %s", failedMessage)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("stream usage incomplete: %w", err)
		}
		if errors.Is(err, bufio.ErrTooLong) {
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI passthrough] SSE line too long: account=%d max_size=%d error=%v", account.ID, maxLineSize, err)
			return err
		}
		if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
			msg := "OpenAI stream disconnected before completion"
			if errText := strings.TrimSpace(err.Error()); errText != "" {
				msg += ": " + errText
			}
			return s.newOpenAIStreamFailoverError(c, account, true, upstreamRequestID, nil, msg)
		}
		if clientDisconnected {
			return fmt.Errorf("stream usage incomplete after disconnect: %w", err)
		}
		logger.LegacyPrintf(
			"service.openai_gateway",
			"[OpenAI passthrough] 流读取异常中断: account=%d request_id=%s err=%v",
			account.ID,
			upstreamRequestID,
			err,
		)
		return fmt.Errorf("stream read error: %w", err)
	}

	if !ttftWatchdogEnabled {
		defer putSSEScannerBuf64K(scanBuf)
		for scanner.Scan() {
			if err := processScannedLine(scanner.Text()); err != nil {
				return resultWithUsage(), err
			}
		}
		if err := finalizeParser(); err != nil {
			return resultWithUsage(), err
		}
		if err := handleScanErr(scanner.Err()); err != nil {
			return resultWithUsage(), err
		}
	} else {
		type scanEvent struct {
			line string
			err  error
		}
		events := make(chan scanEvent, 16)
		done := make(chan struct{})
		sendEvent := func(ev scanEvent) bool {
			select {
			case events <- ev:
				return true
			case <-done:
				return false
			}
		}
		go func() {
			defer putSSEScannerBuf64K(scanBuf)
			defer close(events)
			for scanner.Scan() {
				if !sendEvent(scanEvent{line: scanner.Text()}) {
					return
				}
			}
			if err := scanner.Err(); err != nil {
				_ = sendEvent(scanEvent{err: err})
			}
		}()
		defer close(done)

		for {
			select {
			case ev, ok := <-events:
				if !ok {
					if err := finalizeParser(); err != nil {
						return resultWithUsage(), err
					}
					goto passthroughFinalize
				}
				if err := handleScanErr(ev.err); err != nil {
					return resultWithUsage(), err
				}
				if err := processScannedLine(ev.line); err != nil {
					return resultWithUsage(), err
				}
			case <-ttftCh:
				drained := false
				for !drained {
					select {
					case ev, ok := <-events:
						if !ok {
							if frame, ok := parser.Finish(); ok {
								if err := processFrame(frame); err != nil {
									return resultWithUsage(), err
								}
							}
							drained = true
							break
						}
						if err := handleScanErr(ev.err); err != nil {
							return resultWithUsage(), err
						}
						if err := processScannedLine(ev.line); err != nil {
							return resultWithUsage(), err
						}
						if firstTokenMs != nil {
							drained = true
						}
					default:
						drained = true
					}
				}
				if firstTokenMs != nil {
					continue
				}
				return resultWithUsage(), s.newOpenAITTFTTimeoutFailoverError(ctx, c, account, true, upstreamRequestID, originalModel)
			}
		}
	}
passthroughFinalize:
	if sawFailedEvent {
		return resultWithUsage(), fmt.Errorf("upstream response failed: %s", failedMessage)
	}
	if !clientDisconnected && !sawDone && !sawTerminalEvent && ctx.Err() == nil {
		logger.FromContext(ctx).With(
			zap.String("component", "service.openai_gateway"),
			zap.Int64("account_id", account.ID),
			zap.String("upstream_request_id", upstreamRequestID),
		).Info("OpenAI passthrough 上游流在未收到 [DONE] 时结束，疑似断流")
		if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
			return resultWithUsage(),
				s.newOpenAIStreamFailoverError(c, account, true, upstreamRequestID, nil, "OpenAI stream ended before a terminal event")
		}
		return resultWithUsage(), errors.New("stream usage incomplete: missing terminal event")
	}

	return resultWithUsage(), nil
}

func (s *OpenAIGatewayService) handleNonStreamingResponsePassthrough(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	originalModel string,
	mappedModel string,
) (*openaiNonStreamingResultPassthrough, error) {
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		return nil, err
	}
	_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)

	// Detect SSE responses from upstream and convert to JSON.
	// Some upstreams (e.g. other sub2api instances) may return SSE even when
	// stream=false was requested. Without this conversion the client would
	// receive raw SSE text or a terminal event with empty output.
	if isEventStreamResponse(resp.Header) {
		return s.handlePassthroughSSEToJSON(resp, c, account, body, originalModel, mappedModel)
	}
	if advisoryMsg, matched := classifyOpenAIWSSoftRateLimitAdvisory(body); matched {
		return nil, s.newOpenAISoftRateLimitFailoverError(ctx, c, account, true, resp.Header.Get("x-request-id"), body, advisoryMsg)
	}

	usage := &OpenAIUsage{}
	usageParsed := false
	if len(body) > 0 {
		if parsedUsage, ok := extractOpenAIUsageFromJSONBytes(body); ok {
			*usage = parsedUsage
			usageParsed = true
		}
	}
	if !usageParsed {
		// 兜底：尝试从 SSE 文本中解析 usage
		usage = s.parseSSEUsageFromBody(string(body))
	}
	imageCount := countOpenAIResponseImageOutputsFromJSONBytes(body)

	writeOpenAIPassthroughResponseHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	if originalModel != "" && mappedModel != "" && originalModel != mappedModel {
		body = s.replaceModelInResponseBody(body, mappedModel, originalModel)
	}
	c.Data(resp.StatusCode, contentType, body)
	return &openaiNonStreamingResultPassthrough{usage: usage, responseID: extractOpenAIResponseIDFromJSONBytes(body), imageCount: imageCount}, nil
}

// handlePassthroughSSEToJSON converts an SSE response body into a JSON
// response for the passthrough path. It mirrors handleSSEToJSON while
// preserving passthrough payloads, except compact-only model remapping may
// rewrite model fields back to the original requested model.
func (s *OpenAIGatewayService) handlePassthroughSSEToJSON(resp *http.Response, c *gin.Context, account *Account, body []byte, originalModel string, mappedModel string) (*openaiNonStreamingResultPassthrough, error) {
	bodyText := string(body)
	if advisoryMsg, matched := extractOpenAIWSSoftRateLimitAdvisoryFromSSEBody(bodyText); matched {
		return nil, s.newOpenAISoftRateLimitFailoverError(c.Request.Context(), c, account, true, resp.Header.Get("x-request-id"), body, advisoryMsg)
	}
	finalResponse, ok := extractCodexFinalResponse(bodyText)
	if terminalType, terminalPayload, terminalOK := extractOpenAISSETerminalEvent(bodyText); terminalOK {
		if overloadMsg, matched := classifyOpenAIRetryableOverload(terminalPayload, ""); matched || strings.TrimSpace(terminalType) == "error" {
			if strings.TrimSpace(terminalType) == "error" || matched {
				return nil, s.newOpenAIRetryableOverloadFailoverError(c.Request.Context(), c, account, true, resp.Header.Get("x-request-id"), terminalPayload, overloadMsg)
			}
		}
		if terminalType == "response.failed" {
			_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, terminalPayload)
			msg := extractOpenAISSEErrorMessage(terminalPayload)
			if overloadMsg, matched := classifyOpenAIRetryableOverload(terminalPayload, msg); matched {
				return nil, s.newOpenAIRetryableOverloadFailoverError(c.Request.Context(), c, account, true, resp.Header.Get("x-request-id"), terminalPayload, overloadMsg)
			}
			if msg == "" {
				msg = "Upstream compact response failed"
			}
			return nil, s.writeOpenAINonStreamingProtocolError(resp, c, msg)
		}
		if !ok {
			if response := extractOpenAISSETerminalResponse(terminalPayload); len(response) > 0 {
				finalResponse = response
				ok = true
			}
		}
	}

	usage := &OpenAIUsage{}
	imageCount := 0
	if ok {
		if parsedUsage, parsed := extractOpenAIUsageFromJSONBytes(finalResponse); parsed {
			*usage = parsedUsage
		} else {
			usage = s.parseSSEUsageFromBody(bodyText)
		}
		// When the terminal event has an empty output array, reconstruct
		// output from accumulated delta events so the client gets full content.
		if len(gjson.GetBytes(finalResponse, "output").Array()) == 0 {
			if outputJSON, reconstructed := reconstructResponseOutputFromSSE(bodyText); reconstructed {
				if patched, err := sjson.SetRawBytes(finalResponse, "output", outputJSON); err == nil {
					finalResponse = patched
				}
			}
		}
		body = finalResponse
		if originalModel != "" && mappedModel != "" && originalModel != mappedModel {
			body = s.replaceModelInResponseBody(body, mappedModel, originalModel)
		}
		// Correct tool calls in final response
		body = s.correctToolCallsInResponseBody(body)
		imageCount = countOpenAIResponseImageOutputsFromJSONBytes(body)
	} else {
		terminalType, terminalPayload, terminalOK := extractOpenAISSETerminalEvent(bodyText)
		if terminalOK && terminalType == "response.failed" {
			_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, terminalPayload)
			msg := extractOpenAISSEErrorMessage(terminalPayload)
			if msg == "" {
				msg = "Upstream compact response failed"
			}
			return nil, s.writeOpenAINonStreamingProtocolError(resp, c, msg)
		}
		usage = s.parseSSEUsageFromBody(bodyText)
		if originalModel != "" && mappedModel != "" && originalModel != mappedModel {
			bodyText = s.replaceModelInSSEBody(bodyText, mappedModel, originalModel)
		}
		body = []byte(bodyText)
	}

	writeOpenAIPassthroughResponseHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)

	contentType := "application/json; charset=utf-8"
	if !ok {
		contentType = resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "text/event-stream"
		}
	}
	c.Writer.Header().Set("Content-Type", contentType)
	c.Data(resp.StatusCode, contentType, body)

	return &openaiNonStreamingResultPassthrough{usage: usage, responseID: extractOpenAIResponseIDFromJSONBytes(body), imageCount: imageCount}, nil
}

func writeOpenAIPassthroughResponseHeaders(dst http.Header, src http.Header, filter *responseheaders.CompiledHeaderFilter) {
	if dst == nil || src == nil {
		return
	}
	if filter != nil {
		responseheaders.WriteFilteredHeaders(dst, src, filter)
	} else {
		// 兜底：尽量保留最基础的 content-type
		if v := strings.TrimSpace(src.Get("Content-Type")); v != "" {
			dst.Set("Content-Type", v)
		}
	}
	// 透传模式强制放行 x-codex-* 响应头（若上游返回）。
	// 注意：真实 http.Response.Header 的 key 一般会被 canonicalize；但为了兼容测试/自建响应，
	// 这里用 EqualFold 做一次大小写不敏感的查找。
	getCaseInsensitiveValues := func(h http.Header, want string) []string {
		if h == nil {
			return nil
		}
		for k, vals := range h {
			if strings.EqualFold(k, want) {
				return vals
			}
		}
		return nil
	}

	for _, rawKey := range []string{
		"x-codex-primary-used-percent",
		"x-codex-primary-reset-after-seconds",
		"x-codex-primary-window-minutes",
		"x-codex-secondary-used-percent",
		"x-codex-secondary-reset-after-seconds",
		"x-codex-secondary-window-minutes",
		"x-codex-primary-over-secondary-limit-percent",
	} {
		vals := getCaseInsensitiveValues(src, rawKey)
		if len(vals) == 0 {
			continue
		}
		key := http.CanonicalHeaderKey(rawKey)
		dst.Del(key)
		for _, v := range vals {
			dst.Add(key, v)
		}
	}
}

func (s *OpenAIGatewayService) buildUpstreamRequest(ctx context.Context, c *gin.Context, account *Account, body []byte, token string, isStream bool, promptCacheKey string, isCodexCLI bool) (*http.Request, error) {
	// Determine target URL based on account type
	var targetURL string
	switch account.Type {
	case AccountTypeOAuth:
		// OAuth accounts use ChatGPT internal API
		targetURL = chatgptCodexURL
	case AccountTypeAPIKey:
		// API Key accounts use Platform API or custom base URL
		baseURL := account.GetOpenAIBaseURL()
		if baseURL == "" {
			targetURL = openaiPlatformAPIURL
		} else {
			validatedURL, err := s.validateUpstreamBaseURL(baseURL)
			if err != nil {
				return nil, err
			}
			targetURL = buildOpenAIResponsesURL(validatedURL)
		}
	default:
		targetURL = openaiPlatformAPIURL
	}
	targetURL = appendOpenAIResponsesRequestPathSuffix(targetURL, openAIResponsesRequestPathSuffix(c))

	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))

	// Set authentication header
	req.Header.Set("authorization", "Bearer "+token)

	// Set headers specific to OAuth accounts (ChatGPT internal API)
	if account.Type == AccountTypeOAuth {
		// Required: set Host for ChatGPT API (must use req.Host, not Header.Set)
		req.Host = "chatgpt.com"
		// Required: set chatgpt-account-id header
		chatgptAccountID := account.GetChatGPTAccountID()
		if chatgptAccountID != "" {
			req.Header.Set("chatgpt-account-id", chatgptAccountID)
		}
	}

	// Whitelist passthrough headers
	for key, values := range c.Request.Header {
		lowerKey := strings.ToLower(key)
		if shouldForwardOpenAIRequestHeader(account, lowerKey) {
			for _, v := range values {
				req.Header.Add(key, v)
			}
		}
	}
	if account.Type == AccountTypeOAuth {
		req.Header.Del("conversation_id")
		req.Header.Del("session_id")
		isMessagesBridge := shouldUseOpenAIMessagesBridgeHeaders(c, body) ||
			isOpenAICompatMessagesBridgePromptCacheKey(strings.TrimSpace(promptCacheKey))
		if isMessagesBridge {
			req.Header.Del("originator")
		} else {
			req.Header.Set("originator", resolveOpenAIUpstreamOriginator(c, isCodexCLI))
		}
		officialClient := isCodexCLI
		sessionID := resolveOpenAIUpstreamSessionID(c, promptCacheKey)
		if isOpenAIResponsesCompactPath(c) {
			if strings.TrimSpace(req.Header.Get("accept")) == "" {
				req.Header.Set("accept", "*/*")
			}
			if sessionID == "" {
				sessionID = resolveOpenAICompactSessionID(c, body)
			}
		} else {
			if officialClient || isMessagesBridge {
				req.Header.Del("OpenAI-Beta")
			} else {
				req.Header.Set("OpenAI-Beta", "responses=experimental")
			}
			if strings.TrimSpace(req.Header.Get("accept")) == "" {
				req.Header.Set("accept", "text/event-stream")
			}
		}
		if sessionID != "" && (officialClient || isOpenAIResponsesCompactPath(c) || isMessagesBridge) {
			req.Header.Set("session_id", sessionID)
			req.Header.Del("conversation_id")
		}
		if !isOpenAIResponsesCompactPath(c) && !officialClient && !isMessagesBridge {
			apiKeyID := getAPIKeyIDFromContext(c)
			if sessionID != "" {
				req.Header.Set("session_id", isolateOpenAISessionID(apiKeyID, sessionID))
				req.Header.Set("conversation_id", isolateOpenAISessionID(apiKeyID, sessionID))
			}
		}
	}

	// Apply custom User-Agent if configured
	customUA := account.GetOpenAIUserAgent()
	if customUA != "" {
		req.Header.Set("user-agent", customUA)
	}

	// 若开启 ForceCodexCLI，则强制将上游 User-Agent 伪装为 Codex CLI。
	// 用于网关未透传/改写 User-Agent 时，仍能命中 Codex 侧识别逻辑。
	if s.cfg != nil && s.cfg.Gateway.ForceCodexCLI {
		req.Header.Set("user-agent", codexCLIUserAgent)
	}

	// Ensure required headers exist
	if req.Header.Get("content-type") == "" {
		req.Header.Set("content-type", "application/json")
	}

	// Codex CLI session headers: ensure upstream sees standard Codex headers
	if isCodexCLI && promptCacheKey != "" && account.Type == AccountTypeOAuth {
		if req.Header.Get("X-Client-Request-Id") == "" {
			req.Header.Set("X-Client-Request-Id", promptCacheKey)
		}
		if req.Header.Get("Thread-Id") == "" {
			req.Header.Set("Thread-Id", promptCacheKey)
		}
		if req.Header.Get("X-Codex-Window-Id") == "" {
			req.Header.Set("X-Codex-Window-Id", promptCacheKey+":0")
		}
	}

	return req, nil
}

func (s *OpenAIGatewayService) handleErrorResponse(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	requestBody []byte,
	requestedModel ...string,
) (*OpenAIForwardResult, error) {
	body := s.readUpstreamErrorBody(resp)
	_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)

	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
	upstreamDetail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		upstreamDetail = truncateString(string(body), maxBytes)
	}
	setOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)
	logOpenAIInstructionsRequiredDebug(ctx, c, account, resp.StatusCode, upstreamMsg, requestBody, body)

	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		logger.LegacyPrintf("service.openai_gateway",
			"OpenAI upstream error %d (account=%d platform=%s type=%s): %s",
			resp.StatusCode,
			account.ID,
			account.Platform,
			account.Type,
			truncateForLog(body, s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes),
		)
	}

	if status, errType, errMsg, matched := applyErrorPassthroughRule(
		c,
		PlatformOpenAI,
		resp.StatusCode,
		body,
		http.StatusBadGateway,
		"upstream_error",
		"Upstream request failed",
	); matched {
		MarkResponseCommitted(c)
		c.JSON(status, gin.H{
			"error": gin.H{
				"type":    errType,
				"message": errMsg,
			},
		})
		if upstreamMsg == "" {
			upstreamMsg = errMsg
		}
		if upstreamMsg == "" {
			return nil, fmt.Errorf("upstream error: %d (passthrough rule matched)", resp.StatusCode)
		}
		return nil, fmt.Errorf("upstream error: %d (passthrough rule matched) message=%s", resp.StatusCode, upstreamMsg)
	}

	if shouldExposeOpenAIUpstreamClientError(resp.StatusCode, upstreamMsg) {
		clientErrType := openAIClientVisibleErrorType(resp.StatusCode)
		setOpsUpstreamErrorInternal(c, clientErrType, resp.StatusCode, upstreamMsg, upstreamDetail)
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		c.JSON(resp.StatusCode, gin.H{
			"error": gin.H{
				"type":    clientErrType,
				"message": upstreamMsg,
			},
		})
		return nil, fmt.Errorf("upstream error: %d message=%s", resp.StatusCode, upstreamMsg)
	}

	if isOpenAITransientCapacityError(upstreamMsg) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "failover",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           body,
			RetryableOnSameAccount: false,
		}
	}

	// Check custom error codes
	if !account.ShouldHandleErrorCode(resp.StatusCode) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		MarkResponseCommitted(c)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"type":    "upstream_error",
				"message": "Upstream gateway error",
			},
		})
		if upstreamMsg == "" {
			return nil, fmt.Errorf("upstream error: %d (not in custom error codes)", resp.StatusCode)
		}
		return nil, fmt.Errorf("upstream error: %d (not in custom error codes) message=%s", resp.StatusCode, upstreamMsg)
	}

	// Handle upstream error (mark account status)
	var reqModel string
	if len(requestedModel) > 0 {
		reqModel = strings.TrimSpace(requestedModel[0])
	}
	if reqModel == "" {
		reqModel, _, _ = extractOpenAIRequestMetaFromBody(requestBody)
	}
	shouldDisable := s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body, reqModel)
	kind := "http_error"
	if shouldDisable {
		kind = "failover"
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: resp.StatusCode,
		UpstreamRequestID:  resp.Header.Get("x-request-id"),
		Kind:               kind,
		Message:            upstreamMsg,
		Detail:             upstreamDetail,
	})
	if shouldDisable {
		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           body,
			RetryableOnSameAccount: account.IsPoolMode() && account.IsPoolModeRetryableStatus(resp.StatusCode),
		}
	}

	MarkResponseCommitted(c)

	// Return appropriate error response
	var errType, errMsg string
	var statusCode int

	switch resp.StatusCode {
	case 401:
		statusCode = http.StatusBadGateway
		errType = "upstream_error"
		errMsg = "Upstream authentication failed, please contact administrator"
	case 402:
		statusCode = http.StatusBadGateway
		errType = "upstream_error"
		errMsg = "Upstream payment required: insufficient balance or billing issue"
	case 403:
		statusCode = http.StatusBadGateway
		errType = "upstream_error"
		errMsg = "Upstream access forbidden, please contact administrator"
	case 429:
		statusCode = http.StatusTooManyRequests
		errType = "rate_limit_error"
		errMsg = "Upstream rate limit exceeded, please retry later"
	default:
		statusCode = http.StatusBadGateway
		errType = "upstream_error"
		errMsg = "Upstream request failed"
	}

	c.JSON(statusCode, gin.H{
		"error": gin.H{
			"type":    errType,
			"message": errMsg,
		},
	})

	if upstreamMsg == "" {
		return nil, fmt.Errorf("upstream error: %d", resp.StatusCode)
	}
	return nil, fmt.Errorf("upstream error: %d message=%s", resp.StatusCode, upstreamMsg)
}

// compatErrorWriter is the signature for format-specific error writers used by
// the compat paths (Chat Completions and Anthropic Messages).
type compatErrorWriter func(c *gin.Context, statusCode int, errType, message string)

// handleCompatErrorResponse is the shared non-failover error handler for the
// Chat Completions and Anthropic Messages compat paths. It mirrors the logic of
// handleErrorResponse (passthrough rules, ShouldHandleErrorCode, rate-limit
// tracking, secondary failover) but delegates the final error write to the
// format-specific writer function.
func (s *OpenAIGatewayService) handleCompatErrorResponse(
	resp *http.Response,
	c *gin.Context,
	account *Account,
	writeError compatErrorWriter,
	requestedModel ...string,
) (*OpenAIForwardResult, error) {
	body := s.readUpstreamErrorBody(resp)
	requestCtx := context.Background()
	if c != nil && c.Request != nil {
		requestCtx = c.Request.Context()
	}
	_ = s.markOpenAICyberPolicyIfDetected(requestCtx, account, body)

	upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(body))
	if upstreamMsg == "" {
		upstreamMsg = fmt.Sprintf("Upstream error: %d", resp.StatusCode)
	}
	upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)

	upstreamDetail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		upstreamDetail = truncateString(string(body), maxBytes)
	}
	setOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)

	// Apply error passthrough rules
	if status, errType, errMsg, matched := applyErrorPassthroughRule(
		c, account.Platform, resp.StatusCode, body,
		http.StatusBadGateway, "api_error", "Upstream request failed",
	); matched {
		MarkResponseCommitted(c)
		writeError(c, status, errType, errMsg)
		if upstreamMsg == "" {
			upstreamMsg = errMsg
		}
		if upstreamMsg == "" {
			return nil, fmt.Errorf("upstream error: %d (passthrough rule matched)", resp.StatusCode)
		}
		return nil, fmt.Errorf("upstream error: %d (passthrough rule matched) message=%s", resp.StatusCode, upstreamMsg)
	}

	if shouldExposeOpenAIUpstreamClientError(resp.StatusCode, upstreamMsg) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		writeError(c, resp.StatusCode, openAIClientVisibleErrorType(resp.StatusCode), upstreamMsg)
		return nil, fmt.Errorf("upstream error: %d message=%s", resp.StatusCode, upstreamMsg)
	}

	if isOpenAITransientCapacityError(upstreamMsg) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "failover",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           body,
			ResponseHeaders:        resp.Header.Clone(),
			RetryableOnSameAccount: false,
		}
	}

	// Check custom error codes — if the account does not handle this status,
	// return a generic error without exposing upstream details.
	if !account.ShouldHandleErrorCode(resp.StatusCode) {
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  resp.Header.Get("x-request-id"),
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		MarkResponseCommitted(c)
		writeError(c, http.StatusInternalServerError, "api_error", "Upstream gateway error")
		if upstreamMsg == "" {
			return nil, fmt.Errorf("upstream error: %d (not in custom error codes)", resp.StatusCode)
		}
		return nil, fmt.Errorf("upstream error: %d (not in custom error codes) message=%s", resp.StatusCode, upstreamMsg)
	}

	// Track rate limits and decide whether to trigger secondary failover.
	var modelForCooldown string
	if len(requestedModel) > 0 {
		modelForCooldown = requestedModel[0]
	}
	shouldDisable := s.handleOpenAIAccountUpstreamError(
		requestCtx, account, resp.StatusCode, resp.Header, body, modelForCooldown,
	)
	kind := "http_error"
	if shouldDisable {
		kind = "failover"
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: resp.StatusCode,
		UpstreamRequestID:  resp.Header.Get("x-request-id"),
		Kind:               kind,
		Message:            upstreamMsg,
		Detail:             upstreamDetail,
	})
	if shouldDisable {
		return nil, &UpstreamFailoverError{
			StatusCode:             resp.StatusCode,
			ResponseBody:           body,
			RetryableOnSameAccount: account.IsPoolMode() && account.IsPoolModeRetryableStatus(resp.StatusCode),
		}
	}

	MarkResponseCommitted(c)

	// Map status code to error type and write response
	errType := "api_error"
	switch {
	case resp.StatusCode == 400:
		errType = "invalid_request_error"
	case resp.StatusCode == 404:
		errType = "not_found_error"
	case resp.StatusCode == 429:
		errType = "rate_limit_error"
	case resp.StatusCode >= 500:
		errType = "api_error"
	}

	writeError(c, resp.StatusCode, errType, safeCompatUpstreamErrorMessage(resp.StatusCode))
	return nil, fmt.Errorf("upstream error: %d %s", resp.StatusCode, upstreamMsg)
}

func safeCompatUpstreamErrorMessage(statusCode int) string {
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return "Upstream authentication failed"
	case statusCode == http.StatusTooManyRequests:
		return "Upstream rate limit exceeded"
	case statusCode >= http.StatusInternalServerError:
		return "Upstream service temporarily unavailable"
	default:
		return "Upstream request failed"
	}
}

func shouldExposeOpenAIUpstreamClientError(statusCode int, upstreamMsg string) bool {
	if statusCode < http.StatusBadRequest || statusCode >= http.StatusInternalServerError {
		return false
	}
	switch statusCode {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
		return false
	}
	if isOpenAITransientCapacityError(upstreamMsg) {
		return false
	}
	return strings.TrimSpace(upstreamMsg) != ""
}

func isOpenAITransientCapacityError(upstreamMsg string) bool {
	lower := strings.ToLower(strings.TrimSpace(upstreamMsg))
	if lower == "" {
		return false
	}

	return strings.Contains(lower, "at capacity") ||
		strings.Contains(lower, "no capacity available") ||
		strings.Contains(lower, "try a different model")
}

func openAIClientVisibleErrorType(statusCode int) string {
	switch statusCode {
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return "invalid_request_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	default:
		return "upstream_error"
	}
}

// openaiStreamingResult streaming response result
type openaiStreamingResult struct {
	usage        *OpenAIUsage
	firstTokenMs *int
	responseID   string
	imageCount   int
}

type openaiNonStreamingResult struct {
	usage      *OpenAIUsage
	responseID string
	imageCount int
}

const openAIStreamRetryReplayStateKey = "openai_stream_retry_replay_state"

type openAIStreamRetryReplayState struct {
	accountID              int64
	visibleFrameSignatures []string
	emittedTextPrefix      string
}

type openAIStreamRetryReplayAttempt struct {
	state       *openAIStreamRetryReplayState
	active      bool
	frameIndex  int
	textMatched int
}

func getOpenAIStreamRetryReplayState(c *gin.Context) *openAIStreamRetryReplayState {
	if c == nil {
		return &openAIStreamRetryReplayState{}
	}
	if existing, ok := c.Get(openAIStreamRetryReplayStateKey); ok {
		if state, ok := existing.(*openAIStreamRetryReplayState); ok && state != nil {
			return state
		}
	}
	state := &openAIStreamRetryReplayState{}
	c.Set(openAIStreamRetryReplayStateKey, state)
	return state
}

func ClearOpenAIStreamRetryReplayState(c *gin.Context) {
	if c == nil || c.Keys == nil {
		return
	}
	delete(c.Keys, openAIStreamRetryReplayStateKey)
}

func beginOpenAIStreamRetryReplayAttempt(c *gin.Context, accountID int64) *openAIStreamRetryReplayAttempt {
	state := getOpenAIStreamRetryReplayState(c)
	if accountID > 0 && state.accountID > 0 && state.accountID != accountID {
		ClearOpenAIStreamRetryReplayState(c)
		state = getOpenAIStreamRetryReplayState(c)
	}
	if accountID > 0 {
		state.accountID = accountID
	}
	return &openAIStreamRetryReplayAttempt{
		state:  state,
		active: len(state.visibleFrameSignatures) > 0 || state.emittedTextPrefix != "",
	}
}

func (a *openAIStreamRetryReplayAttempt) filterFrame(frame openAICompatSSEFrame) (openAICompatSSEFrame, bool) {
	if a == nil || a.state == nil || !a.active {
		return frame, true
	}
	if delta, ok := openAIStreamReplayDelta(frame); ok {
		if a.textMatched >= len(a.state.emittedTextPrefix) {
			if a.replayComplete() {
				a.active = false
			}
			return frame, true
		}
		remaining := a.state.emittedTextPrefix[a.textMatched:]
		if strings.HasPrefix(remaining, delta) {
			a.textMatched += len(delta)
			if a.replayComplete() {
				a.active = false
			}
			return openAICompatSSEFrame{}, false
		}
		if strings.HasPrefix(delta, remaining) {
			a.textMatched = len(a.state.emittedTextPrefix)
			suffix := delta[len(remaining):]
			if suffix == "" {
				if a.replayComplete() {
					a.active = false
				}
				return openAICompatSSEFrame{}, false
			}
			frame = patchOpenAIStreamReplayDelta(frame, suffix)
			if a.replayComplete() {
				a.active = false
			}
			return frame, true
		}
		a.active = false
		return frame, true
	}
	signature := openAIStreamReplayFrameSignature(frame)
	if signature == "" {
		if a.replayComplete() {
			a.active = false
		}
		return frame, true
	}
	if a.frameIndex < len(a.state.visibleFrameSignatures) && signature == a.state.visibleFrameSignatures[a.frameIndex] {
		a.frameIndex++
		if a.replayComplete() {
			a.active = false
		}
		return openAICompatSSEFrame{}, false
	}
	if a.replayComplete() {
		a.active = false
	}
	return frame, true
}

func (a *openAIStreamRetryReplayAttempt) replayComplete() bool {
	if a == nil || a.state == nil {
		return true
	}
	return a.frameIndex >= len(a.state.visibleFrameSignatures) && a.textMatched >= len(a.state.emittedTextPrefix)
}

func (a *openAIStreamRetryReplayAttempt) recordEmittedFrame(frame openAICompatSSEFrame) {
	if a == nil || a.state == nil {
		return
	}
	if delta, ok := openAIStreamReplayDelta(frame); ok {
		a.state.emittedTextPrefix += delta
		return
	}
	if signature := openAIStreamReplayFrameSignature(frame); signature != "" {
		a.state.visibleFrameSignatures = append(a.state.visibleFrameSignatures, signature)
	}
}

func openAIStreamReplayFrameSignature(frame openAICompatSSEFrame) string {
	eventType, data := openAIStreamFrameEventTypeAndData(frame)
	metadataSignature := openAICompatSSEFrameMetadataSignature(frame)
	trimmed := strings.TrimSpace(data)
	if trimmed == "" {
		if metadataSignature != "" {
			return metadataSignature
		}
		return strings.TrimSpace(frame.EventType)
	}
	if trimmed == "[DONE]" {
		if metadataSignature != "" {
			return metadataSignature + "\x00[DONE]"
		}
		return "[DONE]"
	}
	if !gjson.Valid(trimmed) {
		signature := strings.TrimSpace(frame.EventType) + "\x00" + trimmed
		if metadataSignature != "" {
			return metadataSignature + "\x00" + signature
		}
		return signature
	}
	normalized := []byte(trimmed)
	for _, path := range []string{"response.id", "response.created_at", "response_id", "id", "sequence_number", "item.id"} {
		if updated, err := sjson.DeleteBytes(normalized, path); err == nil {
			normalized = updated
		}
	}
	signature := eventType + "\x00" + strings.TrimSpace(string(normalized))
	if metadataSignature != "" {
		return metadataSignature + "\x00" + signature
	}
	return signature
}

func openAIStreamReplayDelta(frame openAICompatSSEFrame) (string, bool) {
	eventType, data := openAIStreamFrameEventTypeAndData(frame)
	if !strings.HasSuffix(eventType, ".delta") {
		return "", false
	}
	delta := gjson.Get(data, "delta")
	if !delta.Exists() || delta.Type != gjson.String {
		return "", false
	}
	return delta.String(), true
}

func patchOpenAIStreamReplayDelta(frame openAICompatSSEFrame, suffix string) openAICompatSSEFrame {
	data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, frame.EventType))
	if data == "" || data == "[DONE]" {
		return frame
	}
	patched, err := sjson.Set(data, "delta", suffix)
	if err != nil {
		return frame
	}
	openAICompatSetSSEFrameData(&frame, patched)
	return frame
}

func openAIStreamFrameEventTypeAndData(frame openAICompatSSEFrame) (string, string) {
	eventType := strings.TrimSpace(frame.EventType)
	data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, eventType))
	if data != "" && data != "[DONE]" {
		if payloadType := strings.TrimSpace(gjson.Get(data, "type").String()); payloadType != "" {
			eventType = payloadType
		}
	}
	return eventType, data
}

func openAIStreamFrameStartsClientOutput(frame openAICompatSSEFrame) bool {
	eventType, data := openAIStreamFrameEventTypeAndData(frame)
	return openAIStreamDataStartsClientOutput(data, eventType)
}

func openAIStreamFrameIsSuccessfulTerminal(frame openAICompatSSEFrame) bool {
	eventType, data := openAIStreamFrameEventTypeAndData(frame)
	if strings.TrimSpace(data) == "[DONE]" {
		return false
	}
	switch eventType {
	case "response.completed", "response.done":
		return true
	default:
		return false
	}
}

func openAIStreamFrameString(frame openAICompatSSEFrame) string {
	if len(frame.Fields) > 0 {
		var builder strings.Builder
		for _, field := range frame.Fields {
			_, _ = builder.WriteString(field.Raw)
			_ = builder.WriteByte('\n')
		}
		_ = builder.WriteByte('\n')
		return builder.String()
	}
	var builder strings.Builder
	if eventType := strings.TrimSpace(frame.EventType); eventType != "" {
		_, _ = builder.WriteString("event: ")
		_, _ = builder.WriteString(eventType)
		_ = builder.WriteByte('\n')
	}
	data := frame.Data
	if data == "" {
		_, _ = builder.WriteString("data:\n\n")
		return builder.String()
	}
	for _, line := range strings.Split(data, "\n") {
		_, _ = builder.WriteString("data: ")
		_, _ = builder.WriteString(line)
		_ = builder.WriteByte('\n')
	}
	_ = builder.WriteByte('\n')
	return builder.String()
}

func writeOpenAIResponsesFailedSSE(w io.Writer, responseID, model, code, message string) error {
	responseID = strings.TrimSpace(responseID)
	if responseID == "" {
		responseID = "resp_failed"
	}
	code = strings.TrimSpace(code)
	if code == "" {
		code = "upstream_error"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = code
	}
	payload, err := json.Marshal(gin.H{
		"type": "response.failed",
		"response": gin.H{
			"id":     responseID,
			"object": "response",
			"model":  model,
			"status": "failed",
			"output": []any{},
			"error": gin.H{
				"code":    code,
				"message": message,
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: response.failed\ndata: %s\n\n", payload)
	return err
}

func (s *OpenAIGatewayService) handleStreamingResponse(ctx context.Context, resp *http.Response, c *gin.Context, account *Account, startTime time.Time, originalModel, mappedModel string) (*openaiStreamingResult, error) {
	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}

	// Set SSE response headers
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	// Pass through other headers
	if v := resp.Header.Get("x-request-id"); v != "" {
		c.Header("x-request-id", v)
	}

	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming not supported")
	}
	bufferedWriter := bufio.NewWriterSize(w, 4*1024)
	flushBuffered := func() error {
		if err := bufferedWriter.Flush(); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	usage := &OpenAIUsage{}
	var firstTokenMs *int
	responseID := ""
	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanBuf := getSSEScannerBuf64K()
	scanner.Buffer(scanBuf[:0], maxLineSize)

	streamInterval := time.Duration(0)
	if s.cfg != nil && s.cfg.Gateway.StreamDataIntervalTimeout > 0 {
		streamInterval = time.Duration(s.cfg.Gateway.StreamDataIntervalTimeout) * time.Second
	}
	// 仅监控上游数据间隔超时，不被下游写入阻塞影响
	var intervalTicker *time.Ticker
	if streamInterval > 0 {
		intervalTicker = time.NewTicker(streamInterval)
		defer intervalTicker.Stop()
	}
	var intervalCh <-chan time.Time
	if intervalTicker != nil {
		intervalCh = intervalTicker.C
	}

	keepaliveInterval := time.Duration(0)
	if s.cfg != nil && s.cfg.Gateway.StreamKeepaliveInterval > 0 {
		keepaliveInterval = time.Duration(s.cfg.Gateway.StreamKeepaliveInterval) * time.Second
	}
	// 下游 keepalive 仅用于防止代理空闲断开
	var keepaliveTicker *time.Ticker
	if keepaliveInterval > 0 {
		keepaliveTicker = time.NewTicker(keepaliveInterval)
		defer keepaliveTicker.Stop()
	}
	var keepaliveCh <-chan time.Time
	if keepaliveTicker != nil {
		keepaliveCh = keepaliveTicker.C
	}
	// 记录下游最近一次成功 flush 的时间，用于控制 keepalive 发送频率。
	// 仅有上游 preamble / in_progress 而没有真实下游输出时，也应该允许 keepalive。
	lastDataAt := time.Now()

	// 仅发送一次错误事件，避免多次写入导致协议混乱。
	// 注意：OpenAI `/v1/responses` streaming 事件必须符合 OpenAI Responses schema；
	// 否则下游 SDK（例如 OpenCode）会因为类型校验失败而报错。
	errorEventSent := false
	clientDisconnected := false // 客户端断开后继续 drain 上游以收集 usage
	sawTerminalEvent := false
	sawSuccessfulTerminal := false
	sawFailedEvent := false
	failedMessage := ""
	clientOutputStarted := false
	upstreamRequestID := strings.TrimSpace(resp.Header.Get("x-request-id"))
	var streamFailoverErr error
	imageCounter := newOpenAIImageOutputCounter()
	ttftWatchdogEnabled := s.shouldEnableOpenAITTFTWatchdog(c, account)
	var ttftTimer *time.Timer
	var ttftCh <-chan time.Time
	stopTTFTWatchdog := func() {
		if ttftTimer == nil {
			return
		}
		if !ttftTimer.Stop() {
			select {
			case <-ttftTimer.C:
			default:
			}
		}
		ttftTimer = nil
		ttftCh = nil
	}
	if ttftWatchdogEnabled {
		ttftTimer = time.NewTimer(openAITTFTWatchdogTimeout)
		ttftCh = ttftTimer.C
		defer stopTTFTWatchdog()
	}
	replayAttempt := beginOpenAIStreamRetryReplayAttempt(c, account.ID)
	pendingFrames := make([]openAICompatSSEFrame, 0, 8)
	sendErrorEvent := func(reason string) {
		if errorEventSent || clientDisconnected {
			return
		}
		errorEventSent = true
		if err := flushBuffered(); err != nil {
			clientDisconnected = true
			return
		}
		if isOpenAIResponsesInboundPath(c) {
			if err := writeOpenAIResponsesFailedSSE(bufferedWriter, responseID, originalModel, reason, reason); err != nil {
				clientDisconnected = true
				return
			}
			MarkResponseCommitted(c)
		} else {
			payload := `{"type":"error","sequence_number":0,"error":{"type":"upstream_error","message":` + strconv.Quote(reason) + `,"code":` + strconv.Quote(reason) + `}}`
			if _, err := bufferedWriter.WriteString("data: " + payload + "\n\n"); err != nil {
				clientDisconnected = true
				return
			}
		}
		if err := flushBuffered(); err != nil {
			clientDisconnected = true
			return
		}
		clientOutputStarted = true
	}

	needModelReplace := originalModel != mappedModel
	streamOutputAccumulator := apicompat.NewBufferedResponseAccumulator()
	streamImageOutputs := make([]json.RawMessage, 0, 1)
	streamSeenImages := make(map[string]struct{})
	resultWithUsage := func() *openaiStreamingResult {
		return &openaiStreamingResult{usage: usage, firstTokenMs: firstTokenMs, responseID: responseID, imageCount: imageCounter.Count()}
	}
	writeFrame := func(frame openAICompatSSEFrame, shouldFlush bool) bool {
		frame, emit := replayAttempt.filterFrame(frame)
		if !emit {
			return true
		}
		if _, err := bufferedWriter.WriteString(openAIStreamFrameString(frame)); err != nil {
			clientDisconnected = true
			logger.LegacyPrintf("service.openai_gateway", "Client disconnected during streaming, continuing to drain upstream for billing")
			return false
		}
		replayAttempt.recordEmittedFrame(frame)
		if shouldFlush {
			if err := flushBuffered(); err != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "Client disconnected during streaming flush, continuing to drain upstream for billing")
				return false
			}
			clientOutputStarted = true
			lastDataAt = time.Now()
		}
		return true
	}
	flushPendingFrames := func() bool {
		for _, frame := range pendingFrames {
			if !writeFrame(frame, false) {
				return false
			}
		}
		pendingFrames = pendingFrames[:0]
		return true
	}
	finalizeStream := func() (*openaiStreamingResult, error) {
		if !sawTerminalEvent {
			if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
				return resultWithUsage(), s.newOpenAIStreamFailoverError(
					c,
					account,
					false,
					upstreamRequestID,
					nil,
					"OpenAI stream ended before a terminal event",
				)
			}
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: missing terminal event")
		}
		if sawFailedEvent {
			return resultWithUsage(), fmt.Errorf("upstream response failed: %s", failedMessage)
		}
		if !clientDisconnected {
			hadBufferedData := bufferedWriter.Buffered() > 0
			if err := flushBuffered(); err != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "Client disconnected during final flush, returning collected usage")
			} else if hadBufferedData {
				clientOutputStarted = true
			}
		}
		return resultWithUsage(), nil
	}
	handleScanErr := func(scanErr error) (*openaiStreamingResult, error, bool) {
		if scanErr == nil {
			return nil, nil, false
		}
		if sawTerminalEvent && !sawFailedEvent {
			logger.LegacyPrintf("service.openai_gateway", "Upstream scan ended after terminal event: %v", scanErr)
			return resultWithUsage(), nil, true
		}
		if sawFailedEvent {
			return resultWithUsage(), fmt.Errorf("upstream response failed: %s", failedMessage), true
		}
		// 客户端断开/取消请求时，上游读取往往会返回 context canceled。
		// /v1/responses 的 SSE 事件必须符合 OpenAI 协议；这里不注入自定义 error event，避免下游 SDK 解析失败。
		if errors.Is(scanErr, context.Canceled) || errors.Is(scanErr, context.DeadlineExceeded) {
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: %w", scanErr), true
		}
		if errors.Is(scanErr, bufio.ErrTooLong) {
			logger.LegacyPrintf("service.openai_gateway", "SSE line too long: account=%d max_size=%d error=%v", account.ID, maxLineSize, scanErr)
			sendErrorEvent("response_too_large")
			return resultWithUsage(), scanErr, true
		}
		if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
			msg := "OpenAI stream disconnected before completion"
			if errText := strings.TrimSpace(scanErr.Error()); errText != "" {
				msg += ": " + errText
			}
			return resultWithUsage(), s.newOpenAIStreamFailoverError(c, account, false, upstreamRequestID, nil, msg), true
		}
		// 客户端已断开时，上游出错仅影响体验，不影响计费；返回已收集 usage
		if clientDisconnected {
			return resultWithUsage(), fmt.Errorf("stream usage incomplete after disconnect: %w", scanErr), true
		}
		sendErrorEvent("stream_read_error")
		return resultWithUsage(), fmt.Errorf("stream read error: %w", scanErr), true
	}
	processSSEFrame := func(frame openAICompatSSEFrame, queueDrained bool) {
		if streamFailoverErr != nil {
			return
		}
		eventType, data := openAIStreamFrameEventTypeAndData(frame)
		hasData := strings.TrimSpace(data) != ""
		if needModelReplace && hasData && mappedModel != "" && strings.Contains(data, mappedModel) {
			replacedLine := s.replaceModelInSSELine("data: "+data, mappedModel, originalModel)
			if replacedData, ok := extractOpenAISSEDataLine(replacedLine); ok {
				openAICompatSetSSEFrameData(&frame, replacedData)
				eventType, data = openAIStreamFrameEventTypeAndData(frame)
				hasData = strings.TrimSpace(data) != ""
			}
		}

		forceFlushFailedEvent := false
		startsClientOutput := false
		flushForFirstToken := false
		if hasData {
			dataBytes := []byte(data)
			_ = s.markOpenAICyberPolicyIfDetected(ctx, account, dataBytes)
			if strings.TrimSpace(data) == "[DONE]" && !sawFailedEvent {
				sawSuccessfulTerminal = true
			}
			if openAIStreamEventIsTerminal(data) {
				sawTerminalEvent = true
			}
			if openAIStreamFrameIsSuccessfulTerminal(frame) {
				sawSuccessfulTerminal = true
			}
			if responseID == "" {
				responseID = extractOpenAIResponseIDFromJSONBytes(dataBytes)
			}
			if overloadMsg, matched := classifyOpenAIRetryableOverload(dataBytes, ""); matched {
				if sawSuccessfulTerminal {
					return
				}
				streamFailoverErr = s.newOpenAIRetryableOverloadFailoverError(ctx, c, account, false, upstreamRequestID, dataBytes, overloadMsg)
				return
			}
			if advisoryMsg, matched := classifyOpenAIWSSoftRateLimitAdvisory(dataBytes); matched {
				if !openAIStreamClientOutputStarted(c, clientOutputStarted) {
					streamFailoverErr = s.newOpenAISoftRateLimitFailoverError(ctx, c, account, false, upstreamRequestID, dataBytes, advisoryMsg)
				} else {
					streamFailoverErr = fmt.Errorf("openai soft rate limit advisory: %s", advisoryMsg)
				}
				return
			}

			if eventType == "response.failed" {
				failedMessage = extractOpenAISSEErrorMessage(dataBytes)
				if !openAIStreamClientOutputStarted(c, clientOutputStarted) && openAIStreamFailedEventShouldFailover(dataBytes, failedMessage) {
					sawFailedEvent = true
					streamFailoverErr = s.newOpenAIStreamFailoverError(c, account, false, upstreamRequestID, dataBytes, failedMessage)
					return
				}
				forceFlushFailedEvent = true
				sawFailedEvent = true
			}

			if correctedData, corrected := s.toolCorrector.CorrectToolCallsInSSEBytes(dataBytes); corrected {
				openAICompatSetSSEFrameData(&frame, string(correctedData))
				_, data = openAIStreamFrameEventTypeAndData(frame)
				dataBytes = correctedData
			}
			startsClientOutput = forceFlushFailedEvent || openAIStreamFrameStartsClientOutput(frame)
			flushForFirstToken = firstTokenMs == nil && startsClientOutput && strings.TrimSpace(data) != "[DONE]"
			if flushForFirstToken {
				ms := int(time.Since(startTime).Milliseconds())
				firstTokenMs = &ms
				stopTTFTWatchdog()
			}
			imageCounter.AddSSEData(dataBytes)
			s.parseSSEUsageBytes(dataBytes, usage)
			if imageOutput, ok := extractImageGenerationOutputFromSSEData(dataBytes, streamSeenImages); ok {
				streamImageOutputs = append(streamImageOutputs, imageOutput)
			}
			if responsesStreamEventMayContributeToOutput(eventType) {
				var streamEvent apicompat.ResponsesStreamEvent
				if err := json.Unmarshal(dataBytes, &streamEvent); err == nil {
					streamOutputAccumulator.ProcessEvent(&streamEvent)
				}
			}
			if normalizedData, normalized := normalizeResponsesStreamingTerminalOutput(dataBytes, streamOutputAccumulator, streamImageOutputs); normalized {
				openAICompatSetSSEFrameData(&frame, string(normalizedData))
			}
		}

		if clientDisconnected {
			return
		}
		if !clientOutputStarted && !startsClientOutput {
			pendingFrames = append(pendingFrames, frame)
			return
		}
		shouldFlush := queueDrained && (clientOutputStarted || startsClientOutput)
		if flushForFirstToken {
			shouldFlush = true
		}
		if !clientOutputStarted && len(pendingFrames) > 0 {
			if !flushPendingFrames() {
				return
			}
		}
		_ = writeFrame(frame, shouldFlush)
	}

	// 无超时/无 keepalive 的常见路径走同步扫描，减少 goroutine 与 channel 开销。
	if streamInterval <= 0 && keepaliveInterval <= 0 && ttftCh == nil {
		defer putSSEScannerBuf64K(scanBuf)
		var parser openAICompatSSEFrameParser
		for scanner.Scan() {
			if frame, ok := parser.AddLine(scanner.Text()); ok {
				processSSEFrame(frame, true)
				if streamFailoverErr != nil {
					return resultWithUsage(), streamFailoverErr
				}
			}
		}
		if frame, ok := parser.Finish(); ok {
			processSSEFrame(frame, true)
			if streamFailoverErr != nil {
				return resultWithUsage(), streamFailoverErr
			}
		}
		if result, err, done := handleScanErr(scanner.Err()); done {
			return result, err
		}
		return finalizeStream()
	}

	type scanEvent struct {
		line string
		err  error
	}
	// 独立 goroutine 读取上游，避免读取阻塞影响 keepalive/超时处理
	events := make(chan scanEvent, 16)
	done := make(chan struct{})
	sendEvent := func(ev scanEvent) bool {
		select {
		case events <- ev:
			return true
		case <-done:
			return false
		}
	}
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
	go func(scanBuf *sseScannerBuf64K) {
		defer putSSEScannerBuf64K(scanBuf)
		defer close(events)
		for scanner.Scan() {
			atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			if !sendEvent(scanEvent{line: scanner.Text()}) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			_ = sendEvent(scanEvent{err: err})
		}
	}(scanBuf)
	defer close(done)
	var parser openAICompatSSEFrameParser

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				if frame, ok := parser.Finish(); ok {
					processSSEFrame(frame, true)
					if streamFailoverErr != nil {
						return resultWithUsage(), streamFailoverErr
					}
				}
				return finalizeStream()
			}
			if result, err, done := handleScanErr(ev.err); done {
				return result, err
			}
			if frame, ok := parser.AddLine(ev.line); ok {
				processSSEFrame(frame, len(events) == 0)
				if streamFailoverErr != nil {
					return resultWithUsage(), streamFailoverErr
				}
			}

		case <-intervalCh:
			if ttftCh != nil && firstTokenMs == nil {
				continue
			}
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < streamInterval {
				continue
			}
			if clientDisconnected {
				return resultWithUsage(), fmt.Errorf("stream usage incomplete after timeout")
			}
			logger.LegacyPrintf("service.openai_gateway", "Stream data interval timeout: account=%d model=%s interval=%s", account.ID, originalModel, streamInterval)
			// 处理流超时，可能标记账户为临时不可调度或错误状态
			if s.rateLimitService != nil {
				s.rateLimitService.HandleStreamTimeout(ctx, account, originalModel)
			}
			sendErrorEvent("stream_timeout")
			return resultWithUsage(), fmt.Errorf("stream data interval timeout")

		case <-keepaliveCh:
			if ttftCh != nil && firstTokenMs == nil {
				continue
			}
			if clientDisconnected {
				continue
			}
			if time.Since(lastDataAt) < keepaliveInterval {
				continue
			}
			if _, err := bufferedWriter.WriteString(":\n\n"); err != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "Client disconnected during streaming, continuing to drain upstream for billing")
				continue
			}
			if err := flushBuffered(); err != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "Client disconnected during keepalive flush, continuing to drain upstream for billing")
			} else {
				lastDataAt = time.Now()
			}
		case <-ttftCh:
			drained := false
			for !drained {
				select {
				case ev, ok := <-events:
					if !ok {
						if frame, ok := parser.Finish(); ok {
							processSSEFrame(frame, true)
							if streamFailoverErr != nil {
								return resultWithUsage(), streamFailoverErr
							}
						}
						return finalizeStream()
					}
					if result, err, done := handleScanErr(ev.err); done {
						return result, err
					}
					if frame, ok := parser.AddLine(ev.line); ok {
						processSSEFrame(frame, len(events) == 0)
						if streamFailoverErr != nil {
							return resultWithUsage(), streamFailoverErr
						}
					}
					if firstTokenMs != nil {
						drained = true
					}
				default:
					drained = true
				}
			}
			if firstTokenMs != nil {
				continue
			}
			return resultWithUsage(), s.newOpenAITTFTTimeoutFailoverError(ctx, c, account, false, upstreamRequestID, originalModel)
		}
	}

}

// extractOpenAISSEDataLine 低开销提取 SSE `data:` 行内容。
// 兼容 `data: xxx` 与 `data:xxx` 两种格式。
func extractOpenAISSEDataLine(line string) (string, bool) {
	if !strings.HasPrefix(line, "data:") {
		return "", false
	}
	start := len("data:")
	for start < len(line) {
		if line[start] != ' ' && line[start] != '	' {
			break
		}
		start++
	}
	return line[start:], true
}

type openAICompatSSEFrame struct {
	EventType string
	Data      string
	Fields    []openAICompatSSEField
}

type openAICompatSSEField struct {
	Name  string
	Value string
	Raw   string
}

type openAICompatSSEFrameParser struct {
	eventType string
	dataLines []string
	fields    []openAICompatSSEField
}

func (p *openAICompatSSEFrameParser) AddLine(line string) (openAICompatSSEFrame, bool) {
	line = strings.TrimSuffix(line, "\r")
	line = normalizeOpenAIHTTPResponseTerminalSSELine(line)
	if line == "" {
		return p.dispatch()
	}
	field := newOpenAICompatSSEField(line)
	if field.Name == ":" {
		p.fields = append(p.fields, field)
		return openAICompatSSEFrame{}, false
	}
	if field.Name == "event" {
		if len(p.dataLines) > 0 {
			frame := openAICompatBuildSSEFrame(p.eventType, p.fields)
			p.reset()
			p.eventType = field.Value
			p.fields = append(p.fields, field)
			return frame, openAICompatSSEFrameHasContent(frame)
		}
		p.eventType = field.Value
		p.fields = append(p.fields, field)
		return openAICompatSSEFrame{}, false
	}
	if field.Name == "data" {
		data := field.Value
		if openAICompatShouldDispatchBeforeData(p.dataLines, data) {
			frame := openAICompatBuildSSEFrame(p.eventType, p.fields)
			p.reset()
			p.dataLines = []string{data}
			p.fields = append(p.fields, field)
			return frame, openAICompatSSEFrameHasContent(frame)
		}
		p.dataLines = append(p.dataLines, data)
		p.fields = append(p.fields, field)
		return openAICompatSSEFrame{}, false
	}
	p.fields = append(p.fields, field)
	return openAICompatSSEFrame{}, false
}

func openAICompatShouldDispatchBeforeData(current []string, next string) bool {
	if len(current) == 0 {
		return false
	}
	next = strings.TrimSpace(next)
	if next == "[DONE]" {
		return true
	}
	currentData := strings.TrimSpace(strings.Join(current, "\n"))
	return currentData != "" && gjson.Valid(currentData) && gjson.Valid(next)
}

func (p *openAICompatSSEFrameParser) Finish() (openAICompatSSEFrame, bool) {
	return p.dispatch()
}

func (p *openAICompatSSEFrameParser) dispatch() (openAICompatSSEFrame, bool) {
	frame := openAICompatBuildSSEFrame(p.eventType, p.fields)
	p.reset()
	return frame, openAICompatSSEFrameHasContent(frame)
}

func (p *openAICompatSSEFrameParser) reset() {
	p.eventType = ""
	p.dataLines = nil
	p.fields = nil
}

func newOpenAICompatSSEField(line string) openAICompatSSEField {
	if strings.HasPrefix(line, ":") {
		return openAICompatSSEField{Name: ":", Raw: line}
	}
	name := line
	value := ""
	if idx := strings.IndexByte(line, ':'); idx >= 0 {
		name = line[:idx]
		value = line[idx+1:]
		for len(value) > 0 {
			if value[0] != ' ' && value[0] != '\t' {
				break
			}
			value = value[1:]
		}
	}
	return openAICompatSSEField{Name: name, Value: value, Raw: line}
}

func openAICompatBuildSSEFrame(eventType string, fields []openAICompatSSEField) openAICompatSSEFrame {
	dataLines := make([]string, 0, len(fields))
	copiedFields := make([]openAICompatSSEField, len(fields))
	copy(copiedFields, fields)
	for _, field := range copiedFields {
		if field.Name == "data" {
			dataLines = append(dataLines, field.Value)
		}
	}
	return openAICompatSSEFrame{
		EventType: eventType,
		Data:      strings.Join(dataLines, "\n"),
		Fields:    copiedFields,
	}
}

func openAICompatSSEFrameHasContent(frame openAICompatSSEFrame) bool {
	return len(frame.Fields) > 0 || strings.TrimSpace(frame.Data) != ""
}

func openAICompatSetSSEFrameData(frame *openAICompatSSEFrame, data string) {
	if frame == nil {
		return
	}
	frame.Data = data
	if len(frame.Fields) == 0 {
		return
	}
	dataLines := strings.Split(data, "\n")
	if len(dataLines) == 0 {
		dataLines = []string{""}
	}
	rebuilt := make([]openAICompatSSEField, 0, len(frame.Fields)+len(dataLines))
	inserted := false
	for _, field := range frame.Fields {
		if field.Name != "data" {
			rebuilt = append(rebuilt, field)
			continue
		}
		if inserted {
			continue
		}
		inserted = true
		for _, line := range dataLines {
			rebuilt = append(rebuilt, openAICompatSSEField{
				Name:  "data",
				Value: line,
				Raw:   "data: " + line,
			})
		}
	}
	if !inserted {
		for _, line := range dataLines {
			rebuilt = append(rebuilt, openAICompatSSEField{
				Name:  "data",
				Value: line,
				Raw:   "data: " + line,
			})
		}
	}
	frame.Fields = rebuilt
}

func openAICompatSSEFrameMetadataSignature(frame openAICompatSSEFrame) string {
	if len(frame.Fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(frame.Fields))
	for _, field := range frame.Fields {
		if field.Name == "data" {
			continue
		}
		raw := strings.TrimSpace(field.Raw)
		if raw == "" {
			continue
		}
		parts = append(parts, raw)
	}
	return strings.Join(parts, "\n")
}

func openAICompatSSEFramesFromBody(body string) []openAICompatSSEFrame {
	lines := strings.Split(body, "\n")
	frames := make([]openAICompatSSEFrame, 0, len(lines)/2)
	var parser openAICompatSSEFrameParser
	for _, line := range lines {
		frame, ok := parser.AddLine(line)
		if ok {
			frames = append(frames, frame)
		}
	}
	if frame, ok := parser.Finish(); ok {
		frames = append(frames, frame)
	}
	return frames
}

func openAICompatPayloadWithEventType(payload, eventType string) string {
	eventType = strings.TrimSpace(eventType)
	trimmedPayload := strings.TrimSpace(payload)
	if eventType == "" || trimmedPayload == "" || trimmedPayload == "[DONE]" {
		return payload
	}
	if gjson.Get(payload, "type").Exists() {
		return payload
	}
	patched, err := sjson.Set(payload, "type", eventType)
	if err != nil {
		return payload
	}
	return patched
}

func (s *OpenAIGatewayService) replaceModelInSSELine(line, fromModel, toModel string) string {
	data, ok := extractOpenAISSEDataLine(line)
	if !ok {
		return line
	}
	if data == "" || data == "[DONE]" {
		return line
	}

	// 使用 gjson 精确检查 model 字段，避免全量 JSON 反序列化
	if m := gjson.Get(data, "model"); m.Exists() && m.Str == fromModel {
		newData, err := sjson.Set(data, "model", toModel)
		if err != nil {
			return line
		}
		return "data: " + newData
	}

	// 检查嵌套的 response.model 字段
	if m := gjson.Get(data, "response.model"); m.Exists() && m.Str == fromModel {
		newData, err := sjson.Set(data, "response.model", toModel)
		if err != nil {
			return line
		}
		return "data: " + newData
	}

	return line
}

func normalizeOpenAIHTTPResponseTerminalSSELine(line string) string {
	trimmed := strings.TrimSpace(line)
	if strings.EqualFold(trimmed, "event: response.done") {
		return "event: response.completed"
	}

	data, ok := extractOpenAISSEDataLine(line)
	if !ok || data == "" || data == "[DONE]" {
		return line
	}
	if strings.TrimSpace(gjson.Get(data, "type").String()) != "response.done" {
		return line
	}

	normalized, err := sjson.Set(data, "type", "response.completed")
	if err != nil {
		return line
	}
	return "data: " + normalized
}

// correctToolCallsInResponseBody 修正响应体中的工具调用
func (s *OpenAIGatewayService) correctToolCallsInResponseBody(body []byte) []byte {
	if len(body) == 0 {
		return body
	}

	corrected, changed := s.toolCorrector.CorrectToolCallsInSSEBytes(body)
	if changed {
		return corrected
	}
	return body
}

func (s *OpenAIGatewayService) parseSSEUsage(data string, usage *OpenAIUsage) {
	s.parseSSEUsageBytes([]byte(data), usage)
}

func (s *OpenAIGatewayService) parseSSEUsageBytes(data []byte, usage *OpenAIUsage) {
	if usage == nil {
		return
	}
	parsedUsage, ok := extractOpenAIUsageFromSSEEventBytes(data)
	if !ok {
		return
	}
	*usage = parsedUsage
}

func extractOpenAIUsageFromJSONBytes(body []byte) (OpenAIUsage, bool) {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return OpenAIUsage{}, false
	}
	if parsedUsage, ok := openAIUsageFromGJSON(gjson.GetBytes(body, "usage")); ok {
		return parsedUsage, true
	}
	if parsedUsage, ok := openAIUsageFromGJSON(gjson.GetBytes(body, "response.usage")); ok {
		return parsedUsage, true
	}
	return openAIUsageFromGJSON(gjson.ParseBytes(body))
}

func extractOpenAIUsageFromSSEEventBytes(data []byte) (OpenAIUsage, bool) {
	if len(data) == 0 || !gjson.ValidBytes(data) || bytes.Equal(data, []byte("[DONE]")) {
		return OpenAIUsage{}, false
	}
	if !isOpenAIUsageTerminalEventType(strings.TrimSpace(gjson.GetBytes(data, "type").String())) {
		return OpenAIUsage{}, false
	}
	return openAIUsageFromGJSON(gjson.GetBytes(data, "response.usage"))
}

func extractOpenAIResponseIDFromJSONBytes(body []byte) string {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	if id := strings.TrimSpace(gjson.GetBytes(body, "id").String()); id != "" {
		return id
	}
	return strings.TrimSpace(gjson.GetBytes(body, "response.id").String())
}

func (s *OpenAIGatewayService) bindHTTPResponseAccount(ctx context.Context, c *gin.Context, account *Account, responseID string) {
	if s == nil || account == nil || account.ID <= 0 {
		return
	}
	responseID = strings.TrimSpace(responseID)
	if responseID == "" {
		return
	}
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return
	}
	groupID := getOpenAIGroupIDFromContext(c)
	apiKeyID := getAPIKeyIDFromContext(c)
	ttl := s.openAIWSResponseStickyTTL()
	logOpenAIWSBindResponseAccountWarn(groupID, account.ID, responseID, store.BindResponseAccount(ctx, groupID, apiKeyID, responseID, account.ID, ttl))
}

func openAIUsageFromGJSON(value gjson.Result) (OpenAIUsage, bool) {
	if !value.Exists() || !value.IsObject() {
		return OpenAIUsage{}, false
	}
	inputNode := value.Get("input_tokens")
	hasInput := inputNode.Exists()
	if !hasInput {
		inputNode = value.Get("prompt_tokens")
		hasInput = inputNode.Exists()
	}
	outputNode := value.Get("output_tokens")
	hasOutput := outputNode.Exists()
	if !hasOutput {
		outputNode = value.Get("completion_tokens")
		hasOutput = outputNode.Exists()
	}
	cacheReadNode := value.Get("input_tokens_details.cached_tokens")
	if !cacheReadNode.Exists() {
		cacheReadNode = value.Get("prompt_tokens_details.cached_tokens")
	}
	imageOutputNode := value.Get("output_tokens_details.image_tokens")
	hasImage := imageOutputNode.Exists()
	if !hasImage {
		imageOutputNode = value.Get("completion_tokens_details.image_tokens")
		hasImage = imageOutputNode.Exists()
	}
	if (!hasInput || !hasOutput) && !hasImage {
		return OpenAIUsage{}, false
	}
	return OpenAIUsage{
		InputTokens:              int(inputNode.Int()),
		OutputTokens:             int(outputNode.Int()),
		CacheCreationInputTokens: int(value.Get("cache_creation_input_tokens").Int()),
		CacheReadInputTokens:     int(cacheReadNode.Int()),
		ImageOutputTokens:        int(imageOutputNode.Int()),
	}, true
}

func (s *OpenAIGatewayService) handleNonStreamingResponse(ctx context.Context, resp *http.Response, c *gin.Context, account *Account, originalModel, mappedModel string) (*openaiNonStreamingResult, error) {
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		return nil, err
	}
	_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)

	// Detect SSE responses for ALL account types via Content-Type header.
	// Some OpenAI-compatible upstreams (including other sub2api instances)
	// may return SSE even when stream=false was requested.
	if isEventStreamResponse(resp.Header) {
		return s.handleSSEToJSON(resp, c, account, body, originalModel, mappedModel)
	}
	bodyLooksLikeSSE := bytes.Contains(body, []byte("data:")) || bytes.Contains(body, []byte("event:"))

	// For OAuth accounts, also fall back to a body-content heuristic because
	// the upstream may omit the Content-Type header while still sending SSE.
	// This heuristic is NOT applied to API-key accounts to avoid false
	// positives on JSON responses that coincidentally contain "data:" or
	// "event:" in their text content.
	if account.Type == AccountTypeOAuth && bodyLooksLikeSSE {
		return s.handleSSEToJSON(resp, c, account, body, originalModel, mappedModel)
	}
	if advisoryMsg, matched := classifyOpenAIWSSoftRateLimitAdvisory(body); matched {
		return nil, s.newOpenAISoftRateLimitFailoverError(ctx, c, account, false, resp.Header.Get("x-request-id"), body, advisoryMsg)
	}

	usageValue, usageOK := extractOpenAIUsageFromJSONBytes(body)
	if !gjson.ValidBytes(body) {
		if bodyLooksLikeSSE {
			return s.handleSSEToJSON(resp, c, account, body, originalModel, mappedModel)
		}
		return nil, fmt.Errorf("parse response: invalid json response")
	}
	usage := &OpenAIUsage{}
	if usageOK {
		*usage = usageValue
	}
	imageCount := countOpenAIResponseImageOutputsFromJSONBytes(body)

	// Replace model in response if needed
	if originalModel != mappedModel {
		body = s.replaceModelInResponseBody(body, mappedModel, originalModel)
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)

	contentType := "application/json"
	if s.cfg != nil && !s.cfg.Security.ResponseHeaders.Enabled {
		if upstreamType := resp.Header.Get("Content-Type"); upstreamType != "" {
			contentType = upstreamType
		}
	}

	c.Data(resp.StatusCode, contentType, body)

	return &openaiNonStreamingResult{usage: usage, responseID: extractOpenAIResponseIDFromJSONBytes(body), imageCount: imageCount}, nil
}

func isEventStreamResponse(header http.Header) bool {
	contentType := strings.ToLower(header.Get("Content-Type"))
	return strings.Contains(contentType, "text/event-stream")
}

func (s *OpenAIGatewayService) handleSSEToJSON(resp *http.Response, c *gin.Context, account *Account, body []byte, originalModel, mappedModel string) (*openaiNonStreamingResult, error) {
	bodyText := string(body)
	if advisoryMsg, matched := extractOpenAIWSSoftRateLimitAdvisoryFromSSEBody(bodyText); matched {
		return nil, s.newOpenAISoftRateLimitFailoverError(c.Request.Context(), c, account, false, resp.Header.Get("x-request-id"), body, advisoryMsg)
	}
	finalResponse, ok := extractCodexFinalResponse(bodyText)
	if terminalType, terminalPayload, terminalOK := extractOpenAISSETerminalEvent(bodyText); terminalOK {
		if overloadMsg, matched := classifyOpenAIRetryableOverload(terminalPayload, ""); matched || strings.TrimSpace(terminalType) == "error" {
			if strings.TrimSpace(terminalType) == "error" || matched {
				return nil, s.newOpenAIRetryableOverloadFailoverError(c.Request.Context(), c, account, false, resp.Header.Get("x-request-id"), terminalPayload, overloadMsg)
			}
		}
		if terminalType == "response.failed" {
			_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, terminalPayload)
			msg := extractOpenAISSEErrorMessage(terminalPayload)
			if overloadMsg, matched := classifyOpenAIRetryableOverload(terminalPayload, msg); matched {
				return nil, s.newOpenAIRetryableOverloadFailoverError(c.Request.Context(), c, account, false, resp.Header.Get("x-request-id"), terminalPayload, overloadMsg)
			}
			if msg == "" {
				msg = "Upstream compact response failed"
			}
			return nil, s.writeOpenAINonStreamingProtocolError(resp, c, msg)
		}
		if !ok {
			if response := extractOpenAISSETerminalResponse(terminalPayload); len(response) > 0 {
				finalResponse = response
				ok = true
			}
		}
	}

	usage := &OpenAIUsage{}
	imageCount := 0
	if ok {
		if parsedUsage, parsed := extractOpenAIUsageFromJSONBytes(finalResponse); parsed {
			*usage = parsedUsage
		} else {
			usage = s.parseSSEUsageFromBody(bodyText)
		}
		// When the terminal event has an empty output array, reconstruct
		// output from accumulated delta events so the client gets full content.
		// gjson Array() returns empty slice for null, missing, or empty arrays.
		if len(gjson.GetBytes(finalResponse, "output").Array()) == 0 {
			if outputJSON, reconstructed := reconstructResponseOutputFromSSE(bodyText); reconstructed {
				if patched, err := sjson.SetRawBytes(finalResponse, "output", outputJSON); err == nil {
					finalResponse = patched
				}
			}
		}
		body = finalResponse
		if originalModel != mappedModel {
			body = s.replaceModelInResponseBody(body, mappedModel, originalModel)
		}
		// Correct tool calls in final response
		body = s.correctToolCallsInResponseBody(body)
		imageCount = countOpenAIResponseImageOutputsFromJSONBytes(body)
	} else {
		terminalType, terminalPayload, terminalOK := extractOpenAISSETerminalEvent(bodyText)
		if terminalOK && terminalType == "response.failed" {
			_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, terminalPayload)
			msg := extractOpenAISSEErrorMessage(terminalPayload)
			if msg == "" {
				msg = "Upstream compact response failed"
			}
			return nil, s.writeOpenAINonStreamingProtocolError(resp, c, msg)
		}
		usage = s.parseSSEUsageFromBody(bodyText)
		if originalModel != mappedModel {
			bodyText = s.replaceModelInSSEBody(bodyText, mappedModel, originalModel)
		}
		body = []byte(bodyText)
	}

	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)

	contentType := "application/json; charset=utf-8"
	if !ok {
		contentType = resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "text/event-stream"
		}
	}
	c.Writer.Header().Set("Content-Type", contentType)
	c.Data(resp.StatusCode, contentType, body)

	return &openaiNonStreamingResult{usage: usage, responseID: extractOpenAIResponseIDFromJSONBytes(body), imageCount: imageCount}, nil
}

func extractOpenAISSETerminalEvent(body string) (string, []byte, bool) {
	var terminalType string
	var terminalPayload []byte
	for _, frame := range openAICompatSSEFramesFromBody(body) {
		data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, frame.EventType))
		if data == "" || data == "[DONE]" {
			continue
		}
		eventType := strings.TrimSpace(gjson.Get(data, "type").String())
		if eventType == "response.failed" {
			if terminalType == "" {
				terminalType = eventType
				terminalPayload = []byte(data)
			}
			continue
		}
		if isOpenAIUsageTerminalEventType(eventType) {
			if terminalType == "response.failed" {
				continue
			}
			terminalType = eventType
			terminalPayload = []byte(data)
		}
	}
	if terminalType != "" {
		return terminalType, terminalPayload, true
	}
	return "", nil, false
}

func extractOpenAISSETerminalResponse(terminalPayload []byte) []byte {
	if len(terminalPayload) == 0 {
		return nil
	}
	response := gjson.GetBytes(terminalPayload, "response")
	if !response.Exists() || response.Raw == "" {
		return nil
	}
	trimmed := strings.TrimSpace(response.Raw)
	if !strings.HasPrefix(trimmed, "{") {
		return nil
	}
	return []byte(trimmed)
}

func extractOpenAIWSSoftRateLimitAdvisoryFromSSEBody(body string) (string, bool) {
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		data, ok := extractOpenAISSEDataLine(line)
		if !ok || data == "" || data == "[DONE]" {
			continue
		}
		if advisoryMsg, matched := classifyOpenAIWSSoftRateLimitAdvisory([]byte(data)); matched {
			return advisoryMsg, true
		}
	}
	return "", false
}

func extractOpenAISSEErrorMessage(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	for _, path := range []string{"response.error.message", "error.message", "message"} {
		if msg := strings.TrimSpace(gjson.GetBytes(payload, path).String()); msg != "" {
			return sanitizeUpstreamErrorMessage(msg)
		}
	}
	return sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(payload)))
}

func (s *OpenAIGatewayService) writeOpenAINonStreamingProtocolError(resp *http.Response, c *gin.Context, message string) error {
	message = sanitizeUpstreamErrorMessage(strings.TrimSpace(message))
	if message == "" {
		message = "Upstream returned an invalid non-streaming response"
	}
	setOpsUpstreamError(c, http.StatusBadGateway, message, "")
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.JSON(http.StatusBadGateway, gin.H{
		"error": gin.H{
			"type":    "upstream_error",
			"message": message,
		},
	})
	return fmt.Errorf("non-streaming openai protocol error: %s", message)
}

func extractCodexFinalResponse(body string) ([]byte, bool) {
	var finalResponse []byte
	for _, frame := range openAICompatSSEFramesFromBody(body) {
		data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, frame.EventType))
		if data == "" || data == "[DONE]" {
			continue
		}
		eventType := strings.TrimSpace(gjson.Get(data, "type").String())
		if isOpenAIFinalResponseEnvelopeEventType(eventType) {
			if response := gjson.Get(data, "response"); response.Exists() && response.Type == gjson.JSON && response.Raw != "" {
				if len(finalResponse) == 0 || len(gjson.Get(response.Raw, "output").Array()) > 0 {
					finalResponse = []byte(response.Raw)
				}
			}
		}
	}
	if len(finalResponse) == 0 {
		return nil, false
	}
	return finalResponse, true
}

func isOpenAIUsageTerminalEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "response.completed", "response.done", "response.incomplete", "response.cancelled", "response.canceled":
		return true
	default:
		return false
	}
}

func isOpenAIFinalResponseEnvelopeEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "response.completed", "response.done", "response.incomplete", "response.cancelled", "response.canceled":
		return true
	default:
		return false
	}
}

func normalizeResponsesStreamingTerminalOutput(data []byte, acc *apicompat.BufferedResponseAccumulator, imageOutputs []json.RawMessage) ([]byte, bool) {
	eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
	switch eventType {
	case "response.completed", "response.done", "response.incomplete", "response.cancelled", "response.canceled":
	default:
		return data, false
	}

	output := gjson.GetBytes(data, "response.output")
	hasAccumulatedOutput := (acc != nil && acc.HasContent()) || len(imageOutputs) > 0
	if output.Exists() && output.IsArray() {
		if len(output.Array()) > 0 || !hasAccumulatedOutput {
			return data, false
		}
	}

	outputJSON := []byte("[]")
	if reconstructed, ok := buildResponsesOutputJSON(acc, imageOutputs); ok {
		outputJSON = reconstructed
	}
	updated, err := sjson.SetRawBytes(data, "response.output", outputJSON)
	if err != nil {
		return data, false
	}
	return updated, true
}

func responsesStreamEventMayContributeToOutput(eventType string) bool {
	switch eventType {
	case "response.output_text.delta",
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.custom_tool_call_input.delta",
		"response.reasoning_summary_text.delta",
		"response.reasoning_text.delta":
		return true
	default:
		return false
	}
}

// reconstructResponseOutputFromSSE scans raw SSE body text for delta events and
// returns a JSON-encoded output array reconstructed from accumulated deltas.
// Returns (nil, false) if no content was found in deltas.
func reconstructResponseOutputFromSSE(bodyText string) ([]byte, bool) {
	acc := apicompat.NewBufferedResponseAccumulator()
	imageOutputs := make([]json.RawMessage, 0, 1)
	seenImages := make(map[string]struct{})
	for _, frame := range openAICompatSSEFramesFromBody(bodyText) {
		data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, frame.EventType))
		if data == "" || data == "[DONE]" {
			continue
		}
		if imageOutput, ok := extractImageGenerationOutputFromSSEData([]byte(data), seenImages); ok {
			imageOutputs = append(imageOutputs, imageOutput)
		}
		var event apicompat.ResponsesStreamEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}
		acc.ProcessEvent(&event)
	}
	return buildResponsesOutputJSON(acc, imageOutputs)
}

func buildResponsesOutputJSON(acc *apicompat.BufferedResponseAccumulator, imageOutputs []json.RawMessage) ([]byte, bool) {
	if (acc == nil || !acc.HasContent()) && len(imageOutputs) == 0 {
		return nil, false
	}
	var output []json.RawMessage
	if acc != nil && acc.HasContent() {
		outputJSON, err := json.Marshal(acc.BuildOutput())
		if err == nil {
			_ = json.Unmarshal(outputJSON, &output)
		}
	}
	output = append(output, imageOutputs...)
	if len(output) == 0 {
		return nil, false
	}

	outputJSON, err := json.Marshal(output)
	if err != nil {
		return nil, false
	}
	return outputJSON, true
}

func extractImageGenerationOutputFromSSEData(data []byte, seen map[string]struct{}) (json.RawMessage, bool) {
	if len(data) == 0 || !gjson.ValidBytes(data) {
		return nil, false
	}
	if gjson.GetBytes(data, "type").String() != "response.output_item.done" {
		return nil, false
	}
	item := gjson.GetBytes(data, "item")
	if !item.Exists() || !item.IsObject() || item.Get("type").String() != "image_generation_call" {
		return nil, false
	}
	if strings.TrimSpace(item.Get("result").String()) == "" {
		return nil, false
	}
	key := strings.TrimSpace(item.Get("id").String())
	if key == "" {
		key = strings.TrimSpace(item.Get("output_format").String()) + "|" + strings.TrimSpace(item.Get("result").String())
	}
	if key != "" && seen != nil {
		if _, exists := seen[key]; exists {
			return nil, false
		}
		seen[key] = struct{}{}
	}
	return json.RawMessage(item.Raw), true
}

func (s *OpenAIGatewayService) parseSSEUsageFromBody(body string) *OpenAIUsage {
	usage := &OpenAIUsage{}
	for _, frame := range openAICompatSSEFramesFromBody(body) {
		data := strings.TrimSpace(openAICompatPayloadWithEventType(frame.Data, frame.EventType))
		if data == "" || data == "[DONE]" {
			continue
		}
		s.parseSSEUsageBytes([]byte(data), usage)
	}
	return usage
}

func (s *OpenAIGatewayService) replaceModelInSSEBody(body, fromModel, toModel string) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		if _, ok := extractOpenAISSEDataLine(line); !ok {
			continue
		}
		lines[i] = s.replaceModelInSSELine(line, fromModel, toModel)
	}
	return strings.Join(lines, "\n")
}

func (s *OpenAIGatewayService) validateUpstreamBaseURL(raw string) (string, error) {
	allowInsecureHTTP := false
	allowPrivate := false
	requireAllowlist := true
	var allowedHosts []string
	if s.cfg != nil {
		allowInsecureHTTP = s.cfg.Security.URLAllowlist.AllowInsecureHTTP
		allowPrivate = s.cfg.Security.URLAllowlist.AllowPrivateHosts
		requireAllowlist = s.cfg.Security.URLAllowlist.Enabled
		if requireAllowlist {
			allowedHosts = s.cfg.Security.URLAllowlist.UpstreamHosts
		}
	}
	normalized, err := urlvalidator.ValidateHTTPURL(raw, allowInsecureHTTP, urlvalidator.ValidationOptions{
		AllowedHosts:     allowedHosts,
		RequireAllowlist: requireAllowlist,
		AllowPrivate:     allowPrivate,
	})
	if err != nil {
		return "", fmt.Errorf("invalid base_url: %w", err)
	}
	return normalized, nil
}

// buildOpenAIResponsesURL 组装 OpenAI Responses 端点。
// - base 以 /v1 结尾：追加 /responses
// - base 已是 /responses：原样返回
// - 其他情况：追加 /v1/responses
func buildOpenAIResponsesURL(base string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.HasSuffix(normalized, "/responses") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/responses"
	}
	if hasOpenAIVersionedBasePath(normalized) {
		return normalized + "/responses"
	}
	return normalized + "/v1/responses"
}

func trimOpenAIEncryptedReasoningItems(reqBody map[string]any) bool {
	if len(reqBody) == 0 {
		return false
	}

	inputValue, has := reqBody["input"]
	if !has {
		return false
	}

	nextInput, changed, keep := sanitizeEncryptedReasoningInputItem(inputValue)
	if !changed {
		return false
	}
	if !keep {
		delete(reqBody, "input")
		return true
	}
	reqBody["input"] = nextInput
	return true
}

func trimOpenAIStoreFalseReasoningItems(reqBody map[string]any) bool {
	if len(reqBody) == 0 {
		return false
	}
	rawStore, ok := reqBody["store"]
	if !ok {
		return false
	}
	storeEnabled, ok := rawStore.(bool)
	if !ok || storeEnabled {
		return false
	}

	rawInput, ok := reqBody["input"]
	if !ok || rawInput == nil {
		return false
	}
	input, ok := rawInput.([]any)
	if !ok || len(input) == 0 {
		return false
	}

	filtered, modified := filterCodexInputWithOptions(input, codexInputFilterOptions{})
	if !modified {
		return false
	}
	reqBody["input"] = filtered
	return true
}

func sanitizeEncryptedReasoningInputItem(item any) (next any, changed bool, keep bool) {
	switch typed := item.(type) {
	case []any:
		filtered := typed[:0]
		changed := false
		for _, child := range typed {
			nextChild, childChanged, keep := sanitizeEncryptedReasoningInputItem(child)
			if childChanged {
				changed = true
			}
			if !keep {
				continue
			}
			filtered = append(filtered, nextChild)
		}
		if !changed {
			return item, false, true
		}
		if len(filtered) == 0 {
			return nil, true, false
		}
		return filtered, true, true
	case []map[string]any:
		filtered := typed[:0]
		changed := false
		for _, child := range typed {
			nextChild, childChanged, keep := sanitizeEncryptedReasoningInputItem(child)
			if childChanged {
				changed = true
			}
			if !keep {
				continue
			}
			nextMap, ok := nextChild.(map[string]any)
			if !ok {
				filtered = append(filtered, child)
				continue
			}
			filtered = append(filtered, nextMap)
		}
		if !changed {
			return item, false, true
		}
		if len(filtered) == 0 {
			return nil, true, false
		}
		return filtered, true, true
	case map[string]any:
		if _, hasEncryptedContent := typed["encrypted_content"]; hasEncryptedContent {
			return nil, true, false
		}
		changed := false
		for key, child := range typed {
			nextChild, childChanged, keep := sanitizeEncryptedReasoningInputItem(child)
			if childChanged {
				changed = true
			}
			if !keep {
				delete(typed, key)
				continue
			}
			typed[key] = nextChild
		}
		if !changed {
			return item, false, true
		}
		if len(typed) == 0 {
			return nil, true, false
		}
		itemType, _ := typed["type"].(string)
		if strings.TrimSpace(itemType) == "reasoning" && len(typed) == 1 {
			return nil, true, false
		}
		return typed, true, true
	default:
		return item, false, true
	}
}

func IsOpenAIResponsesCompactPathForTest(c *gin.Context) bool {
	return isOpenAIResponsesCompactPath(c)
}

func OpenAICompactSessionSeedKeyForTest() string {
	return openAICompactSessionSeedKey
}

func NormalizeOpenAICompactRequestBodyForTest(body []byte) ([]byte, bool, error) {
	return normalizeOpenAICompactRequestBody(body)
}

func isOpenAIResponsesCompactPath(c *gin.Context) bool {
	suffix := strings.TrimSpace(openAIResponsesRequestPathSuffix(c))
	return suffix == "/compact" || strings.HasPrefix(suffix, "/compact/")
}

func normalizeOpenAICompactRequestBody(body []byte) ([]byte, bool, error) {
	if len(body) == 0 {
		return body, false, nil
	}

	normalized := []byte(`{}`)
	for _, field := range []string{"model", "input", "instructions", "tools", "parallel_tool_calls", "reasoning", "text", "previous_response_id"} {
		value := gjson.GetBytes(body, field)
		if !value.Exists() {
			continue
		}
		raw := []byte(value.Raw)
		if field == "tools" {
			raw, _ = ensureOpenAICompactDeferredToolSearch(raw)
		}
		next, err := sjson.SetRawBytes(normalized, field, raw)
		if err != nil {
			return body, false, fmt.Errorf("normalize compact body %s: %w", field, err)
		}
		normalized = next
	}

	if bytes.Equal(bytes.TrimSpace(body), bytes.TrimSpace(normalized)) {
		return body, false, nil
	}
	return normalized, true, nil
}

func ensureOpenAICompactDeferredToolSearch(rawTools []byte) ([]byte, bool) {
	var parsed any
	if err := json.Unmarshal(rawTools, &parsed); err != nil {
		return rawTools, false
	}

	switch tools := parsed.(type) {
	case []any:
		hasDeferred := false
		hasToolSearch := false
		for _, rawTool := range tools {
			tool, ok := rawTool.(map[string]any)
			if !ok {
				continue
			}
			if v, _ := tool["defer_loading"].(bool); v {
				hasDeferred = true
			}
			if strings.TrimSpace(firstNonEmptyString(tool["type"])) == "tool_search" {
				hasToolSearch = true
			}
		}
		if !hasDeferred || hasToolSearch {
			return rawTools, false
		}
		tools = append(tools, map[string]any{"type": "tool_search"})
		encoded, err := json.Marshal(tools)
		if err != nil {
			return rawTools, false
		}
		return encoded, true
	case map[string]any:
		deferLoading, _ := tools["defer_loading"].(bool)
		if !deferLoading {
			return rawTools, false
		}
		if _, ok := tools["tool_search"]; ok {
			return rawTools, false
		}
		tools["tool_search"] = map[string]any{"type": "tool_search"}
		encoded, err := json.Marshal(tools)
		if err != nil {
			return rawTools, false
		}
		return encoded, true
	default:
		return rawTools, false
	}
}

func resolveOpenAICompactSessionID(c *gin.Context, body []byte) string {
	if c != nil {
		if sessionID := strings.TrimSpace(c.GetHeader("session_id")); sessionID != "" {
			return sessionID
		}
		if conversationID := strings.TrimSpace(c.GetHeader("conversation_id")); conversationID != "" {
			return conversationID
		}
		if seed, ok := c.Get(openAICompactSessionSeedKey); ok {
			if seedStr, ok := seed.(string); ok && strings.TrimSpace(seedStr) != "" {
				return strings.TrimSpace(seedStr)
			}
		}
	}
	if sessionID := strings.TrimSpace(deriveOpenAIContentSessionSeed(body)); sessionID != "" {
		return contentSessionSeedPrefix + generateSessionUUID(sessionID)
	}
	return uuid.NewString()
}

func openAIResponsesRequestPathSuffix(c *gin.Context) string {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return ""
	}
	normalizedPath := strings.TrimRight(strings.TrimSpace(c.Request.URL.Path), "/")
	if normalizedPath == "" {
		return ""
	}
	idx := strings.LastIndex(normalizedPath, "/responses")
	if idx < 0 {
		return ""
	}
	suffix := normalizedPath[idx+len("/responses"):]
	if suffix == "" || suffix == "/" {
		return ""
	}
	if !strings.HasPrefix(suffix, "/") {
		return ""
	}
	return suffix
}

func appendOpenAIResponsesRequestPathSuffix(baseURL, suffix string) string {
	trimmedBase := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	trimmedSuffix := strings.TrimSpace(suffix)
	if trimmedBase == "" || trimmedSuffix == "" {
		return trimmedBase
	}
	return trimmedBase + trimmedSuffix
}

func (s *OpenAIGatewayService) replaceModelInResponseBody(body []byte, fromModel, toModel string) []byte {
	// 使用 gjson/sjson 精确替换 model 字段，避免全量 JSON 反序列化
	if m := gjson.GetBytes(body, "model"); m.Exists() && m.Str == fromModel {
		newBody, err := sjson.SetBytes(body, "model", toModel)
		if err != nil {
			return body
		}
		return newBody
	}
	return body
}

// OpenAIRecordUsageInput input for recording usage
type OpenAIRecordUsageInput struct {
	Result             *OpenAIForwardResult
	APIKey             *APIKey
	User               *User
	Account            *Account
	Subscription       *UserSubscription
	InboundEndpoint    string
	UpstreamEndpoint   string
	UserAgent          string // 请求的 User-Agent
	IPAddress          string // 请求的客户端 IP 地址
	RequestPayloadHash string
	RequestType        RequestType
	APIKeyService      APIKeyQuotaUpdater
	QuotaPlatform      string // user×platform 配额计量平台：handler 在请求 ctx 内经 QuotaPlatform() 算定后传入
	ChannelUsageFields
}

func normalizeOpenAIRecordUsageRequestType(input *OpenAIRecordUsageInput, result *OpenAIForwardResult) RequestType {
	if result != nil {
		if requestType := result.EffectiveRequestType.Normalize(); requestType != RequestTypeUnknown {
			return requestType
		}
	}
	if input != nil {
		if requestType := input.RequestType.Normalize(); requestType != RequestTypeUnknown {
			return requestType
		}
	}
	if isOpenAIRecordUsageImageRequest(input, result) {
		if input != nil {
			if isOpenAIImages2APIBridgePath(input.InboundEndpoint) || isOpenAIImages2APIBridgePath(input.UpstreamEndpoint) {
				return RequestTypeImageWebBridge
			}
		}
		return RequestTypeImage
	}
	if result == nil {
		return RequestTypeUnknown
	}
	return RequestTypeFromLegacy(result.Stream, result.OpenAIWSMode)
}

func isOpenAIRecordUsageImageRequest(input *OpenAIRecordUsageInput, result *OpenAIForwardResult) bool {
	if result != nil {
		if result.ImageCount > 0 || result.Usage.ImageOutputTokens > 0 {
			return true
		}
	}
	if input == nil {
		return false
	}
	return isOpenAIImagesPath(input.InboundEndpoint) || isOpenAIImagesPath(input.UpstreamEndpoint)
}

func applyOpenAIResponsesImageBillingMeta(result *OpenAIForwardResult, body []byte, fallbackModel string) {
	if result == nil || result.ImageCount <= 0 || len(body) == 0 {
		return
	}

	if imageModel, imageSizeTier, err := resolveOpenAIResponsesImageBillingConfigFromBody(body, fallbackModel); err == nil {
		if result.BillingModel == "" {
			result.BillingModel = imageModel
		}
		if result.ImageSize == "" {
			result.ImageSize = imageSizeTier
		}
	}

	if result.TokenBillingModel != "" {
		return
	}

	tokenBillingModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if tokenBillingModel == "" {
		tokenBillingModel = strings.TrimSpace(fallbackModel)
	}
	if tokenBillingModel != "" && !isOpenAIImageGenerationModel(tokenBillingModel) {
		result.TokenBillingModel = tokenBillingModel
	}
}

func isOpenAIImages2APIBridgePath(path string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(path)), "/images2api/")
}

func isOpenAIImagesPath(path string) bool {
	normalized := strings.ToLower(strings.TrimSpace(path))
	return strings.Contains(normalized, "/images/") || strings.Contains(normalized, "/images2api/")
}

// RecordUsage records usage and deducts balance
func (s *OpenAIGatewayService) RecordUsage(ctx context.Context, input *OpenAIRecordUsageInput) error {
	if input == nil || input.Result == nil {
		return errors.New("record usage input is required")
	}
	result := input.Result
	ApplyOpenAIImageBillingResolution(result)
	if s.rateLimitService != nil && input != nil && input.Account != nil && input.Account.Platform == PlatformOpenAI {
		s.rateLimitService.ResetOpenAI403Counter(ctx, input.Account.ID)
	}

	requestType := normalizeOpenAIRecordUsageRequestType(input, result)

	apiKey := input.APIKey
	user := input.User
	account := input.Account
	subscription := input.Subscription

	actualInputTokens := normalizeRecordedInputTokens(account, result.Usage)

	// Calculate cost
	tokens := UsageTokens{
		InputTokens:         actualInputTokens,
		OutputTokens:        result.Usage.OutputTokens,
		CacheCreationTokens: result.Usage.CacheCreationInputTokens,
		CacheReadTokens:     result.Usage.CacheReadInputTokens,
		ImageOutputTokens:   result.Usage.ImageOutputTokens,
	}

	// Get rate multiplier
	multiplier := 1.0
	if s.cfg != nil {
		multiplier = s.cfg.Default.RateMultiplier
	}
	if apiKey.GroupID != nil && apiKey.Group != nil {
		resolver := s.userGroupRateResolver
		if resolver == nil {
			resolver = newUserGroupRateResolver(nil, nil, resolveUserGroupRateCacheTTL(s.cfg), nil, "service.openai_gateway")
		}
		multiplier = resolver.Resolve(ctx, user.ID, *apiKey.GroupID, apiKey.Group.RateMultiplier)
	}
	effectiveRateMultiplier := multiplier
	if result.ImageCount > 0 {
		effectiveRateMultiplier = resolveImageRateMultiplier(apiKey, multiplier)
	}

	var cost *CostBreakdown
	var err error
	modelView := resolveUsageModelView(usageModelViewInput{
		ResultModel:          result.Model,
		ExplicitBillingModel: result.BillingModel,
		UpstreamModel:        result.UpstreamModel,
		OriginalModel:        input.OriginalModel,
		ChannelMappedModel:   input.ChannelMappedModel,
		BillingModelSource:   input.BillingModelSource,
	})
	var requestedCost *CostBreakdown
	var upstreamCost *CostBreakdown
	var firstSuccessfulCost *CostBreakdown
	serviceTier := ""
	if result.ServiceTier != nil {
		serviceTier = strings.TrimSpace(*result.ServiceTier)
	}
	seenBillingCandidates := make(map[string]struct{}, len(modelView.BillingModelCandidates))
	for _, billingModelCandidate := range modelView.BillingModelCandidates {
		if _, seen := seenBillingCandidates[billingModelCandidate]; seen {
			continue
		}
		seenBillingCandidates[billingModelCandidate] = struct{}{}

		candidateCost, candidateErr := s.calculateOpenAIRecordUsageCost(
			ctx,
			result,
			apiKey,
			billingModelCandidate,
			multiplier,
			effectiveRateMultiplier,
			tokens,
			serviceTier,
			requestType,
		)
		if candidateErr == nil {
			if firstSuccessfulCost == nil {
				firstSuccessfulCost = candidateCost
				cost = candidateCost
				err = nil
			}
			if billingModelCandidate == modelView.RequestedModel {
				requestedCost = candidateCost
			}
			if billingModelCandidate == modelView.UpstreamModel {
				upstreamCost = candidateCost
			}
			continue
		}
		if firstSuccessfulCost == nil {
			err = candidateErr
		}
	}
	if firstSuccessfulCost == nil {
		if isUsagePricingUnavailableError(err) {
			logger.L().With(
				zap.String("component", "service.openai_gateway"),
				zap.Strings("billing_model_candidates", modelView.BillingModelCandidates),
				zap.String("requested_model", input.OriginalModel),
				zap.String("mapped_model", input.ChannelMappedModel),
				zap.String("upstream_model", result.UpstreamModel),
				zap.Int64("api_key_id", apiKey.ID),
				zap.Int64("account_id", account.ID),
			).Warn("openai_usage.pricing_missing_record_zero_cost", zap.Error(err))
			cost = &CostBreakdown{BillingMode: string(BillingModeToken)}
		} else {
			return fmt.Errorf("calculate OpenAI usage cost failed: %w", err)
		}
	}
	selectedCost := chooseHigherPricedUsageCost(modelView.RequestedModel, requestedCost, modelView.UpstreamModel, upstreamCost)
	if selectedCost.Cost != nil {
		cost = selectedCost.Cost
	}

	// Determine billing type
	isSubscriptionBilling := subscription != nil && apiKey.Group != nil && apiKey.Group.IsSubscriptionType()
	billingType := BillingTypeBalance
	if isSubscriptionBilling {
		billingType = BillingTypeSubscription
	}

	// Create usage log
	durationMs := int(result.Duration.Milliseconds())
	accountRateMultiplier := account.BillingRateMultiplier()
	requestID := resolveUsageBillingRequestID(ctx, result.RequestID)
	if result.OpenAIWSMode {
		if upstreamRequestID := strings.TrimSpace(result.RequestID); upstreamRequestID != "" {
			requestID = upstreamRequestID
		}
	}

	usageLog := &UsageLog{
		UserID:                       user.ID,
		APIKeyID:                     apiKey.ID,
		AccountID:                    account.ID,
		Provider:                     strings.TrimSpace(account.Platform),
		RequestID:                    requestID,
		Model:                        result.Model,
		RequestedModel:               modelView.RequestedModel,
		UpstreamModel:                modelView.UsageLogUpstreamModel,
		ServiceTier:                  result.ServiceTier,
		ReasoningEffort:              result.ReasoningEffort,
		InboundEndpoint:              optionalTrimmedStringPtr(input.InboundEndpoint),
		UpstreamEndpoint:             optionalTrimmedStringPtr(input.UpstreamEndpoint),
		InputTokens:                  actualInputTokens,
		OutputTokens:                 result.Usage.OutputTokens,
		CacheCreationTokens:          result.Usage.CacheCreationInputTokens,
		CacheReadTokens:              result.Usage.CacheReadInputTokens,
		ImageOutputTokens:            result.Usage.ImageOutputTokens,
		ImageCount:                   result.ImageCount,
		ImageSize:                    optionalTrimmedStringPtr(result.ImageSize),
		ImageInputSize:               optionalTrimmedStringPtr(result.ImageInputSize),
		ImageOutputSize:              optionalTrimmedStringPtr(result.ImageOutputSize),
		ImageSizeSource:              optionalTrimmedStringPtr(result.ImageSizeSource),
		ImageSizeBreakdown:           result.ImageSizeBreakdown,
		BilledByHigherPricedUpstream: selectedCost.BilledByHigherPricedUpstream,
	}
	if cost != nil {
		usageLog.InputCost = cost.InputCost
		usageLog.OutputCost = cost.OutputCost
		usageLog.ImageOutputCost = cost.ImageOutputCost
		usageLog.CacheCreationCost = cost.CacheCreationCost
		usageLog.CacheReadCost = cost.CacheReadCost
		usageLog.TotalCost = cost.TotalCost
		usageLog.ActualCost = cost.ActualCost
	}
	usageLog.RateMultiplier = effectiveRateMultiplier
	usageLog.AccountRateMultiplier = &accountRateMultiplier
	usageLog.BillingType = billingType
	usageLog.Stream = result.Stream
	usageLog.OpenAIWSMode = result.OpenAIWSMode
	usageLog.OpenAIWSProfile = result.OpenAIWSProfile
	usageLog.OpenAIWSConnReused = result.OpenAIWSConnReused
	usageLog.RequestType = requestType
	usageLog.DurationMs = &durationMs
	usageLog.FirstTokenMs = result.FirstTokenMs
	usageLog.CreatedAt = time.Now()
	// 设置渠道信息
	usageLog.ChannelID = optionalInt64Ptr(input.ChannelID)
	usageLog.ModelMappingChain = optionalTrimmedStringPtr(input.ModelMappingChain)
	// 设置计费模式
	if cost != nil && cost.BillingMode != "" {
		billingMode := cost.BillingMode
		usageLog.BillingMode = &billingMode
	} else if result.ImageCount > 0 {
		billingMode := string(BillingModeImage)
		usageLog.BillingMode = &billingMode
	} else {
		billingMode := string(BillingModeToken)
		usageLog.BillingMode = &billingMode
	}
	// 添加 UserAgent
	if input.UserAgent != "" {
		usageLog.UserAgent = &input.UserAgent
	}

	// 添加 IPAddress
	if input.IPAddress != "" {
		usageLog.IPAddress = &input.IPAddress
	}

	if apiKey.GroupID != nil {
		usageLog.GroupID = apiKey.GroupID
	}
	if subscription != nil {
		usageLog.SubscriptionID = &subscription.ID
	}

	// 计算账号统计定价费用（使用最终上游模型匹配自定义规则）
	if apiKey.GroupID != nil {
		applyAccountStatsCost(ctx, usageLog, s.channelService, s.billingService,
			account.ID, *apiKey.GroupID, modelView.UpstreamModel, result.Model,
			tokens, cost.TotalCost,
		)
	}

	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		writeUsageLogBestEffort(ctx, s.usageLogRepo, usageLog, "service.openai_gateway")
		logger.LegacyPrintf("service.openai_gateway", "[SIMPLE MODE] Usage recorded (not billed): user=%d, tokens=%d", usageLog.UserID, usageLog.TotalTokens())
		s.deferredService.ScheduleLastUsedUpdate(account.ID)
		return nil
	}

	billingErr := func() error {
		quotaPlatform := strings.TrimSpace(input.QuotaPlatform)
		if quotaPlatform == "" {
			quotaPlatform = PlatformFromAPIKey(apiKey)
		}
		_, err := applyUsageBilling(ctx, requestID, usageLog, &postUsageBillingParams{
			Cost:                  cost,
			User:                  user,
			APIKey:                apiKey,
			Account:               account,
			Subscription:          subscription,
			RequestPayloadHash:    resolveUsageBillingPayloadFingerprint(ctx, input.RequestPayloadHash),
			IsSubscriptionBill:    isSubscriptionBilling,
			AccountRateMultiplier: accountRateMultiplier,
			APIKeyService:         input.APIKeyService,
			UsageLog:              usageLog,
			Platform:              quotaPlatform,
		}, s.billingDeps(), s.usageBillingRepo)
		return err
	}()

	if billingErr != nil {
		return billingErr
	}
	writeUsageLogBestEffort(ctx, s.usageLogRepo, usageLog, "service.openai_gateway")

	return nil
}

func normalizeRecordedInputTokens(account *Account, usage OpenAIUsage) int {
	inputTokens := usage.InputTokens
	if account != nil && account.Platform == PlatformKiro {
		if inputTokens < 0 {
			return 0
		}
		return inputTokens
	}

	// OpenAI-style usage reports include cache reads inside input_tokens, but
	// usage_logs stores only the billable non-cached remainder.
	inputTokens -= usage.CacheReadInputTokens
	if inputTokens < 0 {
		return 0
	}
	return inputTokens
}

func (s *OpenAIGatewayService) calculateOpenAIRecordUsageCost(
	ctx context.Context,
	result *OpenAIForwardResult,
	apiKey *APIKey,
	billingModel string,
	multiplier float64,
	imageRateMultiplier float64,
	tokens UsageTokens,
	serviceTier string,
	requestType RequestType,
) (*CostBreakdown, error) {
	if result != nil && result.ImageCount > 0 {
		return s.calculateOpenAIImageRequestCost(ctx, result, apiKey, billingModel, multiplier, imageRateMultiplier, tokens, serviceTier, requestType)
	}
	return s.calculateOpenAITokenUsageCost(ctx, apiKey, billingModel, multiplier, tokens, serviceTier)
}

func isUsagePricingUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrModelPricingUnavailable) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no pricing available") || strings.Contains(msg, "pricing not found")
}

func (s *OpenAIGatewayService) calculateOpenAITokenUsageCost(
	ctx context.Context,
	apiKey *APIKey,
	billingModel string,
	multiplier float64,
	tokens UsageTokens,
	serviceTier string,
) (*CostBreakdown, error) {
	if s.resolver != nil && apiKey != nil && apiKey.Group != nil {
		gid := apiKey.Group.ID
		return s.billingService.CalculateCostUnified(CostInput{
			Ctx:            ctx,
			Model:          billingModel,
			GroupID:        &gid,
			Tokens:         tokens,
			RequestCount:   1,
			RateMultiplier: multiplier,
			ServiceTier:    serviceTier,
			Resolver:       s.resolver,
		})
	}
	return s.billingService.CalculateCostWithServiceTier(billingModel, tokens, multiplier, serviceTier)
}

func (s *OpenAIGatewayService) calculateOpenAIImageRequestCost(
	ctx context.Context,
	result *OpenAIForwardResult,
	apiKey *APIKey,
	billingModel string,
	multiplier float64,
	imageRateMultiplier float64,
	tokens UsageTokens,
	serviceTier string,
	requestType RequestType,
) (*CostBreakdown, error) {
	imageCost := s.calculateOpenAIImageCost(ctx, billingModel, apiKey, result, requestType, imageRateMultiplier)
	if requestType == RequestTypeImageWebBridge {
		return imageCost, nil
	}

	tokenBillingModel := strings.TrimSpace(result.TokenBillingModel)
	if tokenBillingModel == "" {
		return imageCost, nil
	}

	tokenUsage := tokens
	tokenUsage.ImageOutputTokens = 0
	tokenCost, err := s.calculateOpenAITokenUsageCost(ctx, apiKey, tokenBillingModel, multiplier, tokenUsage, serviceTier)
	if err != nil {
		logger.LegacyPrintf("service.openai_gateway", "Calculate image response token cost failed: %v", err)
		return imageCost, nil
	}
	return mergeCostBreakdowns(string(BillingModeImage), tokenCost, imageCost), nil
}

func mergeCostBreakdowns(billingMode string, parts ...*CostBreakdown) *CostBreakdown {
	out := &CostBreakdown{BillingMode: billingMode}
	for _, part := range parts {
		if part == nil {
			continue
		}
		out.InputCost += part.InputCost
		out.OutputCost += part.OutputCost
		out.ImageOutputCost += part.ImageOutputCost
		out.CacheCreationCost += part.CacheCreationCost
		out.CacheReadCost += part.CacheReadCost
		out.TotalCost += part.TotalCost
		out.ActualCost += part.ActualCost
	}
	return out
}

func (s *OpenAIGatewayService) calculateOpenAIImageCost(
	ctx context.Context,
	billingModel string,
	apiKey *APIKey,
	result *OpenAIForwardResult,
	requestType RequestType,
	rateMultiplier float64,
) *CostBreakdown {
	if s.resolver != nil && apiKey != nil && apiKey.Group != nil {
		gid := apiKey.Group.ID
		resolved := s.resolver.Resolve(ctx, PricingInput{Model: billingModel, GroupID: &gid})
		if resolved != nil && resolved.Source == PricingSourceChannel && (resolved.Mode == BillingModeImage || resolved.Mode == BillingModePerRequest) {
			cost, err := s.billingService.CalculateCostUnified(CostInput{
				Ctx:            ctx,
				Model:          billingModel,
				GroupID:        &gid,
				RequestCount:   result.ImageCount,
				SizeTier:       result.ImageSize,
				RateMultiplier: rateMultiplier,
				Resolver:       s.resolver,
				Resolved:       resolved,
			})
			if err == nil {
				return cost
			}
			logger.LegacyPrintf("service.openai_gateway", "Calculate OpenAI image unified cost failed: %v", err)
		}
	}

	var groupConfig *ImagePriceConfig
	if apiKey != nil && apiKey.Group != nil {
		groupConfig = apiKey.Group.GetImagePriceConfigForRequestType(requestType.Normalize())
	}
	return s.billingService.CalculateImageCost(billingModel, result.ImageSize, result.ImageCount, groupConfig, rateMultiplier)
}

// ParseCodexRateLimitHeaders extracts Codex usage limits from response headers.
// Exported for use in ratelimit_service when handling OpenAI 429 responses.
func ParseCodexRateLimitHeaders(headers http.Header) *OpenAICodexUsageSnapshot {
	snapshot := &OpenAICodexUsageSnapshot{}
	hasData := false

	// Helper to parse float64 from header
	parseFloat := func(key string) *float64 {
		if v := headers.Get(key); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return &f
			}
		}
		return nil
	}

	// Helper to parse int from header
	parseInt := func(key string) *int {
		if v := headers.Get(key); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				return &i
			}
		}
		return nil
	}

	// Primary (weekly) limits
	if v := parseFloat("x-codex-primary-used-percent"); v != nil {
		snapshot.PrimaryUsedPercent = v
		hasData = true
	}
	if v := parseInt("x-codex-primary-reset-after-seconds"); v != nil {
		snapshot.PrimaryResetAfterSeconds = v
		hasData = true
	}
	if v := parseInt("x-codex-primary-window-minutes"); v != nil {
		snapshot.PrimaryWindowMinutes = v
		hasData = true
	}

	// Secondary (5h) limits
	if v := parseFloat("x-codex-secondary-used-percent"); v != nil {
		snapshot.SecondaryUsedPercent = v
		hasData = true
	}
	if v := parseInt("x-codex-secondary-reset-after-seconds"); v != nil {
		snapshot.SecondaryResetAfterSeconds = v
		hasData = true
	}
	if v := parseInt("x-codex-secondary-window-minutes"); v != nil {
		snapshot.SecondaryWindowMinutes = v
		hasData = true
	}

	// Overflow ratio
	if v := parseFloat("x-codex-primary-over-secondary-limit-percent"); v != nil {
		snapshot.PrimaryOverSecondaryPercent = v
		hasData = true
	}

	if !hasData {
		return nil
	}

	snapshot.UpdatedAt = time.Now().Format(time.RFC3339)
	return snapshot
}

func codexSnapshotBaseTime(snapshot *OpenAICodexUsageSnapshot, fallback time.Time) time.Time {
	if snapshot == nil {
		return fallback
	}
	if snapshot.UpdatedAt == "" {
		return fallback
	}
	base, err := time.Parse(time.RFC3339, snapshot.UpdatedAt)
	if err != nil {
		return fallback
	}
	return base
}

func codexResetAtRFC3339(base time.Time, resetAfterSeconds *int) *string {
	if resetAfterSeconds == nil {
		return nil
	}
	sec := *resetAfterSeconds
	if sec < 0 {
		sec = 0
	}
	resetAt := base.Add(time.Duration(sec) * time.Second).Format(time.RFC3339)
	return &resetAt
}

func buildCodexUsageExtraUpdates(snapshot *OpenAICodexUsageSnapshot, fallbackNow time.Time) map[string]any {
	if snapshot == nil {
		return nil
	}

	baseTime := codexSnapshotBaseTime(snapshot, fallbackNow)
	updates := make(map[string]any)

	// 保存原始 primary/secondary 字段，便于排查问题
	if snapshot.PrimaryUsedPercent != nil {
		updates["codex_primary_used_percent"] = *snapshot.PrimaryUsedPercent
	}
	if snapshot.PrimaryResetAfterSeconds != nil {
		updates["codex_primary_reset_after_seconds"] = *snapshot.PrimaryResetAfterSeconds
	}
	if snapshot.PrimaryWindowMinutes != nil {
		updates["codex_primary_window_minutes"] = *snapshot.PrimaryWindowMinutes
	}
	if snapshot.SecondaryUsedPercent != nil {
		updates["codex_secondary_used_percent"] = *snapshot.SecondaryUsedPercent
	}
	if snapshot.SecondaryResetAfterSeconds != nil {
		updates["codex_secondary_reset_after_seconds"] = *snapshot.SecondaryResetAfterSeconds
	}
	if snapshot.SecondaryWindowMinutes != nil {
		updates["codex_secondary_window_minutes"] = *snapshot.SecondaryWindowMinutes
	}
	if snapshot.PrimaryOverSecondaryPercent != nil {
		updates["codex_primary_over_secondary_percent"] = *snapshot.PrimaryOverSecondaryPercent
	}
	updates["codex_usage_updated_at"] = baseTime.Format(time.RFC3339)

	// 归一化到 5h/7d 规范字段
	if normalized := snapshot.Normalize(); normalized != nil {
		if normalized.Used5hPercent != nil {
			updates["codex_5h_used_percent"] = *normalized.Used5hPercent
		}
		if normalized.Reset5hSeconds != nil {
			updates["codex_5h_reset_after_seconds"] = *normalized.Reset5hSeconds
		}
		if normalized.Window5hMinutes != nil {
			updates["codex_5h_window_minutes"] = *normalized.Window5hMinutes
		}
		if normalized.Used7dPercent != nil {
			updates["codex_7d_used_percent"] = *normalized.Used7dPercent
		}
		if normalized.Reset7dSeconds != nil {
			updates["codex_7d_reset_after_seconds"] = *normalized.Reset7dSeconds
		}
		if normalized.Window7dMinutes != nil {
			updates["codex_7d_window_minutes"] = *normalized.Window7dMinutes
		}
		if reset5hAt := codexResetAtRFC3339(baseTime, normalized.Reset5hSeconds); reset5hAt != nil {
			updates["codex_5h_reset_at"] = *reset5hAt
		}
		if reset7dAt := codexResetAtRFC3339(baseTime, normalized.Reset7dSeconds); reset7dAt != nil {
			updates["codex_7d_reset_at"] = *reset7dAt
		}
	}

	return updates
}

// updateCodexUsageSnapshot saves the Codex usage snapshot to account's Extra field
func (s *OpenAIGatewayService) updateCodexUsageSnapshot(ctx context.Context, accountID int64, snapshot *OpenAICodexUsageSnapshot) {
	if snapshot == nil {
		return
	}
	if s == nil || s.accountRepo == nil {
		return
	}

	now := time.Now()
	updates := buildCodexUsageExtraUpdates(snapshot, now)
	if len(updates) == 0 {
		return
	}
	if !s.getCodexSnapshotThrottle().Allow(accountID, now) {
		return
	}

	go func() {
		updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.accountRepo.UpdateExtra(updateCtx, accountID, updates)
	}()
}

func (s *OpenAIGatewayService) UpdateCodexUsageSnapshotFromHeaders(ctx context.Context, accountID int64, headers http.Header) {
	if accountID <= 0 || headers == nil {
		return
	}
	if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil {
		s.updateCodexUsageSnapshot(ctx, accountID, snapshot)
	}
}

func getOpenAIReasoningEffortFromReqBody(reqBody map[string]any) (value string, present bool) {
	if reqBody == nil {
		return "", false
	}

	// Primary: reasoning.effort
	if reasoning, ok := reqBody["reasoning"].(map[string]any); ok {
		if effort, ok := reasoning["effort"].(string); ok {
			return normalizeOpenAIReasoningEffort(effort), true
		}
	}

	// Fallback: some clients may use a flat field.
	if effort, ok := reqBody["reasoning_effort"].(string); ok {
		return normalizeOpenAIReasoningEffort(effort), true
	}

	return "", false
}

func deriveOpenAIReasoningEffortFromModel(model string) string {
	if strings.TrimSpace(model) == "" {
		return ""
	}

	modelID := strings.TrimSpace(model)
	if strings.Contains(modelID, "/") {
		parts := strings.Split(modelID, "/")
		modelID = parts[len(parts)-1]
	}

	parts := strings.FieldsFunc(strings.ToLower(modelID), func(r rune) bool {
		switch r {
		case '-', '_', ' ':
			return true
		default:
			return false
		}
	})
	if len(parts) == 0 {
		return ""
	}

	return normalizeOpenAIReasoningEffort(parts[len(parts)-1])
}

type openAIRequestView struct {
	body               []byte
	Model              string
	Stream             bool
	PromptCacheKey     string
	PreviousResponseID string
	ServiceTier        string
	ReasoningEffort    string
	patches            []openAIRequestPatch
	patchesDisabled    bool
}

type openAIRequestPatch struct {
	path   string
	delete bool
	value  any
}

func newOpenAIRequestView(body []byte) openAIRequestView {
	if len(body) == 0 {
		return openAIRequestView{}
	}
	return openAIRequestView{
		body:               body,
		Model:              strings.TrimSpace(gjson.GetBytes(body, "model").String()),
		Stream:             gjson.GetBytes(body, "stream").Bool(),
		PromptCacheKey:     strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String()),
		PreviousResponseID: strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()),
		ServiceTier:        strings.TrimSpace(gjson.GetBytes(body, "service_tier").String()),
		ReasoningEffort:    strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String()),
	}
}

// Decode 保留阶段一既有 full-map 行为；后续阶段会把调用点下沉到复杂分支。
func (v openAIRequestView) Decode(c *gin.Context) (map[string]any, error) {
	return getOpenAIRequestBodyMap(c, v.body)
}

func (v *openAIRequestView) MarkPatchSet(path string, value any) {
	if v == nil || v.patchesDisabled {
		return
	}
	path = strings.TrimSpace(path)
	if !isSimpleOpenAIRequestPatchPath(path) {
		v.DisablePatches()
		return
	}
	v.patches = append(v.patches, openAIRequestPatch{path: path, value: value})
}

func (v *openAIRequestView) MarkPatchDelete(path string) {
	if v == nil || v.patchesDisabled {
		return
	}
	path = strings.TrimSpace(path)
	if !isSimpleOpenAIRequestPatchPath(path) {
		v.DisablePatches()
		return
	}
	v.patches = append(v.patches, openAIRequestPatch{path: path, delete: true})
}

func isSimpleOpenAIRequestPatchPath(path string) bool {
	if path == "" || strings.ContainsRune(path, '\\') {
		return false
	}
	for _, part := range strings.Split(path, ".") {
		if strings.TrimSpace(part) == "" {
			return false
		}
	}
	return true
}

func (v *openAIRequestView) DisablePatches() {
	if v == nil {
		return
	}
	v.patchesDisabled = true
	v.patches = nil
}

func (v openAIRequestView) HasPatches() bool {
	return !v.patchesDisabled && len(v.patches) > 0
}

func (v openAIRequestView) ApplyPatches() ([]byte, error) {
	if v.patchesDisabled || len(v.patches) == 0 {
		return nil, errors.New("openai request patches disabled")
	}
	body := v.body
	for _, patch := range v.patches {
		var err error
		if patch.delete {
			body, err = sjson.DeleteBytes(body, patch.path)
		} else {
			body, err = sjson.SetBytes(body, patch.path, patch.value)
		}
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

func setOpenAIRequestMapPath(reqBody map[string]any, path string, value any) {
	path = strings.TrimSpace(path)
	if reqBody == nil || path == "" {
		return
	}
	parts := strings.Split(path, ".")
	current := reqBody
	for _, part := range parts[:len(parts)-1] {
		part = strings.TrimSpace(part)
		if part == "" {
			return
		}
		next, _ := current[part].(map[string]any)
		if next == nil {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	if last != "" {
		current[last] = value
	}
}

func deleteOpenAIRequestMapPath(reqBody map[string]any, path string) {
	path = strings.TrimSpace(path)
	if reqBody == nil || path == "" {
		return
	}
	parts := strings.Split(path, ".")
	current := reqBody
	for _, part := range parts[:len(parts)-1] {
		part = strings.TrimSpace(part)
		if part == "" {
			return
		}
		next, _ := current[part].(map[string]any)
		if next == nil {
			return
		}
		current = next
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	if last != "" {
		delete(current, last)
	}
}

func extractOpenAIRequestMetaFromBody(body []byte) (model string, stream bool, promptCacheKey string) {
	view := newOpenAIRequestView(body)
	return view.Model, view.Stream, view.PromptCacheKey
}

// normalizeOpenAIPassthroughOAuthBody 将透传 OAuth 请求体收敛为旧链路关键行为：
// 1) store=false 2) 非 compact 保持 stream=true；compact 强制 stream=false
func normalizeOpenAIPassthroughOAuthBody(body []byte, compact bool) ([]byte, bool, error) {
	if len(body) == 0 {
		return body, false, nil
	}

	normalizedIngress, err := normalizeOpenAIResponsesIngress(body)
	if err != nil {
		return body, false, err
	}
	changed := false
	switch {
	case len(normalizedIngress.FullReplayBody) > 0 && normalizedIngress.FullReplaySource != "" && normalizedIngress.FullReplaySource != "input":
		body = normalizedIngress.FullReplayBody
		changed = true
	case len(normalizedIngress.FullReplayBody) > 0 && normalizedIngress.FullReplaySource == "input":
		body = normalizedIngress.PrimaryBody
	case len(normalizedIngress.PrimaryBody) > 0 && !bytes.Equal(normalizedIngress.PrimaryBody, body):
		body = normalizedIngress.PrimaryBody
		changed = true
	}

	var reqBody map[string]any
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return body, false, fmt.Errorf("normalize passthrough body parse: %w", err)
	}

	for _, field := range openAIChatGPTInternalUnsupportedFields {
		if _, ok := reqBody[field]; ok {
			delete(reqBody, field)
			changed = true
		}
	}

	if compact {
		if _, ok := reqBody["store"]; ok {
			delete(reqBody, "store")
			changed = true
		}
		if _, ok := reqBody["stream"]; ok {
			delete(reqBody, "stream")
			changed = true
		}
	} else {
		if store, ok := reqBody["store"].(bool); !ok || store {
			reqBody["store"] = false
			changed = true
		}
		if stream, ok := reqBody["stream"].(bool); !ok || !stream {
			reqBody["stream"] = true
			changed = true
		}
		if inputStr, ok := reqBody["input"].(string); ok {
			if strings.TrimSpace(inputStr) != "" {
				reqBody["input"] = []any{
					map[string]any{
						"type":    "message",
						"role":    "user",
						"content": inputStr,
					},
				}
			} else {
				reqBody["input"] = []any{}
			}
			changed = true
		}
	}
	sanitizeOrphanToolOutputs := strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"])) == ""
	if normalizeOpenAIResponsesInputToolRolesWithOptions(reqBody, sanitizeOrphanToolOutputs) {
		changed = true
	}
	if input, ok := reqBody["input"].([]any); ok {
		if normalizedInput, modified := normalizeCodexMessageContentText(input); modified {
			reqBody["input"] = normalizedInput
			input = normalizedInput
			changed = true
		}
		if normalizedInput, modified := normalizeOpenAIResponsesMessageContentPartTypes(input); modified {
			reqBody["input"] = normalizedInput
			changed = true
		}
	}
	if trimOpenAIStoreFalseReasoningItems(reqBody) {
		changed = true
	}
	if !compact {
		if normalizeCodexTools(reqBody) {
			changed = true
		}
		if normalizeCodexToolChoice(reqBody) {
			changed = true
		}
	}
	if extractSystemMessagesFromInput(reqBody) {
		changed = true
	}
	if normalizeOpenAIStrictFunctionToolSchemas(reqBody) {
		changed = true
	}
	if normalizeOpenAIResponseFormatSchemas(reqBody) {
		changed = true
	}
	if normalizeOpenAIToolSchemaLookaroundPatterns(reqBody) {
		changed = true
	}

	if !changed {
		return body, false, nil
	}

	normalized, err := marshalOpenAIResponsesRequestBodyOrdered(reqBody)
	if err != nil {
		return body, false, fmt.Errorf("normalize passthrough body serialize: %w", err)
	}
	return normalized, true, nil
}

func normalizeOpenAIPassthroughBaseBody(body []byte, compact bool, stripTopP bool) ([]byte, bool, error) {
	if len(body) == 0 {
		return body, false, nil
	}

	var reqBody map[string]any
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return body, false, fmt.Errorf("normalize passthrough base body parse: %w", err)
	}

	changed := false
	unsupportedFields := append([]string(nil), openAIResponsesUnsupportedFields...)
	if stripTopP {
		unsupportedFields = append(unsupportedFields, "top_p")
	}
	for _, unsupportedField := range unsupportedFields {
		if _, ok := reqBody[unsupportedField]; ok {
			delete(reqBody, unsupportedField)
			changed = true
			if unsupportedField == "temperature" || unsupportedField == "top_p" || unsupportedField == "verbosity" {
				recordOpenAICompatStrippedField(unsupportedField)
			}
		}
	}

	if !compact {
		if inputStr, ok := reqBody["input"].(string); ok {
			if strings.TrimSpace(inputStr) != "" {
				reqBody["input"] = []any{
					map[string]any{
						"type":    "message",
						"role":    "user",
						"content": inputStr,
					},
				}
			} else {
				reqBody["input"] = []any{}
			}
			changed = true
		}
	}
	sanitizeOrphanToolOutputs := strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"])) == ""
	if normalizeOpenAIResponsesInputToolRolesWithOptions(reqBody, sanitizeOrphanToolOutputs) {
		changed = true
	}
	if trimOpenAIStoreFalseReasoningItems(reqBody) {
		changed = true
	}
	if normalizeOpenAIResponseFormatSchemas(reqBody) {
		changed = true
	}
	if normalizeOpenAIToolSchemaLookaroundPatterns(reqBody) {
		changed = true
	}

	if !changed {
		return body, false, nil
	}

	normalized, err := marshalOpenAIResponsesRequestBodyOrdered(reqBody)
	if err != nil {
		return body, false, fmt.Errorf("normalize passthrough base body serialize: %w", err)
	}
	return normalized, true, nil
}

func shouldStripTopPForResponsesUpstream(account *Account) bool {
	if account == nil || account.Platform != PlatformOpenAI {
		return false
	}
	if account.Type == AccountTypeOAuth {
		return true
	}
	if account.Type != AccountTypeAPIKey {
		return false
	}
	baseURL := account.GetOpenAIBaseURL()
	if baseURL == "" {
		return true
	}
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return strings.Contains(strings.ToLower(baseURL), "api.openai.com")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	return host == "api.openai.com"
}

func normalizeOpenAIResponsesInputToolRolesWithOptions(reqBody map[string]any, sanitizeOrphans bool) bool {
	if reqBody == nil {
		return false
	}
	previousResponseID := strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"]))
	input, ok := reqBody["input"].([]any)
	if !ok {
		return false
	}
	normalized, changed := normalizeCodexToolRoleMessages(input)
	if changed {
		reqBody["input"] = normalized
		input = normalized
	}
	if previousResponseID != "" && strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"])) == "" {
		reqBody["previous_response_id"] = previousResponseID
		changed = true
	}
	if sanitizeOrphans && sanitizeOpenAIResponsesOrphanToolOutputs(reqBody, input, previousResponseID != "") {
		return true
	}
	return changed
}

func sanitizeOpenAIResponsesOrphanToolOutputs(reqBody map[string]any, input []any, hasPreviousResponseID bool) bool {
	if len(input) == 0 || hasPreviousResponseID {
		return false
	}

	toolCallIDs := make(map[string]struct{})
	referenceIDs := make(map[string]struct{})
	for _, item := range input {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if strings.TrimSpace(firstNonEmptyString(m["type"])) == "item_reference" {
			if refID, ok := m["id"].(string); ok && strings.TrimSpace(refID) != "" {
				referenceIDs[strings.TrimSpace(refID)] = struct{}{}
			}
			continue
		}
		switch strings.TrimSpace(firstNonEmptyString(m["type"])) {
		case "function_call", "tool_call", "local_shell_call", "tool_search_call", "custom_tool_call", "mcp_tool_call":
			if callID := toolCallContextIDFromMap(m); callID != "" {
				toolCallIDs[callID] = struct{}{}
			}
		}
	}

	modified := false
	normalized := make([]any, 0, len(input))
	for _, item := range input {
		m, ok := item.(map[string]any)
		if !ok {
			normalized = append(normalized, item)
			continue
		}
		itemType := strings.TrimSpace(firstNonEmptyString(m["type"]))
		if !isToolContinuationOutputItemType(itemType) {
			normalized = append(normalized, item)
			continue
		}
		callID, _ := m["call_id"].(string)
		callID = strings.TrimSpace(callID)
		if callID != "" {
			if _, ok := toolCallIDs[callID]; ok {
				normalized = append(normalized, item)
				continue
			}
			if _, ok := referenceIDs[callID]; ok {
				normalized = append(normalized, item)
				continue
			}
		}

		output := strings.TrimSpace(firstNonEmptyString(m["output"]))
		if output == "" && m["output"] != nil {
			if raw, err := json.Marshal(m["output"]); err == nil {
				output = string(raw)
			}
		}
		normalized = append(normalized, map[string]any{
			"type":    "message",
			"role":    "user",
			"content": output,
		})
		modified = true
	}
	if !modified {
		return false
	}
	reqBody["input"] = normalized
	return true
}

func shouldDetachLegacyOAuthPassthroughContext(account *Account, reqStream bool, body []byte) bool {
	if account == nil || account.Type != AccountTypeOAuth {
		return reqStream
	}
	if reqStream {
		return true
	}
	return gjson.GetBytes(body, "stream").Bool()
}

func isOpenAIResponsesInboundPath(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	path := strings.TrimSpace(c.Request.URL.Path)
	return strings.Contains(path, "/responses")
}

func isOpenAIGatewayResponsesPath(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	path := strings.TrimRight(strings.TrimSpace(c.Request.URL.Path), "/")
	return path == "/openai/v1/responses" || strings.HasPrefix(path, "/openai/v1/responses/")
}

func shouldPatchDefaultCodexSynthInstructions(c *gin.Context, isCodexCLI bool, isMessagesBridgeRequest bool) bool {
	if isMessagesBridgeRequest {
		return false
	}
	return isCodexCLI || isOpenAIGatewayResponsesPath(c)
}

func shouldInjectDefaultInstructionsForOpenAIResponses(c *gin.Context, account *Account, isMessagesBridgeRequest bool, isCompactRequest bool) bool {
	_ = isCompactRequest
	if account == nil {
		return false
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return false
	}
	if isMessagesBridgeRequest {
		return true
	}
	return isOpenAIResponsesInboundPath(c)
}

func shouldInjectDefaultInstructionsForOpenAIMessagesBridge(c *gin.Context) bool {
	if c != nil && c.Request != nil && IsClaudeCodeClient(c.Request.Context()) {
		return true
	}
	return isOpenAICompatMessagesBridgeContext(c)
}

func finalizeOpenAIResponsesOAuthUpstreamBody(c *gin.Context, account *Account, reqModel string, body []byte) ([]byte, bool, error) {
	if account == nil || account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return body, false, nil
	}
	if !isOpenAIResponsesInboundPath(c) || len(body) == 0 {
		return body, false, nil
	}

	var reqBody map[string]any
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return body, false, fmt.Errorf("parse final openai oauth responses body: %w", err)
	}

	isCompact := isOpenAIResponsesCompactPath(c)
	isMessagesBridge := isOpenAICompatMessagesBridgeContext(c) || isOpenAICompatMessagesBridgeRequestBody(reqBody)

	normalizedBody, normalized, err := normalizeOpenAIPassthroughOAuthBody(body, isCompact)
	if err != nil {
		return body, false, err
	}
	body = normalizedBody
	changed := normalized

	if shouldInjectDefaultInstructionsForOpenAIResponses(c, account, isMessagesBridge, isCompact) {
		bodyWithInstructions, injected, err := ensureOpenAIPassthroughInstructions(c, reqModel, body)
		if err != nil {
			return body, false, err
		}
		body = bodyWithInstructions
		changed = changed || injected
	}
	if isMessagesBridge && gjson.GetBytes(body, "prompt_cache_key").Exists() {
		bodyWithoutPromptCacheKey, err := sjson.DeleteBytes(body, "prompt_cache_key")
		if err != nil {
			return body, false, fmt.Errorf("delete messages bridge prompt_cache_key: %w", err)
		}
		body = bodyWithoutPromptCacheKey
		changed = true
	}
	setOpenAITTFTWatchdogBypass(c, IsImageGenerationIntent(openAIResponsesEndpoint, reqModel, body))

	return body, changed, nil
}

func ensureOpenAIPassthroughInstructions(c *gin.Context, reqModel string, body []byte) ([]byte, bool, error) {
	_ = reqModel
	instructions := gjson.GetBytes(body, "instructions")
	if instructions.Exists() && instructions.Type == gjson.String && strings.TrimSpace(instructions.String()) != "" {
		return body, false, nil
	}

	defaultInstructions := strings.TrimSpace(openai.DefaultInstructions)
	if defaultInstructions == "" {
		defaultInstructions = "You are a helpful coding assistant."
	}
	updated, err := sjson.SetBytes(body, "instructions", defaultInstructions)
	if err != nil {
		return body, false, fmt.Errorf("inject passthrough instructions: %w", err)
	}
	return updated, true, nil
}

func detectOpenAIPassthroughInstructionsRejectReason(c *gin.Context, reqModel string, body []byte, forceCodexCLI bool) string {
	_ = reqModel
	if !isOpenAICodexOfficialClientRequest(c) && !forceCodexCLI {
		return ""
	}

	instructions := gjson.GetBytes(body, "instructions")
	if !instructions.Exists() {
		return "instructions_missing"
	}
	if instructions.Type != gjson.String {
		return "instructions_not_string"
	}
	if strings.TrimSpace(instructions.String()) == "" {
		return "instructions_empty"
	}
	return ""
}

func extractOpenAIReasoningEffortFromBody(body []byte, requestedModel string) *string {
	reasoningEffort := strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String())
	if reasoningEffort == "" {
		reasoningEffort = strings.TrimSpace(gjson.GetBytes(body, "reasoning_effort").String())
	}
	if reasoningEffort != "" {
		normalized := normalizeOpenAIReasoningEffort(reasoningEffort)
		if normalized == "" {
			return nil
		}
		return &normalized
	}

	value := deriveOpenAIReasoningEffortFromModel(requestedModel)
	if value == "" {
		return nil
	}
	return &value
}

func extractOpenAIServiceTier(reqBody map[string]any) *string {
	if reqBody == nil {
		return nil
	}
	raw, ok := reqBody["service_tier"].(string)
	if !ok {
		return nil
	}
	return normalizeOpenAIServiceTier(raw)
}

func extractOpenAIServiceTierFromBody(body []byte) *string {
	if len(body) == 0 {
		return nil
	}
	return normalizeOpenAIServiceTier(gjson.GetBytes(body, "service_tier").String())
}

func normalizeOpenAIServiceTier(raw string) *string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return nil
	}
	if value == "fast" {
		value = "priority"
	}
	switch value {
	case "priority", "flex", "auto", "default", "scale":
		return &value
	default:
		return nil
	}
}

type OpenAIFastBlockedError struct {
	Message string
}

func (e *OpenAIFastBlockedError) Error() string { return e.Message }

func (s *OpenAIGatewayService) evaluateOpenAIFastPolicy(ctx context.Context, account *Account, model, serviceTier string) (action, errMsg string) {
	if s == nil || s.settingService == nil {
		return BetaPolicyActionPass, ""
	}
	tier := strings.ToLower(strings.TrimSpace(serviceTier))
	if tier == "" {
		return BetaPolicyActionPass, ""
	}
	settings := openAIFastPolicySettingsFromContext(ctx)
	if settings == nil {
		fetched, err := s.settingService.GetOpenAIFastPolicySettings(ctx)
		if err != nil || fetched == nil {
			return BetaPolicyActionPass, ""
		}
		settings = fetched
	}
	return evaluateOpenAIFastPolicyWithSettings(settings, account, model, tier)
}

func evaluateOpenAIFastPolicyWithSettings(settings *OpenAIFastPolicySettings, account *Account, model, tier string) (action, errMsg string) {
	if settings == nil {
		return BetaPolicyActionPass, ""
	}
	isOAuth := account != nil && account.IsOAuth()
	isBedrock := account != nil && account.IsBedrock()
	for _, rule := range settings.Rules {
		if !betaPolicyScopeMatches(rule.Scope, isOAuth, isBedrock) {
			continue
		}
		ruleTier := strings.ToLower(strings.TrimSpace(rule.ServiceTier))
		if ruleTier != "" && ruleTier != OpenAIFastTierAny && ruleTier != tier {
			continue
		}
		eff := BetaPolicyRule{
			Action:               rule.Action,
			ErrorMessage:         rule.ErrorMessage,
			ModelWhitelist:       rule.ModelWhitelist,
			FallbackAction:       rule.FallbackAction,
			FallbackErrorMessage: rule.FallbackErrorMessage,
		}
		return resolveRuleAction(eff, model)
	}
	return BetaPolicyActionPass, ""
}

type openAIFastPolicyCtxKeyType struct{}

var openAIFastPolicyCtxKey = openAIFastPolicyCtxKeyType{}

func withOpenAIFastPolicyContext(ctx context.Context, settings *OpenAIFastPolicySettings) context.Context {
	if ctx == nil || settings == nil {
		return ctx
	}
	return context.WithValue(ctx, openAIFastPolicyCtxKey, settings)
}

func openAIFastPolicySettingsFromContext(ctx context.Context) *OpenAIFastPolicySettings {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(openAIFastPolicyCtxKey).(*OpenAIFastPolicySettings); ok {
		return v
	}
	return nil
}

func (s *OpenAIGatewayService) applyOpenAIFastPolicyToBody(ctx context.Context, account *Account, model string, body []byte) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}
	rawTier := gjson.GetBytes(body, "service_tier").String()
	if rawTier == "" {
		return body, nil
	}
	normTier := normalizedOpenAIServiceTierValue(rawTier)
	if normTier == "" {
		return body, nil
	}
	action, errMsg := s.evaluateOpenAIFastPolicy(ctx, account, model, normTier)
	switch action {
	case BetaPolicyActionBlock:
		msg := errMsg
		if msg == "" {
			msg = fmt.Sprintf("openai service_tier=%s is not allowed for model %s", normTier, model)
		}
		return body, &OpenAIFastBlockedError{Message: msg}
	case BetaPolicyActionFilter:
		trimmed, err := sjson.DeleteBytes(body, "service_tier")
		if err != nil {
			return body, fmt.Errorf("strip service_tier from body: %w", err)
		}
		return trimmed, nil
	default:
		if normTier == rawTier {
			return body, nil
		}
		updated, err := sjson.SetBytes(body, "service_tier", normTier)
		if err != nil {
			return body, fmt.Errorf("normalize service_tier on pass: %w", err)
		}
		return updated, nil
	}
}

func writeOpenAIFastPolicyBlockedResponse(c *gin.Context, err *OpenAIFastBlockedError) {
	if c == nil || err == nil {
		return
	}
	MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
	c.JSON(http.StatusForbidden, gin.H{
		"error": gin.H{
			"type":    "permission_error",
			"message": err.Message,
		},
	})
}

func (s *OpenAIGatewayService) applyOpenAIFastPolicyToWSResponseCreate(ctx context.Context, account *Account, model string, frame []byte) ([]byte, *OpenAIFastBlockedError, error) {
	if len(frame) == 0 || !gjson.ValidBytes(frame) {
		return frame, nil, nil
	}
	frameType := strings.TrimSpace(gjson.GetBytes(frame, "type").String())
	if frameType != "response.create" {
		return frame, nil, nil
	}
	rawTier := gjson.GetBytes(frame, "service_tier").String()
	if rawTier == "" {
		return frame, nil, nil
	}
	normTier := normalizedOpenAIServiceTierValue(rawTier)
	if normTier == "" {
		return frame, nil, nil
	}
	action, errMsg := s.evaluateOpenAIFastPolicy(ctx, account, model, normTier)
	switch action {
	case BetaPolicyActionBlock:
		msg := errMsg
		if msg == "" {
			msg = fmt.Sprintf("openai service_tier=%s is not allowed for model %s", normTier, model)
		}
		return frame, &OpenAIFastBlockedError{Message: msg}, nil
	case BetaPolicyActionFilter:
		trimmed, err := sjson.DeleteBytes(frame, "service_tier")
		if err != nil {
			return frame, nil, fmt.Errorf("strip service_tier from ws frame: %w", err)
		}
		return trimmed, nil, nil
	default:
		if normTier == rawTier {
			return frame, nil, nil
		}
		updated, err := sjson.SetBytes(frame, "service_tier", normTier)
		if err != nil {
			return frame, nil, fmt.Errorf("normalize service_tier on ws pass: %w", err)
		}
		return updated, nil, nil
	}
}

func newOpenAIFastPolicyWSEventID() string {
	id, err := uuid.NewRandom()
	if err != nil {
		return "evt_openai_fast_policy"
	}
	return "evt_" + strings.ReplaceAll(id.String(), "-", "")
}

func buildOpenAIFastPolicyBlockedWSEvent(err *OpenAIFastBlockedError) []byte {
	if err == nil {
		return nil
	}
	eventID := newOpenAIFastPolicyWSEventID()
	payload, mErr := json.Marshal(map[string]any{
		"event_id": eventID,
		"type":     "error",
		"error": map[string]any{
			"type":    "invalid_request_error",
			"code":    "policy_violation",
			"message": err.Message,
		},
	})
	if mErr != nil {
		return []byte(`{"event_id":"` + eventID + `","type":"error","error":{"type":"invalid_request_error","code":"policy_violation","message":"openai fast policy blocked this request"}}`)
	}
	return payload
}

func openAIRequestBodyMayContainImageInput(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	input := gjson.GetBytes(body, "input")
	messages := gjson.GetBytes(body, "messages.#-1")
	return openAIJSONValueMayContainImageInput(input) || openAIJSONValueMayContainImageInput(messages)
}

func openAIJSONValueMayContainImageInput(value gjson.Result) bool {
	if !value.Exists() {
		return false
	}
	if value.IsArray() {
		found := false
		value.ForEach(func(_, item gjson.Result) bool {
			if openAIJSONValueMayContainImageInput(item) {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if value.IsObject() {
		if strings.TrimSpace(value.Get("type").String()) == "input_image" || value.Get("image_url").Exists() {
			return true
		}
		return openAIJSONValueMayContainImageInput(value.Get("content"))
	}
	return false
}

func openAIRequestBodyMayContainEmptyBase64InputImage(body []byte) bool {
	if len(body) == 0 || !openAIRequestBodyMayContainInputImageToken(body) {
		return false
	}
	input := gjson.GetBytes(body, "input")
	if !input.Exists() {
		return false
	}
	return openAIJSONValueMayContainEmptyBase64InputImage(input)
}

func shouldDecodeOpenAIResponsesImageToolMutationBody(body []byte, codexBridgeEnabled bool, isMessagesBridgeRequest bool, allowImageGeneration bool, imageIntent bool, account *Account) bool {
	if codexBridgeEnabled && !isMessagesBridgeRequest {
		return true
	}
	if openAIRequestBodyImageGenerationToolNeedsNormalization(body) {
		return true
	}
	if !openAIRequestBodyHasImageGenerationTool(body) {
		return false
	}
	if !allowImageGeneration {
		return true
	}
	return accountShouldStripDeclaredImageGenerationTool(account, imageIntent)
}

func openAIRequestBodyStoreFalseMayContainReasoningInputItem(body []byte) bool {
	root := parseRawJSONView(body)
	store := root.Get("store")
	if !store.Exists() || store.Type != gjson.False {
		return false
	}
	return openAIJSONValueMayContainReasoningInputItem(root.Get("input"))
}

func openAIJSONValueMayContainReasoningInputItem(value gjson.Result) bool {
	if !value.Exists() {
		return false
	}
	if value.IsArray() {
		found := false
		value.ForEach(func(_, item gjson.Result) bool {
			if openAIJSONValueMayContainReasoningInputItem(item) {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if value.IsObject() {
		if strings.TrimSpace(value.Get("type").String()) == "reasoning" {
			return true
		}
		return openAIJSONValueMayContainReasoningInputItem(value.Get("content"))
	}
	return false
}

func openAIRequestBodyMayContainInputImageToken(body []byte) bool {
	if bytes.Contains(body, []byte("input_image")) {
		return true
	}
	// JSON 字符串任意字符都可能被 unicode escape，遇到 \u 时交给 gjson 解码后的结构扫描兜底。
	return bytes.Contains(body, []byte("\\u"))
}

func openAIJSONValueMayContainEmptyBase64InputImage(value gjson.Result) bool {
	if !value.Exists() {
		return false
	}
	if value.IsArray() {
		found := false
		value.ForEach(func(_, item gjson.Result) bool {
			if openAIJSONValueMayContainEmptyBase64InputImage(item) {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if value.IsObject() {
		if strings.TrimSpace(value.Get("type").String()) == "input_image" && isEmptyBase64DataURI(value.Get("image_url").String()) {
			return true
		}
		return openAIJSONValueMayContainEmptyBase64InputImage(value.Get("content"))
	}
	return false
}

func sanitizeEmptyBase64InputImagesInOpenAIBody(body []byte) ([]byte, bool, error) {
	if !openAIRequestBodyMayContainEmptyBase64InputImage(body) {
		return body, false, nil
	}

	var reqBody map[string]any
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return body, false, fmt.Errorf("sanitize request body: %w", err)
	}
	if !sanitizeEmptyBase64InputImagesInOpenAIRequestBodyMap(reqBody) {
		return body, false, nil
	}
	normalized, err := marshalOpenAIResponsesRequestBodyOrdered(reqBody)
	if err != nil {
		return body, false, fmt.Errorf("serialize sanitized request body: %w", err)
	}
	return normalized, true, nil
}

func sanitizeEmptyBase64InputImagesInOpenAIRequestBodyMap(reqBody map[string]any) bool {
	if reqBody == nil {
		return false
	}
	input, ok := reqBody["input"]
	if !ok {
		return false
	}
	normalizedInput, changed := sanitizeEmptyBase64InputImagesInOpenAIInput(input)
	if !changed {
		return false
	}
	reqBody["input"] = normalizedInput
	return true
}

func sanitizeEmptyBase64InputImagesInOpenAIInput(input any) (any, bool) {
	items, ok := input.([]any)
	if !ok {
		return input, false
	}

	normalizedItems := make([]any, 0, len(items))
	changed := false
	for _, item := range items {
		itemMap, ok := item.(map[string]any)
		if !ok {
			normalizedItems = append(normalizedItems, item)
			continue
		}
		if shouldDropEmptyBase64InputImagePart(itemMap) {
			changed = true
			continue
		}
		content, ok := itemMap["content"]
		if !ok {
			normalizedItems = append(normalizedItems, itemMap)
			continue
		}
		parts, ok := content.([]any)
		if !ok {
			normalizedItems = append(normalizedItems, itemMap)
			continue
		}

		normalizedParts := make([]any, 0, len(parts))
		itemChanged := false
		for _, part := range parts {
			if shouldDropEmptyBase64InputImagePart(part) {
				changed = true
				itemChanged = true
				continue
			}
			normalizedParts = append(normalizedParts, part)
		}
		if itemChanged {
			if len(normalizedParts) == 0 {
				continue
			}
			itemMap["content"] = normalizedParts
		}
		normalizedItems = append(normalizedItems, itemMap)
	}
	if !changed {
		return input, false
	}
	return normalizedItems, true
}

func shouldDropEmptyBase64InputImagePart(part any) bool {
	partMap, ok := part.(map[string]any)
	if !ok {
		return false
	}
	typeValue, _ := partMap["type"].(string)
	if strings.TrimSpace(typeValue) != "input_image" {
		return false
	}
	imageURL, _ := partMap["image_url"].(string)
	return isEmptyBase64DataURI(imageURL)
}

func isEmptyBase64DataURI(raw string) bool {
	if !strings.HasPrefix(raw, "data:") {
		return false
	}
	rest := strings.TrimPrefix(raw, "data:")
	semicolonIdx := strings.Index(rest, ";")
	if semicolonIdx < 0 {
		return false
	}
	rest = rest[semicolonIdx+1:]
	if !strings.HasPrefix(rest, "base64,") {
		return false
	}
	return strings.TrimSpace(strings.TrimPrefix(rest, "base64,")) == ""
}

func getOpenAIRequestBodyMap(_ *gin.Context, body []byte) (map[string]any, error) {
	var reqBody map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&reqBody); err != nil {
		return nil, fmt.Errorf("parse request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parse request: unexpected trailing JSON value")
		}
		return nil, fmt.Errorf("parse request: %w", err)
	}
	return reqBody, nil
}

func extractOpenAIReasoningEffort(reqBody map[string]any, requestedModel string) *string {
	if value, present := getOpenAIReasoningEffortFromReqBody(reqBody); present {
		if value == "" {
			return nil
		}
		return &value
	}

	value := deriveOpenAIReasoningEffortFromModel(requestedModel)
	if value == "" {
		return nil
	}
	return &value
}

func normalizeOpenAIReasoningEffort(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return ""
	}

	// Normalize separators for "x-high"/"x_high" variants.
	value = strings.NewReplacer("-", "", "_", "", " ", "").Replace(value)

	switch value {
	case "none", "minimal":
		return ""
	case "low", "medium", "high":
		return value
	case "xhigh", "extrahigh":
		return "xhigh"
	default:
		// Only store known effort levels for now to keep UI consistent.
		return ""
	}
}
