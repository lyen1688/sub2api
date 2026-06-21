package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// openaiCCRawAllowedHeaders 是 CC 直转路径专用的客户端 header 透传白名单。
//
// **关键**：不能复用 openaiAllowedHeaders——后者含 Codex 客户端专属 header
// （originator / session_id / x-codex-turn-state / x-codex-turn-metadata / conversation_id），
// 这些在 ChatGPT OAuth 上游是必需的，但透传给 DeepSeek/Kimi/GLM 等第三方
// OpenAI 兼容上游会造成：
//   - 完全忽略（多数友好厂商）——隐性污染上游统计
//   - 400 "unknown parameter"（严格上游）——可见错误
//
// 这里仅放行通用 HTTP header；content-type / authorization / accept 由上下文
// 显式设置，不依赖透传。
//
// 参见决策记录：
// pensieve/short-term/maxims/dont-reuse-shared-headers-whitelist-across-different-upstream-trust-domains
var openaiCCRawAllowedHeaders = map[string]bool{
	"accept-language": true,
	"user-agent":      true,
}

var openaiRawChatResponsesOnlyFields = []string{
	"include",
	"store",
	"parallel_tool_calls",
	"previous_response_id",
	"reasoning",
}

// forwardAsRawChatCompletions 直转客户端的 Chat Completions 请求到上游
// `{base_url}/v1/chat/completions`，**不**做 CC↔Responses 协议转换。
//
// 适用场景：account.platform=openai && account.type=apikey && 上游已被探测确认
// 不支持 /v1/responses 端点（如 DeepSeek/Kimi/GLM/Qwen 等第三方 OpenAI 兼容上游）。
//
// 与 ForwardAsChatCompletions 的关键差异：
//
//   - 不调用 apicompat.ChatCompletionsToResponses，body 仅做模型 ID 改写
//   - 上游 URL 拼到 /v1/chat/completions 而非 /v1/responses
//   - 流式响应 SSE 直接透传给客户端（上游 chunk 已是 CC 格式）
//   - 非流式响应 JSON 直接透传，仅按需提取 usage
//   - 不应用 codex OAuth transform（APIKey 路径无 OAuth）
//   - 不注入 prompt_cache_key（OAuth 专属机制）
//
// 调用入口：openai_gateway_chat_completions.go::ForwardAsChatCompletions
// 在函数顶部按 openai_compat.ShouldUseResponsesAPI 分流。
func (s *OpenAIGatewayService) forwardAsRawChatCompletions(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	promptCacheKey string,
	modelHints ...string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	defaultMappedModel := ""
	selectedFallbackModel := ""
	if len(modelHints) > 0 {
		defaultMappedModel = modelHints[0]
	}
	if len(modelHints) > 1 {
		selectedFallbackModel = modelHints[1]
	}

	// 1. Parse minimal fields needed for routing/billing
	originalModel := gjson.GetBytes(body, "model").String()
	if originalModel == "" {
		writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return nil, fmt.Errorf("missing model in request")
	}
	clientStream := gjson.GetBytes(body, "stream").Bool()

	// 1b. Extract reasoning effort and service tier from the raw body before any transformation.
	reasoningEffort := extractOpenAIReasoningEffortFromBody(body, originalModel)
	var serviceTier *string

	// 2. Resolve model mapping (same as ForwardAsChatCompletions)
	billingModel := resolveOpenAIForwardModelWithSettingsAndSelectedFallback(ctx, s.settingService, account, originalModel, defaultMappedModel, selectedFallbackModel)
	upstreamModel := normalizeOpenAIModelForUpstream(account, billingModel)

	// 3. Rewrite model in body (no protocol conversion)
	upstreamBody := body
	if upstreamModel != originalModel {
		upstreamBody = ReplaceModelInBody(body, upstreamModel)
	}
	compatBody, compatErr := normalizeRawChatCompletionsCompatBody(upstreamBody)
	if compatErr != nil {
		return nil, fmt.Errorf("normalize raw chat compat body: %w", compatErr)
	}
	upstreamBody = compatBody
	promptCacheKey = strings.TrimSpace(promptCacheKey)
	if promptCacheKey != "" {
		existingPromptCacheKey := gjson.GetBytes(upstreamBody, "prompt_cache_key")
		if !existingPromptCacheKey.Exists() || existingPromptCacheKey.Type != gjson.String || strings.TrimSpace(existingPromptCacheKey.String()) == "" {
			updatedBody, setErr := sjson.SetBytes(upstreamBody, "prompt_cache_key", promptCacheKey)
			if setErr != nil {
				return nil, fmt.Errorf("inject prompt_cache_key for raw compat body: %w", setErr)
			}
			upstreamBody = updatedBody
			recordOpenAICompatPromptCacheInjected()
		}
	}

	// 4. Apply OpenAI fast policy on the CC body
	updatedBody, policyErr := s.applyOpenAIFastPolicyToBody(ctx, account, upstreamModel, upstreamBody)
	if policyErr != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(policyErr, &blocked) {
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
			writeChatCompletionsError(c, http.StatusForbidden, "permission_error", blocked.Message)
		}
		return nil, policyErr
	}
	upstreamBody = updatedBody
	serviceTier = extractOpenAIServiceTierFromBody(upstreamBody)
	if clientStream {
		var usageErr error
		upstreamBody, usageErr = ensureOpenAIChatStreamUsage(upstreamBody)
		if usageErr != nil {
			return nil, fmt.Errorf("enable stream usage: %w", usageErr)
		}
	}

	logger.L().Debug("openai chat_completions raw: forwarding without protocol conversion",
		zap.Int64("account_id", account.ID),
		zap.String("original_model", originalModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("stream", clientStream),
	)

	// 5. Build upstream request
	apiKey := account.GetOpenAIApiKey()
	if apiKey == "" {
		return nil, fmt.Errorf("account %d missing api_key", account.ID)
	}
	baseURL := account.GetOpenAIBaseURL()
	if baseURL == "" {
		baseURL = "https://api.openai.com"
	}
	validatedURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base_url: %w", err)
	}
	targetURL := buildOpenAIChatCompletionsURL(validatedURL)

	upstreamCtx, releaseUpstreamCtx := detachUpstreamContext(ctx)
	upstreamReq, err := http.NewRequestWithContext(upstreamCtx, http.MethodPost, targetURL, bytes.NewReader(upstreamBody))
	releaseUpstreamCtx()
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	upstreamReq = upstreamReq.WithContext(WithHTTPUpstreamProfile(upstreamReq.Context(), HTTPUpstreamProfileOpenAI))
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Authorization", "Bearer "+apiKey)
	if clientStream {
		upstreamReq.Header.Set("Accept", "text/event-stream")
	} else {
		upstreamReq.Header.Set("Accept", "application/json")
	}

	// 透传白名单中的客户端 header。详见 openaiCCRawAllowedHeaders 的设计说明。
	for key, values := range c.Request.Header {
		lowerKey := strings.ToLower(key)
		if openaiCCRawAllowedHeaders[lowerKey] {
			for _, v := range values {
				upstreamReq.Header.Add(key, v)
			}
		}
	}
	customUA := account.GetOpenAIUserAgent()
	if customUA != "" {
		upstreamReq.Header.Set("user-agent", customUA)
	}

	// 6. Send request
	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, account.IsOpenAIPassthroughEnabled())
	resp, err := s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
	if err != nil {
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		detail := recordDetailedUpstreamTransportError(c, err)
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: 0,
			Kind:               "request_error",
			Message:            safeErr,
		})
		writeChatCompletionsError(c, http.StatusBadGateway, detail.ErrorType, formatUpstreamRequestFailed(detail, "Upstream request failed"))
		return nil, fmt.Errorf("upstream request failed: %s", safeErr)
	}
	defer func() { _ = resp.Body.Close() }()

	// 7. Handle error response with failover
	if resp.StatusCode >= 400 {
		respBody := s.readUpstreamErrorBody(resp)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(respBody))

		upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
		upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
		if s.shouldFailoverOpenAIUpstreamResponse(resp.StatusCode, upstreamMsg, respBody) {
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
			s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody, upstreamModel)
			return nil, &UpstreamFailoverError{
				StatusCode:             resp.StatusCode,
				ResponseBody:           respBody,
				RetryableOnSameAccount: account.IsPoolMode() && (account.IsPoolModeRetryableStatus(resp.StatusCode) || isOpenAITransientProcessingError(resp.StatusCode, upstreamMsg, respBody)),
			}
		}
		return s.handleChatCompletionsErrorResponse(resp, c, account, billingModel)
	}

	// 8. Forward response
	if clientStream {
		return s.streamRawChatCompletions(c, resp, account, originalModel, billingModel, upstreamModel, reasoningEffort, serviceTier, startTime, len(body))
	}
	return s.bufferRawChatCompletions(c, resp, account, originalModel, billingModel, upstreamModel, reasoningEffort, serviceTier, startTime)
}

const openAISilentRefusalMinRequestBodyBytes = 1024

func IsOpenAISilentRefusalErrorBody(body []byte) bool {
	return gjson.GetBytes(body, "error.code").String() == "silent_refusal"
}

func isOpenAIChatVisibleOutputChunk(payload string) bool {
	if strings.TrimSpace(payload) == "" {
		return false
	}
	choices := gjson.Get(payload, "choices")
	if !choices.Exists() || !choices.IsArray() {
		return false
	}
	for _, choice := range choices.Array() {
		if choice.Get("delta.tool_calls").Exists() && len(choice.Get("delta.tool_calls").Array()) > 0 {
			return true
		}
		if strings.TrimSpace(choice.Get("delta.reasoning_content").String()) != "" {
			return true
		}
		if strings.TrimSpace(choice.Get("delta.content").String()) != "" {
			return true
		}
	}
	return false
}

func buildOpenAISilentRefusalErrorBody() []byte {
	return []byte(`{"error":{"type":"upstream_error","code":"silent_refusal","message":"OpenAI upstream returned no visible output before stop"}}`)
}

// streamRawChatCompletions 透传上游 CC SSE 流到客户端，并提取 usage（包括
// 末尾 [DONE] 之前的 chunk 中的 usage 字段，按 OpenAI CC 协议）。
//
// usage 字段仅在客户端请求 stream_options.include_usage=true 时出现于上游响应中。
// 网关会对上游强制打开 include_usage 以保证计费完整，并原样向下游透传 usage，
// 让级联代理或下游计费系统也能拿到完整用量。
func (s *OpenAIGatewayService) streamRawChatCompletions(
	c *gin.Context,
	resp *http.Response,
	account *Account,
	originalModel string,
	billingModel string,
	upstreamModel string,
	reasoningEffort *string,
	serviceTier *string,
	startTime time.Time,
	requestBodyBytes int,
) (*OpenAIForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	shouldDetectSilentRefusal := requestBodyBytes >= openAISilentRefusalMinRequestBodyBytes

	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	var usage OpenAIUsage
	var firstTokenMs *int
	clientDisconnected := false
	responseStarted := false
	sawVisibleOutput := false
	pendingLines := make([]string, 0, 8)
	startResponse := func() {
		if responseStarted {
			return
		}
		if s.responseHeaderFilter != nil {
			responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
		}
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("X-Accel-Buffering", "no")
		c.Writer.WriteHeader(http.StatusOK)
		responseStarted = true
	}
	flushPending := func() {
		if len(pendingLines) == 0 || clientDisconnected {
			pendingLines = pendingLines[:0]
			return
		}
		startResponse()
		for _, line := range pendingLines {
			if _, werr := c.Writer.WriteString(line + "\n"); werr != nil {
				clientDisconnected = true
				logger.L().Debug("openai chat_completions raw: client disconnected during pending flush",
					zap.Error(werr),
					zap.String("request_id", requestID),
				)
				break
			}
			if line == "" {
				c.Writer.Flush()
			}
		}
		pendingLines = pendingLines[:0]
	}

	for scanner.Scan() {
		line := scanner.Text()
		if payload, ok := extractOpenAISSEDataLine(line); ok {
			trimmedPayload := strings.TrimSpace(payload)
			if trimmedPayload != "[DONE]" {
				_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, []byte(payload))
				usageOnlyChunk := isOpenAIChatUsageOnlyStreamChunk(payload)
				if u := extractCCStreamUsage(payload); u != nil {
					usage = *u
				}
				if isOpenAIChatVisibleOutputChunk(payload) {
					sawVisibleOutput = true
				}
				if firstTokenMs == nil && !usageOnlyChunk {
					elapsed := int(time.Since(startTime).Milliseconds())
					firstTokenMs = &elapsed
				}
			}
		}

		if shouldDetectSilentRefusal && !responseStarted {
			pendingLines = append(pendingLines, line)
			if sawVisibleOutput {
				flushPending()
			}
			continue
		}

		if !clientDisconnected {
			startResponse()
			if _, werr := c.Writer.WriteString(line + "\n"); werr != nil {
				clientDisconnected = true
				logger.L().Debug("openai chat_completions raw: client disconnected, continuing to drain upstream for billing",
					zap.Error(werr),
					zap.String("request_id", requestID),
				)
			}
		}
		if line == "" {
			if !clientDisconnected {
				c.Writer.Flush()
			}
			continue
		}
		if !clientDisconnected {
			c.Writer.Flush()
		}
	}

	if shouldDetectSilentRefusal && !responseStarted && !sawVisibleOutput {
		message := "OpenAI upstream returned no visible output before stop"
		responseBody := buildOpenAISilentRefusalErrorBody()
		setOpsUpstreamError(c, http.StatusBadGateway, message, "")
		event := OpsUpstreamErrorEvent{
			Platform:           PlatformOpenAI,
			UpstreamStatusCode: http.StatusBadGateway,
			UpstreamRequestID:  strings.TrimSpace(requestID),
			Kind:               "failover",
			Message:            message,
		}
		if account != nil {
			event.AccountID = account.ID
			event.AccountName = account.Name
		}
		appendOpsUpstreamError(c, event)
		if account != nil {
			_ = s.handleOpenAIAccountUpstreamError(c.Request.Context(), account, http.StatusBadGateway, resp.Header, responseBody)
		}
		return nil, &UpstreamFailoverError{
			StatusCode:      http.StatusBadGateway,
			ResponseBody:    responseBody,
			ResponseHeaders: resp.Header.Clone(),
		}
	}

	if err := scanner.Err(); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.L().Warn("openai chat_completions raw: stream read error",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
		}
	}

	return &OpenAIForwardResult{
		RequestID:       requestID,
		Usage:           usage,
		Model:           originalModel,
		BillingModel:    billingModel,
		UpstreamModel:   upstreamModel,
		ReasoningEffort: reasoningEffort,
		ServiceTier:     serviceTier,
		Stream:          true,
		Duration:        time.Since(startTime),
		FirstTokenMs:    firstTokenMs,
	}, nil
}

// ensureOpenAIChatStreamUsage 确保 raw Chat Completions 流式请求会让上游返回 usage。
// usage 也会继续向下游透传，支持级联代理和下游计费系统。
func ensureOpenAIChatStreamUsage(body []byte) ([]byte, error) {
	updated, err := sjson.SetBytes(body, "stream_options.include_usage", true)
	if err != nil {
		return body, err
	}
	return updated, nil
}

func isOpenAIChatUsageOnlyStreamChunk(payload string) bool {
	if strings.TrimSpace(payload) == "" {
		return false
	}
	if !gjson.Get(payload, "usage").Exists() {
		return false
	}
	choices := gjson.Get(payload, "choices")
	return choices.Exists() && choices.IsArray() && len(choices.Array()) == 0
}

// extractCCStreamUsage 从单个 CC 流式 chunk 的 payload 中提取 usage 字段。
// CC 协议中 usage 仅出现在末尾 chunk（且仅当 include_usage 生效时），
// 但上游可能在多个 chunk 中重复——总是用最新值。
func extractCCStreamUsage(payload string) *OpenAIUsage {
	usageResult := gjson.Get(payload, "usage")
	if !usageResult.Exists() || !usageResult.IsObject() {
		return nil
	}
	u := OpenAIUsage{
		InputTokens:  int(gjson.Get(payload, "usage.prompt_tokens").Int()),
		OutputTokens: int(gjson.Get(payload, "usage.completion_tokens").Int()),
	}
	if cached := gjson.Get(payload, "usage.prompt_tokens_details.cached_tokens"); cached.Exists() {
		u.CacheReadInputTokens = int(cached.Int())
	}
	return &u
}

// bufferRawChatCompletions 透传上游 CC 非流式 JSON 响应。
func (s *OpenAIGatewayService) bufferRawChatCompletions(
	c *gin.Context,
	resp *http.Response,
	account *Account,
	originalModel string,
	billingModel string,
	upstreamModel string,
	reasoningEffort *string,
	serviceTier *string,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		if !errors.Is(err, ErrUpstreamResponseBodyTooLarge) {
			writeChatCompletionsError(c, http.StatusBadGateway, "api_error", "Failed to read upstream response")
		}
		return nil, fmt.Errorf("read upstream body: %w", err)
	}
	_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, respBody)

	var ccResp apicompat.ChatCompletionsResponse
	var usage OpenAIUsage
	if err := json.Unmarshal(respBody, &ccResp); err == nil && ccResp.Usage != nil {
		usage = OpenAIUsage{
			InputTokens:  ccResp.Usage.PromptTokens,
			OutputTokens: ccResp.Usage.CompletionTokens,
		}
		if ccResp.Usage.PromptTokensDetails != nil {
			usage.CacheReadInputTokens = ccResp.Usage.PromptTokensDetails.CachedTokens
		}
	}

	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		c.Writer.Header().Set("Content-Type", ct)
	} else {
		c.Writer.Header().Set("Content-Type", "application/json")
	}
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write(respBody)

	return &OpenAIForwardResult{
		RequestID:       requestID,
		Usage:           usage,
		Model:           originalModel,
		BillingModel:    billingModel,
		UpstreamModel:   upstreamModel,
		ReasoningEffort: reasoningEffort,
		ServiceTier:     serviceTier,
		Stream:          false,
		Duration:        time.Since(startTime),
	}, nil
}

// buildOpenAIChatCompletionsURL 拼接上游 Chat Completions 端点 URL。
//
//   - base 已是 /chat/completions：原样返回
//   - base 以 /v1 结尾：追加 /chat/completions
//   - 其他情况：追加 /v1/chat/completions
//
// 与 buildOpenAIResponsesURL 是姐妹函数。
func buildOpenAIChatCompletionsURL(base string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.HasSuffix(normalized, "/chat/completions") {
		return normalized
	}
	if idx := strings.LastIndex(normalized, "/responses"); idx >= 0 {
		suffix := normalized[idx+len("/responses"):]
		if suffix == "" || strings.HasPrefix(suffix, "/") {
			return normalized[:idx] + "/chat/completions"
		}
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/chat/completions"
	}
	if hasOpenAIVersionedBasePath(normalized) {
		return normalized + "/chat/completions"
	}
	return normalized + "/v1/chat/completions"
}

func hasOpenAIVersionedBasePath(base string) bool {
	idx := strings.LastIndex(strings.TrimRight(strings.TrimSpace(base), "/"), "/")
	if idx < 0 || idx == len(base)-1 {
		return false
	}
	segment := base[idx+1:]
	if len(segment) < 2 || segment[0] != 'v' {
		return false
	}
	for _, ch := range segment[1:] {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func normalizeRawChatCompletionsCompatBody(body []byte) ([]byte, error) {
	updated := body

	if !gjson.GetBytes(updated, "messages").Exists() && gjson.GetBytes(updated, "input").Exists() {
		messagesRaw, err := convertResponsesInputRawToChatMessages(gjson.GetBytes(updated, "input").Raw)
		if err != nil {
			return nil, err
		}
		updated, err = sjson.SetRawBytes(updated, "messages", messagesRaw)
		if err != nil {
			return nil, fmt.Errorf("set messages from input: %w", err)
		}
		updated, err = sjson.DeleteBytes(updated, "input")
		if err != nil {
			return nil, fmt.Errorf("delete input after conversion: %w", err)
		}
	}

	if !gjson.GetBytes(updated, "max_completion_tokens").Exists() && !gjson.GetBytes(updated, "max_tokens").Exists() {
		if maxOutputTokens := gjson.GetBytes(updated, "max_output_tokens"); maxOutputTokens.Exists() {
			next, err := sjson.SetRawBytes(updated, "max_completion_tokens", []byte(maxOutputTokens.Raw))
			if err != nil {
				return nil, fmt.Errorf("map max_output_tokens to max_completion_tokens: %w", err)
			}
			updated = next
		}
	}
	if maxOutputTokens := gjson.GetBytes(updated, "max_output_tokens"); maxOutputTokens.Exists() {
		next, err := sjson.DeleteBytes(updated, "max_output_tokens")
		if err != nil {
			return nil, fmt.Errorf("delete max_output_tokens after conversion: %w", err)
		}
		updated = next
	}

	if !gjson.GetBytes(updated, "reasoning_effort").Exists() {
		if effort := strings.TrimSpace(gjson.GetBytes(updated, "reasoning.effort").String()); effort != "" {
			next, err := sjson.SetBytes(updated, "reasoning_effort", effort)
			if err != nil {
				return nil, fmt.Errorf("map reasoning.effort to reasoning_effort: %w", err)
			}
			updated = next
		}
	}

	for _, field := range openaiRawChatResponsesOnlyFields {
		next, err := sjson.DeleteBytes(updated, field)
		if err != nil {
			return nil, fmt.Errorf("delete responses-only field %s: %w", field, err)
		}
		updated = next
	}

	return updated, nil
}

func convertResponsesInputRawToChatMessages(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return json.Marshal([]apicompat.ChatMessage{})
	}

	var asText string
	if err := json.Unmarshal([]byte(raw), &asText); err == nil {
		return json.Marshal([]apicompat.ChatMessage{{
			Role:    "user",
			Content: mustMarshalRawJSON(asText),
		}})
	}

	var items []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, fmt.Errorf("parse responses input array: %w", err)
	}

	messages := make([]apicompat.ChatMessage, 0, len(items))
	for _, item := range items {
		converted, err := convertResponsesInputRawItemToChatMessages(item)
		if err != nil {
			return nil, err
		}
		messages = append(messages, converted...)
	}

	return json.Marshal(messages)
}

func convertResponsesInputRawItemToChatMessages(raw json.RawMessage) ([]apicompat.ChatMessage, error) {
	itemType := strings.TrimSpace(gjson.GetBytes(raw, "type").String())
	switch itemType {
	case "", "message":
		role := strings.TrimSpace(gjson.GetBytes(raw, "role").String())
		if role == "" {
			role = "user"
		}
		content, err := convertResponsesMessageContentToChatContent(json.RawMessage(gjson.GetBytes(raw, "content").Raw))
		if err != nil {
			return nil, fmt.Errorf("convert %s message content: %w", role, err)
		}
		return []apicompat.ChatMessage{{
			Role:    role,
			Content: content,
		}}, nil
	case "input_text":
		text := gjson.GetBytes(raw, "text").String()
		if text == "" {
			text = gjson.GetBytes(raw, "content").String()
		}
		return []apicompat.ChatMessage{{
			Role:    "user",
			Content: mustMarshalRawJSON(text),
		}}, nil
	case "input_image":
		imageURL := firstNonEmptyRawChat(
			strings.TrimSpace(gjson.GetBytes(raw, "image_url").String()),
			strings.TrimSpace(gjson.GetBytes(raw, "image_url.url").String()),
			strings.TrimSpace(gjson.GetBytes(raw, "file_url").String()),
		)
		if imageURL == "" {
			return nil, nil
		}
		content, err := json.Marshal([]apicompat.ChatContentPart{{
			Type: "image_url",
			ImageURL: &apicompat.ChatImageURL{
				URL: imageURL,
			},
		}})
		if err != nil {
			return nil, err
		}
		return []apicompat.ChatMessage{{
			Role:    "user",
			Content: content,
		}}, nil
	case "function_call":
		callID := strings.TrimSpace(gjson.GetBytes(raw, "call_id").String())
		if callID == "" {
			callID = strings.TrimSpace(gjson.GetBytes(raw, "id").String())
		}
		toolCall := apicompat.ChatToolCall{
			ID:   callID,
			Type: "function",
			Function: apicompat.ChatFunctionCall{
				Name:      strings.TrimSpace(gjson.GetBytes(raw, "name").String()),
				Arguments: strings.TrimSpace(gjson.GetBytes(raw, "arguments").String()),
			},
		}
		return []apicompat.ChatMessage{{
			Role:      "assistant",
			ToolCalls: []apicompat.ChatToolCall{toolCall},
		}}, nil
	case "function_call_output":
		output := strings.TrimSpace(gjson.GetBytes(raw, "output").String())
		if output == "" {
			output = "(empty)"
		}
		return []apicompat.ChatMessage{{
			Role:       "tool",
			ToolCallID: strings.TrimSpace(gjson.GetBytes(raw, "call_id").String()),
			Content:    mustMarshalRawJSON(output),
		}}, nil
	default:
		return nil, nil
	}
}

func convertResponsesMessageContentToChatContent(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return mustMarshalRawJSON(""), nil
	}

	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return mustMarshalRawJSON(text), nil
	}

	var parts []apicompat.ResponsesContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("parse content as string or parts array: %w", err)
	}

	chatParts := make([]apicompat.ChatContentPart, 0, len(parts))
	textParts := make([]string, 0, len(parts))
	hasNonText := false
	for _, part := range parts {
		switch part.Type {
		case "", "text", "input_text", "output_text":
			if part.Text != "" {
				textParts = append(textParts, part.Text)
				chatParts = append(chatParts, apicompat.ChatContentPart{
					Type: "text",
					Text: part.Text,
				})
			}
		case "input_image":
			imageURL := firstNonEmptyRawChat(strings.TrimSpace(part.ImageURL), strings.TrimSpace(part.FileURL))
			if imageURL == "" {
				continue
			}
			hasNonText = true
			chatParts = append(chatParts, apicompat.ChatContentPart{
				Type: "image_url",
				ImageURL: &apicompat.ChatImageURL{
					URL: imageURL,
				},
			})
		}
	}

	if len(chatParts) == 0 {
		return mustMarshalRawJSON(strings.Join(textParts, "")), nil
	}
	if !hasNonText {
		return mustMarshalRawJSON(strings.Join(textParts, "")), nil
	}

	out, err := json.Marshal(chatParts)
	if err != nil {
		return nil, fmt.Errorf("marshal chat content parts: %w", err)
	}
	return out, nil
}

func mustMarshalRawJSON(value string) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func firstNonEmptyRawChat(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
