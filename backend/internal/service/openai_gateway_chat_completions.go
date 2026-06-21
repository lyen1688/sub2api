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
	"net/http/httptest"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

// cursorResponsesUnsupportedFields are top-level Responses API parameters that
// Codex upstreams reject with "Unsupported parameter: ...". They must be
// stripped when forwarding a raw client body through the Responses-shape
// short-circuit in ForwardAsChatCompletions (see isResponsesShape branch).
// The normal Chat Completions → Responses conversion path is unaffected
// because ChatCompletionsRequest has no fields for these parameters — unknown
// fields are dropped naturally by json.Unmarshal. Kept semantically in sync
// with the list in openai_gateway_service.go:2034 used by the /v1/responses
// passthrough path.
var cursorResponsesUnsupportedFields = []string{
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

// ForwardAsChatCompletions accepts a Chat Completions request body, converts it
// to OpenAI Responses API format, forwards to the OpenAI upstream, and converts
// the response back to Chat Completions format. All account types (OAuth and API
// Key) go through the Responses API conversion path since the upstream only
// exposes the /v1/responses endpoint.
func (s *OpenAIGatewayService) ForwardAsChatCompletions(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	selectedFallbackModels ...string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()

	// 1. Parse Chat Completions request
	var chatReq apicompat.ChatCompletionsRequest
	if err := json.Unmarshal(body, &chatReq); err != nil {
		return nil, fmt.Errorf("parse chat completions request: %w", err)
	}
	originalModel := chatReq.Model
	clientStream := chatReq.Stream
	includeUsage := chatReq.StreamOptions != nil && chatReq.StreamOptions.IncludeUsage
	selectedFallbackModel := ""
	if len(selectedFallbackModels) > 0 {
		selectedFallbackModel = selectedFallbackModels[0]
	}

	// 2. Resolve model mapping early so compat prompt_cache_key injection can
	// derive a stable seed from the final upstream model family.
	billingModel := resolveOpenAIForwardModelWithSettingsAndSelectedFallback(ctx, s.settingService, account, originalModel, defaultMappedModel, selectedFallbackModel)
	upstreamModel := normalizeOpenAIModelForUpstream(account, billingModel)
	if isOpenAIImageGenerationModel(upstreamModel) {
		return s.forwardImageOnlyChatCompletions(ctx, c, account, body, originalModel, billingModel, upstreamModel, clientStream, includeUsage, startTime)
	}

	promptCacheKey = strings.TrimSpace(promptCacheKey)
	compatPromptCacheInjected := false
	if promptCacheKey == "" && account.Type == AccountTypeOAuth && shouldAutoInjectPromptCacheKeyForCompat(upstreamModel) {
		promptCacheKey = deriveCompatPromptCacheKey(&chatReq, upstreamModel)
		compatPromptCacheInjected = promptCacheKey != ""
	}

	// 3. Build the upstream (Responses API) body.
	//
	// Cursor compatibility: some clients (notably Cursor cloud) send Responses
	// API shaped bodies — `input: [...]` with no `messages` field — to the
	// /v1/chat/completions URL. Running those through ChatCompletionsToResponses
	// would silently drop Cursor's `input` array (the struct has no Input field)
	// and produce `input: null`, which Codex upstreams reject with
	// "Invalid type for 'input': expected a string, but got an object".
	//
	// Detect that shape and forward the raw body as-is, only rewriting `model`
	// to the resolved upstream model. The downstream codex OAuth transform will
	// still normalize store/stream/instructions/etc.
	isResponsesShape := !gjson.GetBytes(body, "messages").Exists() && gjson.GetBytes(body, "input").Exists()
	if account.Platform == PlatformOpenAI && account.Type == AccountTypeAPIKey && account.IsAnthropicMessagesUpstream() &&
		!ShouldForwardOpenAITextMessagesViaChatCompletions(account) && !isResponsesShape {
		return s.forwardCCToAnthropicMessages(ctx, c, account, body, startTime)
	}
	if account.Platform == PlatformOpenAI && account.Type == AccountTypeAPIKey && !openai_compat.ShouldUseResponsesAPI(account.Extra) {
		return s.forwardAsRawChatCompletions(ctx, c, account, body, promptCacheKey, defaultMappedModel, selectedFallbackModel)
	}

	var (
		responsesReq  *apicompat.ResponsesRequest
		responsesBody []byte
		err           error
	)
	if isResponsesShape {
		responsesBody, err = sjson.SetBytes(body, "model", upstreamModel)
		if err != nil {
			return nil, fmt.Errorf("rewrite model in responses-shape body: %w", err)
		}
		// Strip Responses API parameters that no Codex upstream accepts.
		// Because this branch forwards the raw body (the normal path rebuilds
		// it from ChatCompletionsRequest and drops unknown fields naturally),
		// we must filter these fields explicitly here — otherwise the upstream
		// rejects the request with "Unsupported parameter: ...".
		for _, field := range cursorResponsesUnsupportedFields {
			if stripped, derr := sjson.DeleteBytes(responsesBody, field); derr == nil {
				responsesBody = stripped
			}
		}
		if shouldStripTopPForResponsesUpstream(account) {
			if stripped, derr := sjson.DeleteBytes(responsesBody, "top_p"); derr == nil {
				responsesBody = stripped
			}
		}
		responsesBody, normalizedServiceTier, err := normalizeResponsesBodyServiceTier(responsesBody)
		if err != nil {
			return nil, fmt.Errorf("normalize service_tier in responses-shape body: %w", err)
		}
		// Minimal stub populated from the raw body so downstream billing
		// propagation (ServiceTier, ReasoningEffort) keeps working.
		responsesReq = &apicompat.ResponsesRequest{
			Model:       upstreamModel,
			ServiceTier: normalizedServiceTier,
		}
		if effort := gjson.GetBytes(responsesBody, "reasoning.effort").String(); effort != "" {
			responsesReq.Reasoning = &apicompat.ResponsesReasoning{Effort: effort}
		}
	} else {
		// Normal path: convert Chat Completions → Responses.
		// ChatCompletionsToResponses always sets Stream=true (upstream always streams).
		responsesReq, err = apicompat.ChatCompletionsToResponses(&chatReq)
		if err != nil {
			return nil, fmt.Errorf("convert chat completions to responses: %w", err)
		}
		responsesReq.Model = upstreamModel
		normalizeResponsesRequestServiceTier(responsesReq)
		responsesBody, err = json.Marshal(responsesReq)
		if err != nil {
			return nil, fmt.Errorf("marshal responses request: %w", err)
		}
	}
	responsesBody, err = s.applyOpenAIFastPolicyToBody(ctx, account, upstreamModel, responsesBody)
	if err != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(err, &blocked) {
			writeChatCompletionsError(c, http.StatusForbidden, "permission_error", blocked.Message)
		}
		return nil, err
	}
	if err := syncResponsesRequestBillingMetaFromBody(responsesReq, responsesBody); err != nil {
		return nil, fmt.Errorf("sync responses request billing meta after fast policy: %w", err)
	}

	logFields := []zap.Field{
		zap.Int64("account_id", account.ID),
		zap.String("original_model", originalModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("stream", clientStream),
		zap.Bool("responses_shape", isResponsesShape),
	}
	if compatPromptCacheInjected {
		recordOpenAICompatPromptCacheInjected()
		logFields = append(logFields,
			zap.Bool("compat_prompt_cache_key_injected", true),
			zap.String("compat_prompt_cache_key_sha256", hashSensitiveValueForLog(promptCacheKey)),
		)
	}
	logger.L().Debug("openai chat_completions: model mapping applied", logFields...)

	var oauthReqBody map[string]any
	switch account.Type {
	case AccountTypeOAuth:
		var reqBody map[string]any
		if err := json.Unmarshal(responsesBody, &reqBody); err != nil {
			return nil, fmt.Errorf("unmarshal for compat transform: %w", err)
		}
		applyEmbeddedDefaultInstructions(reqBody)
		codexResult := applyCodexOAuthTransformWithInputMode(reqBody, false, false, codexTransformInputModePreservePrefix)
		if codexResult.NormalizedModel != "" {
			upstreamModel = codexResult.NormalizedModel
		}
		if input, ok := reqBody["input"].([]any); ok {
			sanitizeOpenAIResponsesOrphanToolOutputs(reqBody, input, strings.TrimSpace(firstNonEmptyString(reqBody["previous_response_id"])) != "")
		}
		if codexResult.PromptCacheKey != "" {
			promptCacheKey = codexResult.PromptCacheKey
		}
		if promptCacheKey != "" {
			reqBody["prompt_cache_key"] = promptCacheKey
		}
		responsesBody, err = marshalOpenAIResponsesRequestBodyOrdered(reqBody)
		if err != nil {
			return nil, fmt.Errorf("remarshal after compat transform: %w", err)
		}
		oauthReqBody = reqBody
	case AccountTypeAPIKey:
		// For API key accounts (including OpenAI-compatible upstream gateways),
		// propagate promptCacheKey without rewriting the entire body unless needed.
		if trimmedKey := strings.TrimSpace(promptCacheKey); trimmedKey != "" {
			existingPromptCacheKey := gjson.GetBytes(responsesBody, "prompt_cache_key")
			if !existingPromptCacheKey.Exists() || existingPromptCacheKey.Type != gjson.String || strings.TrimSpace(existingPromptCacheKey.String()) == "" {
				updated, setErr := sjson.SetBytes(responsesBody, "prompt_cache_key", trimmedKey)
				if setErr != nil {
					return nil, fmt.Errorf("inject prompt_cache_key for compat body: %w", setErr)
				}
				responsesBody = updated
				recordOpenAICompatPromptCacheInjected()
			}
		}
	}

	if account.Type == AccountTypeAPIKey {
		if trimmedKey := strings.TrimSpace(promptCacheKey); trimmedKey != "" {
			var reqBody map[string]any
			if err := json.Unmarshal(responsesBody, &reqBody); err != nil {
				return nil, fmt.Errorf("unmarshal for prompt cache key injection: %w", err)
			}
			if existing, ok := reqBody["prompt_cache_key"].(string); !ok || strings.TrimSpace(existing) == "" {
				reqBody["prompt_cache_key"] = trimmedKey
				responsesBody, err = json.Marshal(reqBody)
				if err != nil {
					return nil, fmt.Errorf("remarshal after prompt cache key injection: %w", err)
				}
			}
		}
	}

	// 4b. Apply OpenAI fast policy (may filter service_tier or block the request).
	updatedBody, policyErr := s.applyOpenAIFastPolicyToBody(ctx, account, upstreamModel, responsesBody)
	if policyErr != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(policyErr, &blocked) {
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
			writeChatCompletionsError(c, http.StatusForbidden, "permission_error", blocked.Message)
		}
		return nil, policyErr
	}
	responsesBody = updatedBody
	if err := syncResponsesRequestBillingMetaFromBody(responsesReq, responsesBody); err != nil {
		return nil, fmt.Errorf("sync responses request billing meta after final fast policy: %w", err)
	}
	// 5. Get access token
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	// 6. Build upstream request.
	// Responses upstreams always stream; detach from client cancel so we can drain
	// terminal usage even when the downstream request has already gone away.
	upstreamCtx, releaseUpstreamCtx := detachUpstreamContext(ctx)
	upstreamReq, err := s.buildUpstreamRequest(upstreamCtx, c, account, responsesBody, token, true, promptCacheKey, isOpenAICodexOfficialClientRequest(c))
	releaseUpstreamCtx()
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}

	if account.Type == AccountTypeAPIKey && promptCacheKey != "" {
		apiKeyID := getAPIKeyIDFromContext(c)
		upstreamReq.Header.Set("session_id", generateSessionUUID(isolateOpenAISessionID(apiKeyID, promptCacheKey)))
	}

	// 7. Send request
	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	httpCodexCompatRetryTried := false
	httpRawChatFallbackRetryTried := false
	var resp *http.Response
	for {
		s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, account.IsOpenAIPassthroughEnabled())
		SetOpsLatencyMs(c, OpsOpenAIForwardPrepareLatencyMsKey, time.Since(startTime).Milliseconds())
		upstreamStart := time.Now()
		resp, err = s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
		SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
		if err != nil {
			return nil, s.handleOpenAIUpstreamTransportError(ctx, c, account, err, false)
		}
		defer func() { _ = resp.Body.Close() }()

		// 8. Handle error response with failover
		if resp.StatusCode >= 400 {
			recordOpenAICompatUpstreamStatus(upstreamModel, resp.StatusCode)
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(respBody))

			upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
			upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
			upstreamCode := extractUpstreamErrorCode(respBody)
			if !httpRawChatFallbackRetryTried &&
				account.Platform == PlatformOpenAI &&
				account.Type == AccountTypeAPIKey &&
				openai_compat.ResolveResponsesSupport(account.Extra) == openai_compat.ResponsesSupportUnknown &&
				!isResponsesEndpointSupportedByStatus(resp.StatusCode) {
				return s.forwardAsRawChatCompletions(ctx, c, account, body, promptCacheKey, defaultMappedModel, selectedFallbackModel)
			}
			if !httpCodexCompatRetryTried && account.Type == AccountTypeOAuth && oauthReqBody != nil {
				if fallbackReason := classifyOpenAICodexCompatFallback(resp.StatusCode, upstreamCode, upstreamMsg, respBody); fallbackReason != "" {
					updatedBody, codexResult, updatedPromptCacheKey, remarshalErr := remarshalOpenAIOAuthCompatFallbackBody(oauthReqBody, promptCacheKey, fallbackReason)
					if remarshalErr != nil {
						return nil, fmt.Errorf("remarshal chat compat fallback body: %w", remarshalErr)
					}
					httpCodexCompatRetryTried = true
					if codexResult.NormalizedModel != "" {
						upstreamModel = codexResult.NormalizedModel
					}
					if codexResult.Modified {
						responsesBody = updatedBody
						promptCacheKey = updatedPromptCacheKey
						upstreamCtx, releaseUpstreamCtx = detachUpstreamContext(ctx)
						upstreamReq, err = s.buildUpstreamRequest(upstreamCtx, c, account, responsesBody, token, true, promptCacheKey, isOpenAICodexOfficialClientRequest(c))
						releaseUpstreamCtx()
						if err != nil {
							return nil, fmt.Errorf("build upstream request after chat compat fallback: %w", err)
						}
						if account.Type == AccountTypeAPIKey && promptCacheKey != "" {
							apiKeyID := getAPIKeyIDFromContext(c)
							upstreamReq.Header.Set("session_id", generateSessionUUID(isolateOpenAISessionID(apiKeyID, promptCacheKey)))
						}
						logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Retrying compat chat request once with Codex compat fallback (account: %s, reason: %s)", account.Name, fallbackReason)
						continue
					}
				}
			}
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
				if s.rateLimitService != nil {
					s.rateLimitService.HandleUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody)
				}
				s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody, billingModel)
				return nil, &UpstreamFailoverError{
					StatusCode:             resp.StatusCode,
					ResponseBody:           respBody,
					RetryableOnSameAccount: account.IsPoolMode() && (account.IsPoolModeRetryableStatus(resp.StatusCode) || isOpenAITransientProcessingError(resp.StatusCode, upstreamMsg, respBody)),
				}
			}
			return s.handleChatCompletionsErrorResponse(resp, c, account, billingModel)
		}
		break
	}

	// 9. Handle normal response
	var result *OpenAIForwardResult
	var handleErr error
	if clientStream {
		result, handleErr = s.handleChatStreamingResponse(resp, c, account, originalModel, billingModel, upstreamModel, startTime)
	} else {
		result, handleErr = s.handleChatBufferedStreamingResponse(resp, c, account, originalModel, billingModel, upstreamModel, startTime)
	}

	// Propagate ServiceTier and ReasoningEffort to result for billing
	if handleErr == nil && result != nil {
		if responsesReq.ServiceTier != "" {
			st := responsesReq.ServiceTier
			result.ServiceTier = &st
		}
		if responsesReq.Reasoning != nil && responsesReq.Reasoning.Effort != "" {
			re := responsesReq.Reasoning.Effort
			result.ReasoningEffort = &re
		}
	}

	// Extract and save Codex usage snapshot from response headers (for OAuth accounts)
	if handleErr == nil && account.Type == AccountTypeOAuth {
		if snapshot := ParseCodexRateLimitHeaders(resp.Header); snapshot != nil {
			s.updateCodexUsageSnapshot(ctx, account.ID, snapshot)
		}
	}

	return result, handleErr
}

func (s *OpenAIGatewayService) forwardImageOnlyChatCompletions(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	originalModel string,
	billingModel string,
	upstreamModel string,
	clientStream bool,
	includeUsage bool,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	imagePath, imageBody, err := buildOpenAIImagesRequestFromChatCompletions(body, upstreamModel)
	if err != nil {
		message := sanitizeUpstreamErrorMessage(err.Error())
		writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", message)
		return nil, err
	}

	imageCtx, recorder, err := newOpenAIImageBridgeGinContext(c, imagePath, imageBody)
	if err != nil {
		writeChatCompletionsError(c, http.StatusInternalServerError, "api_error", "failed to initialize image bridge request")
		return nil, fmt.Errorf("build image bridge context: %w", err)
	}

	parsed, err := s.ParseOpenAIImagesRequest(imageCtx, imageBody)
	if err != nil {
		message := sanitizeUpstreamErrorMessage(err.Error())
		writeChatCompletionsError(c, http.StatusBadRequest, "invalid_request_error", message)
		return nil, err
	}

	imageResult, err := s.ForwardImages(ctx, imageCtx, account, imageBody, parsed, billingModel)
	if err != nil {
		statusCode := recorder.Code
		if statusCode < 400 {
			statusCode = http.StatusBadGateway
		}
		errType := "upstream_error"
		if statusCode >= 400 && statusCode < 500 {
			errType = "invalid_request_error"
		}
		message := strings.TrimSpace(extractUpstreamErrorMessage(recorder.Body.Bytes()))
		if message == "" {
			message = sanitizeUpstreamErrorMessage(err.Error())
		}
		writeChatCompletionsError(c, statusCode, errType, message)
		return nil, err
	}

	chatResp, err := buildChatCompletionsImageBridgeResponse(recorder.Body.Bytes(), originalModel, imageResult.RequestID)
	if err != nil {
		writeChatCompletionsError(c, http.StatusBadGateway, "api_error", err.Error())
		return nil, err
	}

	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), imageResult.ResponseHeaders, s.responseHeaderFilter)
	}
	if clientStream {
		firstTokenMs, streamErr := writeChatCompletionsImageBridgeStream(c, chatResp, includeUsage)
		if streamErr != nil {
			return nil, streamErr
		}
		imageResult.FirstTokenMs = firstTokenMs
	} else {
		c.JSON(http.StatusOK, chatResp)
	}

	return &OpenAIForwardResult{
		RequestID:              imageResult.RequestID,
		ResponseID:             imageResult.ResponseID,
		Usage:                  imageResult.Usage,
		Model:                  originalModel,
		BillingModel:           billingModel,
		TokenBillingModel:      imageResult.TokenBillingModel,
		ImageUsageTokenBilling: imageResult.ImageUsageTokenBilling,
		UpstreamModel:          imageResult.UpstreamModel,
		ServiceTier:            imageResult.ServiceTier,
		ReasoningEffort:        imageResult.ReasoningEffort,
		Stream:                 clientStream,
		EffectiveRequestType:   imageResult.EffectiveRequestType,
		ResponseHeaders:        imageResult.ResponseHeaders,
		Duration:               time.Since(startTime),
		FirstTokenMs:           imageResult.FirstTokenMs,
		ImageCount:             imageResult.ImageCount,
		ImageSize:              imageResult.ImageSize,
	}, nil
}

func newOpenAIImageBridgeGinContext(parent *gin.Context, path string, body []byte) (*gin.Context, *httptest.ResponseRecorder, error) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	ctx := context.Background()
	method := http.MethodPost
	headers := http.Header{}
	if parent != nil && parent.Request != nil {
		ctx = parent.Request.Context()
		method = parent.Request.Method
		headers = parent.Request.Header.Clone()
	}

	req, err := http.NewRequestWithContext(ctx, method, "http://openai-image-bridge.local"+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header = headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	ginCtx.Request = req
	return ginCtx, recorder, nil
}

func buildOpenAIImagesRequestFromChatCompletions(body []byte, model string) (string, []byte, error) {
	prompt, imageURLs := extractOpenAIImageChatPromptAndImages(gjson.GetBytes(body, "messages"))
	if prompt == "" {
		return "", nil, fmt.Errorf("image-only chat completions requires a text prompt in the last user message")
	}

	reqBody := map[string]any{
		"model":           strings.TrimSpace(model),
		"prompt":          prompt,
		"response_format": "b64_json",
		"stream":          false,
	}
	endpoint := openAIImagesGenerationsEndpoint
	if len(imageURLs) > 0 {
		endpoint = openAIImagesEditsEndpoint
		images := make([]map[string]string, 0, len(imageURLs))
		for _, imageURL := range imageURLs {
			images = append(images, map[string]string{"image_url": imageURL})
		}
		reqBody["images"] = images
	}

	for _, field := range []string{"size", "quality", "background", "output_format", "moderation", "input_fidelity", "style"} {
		value := gjson.GetBytes(body, field)
		if !value.Exists() {
			continue
		}
		if trimmed := strings.TrimSpace(value.String()); trimmed != "" {
			reqBody[field] = trimmed
		}
	}
	for _, field := range []string{"n", "output_compression", "partial_images"} {
		value := gjson.GetBytes(body, field)
		if value.Exists() && value.Type == gjson.Number {
			reqBody[field] = int(value.Int())
		}
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return "", nil, fmt.Errorf("marshal image bridge request: %w", err)
	}
	return endpoint, reqJSON, nil
}

func extractOpenAIImageChatPromptAndImages(messages gjson.Result) (string, []string) {
	if !messages.IsArray() {
		return "", nil
	}
	var (
		prompt string
		images []string
	)
	messages.ForEach(func(_, msg gjson.Result) bool {
		if strings.ToLower(strings.TrimSpace(msg.Get("role").String())) != "user" {
			return true
		}
		candidatePrompt, candidateImages := extractOpenAIImageChatContent(msg.Get("content"))
		if candidatePrompt != "" || len(candidateImages) > 0 {
			prompt = candidatePrompt
			images = candidateImages
		}
		return true
	})
	return prompt, dedupeStrings(images)
}

func extractOpenAIImageChatContent(content gjson.Result) (string, []string) {
	switch {
	case !content.Exists():
		return "", nil
	case content.Type == gjson.String:
		return strings.TrimSpace(content.String()), nil
	case content.IsArray():
		var promptParts []string
		var images []string
		content.ForEach(func(_, item gjson.Result) bool {
			itemPrompt, itemImages := extractOpenAIImageChatContent(item)
			if itemPrompt != "" {
				promptParts = append(promptParts, itemPrompt)
			}
			images = append(images, itemImages...)
			return true
		})
		return strings.TrimSpace(strings.Join(promptParts, "\n")), images
	case content.IsObject():
		typ := strings.ToLower(strings.TrimSpace(content.Get("type").String()))
		switch typ {
		case "", "text", "input_text":
			if text := strings.TrimSpace(content.Get("text").String()); text != "" {
				return text, nil
			}
			return extractOpenAIImageChatContent(content.Get("content"))
		case "image_url":
			imageURL := strings.TrimSpace(content.Get("image_url.url").String())
			if imageURL == "" {
				imageURL = strings.TrimSpace(content.Get("image_url").String())
			}
			if imageURL != "" {
				return "", []string{imageURL}
			}
		}
	}
	return "", nil
}

func buildChatCompletionsImageBridgeResponse(raw []byte, model string, requestID string) (*apicompat.ChatCompletionsResponse, error) {
	if len(raw) == 0 || !gjson.ValidBytes(raw) {
		return nil, fmt.Errorf("image bridge returned invalid json response")
	}

	items := gjson.GetBytes(raw, "data")
	if !items.Exists() || !items.IsArray() {
		return nil, fmt.Errorf("image bridge returned no images")
	}

	topLevelFormat := strings.TrimSpace(gjson.GetBytes(raw, "output_format").String())
	parts := make([]apicompat.ChatContentPart, 0, len(items.Array()))
	for _, item := range items.Array() {
		imageURL := strings.TrimSpace(item.Get("url").String())
		if imageURL == "" {
			b64 := strings.TrimSpace(item.Get("b64_json").String())
			if b64 == "" {
				continue
			}
			outputFormat := strings.TrimSpace(item.Get("output_format").String())
			if outputFormat == "" {
				outputFormat = topLevelFormat
			}
			imageURL = "data:" + openAIImageOutputMIMEType(outputFormat) + ";base64," + b64
		}
		parts = append(parts, apicompat.ChatContentPart{
			Type:     "image_url",
			ImageURL: &apicompat.ChatImageURL{URL: imageURL},
		})
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("image bridge returned no image payloads")
	}

	content, err := json.Marshal(parts)
	if err != nil {
		return nil, fmt.Errorf("marshal chat image content: %w", err)
	}

	createdAt := gjson.GetBytes(raw, "created").Int()
	if createdAt <= 0 {
		createdAt = time.Now().Unix()
	}
	chatID := strings.TrimSpace(requestID)
	if chatID == "" {
		chatID = fmt.Sprintf("chatcmpl_img_%d", createdAt)
	}

	var usage *apicompat.ChatUsage
	if parsedUsage, ok := extractOpenAIUsageFromJSONBytes(raw); ok {
		if parsedUsage.InputTokens > 0 || parsedUsage.OutputTokens > 0 || parsedUsage.CacheReadInputTokens > 0 {
			usage = &apicompat.ChatUsage{
				PromptTokens:     parsedUsage.InputTokens,
				CompletionTokens: parsedUsage.OutputTokens,
				TotalTokens:      parsedUsage.InputTokens + parsedUsage.OutputTokens,
			}
			if parsedUsage.CacheReadInputTokens > 0 {
				usage.PromptTokensDetails = &apicompat.ChatTokenDetails{
					CachedTokens: parsedUsage.CacheReadInputTokens,
				}
			}
		}
	}

	return &apicompat.ChatCompletionsResponse{
		ID:      chatID,
		Object:  "chat.completion",
		Created: createdAt,
		Model:   strings.TrimSpace(model),
		Choices: []apicompat.ChatChoice{{
			Index: 0,
			Message: apicompat.ChatMessage{
				Role:    "assistant",
				Content: content,
			},
			FinishReason: "stop",
		}},
		Usage: usage,
	}, nil
}

func writeChatCompletionsImageBridgeStream(
	c *gin.Context,
	chatResp *apicompat.ChatCompletionsResponse,
	includeUsage bool,
) (*int, error) {
	if c == nil || c.Writer == nil {
		return nil, fmt.Errorf("missing response writer")
	}
	if chatResp == nil || len(chatResp.Choices) == 0 {
		return nil, fmt.Errorf("image bridge returned no chat completion choices")
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming is not supported by response writer")
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	contentParts := extractChatCompletionsImageBridgeStreamContents(chatResp)
	chunks := []apicompat.ChatCompletionsChunk{
		{
			ID:      chatResp.ID,
			Object:  "chat.completion.chunk",
			Created: chatResp.Created,
			Model:   chatResp.Model,
			Choices: []apicompat.ChatChunkChoice{{
				Index:        0,
				Delta:        apicompat.ChatDelta{Role: "assistant"},
				FinishReason: nil,
			}},
		},
	}
	for idx, contentPart := range contentParts {
		if strings.TrimSpace(contentPart) == "" {
			continue
		}
		contentCopy := contentPart
		if idx > 0 {
			contentCopy = "\n" + contentCopy
		}
		chunks = append(chunks, apicompat.ChatCompletionsChunk{
			ID:      chatResp.ID,
			Object:  "chat.completion.chunk",
			Created: chatResp.Created,
			Model:   chatResp.Model,
			Choices: []apicompat.ChatChunkChoice{{
				Index:        0,
				Delta:        apicompat.ChatDelta{Content: &contentCopy},
				FinishReason: nil,
			}},
		})
	}
	empty := ""
	finish := "stop"
	chunks = append(chunks, apicompat.ChatCompletionsChunk{
		ID:      chatResp.ID,
		Object:  "chat.completion.chunk",
		Created: chatResp.Created,
		Model:   chatResp.Model,
		Choices: []apicompat.ChatChunkChoice{{
			Index:        0,
			Delta:        apicompat.ChatDelta{Content: &empty},
			FinishReason: &finish,
		}},
	})
	if includeUsage && chatResp.Usage != nil {
		chunks = append(chunks, apicompat.ChatCompletionsChunk{
			ID:      chatResp.ID,
			Object:  "chat.completion.chunk",
			Created: chatResp.Created,
			Model:   chatResp.Model,
			Choices: []apicompat.ChatChunkChoice{},
			Usage:   chatResp.Usage,
		})
	}

	var firstTokenMs *int
	streamStart := time.Now()
	for idx, chunk := range chunks {
		sse, err := apicompat.ChatChunkToSSE(chunk)
		if err != nil {
			return firstTokenMs, err
		}
		if _, err := fmt.Fprint(c.Writer, sse); err != nil {
			return firstTokenMs, err
		}
		if idx == 0 {
			ms := int(time.Since(streamStart).Milliseconds())
			firstTokenMs = &ms
		}
	}
	fmt.Fprint(c.Writer, "data: [DONE]\n\n") //nolint:errcheck
	flusher.Flush()
	return firstTokenMs, nil
}

func extractChatCompletionsImageBridgeStreamContents(chatResp *apicompat.ChatCompletionsResponse) []string {
	if chatResp == nil || len(chatResp.Choices) == 0 {
		return nil
	}
	raw := chatResp.Choices[0].Message.Content
	if len(raw) == 0 || !gjson.ValidBytes(raw) {
		return nil
	}
	content := gjson.ParseBytes(raw)
	if content.Type == gjson.String {
		if trimmed := strings.TrimSpace(content.String()); trimmed != "" {
			return []string{trimmed}
		}
		return nil
	}
	if !content.IsArray() {
		return nil
	}
	var parts []string
	for _, item := range content.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "image_url" {
			continue
		}
		imageURL := strings.TrimSpace(item.Get("image_url.url").String())
		if imageURL == "" {
			imageURL = strings.TrimSpace(item.Get("image_url").String())
		}
		if imageURL != "" {
			parts = append(parts, imageURL)
		}
	}
	return parts
}

func normalizeResponsesRequestServiceTier(req *apicompat.ResponsesRequest) {
	if req == nil {
		return
	}
	req.ServiceTier = normalizedOpenAIServiceTierValue(req.ServiceTier)
}

func normalizeResponsesBodyServiceTier(body []byte) ([]byte, string, error) {
	if len(body) == 0 {
		return body, "", nil
	}
	rawServiceTier := gjson.GetBytes(body, "service_tier").String()
	if rawServiceTier == "" {
		return body, "", nil
	}
	normalizedServiceTier := normalizedOpenAIServiceTierValue(rawServiceTier)
	if normalizedServiceTier == "" {
		trimmed, err := sjson.DeleteBytes(body, "service_tier")
		return trimmed, "", err
	}
	if normalizedServiceTier == rawServiceTier {
		return body, normalizedServiceTier, nil
	}
	trimmed, err := sjson.SetBytes(body, "service_tier", normalizedServiceTier)
	return trimmed, normalizedServiceTier, err
}

func normalizedOpenAIServiceTierValue(raw string) string {
	normalized := normalizeOpenAIServiceTier(raw)
	if normalized == nil {
		return ""
	}
	return *normalized
}

func syncResponsesRequestBillingMetaFromBody(req *apicompat.ResponsesRequest, body []byte) error {
	if req == nil || len(body) == 0 {
		return nil
	}
	var meta struct {
		ServiceTier string                        `json:"service_tier"`
		Reasoning   *apicompat.ResponsesReasoning `json:"reasoning"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return err
	}
	req.ServiceTier = meta.ServiceTier
	req.Reasoning = meta.Reasoning
	return nil
}

// handleChatCompletionsErrorResponse reads an upstream error and returns it in
// OpenAI Chat Completions error format.
func (s *OpenAIGatewayService) handleChatCompletionsErrorResponse(
	resp *http.Response,
	c *gin.Context,
	account *Account,
	requestedModel ...string,
) (*OpenAIForwardResult, error) {
	return s.handleCompatErrorResponse(resp, c, account, writeChatCompletionsError, requestedModel...)
}

// handleChatBufferedStreamingResponse reads all Responses SSE events from the
// upstream, finds the terminal event, converts to a Chat Completions JSON
// response, and writes it to the client.
func (s *OpenAIGatewayService) handleChatBufferedStreamingResponse(
	resp *http.Response,
	c *gin.Context,
	account *Account,
	originalModel string,
	billingModel string,
	upstreamModel string,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	var finalResponse *apicompat.ResponsesResponse
	finalEventType := ""
	var finalFailurePayload []byte
	var usage OpenAIUsage
	acc := apicompat.NewBufferedResponseAccumulator()
	processFrame := func(frame openAICompatSSEFrame) bool {
		payload := openAICompatPayloadWithEventType(frame.Data, frame.EventType)

		var event apicompat.ResponsesStreamEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			logger.L().Warn("openai chat_completions buffered: failed to parse event",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
			return false
		}

		// Accumulate delta content for fallback when terminal output is empty.
		acc.ProcessEvent(&event)

		if (event.Type == "response.completed" || event.Type == "response.done" ||
			event.Type == "response.incomplete" || event.Type == "response.failed") &&
			event.Response != nil {
			if event.Type == "response.failed" {
				_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, []byte(payload))
			}
			finalResponse = event.Response
			finalEventType = event.Type
			if event.Type == "response.failed" {
				finalFailurePayload = []byte(payload)
			}
			if event.Usage != nil {
				usage = OpenAIUsage{
					InputTokens:  event.Usage.InputTokens,
					OutputTokens: event.Usage.OutputTokens,
				}
				if event.Usage.InputTokensDetails != nil {
					usage.CacheReadInputTokens = event.Usage.InputTokensDetails.CachedTokens
				}
				if finalResponse.Usage == nil {
					finalResponse.Usage = event.Usage
				}
			}
			if event.Response.Usage != nil {
				usage = OpenAIUsage{
					InputTokens:  event.Response.Usage.InputTokens,
					OutputTokens: event.Response.Usage.OutputTokens,
				}
				if event.Response.Usage.InputTokensDetails != nil {
					usage.CacheReadInputTokens = event.Response.Usage.InputTokensDetails.CachedTokens
				}
			}
			return true
		}
		return false
	}

	var parser openAICompatSSEFrameParser
	for scanner.Scan() {
		line := scanner.Text()
		frame, ok := parser.AddLine(line)
		if !ok {
			continue
		}
		if processFrame(frame) {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.L().Warn("openai chat_completions buffered: read error",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
		}
	}
	if finalResponse == nil {
		if frame, ok := parser.Finish(); ok {
			processFrame(frame)
		}
	}

	if finalResponse == nil {
		writeChatCompletionsError(c, http.StatusBadGateway, "api_error", "Upstream stream ended without a terminal response event")
		return nil, fmt.Errorf("upstream stream ended without terminal event")
	}
	if finalEventType == "response.failed" || strings.EqualFold(strings.TrimSpace(finalResponse.Status), "failed") {
		errMessage := extractResponsesFailureMessage(finalResponse, nil)
		if errMessage == "" {
			errMessage = "Upstream response failed"
		}
		if !openAIStreamFailedEventShouldFailover(finalFailurePayload, errMessage) {
			writeChatCompletionsError(c, http.StatusBadGateway, "upstream_error", errMessage)
			return nil, fmt.Errorf("upstream response failed: %s", errMessage)
		}
		return nil, newChatCompletionsResponseFailedFailover(resp, finalFailurePayload, errMessage)
	}

	// When the terminal event has an empty output array, reconstruct from
	// accumulated delta events so the client receives the full content.
	acc.SupplementResponseOutput(finalResponse)

	chatResp := apicompat.ResponsesToChatCompletions(finalResponse, originalModel)

	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	// 非流式响应必须为标准 JSON。上游被强制流式，其响应头 Content-Type 为
	// text/event-stream，会经 WriteFilteredHeaders 透传进来；而 c.JSON 走 Gin 的
	// writeContentType 仅在头不存在时才设置，无法覆盖。这里显式 Set 强制改回 JSON，
	// 否则下游"看头判流式"的中间层（如 new-api）会把本应聚合的 JSON 当成 SSE 处理。
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.JSON(http.StatusOK, chatResp)

	return &OpenAIForwardResult{
		RequestID:     requestID,
		Usage:         usage,
		Model:         originalModel,
		BillingModel:  billingModel,
		UpstreamModel: upstreamModel,
		Stream:        false,
		Duration:      time.Since(startTime),
	}, nil
}

// handleChatStreamingResponse reads Responses SSE events from upstream,
// converts each to Chat Completions SSE chunks, and writes them to the client.
func (s *OpenAIGatewayService) handleChatStreamingResponse(
	resp *http.Response,
	c *gin.Context,
	account *Account,
	originalModel string,
	billingModel string,
	upstreamModel string,
	startTime time.Time,
	_ ...int,
) (*OpenAIForwardResult, error) {
	requestID := resp.Header.Get("x-request-id")

	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")

	state := apicompat.NewResponsesEventToChatState()
	state.Model = originalModel
	// 网关作为计费链路的一环，不能把下游 usage 输出绑定到客户端是否显式请求。
	// raw Chat Completions 直转路径已经强制透出 usage，这里保持同样行为，避免级联代理计费为 0。
	state.IncludeUsage = true

	var usage OpenAIUsage
	var firstTokenMs *int
	firstChunk := true
	sawTerminalEvent := false
	clientDisconnected := false
	downstreamFlushed := false
	pendingSSE := make([]string, 0, 1)
	streamFailed := false
	streamFailedErr := error(nil)

	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	resultWithUsage := func() *OpenAIForwardResult {
		return &OpenAIForwardResult{
			RequestID:     requestID,
			Usage:         usage,
			Model:         originalModel,
			BillingModel:  billingModel,
			UpstreamModel: upstreamModel,
			Stream:        true,
			Duration:      time.Since(startTime),
			FirstTokenMs:  firstTokenMs,
		}
	}
	resultForStreamFailure := func() (*OpenAIForwardResult, error) {
		var failoverErr *UpstreamFailoverError
		if errors.As(streamFailedErr, &failoverErr) {
			return nil, streamFailedErr
		}
		return resultWithUsage(), streamFailedErr
	}
	writeSSE := func(sse string) bool {
		if clientDisconnected {
			return false
		}
		if _, err := fmt.Fprint(c.Writer, sse); err != nil {
			logger.L().Info("openai chat_completions stream: client disconnected",
				zap.String("request_id", requestID),
			)
			clientDisconnected = true
			return false
		}
		return true
	}
	flushPendingSSE := func() bool {
		for _, sse := range pendingSSE {
			if !writeSSE(sse) {
				return false
			}
		}
		pendingSSE = pendingSSE[:0]
		return true
	}

	processDataLine := func(payload string) bool {
		var event apicompat.ResponsesStreamEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			logger.L().Warn("openai chat_completions stream: failed to parse event",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
			return false
		}
		if event.Type == "response.failed" {
			_ = s.markOpenAICyberPolicyIfDetected(c.Request.Context(), account, []byte(payload))
			streamFailed = true
			errMessage := extractResponsesFailureMessage(event.Response, []byte(payload))
			if errMessage == "" {
				errMessage = "Upstream response failed"
			}
			if !downstreamFlushed && !c.Writer.Written() {
				if openAIStreamFailedEventShouldFailover([]byte(payload), errMessage) {
					streamFailedErr = newChatCompletionsResponseFailedFailover(resp, []byte(payload), errMessage)
				} else {
					writeChatCompletionsError(c, http.StatusBadGateway, "upstream_error", errMessage)
					downstreamFlushed = true
					streamFailedErr = fmt.Errorf("upstream response failed: %s", errMessage)
				}
				return true
			}
			if writeErr := writeChatCompletionsStreamError(c.Writer, errMessage); writeErr != nil {
				logger.L().Info("openai chat_completions stream: client disconnected while writing failure signal",
					zap.String("request_id", requestID),
				)
				streamFailedErr = fmt.Errorf("upstream response failed: %s", errMessage)
				return true
			}
			c.Writer.Flush()
			downstreamFlushed = true
			streamFailedErr = fmt.Errorf("upstream response failed: %s", errMessage)
			return true
		}
		eventStartsOutput := openAIStreamDataStartsClientOutput(payload, event.Type)
		if firstChunk && eventStartsOutput {
			firstChunk = false
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}

		isTerminalEvent := isOpenAICompatResponsesTerminalEvent(event.Type)
		if isTerminalEvent {
			if event.Usage != nil {
				usage = copyOpenAIUsageFromResponsesUsage(event.Usage)
			}
			if event.Response != nil && event.Response.Usage != nil {
				usage = copyOpenAIUsageFromResponsesUsage(event.Response.Usage)
			}
			sawTerminalEvent = true
		}
		chunks := apicompat.ResponsesEventToChatChunks(&event, state)
		wroteChunk := false
		for _, chunk := range chunks {
			sse, err := apicompat.ChatChunkToSSE(chunk)
			if err != nil {
				logger.L().Warn("openai chat_completions stream: failed to marshal chunk",
					zap.Error(err),
					zap.String("request_id", requestID),
				)
				continue
			}
			if !eventStartsOutput && !downstreamFlushed {
				pendingSSE = append(pendingSSE, sse)
				continue
			}
			if eventStartsOutput && !downstreamFlushed && len(pendingSSE) > 0 {
				if !flushPendingSSE() {
					break
				}
				wroteChunk = true
			}
			if !writeSSE(sse) {
				break
			}
			wroteChunk = true
		}
		if wroteChunk && !clientDisconnected {
			c.Writer.Flush()
			downstreamFlushed = true
		}
		return isTerminalEvent
	}

	finalizeStream := func() (*OpenAIForwardResult, error) {
		if streamFailed {
			return resultForStreamFailure()
		}
		if !sawTerminalEvent {
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: missing terminal event")
		}
		if clientDisconnected {
			return resultWithUsage(), nil
		}
		if finalChunks := apicompat.FinalizeResponsesChatStream(state); len(finalChunks) > 0 {
			wroteFinalChunk := false
			if len(pendingSSE) > 0 {
				if !flushPendingSSE() {
					return resultWithUsage(), nil
				}
				wroteFinalChunk = true
			}
			for _, chunk := range finalChunks {
				sse, err := apicompat.ChatChunkToSSE(chunk)
				if err != nil {
					continue
				}
				if !writeSSE(sse) {
					return resultWithUsage(), nil
				}
				wroteFinalChunk = true
			}
			if wroteFinalChunk && !clientDisconnected {
				c.Writer.Flush()
				downstreamFlushed = true
			}
		}
		// Send [DONE] sentinel
		fmt.Fprint(c.Writer, "data: [DONE]\n\n") //nolint:errcheck
		c.Writer.Flush()
		downstreamFlushed = true
		return resultWithUsage(), nil
	}

	handleScanErr := func(err error) {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.L().Warn("openai chat_completions stream: read error",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
		}
	}
	processFrame := func(frame openAICompatSSEFrame) bool {
		payload := openAICompatPayloadWithEventType(frame.Data, frame.EventType)
		if strings.TrimSpace(payload) == "[DONE]" {
			return false
		}
		return processDataLine(payload)
	}

	// Determine keepalive interval
	keepaliveInterval := time.Duration(0)
	if s.cfg != nil && s.cfg.Gateway.StreamKeepaliveInterval > 0 {
		keepaliveInterval = time.Duration(s.cfg.Gateway.StreamKeepaliveInterval) * time.Second
	}

	// No keepalive: fast synchronous path
	if keepaliveInterval <= 0 {
		var parser openAICompatSSEFrameParser
		for scanner.Scan() {
			line := scanner.Text()
			frame, ok := parser.AddLine(line)
			if !ok {
				continue
			}
			if strings.TrimSpace(frame.Data) == "[DONE]" {
				continue
			}
			if processFrame(frame) {
				if streamFailed {
					return resultForStreamFailure()
				}
				return finalizeStream()
			}
		}
		if frame, ok := parser.Finish(); ok {
			if strings.TrimSpace(frame.Data) != "[DONE]" && processFrame(frame) {
				if streamFailed {
					return resultForStreamFailure()
				}
				return finalizeStream()
			}
		}
		handleScanErr(scanner.Err())
		return finalizeStream()
	}

	// With keepalive: goroutine + channel + select
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

	keepaliveTicker := time.NewTicker(keepaliveInterval)
	defer keepaliveTicker.Stop()
	lastDataAt := time.Now()
	var parser openAICompatSSEFrameParser

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				if frame, ok := parser.Finish(); ok {
					if strings.TrimSpace(frame.Data) != "[DONE]" && processFrame(frame) {
						if streamFailed {
							return resultForStreamFailure()
						}
						return finalizeStream()
					}
				}
				return finalizeStream()
			}
			if ev.err != nil {
				handleScanErr(ev.err)
				return finalizeStream()
			}
			lastDataAt = time.Now()
			line := ev.line
			frame, ok := parser.AddLine(line)
			if !ok {
				continue
			}
			if strings.TrimSpace(frame.Data) == "[DONE]" {
				continue
			}
			if processFrame(frame) {
				if streamFailed {
					return resultForStreamFailure()
				}
				return finalizeStream()
			}

		case <-keepaliveTicker.C:
			if time.Since(lastDataAt) < keepaliveInterval {
				continue
			}
			if !downstreamFlushed && !c.Writer.Written() {
				continue
			}
			// Send SSE comment as keepalive
			if _, err := fmt.Fprint(c.Writer, ":\n\n"); err != nil {
				logger.L().Info("openai chat_completions stream: client disconnected during keepalive",
					zap.String("request_id", requestID),
				)
				return resultWithUsage(), nil
			}
			c.Writer.Flush()
			downstreamFlushed = true
		}
	}
}

func newChatCompletionsResponseFailedFailover(resp *http.Response, payload []byte, message string) *UpstreamFailoverError {
	body := bytes.TrimSpace(payload)
	if len(body) == 0 {
		if fallbackBody, err := json.Marshal(gin.H{
			"error": gin.H{
				"type":    "upstream_error",
				"message": sanitizeUpstreamErrorMessage(message),
			},
		}); err == nil {
			body = fallbackBody
		} else {
			body = []byte(sanitizeUpstreamErrorMessage(message))
		}
	}

	var headers http.Header
	if resp != nil && resp.Header != nil {
		headers = resp.Header.Clone()
	}
	return &UpstreamFailoverError{
		StatusCode:      http.StatusBadGateway,
		ResponseBody:    body,
		ResponseHeaders: headers,
	}
}

func extractResponsesFailureMessage(resp *apicompat.ResponsesResponse, payload []byte) string {
	if resp != nil && resp.Error != nil {
		if msg := sanitizeUpstreamErrorMessage(strings.TrimSpace(resp.Error.Message)); msg != "" {
			return msg
		}
	}
	for _, path := range []string{"response.error.message", "error.message", "message"} {
		if msg := sanitizeUpstreamErrorMessage(strings.TrimSpace(gjson.GetBytes(payload, path).String())); msg != "" {
			return msg
		}
	}
	return ""
}

func writeChatCompletionsStreamError(w io.Writer, message string) error {
	message = sanitizeUpstreamErrorMessage(strings.TrimSpace(message))
	if message == "" {
		message = "Upstream response failed"
	}
	payload, err := json.Marshal(gin.H{
		"error": gin.H{
			"type":    "upstream_error",
			"message": message,
		},
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

// writeChatCompletionsError writes an error response in OpenAI Chat Completions format.
func writeChatCompletionsError(c *gin.Context, statusCode int, errType, message string) {
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	MarkResponseCommitted(c)
	c.JSON(statusCode, gin.H{
		"error": gin.H{
			"type":    errType,
			"message": message,
		},
	})
}
