package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// forwardCCToAnthropicMessages converts a CC request to Anthropic Messages format,
// forwards to an Anthropic-compatible upstream (e.g. opencode.ai MiniMax/Qwen),
// and converts the response back to CC format.
func (s *OpenAIGatewayService) forwardCCToAnthropicMessages(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	// 1. Parse CC request
	var ccReq apicompat.ChatCompletionsRequest
	if err := json.Unmarshal(body, &ccReq); err != nil {
		return nil, fmt.Errorf("parse chat completions request: %w", err)
	}
	originalModel := ccReq.Model
	clientStream := ccReq.Stream
	includeUsage := ccReq.StreamOptions != nil && ccReq.StreamOptions.IncludeUsage

	// 2. Convert CC → Responses → Anthropic Messages
	responsesReq, err := apicompat.ChatCompletionsToResponses(&ccReq)
	if err != nil {
		return nil, fmt.Errorf("convert cc to responses: %w", err)
	}
	anthropicReq, err := apicompat.ResponsesToAnthropicRequest(responsesReq)
	if err != nil {
		return nil, fmt.Errorf("convert responses to anthropic: %w", err)
	}
	anthropicReq.Stream = true

	// 3. Model mapping
	mappedModel := resolveOpenAIForwardModelWithSettings(ctx, s.settingService, account, originalModel, "")
	anthropicReq.Model = mappedModel

	anthropicBody, err := json.Marshal(anthropicReq)
	if err != nil {
		return nil, fmt.Errorf("marshal anthropic request: %w", err)
	}

	logger.L().Debug("openai forward_cc_to_messages: converting and forwarding",
		zap.Int64("account_id", account.ID),
		zap.String("original_model", originalModel),
		zap.String("mapped_model", mappedModel),
		zap.Bool("client_stream", clientStream),
	)

	// 4. Build upstream request
	apiKey := account.GetOpenAIApiKey()
	if apiKey == "" {
		return nil, fmt.Errorf("account %d missing api_key", account.ID)
	}
	baseURL := account.GetOpenAIBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("account %d missing base_url for messages upstream", account.ID)
	}
	validatedURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base_url: %w", err)
	}
	targetURL := buildMessagesTargetURL(validatedURL)

	upstreamCtx, releaseCtx := detachUpstreamContext(ctx)
	upstreamReq, err := http.NewRequestWithContext(upstreamCtx, http.MethodPost, targetURL, bytes.NewReader(anthropicBody))
	releaseCtx()
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("x-api-key", apiKey)
	upstreamReq.Header.Set("anthropic-version", "2023-06-01")
	upstreamReq.Header.Set("Accept", "text/event-stream")

	// 5. Send request
	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, account.IsOpenAIPassthroughEnabled())
	resp, err := s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
	if err != nil {
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:    account.Platform,
			AccountID:   account.ID,
			AccountName: account.Name,
			Kind:        "request_error",
			Message:     safeErr,
		})
		writeChatCompletionsError(c, http.StatusBadGateway, "server_error", "Upstream request failed: "+safeErr)
		return nil, fmt.Errorf("upstream request failed: %s", safeErr)
	}
	defer func() { _ = resp.Body.Close() }()

	// 6. Handle error responses
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		upstreamMsg := sanitizeUpstreamErrorMessage(extractUpstreamErrorMessage(respBody))
		if s.shouldFailoverOpenAIUpstreamResponse(resp.StatusCode, upstreamMsg, respBody) {
			appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
				Platform:           account.Platform,
				AccountID:          account.ID,
				AccountName:        account.Name,
				UpstreamStatusCode: resp.StatusCode,
				Kind:               "failover",
				Message:            upstreamMsg,
			})
			return nil, &UpstreamFailoverError{
				StatusCode:   resp.StatusCode,
				ResponseBody: respBody,
			}
		}
		writeChatCompletionsError(c, resp.StatusCode, "upstream_error", upstreamMsg)
		return nil, fmt.Errorf("upstream error %d: %s", resp.StatusCode, upstreamMsg)
	}

	// 7. Stream Anthropic SSE response back as CC format
	return s.streamAnthropicResponseAsCC(ctx, c, resp, originalModel, mappedModel, clientStream, includeUsage, startTime)
}

// streamAnthropicResponseAsCC reads an Anthropic SSE stream and converts to CC
// chunks, writing them to the client.
func (s *OpenAIGatewayService) streamAnthropicResponseAsCC(
	ctx context.Context,
	c *gin.Context,
	resp *http.Response,
	originalModel, upstreamModel string,
	clientStream, includeUsage bool,
	startTime time.Time,
) (*OpenAIForwardResult, error) {
	state := apicompat.NewAnthropicToCCChunkState(originalModel)

	if clientStream {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("X-Accel-Buffering", "no")
		c.Writer.WriteHeader(http.StatusOK)
	}

	var firstTokenMs *int
	clientDisconnect := false
	var allChunks []apicompat.ChatCompletionsChunk

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if ctx.Err() != nil {
			clientDisconnect = true
			break
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var evt apicompat.AnthropicStreamEvent
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}

		chunks := apicompat.AnthropicEventToCCChunks(&evt, state)
		for _, chunk := range chunks {
			if clientStream {
				if firstTokenMs == nil && len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != nil {
					ms := int(time.Since(startTime).Milliseconds())
					firstTokenMs = &ms
				}
				chunkJSON, _ := json.Marshal(chunk)
				_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", chunkJSON)
				c.Writer.Flush()
			} else {
				allChunks = append(allChunks, chunk)
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil && !clientDisconnect {
		return nil, fmt.Errorf("upstream stream read: %w", scanErr)
	}

	if clientStream && !clientDisconnect {
		if includeUsage && state.Usage != nil {
			usageChunk := state.BuildUsageChunk()
			chunkJSON, _ := json.Marshal(usageChunk)
			_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", chunkJSON)
		}
		_, _ = fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
		c.Writer.Flush()
	} else if !clientStream {
		ccResp := state.BuildNonStreamingResponse(allChunks)
		c.JSON(http.StatusOK, ccResp)
	}

	usage := openAIUsageFromState(state)

	return &OpenAIForwardResult{
		Model:            originalModel,
		UpstreamModel:    upstreamModel,
		Usage:            usage,
		Stream:           clientStream,
		Duration:         time.Since(startTime),
		FirstTokenMs:     firstTokenMs,
		ClientDisconnect: clientDisconnect,
	}, nil
}

func buildMessagesTargetURL(base string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.HasSuffix(normalized, "/messages") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/messages"
	}
	return normalized + "/v1/messages"
}

func openAIUsageFromState(state *apicompat.AnthropicToCCChunkState) OpenAIUsage {
	if state.Usage == nil {
		return OpenAIUsage{}
	}
	return OpenAIUsage{
		InputTokens:              state.Usage.InputTokens,
		OutputTokens:             state.Usage.OutputTokens,
		CacheReadInputTokens:     state.Usage.CacheReadInputTokens,
		CacheCreationInputTokens: state.Usage.CacheCreationInputTokens,
	}
}
