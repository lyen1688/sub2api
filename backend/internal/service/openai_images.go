package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha3"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	neturl "net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/imroc/req/v3"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	openAIImagesGenerationsEndpoint     = "/v1/images/generations"
	openAIImagesEditsEndpoint           = "/v1/images/edits"
	openAIImages2APIGenerationsEndpoint = "/v1/images2api/generations"
	openAIImages2APIEditsEndpoint       = "/v1/images2api/edits"

	openAIImagesGenerationsURL = "https://api.openai.com/v1/images/generations"
	openAIImagesEditsURL       = "https://api.openai.com/v1/images/edits"

	openAIChatGPTStartURL                    = "https://chatgpt.com/"
	openAIChatGPTSentinelPingURL             = "https://chatgpt.com/backend-api/sentinel/ping"
	openAIChatGPTFilesURL                    = "https://chatgpt.com/backend-api/files"
	openAIChatGPTFilesLibraryURL             = "https://chatgpt.com/backend-api/files/library"
	openAIChatGPTFilesProcessUploadURL       = "https://chatgpt.com/backend-api/files/process_upload_stream"
	openAIChatGPTConversationInitURL         = "https://chatgpt.com/backend-api/conversation/init"
	openAIChatGPTConversationURL             = "https://chatgpt.com/backend-api/f/conversation"
	openAIChatGPTConversationPrepareURL      = "https://chatgpt.com/backend-api/f/conversation/prepare"
	openAIChatGPTConversationAsyncURL        = "https://chatgpt.com/backend-api/conversation/%s/async-status"
	openAIChatGPTChatRequirementsPrepareURL  = "https://chatgpt.com/backend-api/sentinel/chat-requirements/prepare"
	openAIChatGPTChatRequirementsFinalizeURL = "https://chatgpt.com/backend-api/sentinel/chat-requirements/finalize"
	openAIChatGPTConversationModelAuto       = "auto"
	openAIChatGPTConversationModelPaid       = "gpt-5-5-thinking"
	openAIImageBackendUserAgent              = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	openAIImageRequirementsDiff              = "0fffff"
	openAIImageMaxDownloadBytes              = 20 << 20 // 20MB per image download
	openAIImageMaxUploadPartSize             = 50 << 20 // 50MB per multipart upload part
	openAIImagesResponsesMainModel           = "gpt-5.4-mini"
)

type OpenAIImagesCapability string

func resolveOpenAIResponsesImageMainModel(ctx context.Context) string {
	if ctx == nil {
		return NormalizeOpenAIImageMainModel("")
	}
	group, _ := ctx.Value(ctxkey.Group).(*Group)
	if !IsGroupContextValid(group) {
		return NormalizeOpenAIImageMainModel("")
	}
	return group.EffectiveOpenAIImageMainModel()
}

const (
	OpenAIImagesCapabilityBasic  OpenAIImagesCapability = "images-basic"
	OpenAIImagesCapabilityNative OpenAIImagesCapability = "images-native"
)

type OpenAIImagesUpload struct {
	FieldName   string
	FileName    string
	ContentType string
	Data        []byte
	Width       int
	Height      int
}

type OpenAIImagesInputRef struct {
	ImageURL string
	FileID   string
}

type OpenAIImagesRequest struct {
	Endpoint           string
	OriginalEndpoint   string
	ContentType        string
	Multipart          bool
	ConversationID     string
	ParentMessageID    string
	OriginalFileID     string
	OriginalGenID      string
	Model              string
	ExplicitModel      bool
	Prompt             string
	Stream             bool
	N                  int
	Size               string
	ExplicitSize       bool
	SizeTier           string
	ResponseFormat     string
	Quality            string
	Background         string
	OutputFormat       string
	Moderation         string
	InputFidelity      string
	Style              string
	OutputCompression  *int
	PartialImages      *int
	HasMask            bool
	HasNativeOptions   bool
	RequiredCapability OpenAIImagesCapability
	InputImages        []OpenAIImagesInputRef
	InputImageURLs     []string
	InputImageFileIDs  []string
	MaskImageURL       string
	MaskFileID         string
	Uploads            []OpenAIImagesUpload
	MaskUpload         *OpenAIImagesUpload
	Body               []byte
	bodyHash           string
}

func (r *OpenAIImagesRequest) ModerationBody() []byte {
	if r == nil {
		return nil
	}
	payload := map[string]any{}
	if prompt := strings.TrimSpace(r.Prompt); prompt != "" {
		payload["prompt"] = prompt
	}
	images := r.moderationImages()
	if len(images) > 0 {
		payload["images"] = images
	}
	if len(payload) == 0 {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return body
}

func (r *OpenAIImagesRequest) moderationImages() []map[string]string {
	if r == nil {
		return nil
	}
	images := make([]map[string]string, 0, len(r.InputImages)+len(r.Uploads)+1)
	for _, image := range r.orderedInputImages() {
		if imageURL := strings.TrimSpace(image.ImageURL); imageURL != "" {
			images = append(images, map[string]string{"image_url": imageURL})
		}
	}
	for _, upload := range r.Uploads {
		if dataURL := upload.ModerationDataURL(); dataURL != "" {
			images = append(images, map[string]string{"image_url": dataURL})
		}
	}
	if maskURL := strings.TrimSpace(r.MaskImageURL); maskURL != "" {
		images = append(images, map[string]string{"image_url": maskURL})
	}
	if r.MaskUpload != nil {
		if dataURL := r.MaskUpload.ModerationDataURL(); dataURL != "" {
			images = append(images, map[string]string{"image_url": dataURL})
		}
	}
	return images
}

func (r *OpenAIImagesRequest) orderedInputImages() []OpenAIImagesInputRef {
	if r == nil {
		return nil
	}
	if len(r.InputImages) > 0 {
		return append([]OpenAIImagesInputRef(nil), r.InputImages...)
	}
	if len(r.InputImageURLs) == 0 && len(r.InputImageFileIDs) == 0 {
		return nil
	}
	images := make([]OpenAIImagesInputRef, 0, len(r.InputImageURLs)+len(r.InputImageFileIDs))
	for _, imageURL := range r.InputImageURLs {
		if trimmed := strings.TrimSpace(imageURL); trimmed != "" {
			images = append(images, OpenAIImagesInputRef{ImageURL: trimmed})
		}
	}
	for _, fileID := range r.InputImageFileIDs {
		if trimmed := strings.TrimSpace(fileID); trimmed != "" {
			images = append(images, OpenAIImagesInputRef{FileID: trimmed})
		}
	}
	return images
}

func (u OpenAIImagesUpload) ModerationDataURL() string {
	if len(u.Data) == 0 {
		return ""
	}
	contentType := strings.TrimSpace(u.ContentType)
	if contentType == "" {
		contentType = http.DetectContentType(u.Data)
	}
	if !strings.HasPrefix(strings.ToLower(contentType), "image/") {
		return ""
	}
	return fmt.Sprintf("data:%s;base64,%s", contentType, base64.StdEncoding.EncodeToString(u.Data))
}

func (r *OpenAIImagesRequest) IsEdits() bool {
	return r != nil && isOpenAIImagesEditsEndpoint(r.Endpoint)
}

func (r *OpenAIImagesRequest) IsLegacyBridge() bool {
	return r != nil && isOpenAIImages2APIEndpoint(r.Endpoint)
}

func (r *OpenAIImagesRequest) IsExplicitLegacyBridge() bool {
	return r != nil && isOpenAIImages2APIEndpoint(r.OriginalEndpoint)
}

func (r *OpenAIImagesRequest) LegacyOriginalFileID() string {
	if r == nil {
		return ""
	}
	if originalFileID := strings.TrimSpace(r.OriginalFileID); originalFileID != "" {
		return originalFileID
	}
	for _, fileID := range r.InputImageFileIDs {
		if trimmed := strings.TrimSpace(fileID); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (r *OpenAIImagesRequest) LegacyMaskFileID() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.MaskFileID)
}

func (r *OpenAIImagesRequest) UsesLegacyInpainting() bool {
	if r == nil || !r.IsLegacyBridge() || !r.IsEdits() {
		return false
	}
	if strings.TrimSpace(r.OriginalGenID) == "" {
		return false
	}
	if r.LegacyOriginalFileID() == "" {
		return false
	}
	return r.LegacyMaskFileID() != "" || r.MaskUpload != nil
}

func (r *OpenAIImagesRequest) HasAnyLegacyInpaintingInput() bool {
	if r == nil || !r.IsLegacyBridge() || !r.IsEdits() {
		return false
	}
	return strings.TrimSpace(r.OriginalFileID) != "" ||
		strings.TrimSpace(r.OriginalGenID) != "" ||
		len(r.InputImageFileIDs) > 0 ||
		strings.TrimSpace(r.MaskFileID) != "" ||
		r.MaskUpload != nil ||
		strings.TrimSpace(r.MaskImageURL) != ""
}

func (r *OpenAIImagesRequest) StickySessionSeed() string {
	if r == nil {
		return ""
	}
	parts := []string{
		"openai-images",
		strings.TrimSpace(r.Endpoint),
		strings.TrimSpace(r.Model),
		strings.TrimSpace(r.Size),
		strings.TrimSpace(r.Prompt),
	}
	seed := strings.Join(parts, "|")
	if strings.TrimSpace(r.Prompt) == "" && r.bodyHash != "" {
		seed += "|body=" + r.bodyHash
	}
	sum := sha256.Sum256([]byte(seed))
	return "openai-images-" + hex.EncodeToString(sum[:8])
}

func (s *OpenAIGatewayService) ParseOpenAIImagesRequest(c *gin.Context, body []byte) (*OpenAIImagesRequest, error) {
	if c == nil || c.Request == nil {
		return nil, fmt.Errorf("missing request context")
	}
	endpoint := normalizeOpenAIImagesEndpointPath(c.Request.URL.Path)
	if endpoint == "" {
		return nil, fmt.Errorf("unsupported images endpoint")
	}

	contentType := strings.TrimSpace(c.GetHeader("Content-Type"))
	req := &OpenAIImagesRequest{
		Endpoint:         endpoint,
		OriginalEndpoint: endpoint,
		ContentType:      contentType,
		N:                1,
		Body:             body,
	}
	if len(body) > 0 {
		sum := sha256.Sum256(body)
		req.bodyHash = hex.EncodeToString(sum[:8])
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		req.Multipart = true
		if parseErr := parseOpenAIImagesMultipartRequest(body, contentType, req); parseErr != nil {
			return nil, parseErr
		}
	} else {
		if len(body) == 0 {
			return nil, fmt.Errorf("request body is empty")
		}
		if !gjson.ValidBytes(body) {
			return nil, fmt.Errorf("failed to parse request body")
		}
		if parseErr := parseOpenAIImagesJSONRequest(body, req); parseErr != nil {
			return nil, parseErr
		}
	}

	applyOpenAIImagesDefaults(req)
	if err := validateOpenAIImagesModel(req.Model); err != nil {
		return nil, err
	}
	if err := validateOpenAIImageSize(req.Size); err != nil {
		return nil, err
	}
	if err := validateOpenAIImageRequestLimits(req); err != nil {
		return nil, err
	}
	req.SizeTier = normalizeOpenAIImageSizeTier(req.Size)
	req.RequiredCapability = classifyOpenAIImagesCapability(req)
	return req, nil
}

func parseOpenAIImagesJSONRequest(body []byte, req *OpenAIImagesRequest) error {
	if modelResult := gjson.GetBytes(body, "model"); modelResult.Exists() {
		req.Model = strings.TrimSpace(modelResult.String())
		req.ExplicitModel = req.Model != ""
	}
	req.Prompt = strings.TrimSpace(gjson.GetBytes(body, "prompt").String())

	if streamResult := gjson.GetBytes(body, "stream"); streamResult.Exists() {
		if streamResult.Type != gjson.True && streamResult.Type != gjson.False {
			return fmt.Errorf("invalid stream field type")
		}
		req.Stream = streamResult.Bool()
	}

	if nResult := gjson.GetBytes(body, "n"); nResult.Exists() {
		if nResult.Type != gjson.Number {
			return fmt.Errorf("invalid n field type")
		}
		req.N = int(nResult.Int())
		if req.N <= 0 {
			return fmt.Errorf("n must be greater than 0")
		}
	}

	if sizeResult := gjson.GetBytes(body, "size"); sizeResult.Exists() {
		req.Size = strings.TrimSpace(sizeResult.String())
		req.ExplicitSize = req.Size != ""
	}
	req.ResponseFormat = strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "response_format").String()))
	req.ConversationID = strings.TrimSpace(gjson.GetBytes(body, "conversation_id").String())
	req.ParentMessageID = strings.TrimSpace(gjson.GetBytes(body, "parent_message_id").String())
	req.OriginalFileID = strings.TrimSpace(gjson.GetBytes(body, "original_file_id").String())
	req.OriginalGenID = strings.TrimSpace(gjson.GetBytes(body, "original_gen_id").String())
	req.Quality = strings.TrimSpace(gjson.GetBytes(body, "quality").String())
	req.Background = strings.TrimSpace(gjson.GetBytes(body, "background").String())
	req.OutputFormat = strings.TrimSpace(gjson.GetBytes(body, "output_format").String())
	req.Moderation = strings.TrimSpace(gjson.GetBytes(body, "moderation").String())
	req.InputFidelity = strings.TrimSpace(gjson.GetBytes(body, "input_fidelity").String())
	req.Style = strings.TrimSpace(gjson.GetBytes(body, "style").String())
	req.HasMask = gjson.GetBytes(body, "mask").Exists()
	if outputCompression := gjson.GetBytes(body, "output_compression"); outputCompression.Exists() {
		if outputCompression.Type != gjson.Number {
			return fmt.Errorf("invalid output_compression field type")
		}
		v := int(outputCompression.Int())
		req.OutputCompression = &v
	}
	if partialImages := gjson.GetBytes(body, "partial_images"); partialImages.Exists() {
		if partialImages.Type != gjson.Number {
			return fmt.Errorf("invalid partial_images field type")
		}
		v := int(partialImages.Int())
		req.PartialImages = &v
	}
	if req.IsEdits() {
		images := gjson.GetBytes(body, "images")
		if images.Exists() {
			if !images.IsArray() {
				return fmt.Errorf("invalid images field type")
			}
			for _, item := range images.Array() {
				if imageURL := strings.TrimSpace(item.Get("image_url").String()); imageURL != "" {
					req.InputImageURLs = append(req.InputImageURLs, imageURL)
					req.InputImages = append(req.InputImages, OpenAIImagesInputRef{ImageURL: imageURL})
					continue
				}
				if fileID := strings.TrimSpace(item.Get("file_id").String()); fileID != "" {
					req.InputImageFileIDs = append(req.InputImageFileIDs, fileID)
					req.InputImages = append(req.InputImages, OpenAIImagesInputRef{FileID: fileID})
					continue
				}
			}
		}
		if maskImageURL := strings.TrimSpace(gjson.GetBytes(body, "mask.image_url").String()); maskImageURL != "" {
			req.MaskImageURL = maskImageURL
			req.HasMask = true
		}
		if maskFileID := strings.TrimSpace(gjson.GetBytes(body, "mask.file_id").String()); maskFileID != "" {
			req.MaskFileID = maskFileID
			req.HasMask = true
		}
		if topMaskFileID := strings.TrimSpace(gjson.GetBytes(body, "mask_file_id").String()); topMaskFileID != "" {
			req.MaskFileID = topMaskFileID
			req.HasMask = true
		}
		if len(req.InputImageURLs) == 0 && len(req.InputImageFileIDs) == 0 {
			return fmt.Errorf("images[].image_url or images[].file_id is required")
		}
		if req.OriginalFileID == "" && len(req.InputImageFileIDs) > 0 {
			req.OriginalFileID = strings.TrimSpace(req.InputImageFileIDs[0])
		}
	}
	req.HasNativeOptions = hasOpenAINativeImageOptions(func(path string) bool {
		return gjson.GetBytes(body, path).Exists()
	})
	return nil
}

func parseOpenAIImagesMultipartRequest(body []byte, contentType string, req *OpenAIImagesRequest) error {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("invalid multipart content-type: %w", err)
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return fmt.Errorf("multipart boundary is required")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read multipart body: %w", err)
		}
		name := strings.TrimSpace(part.FormName())
		if name == "" {
			_ = part.Close()
			continue
		}

		data, err := readAllWithLimitDetection(part, int64(openAIImageMaxUploadPartSize))
		_ = part.Close()
		if err != nil {
			return fmt.Errorf("read multipart field %s: %w", name, err)
		}

		fileName := strings.TrimSpace(part.FileName())
		if fileName != "" {
			partContentType := strings.TrimSpace(part.Header.Get("Content-Type"))
			if name == "mask" && len(data) > 0 {
				req.HasMask = true
				width, height := parseOpenAIImageDimensions(part.Header)
				maskUpload := OpenAIImagesUpload{
					FieldName:   name,
					FileName:    fileName,
					ContentType: partContentType,
					Data:        data,
					Width:       width,
					Height:      height,
				}
				req.MaskUpload = &maskUpload
			}
			if name == "image" || strings.HasPrefix(name, "image[") {
				width, height := parseOpenAIImageDimensions(part.Header)
				req.Uploads = append(req.Uploads, OpenAIImagesUpload{
					FieldName:   name,
					FileName:    fileName,
					ContentType: partContentType,
					Data:        data,
					Width:       width,
					Height:      height,
				})
			}
			continue
		}

		value := strings.TrimSpace(string(data))
		switch name {
		case "conversation_id":
			req.ConversationID = value
		case "parent_message_id":
			req.ParentMessageID = value
		case "original_file_id":
			req.OriginalFileID = value
		case "original_gen_id":
			req.OriginalGenID = value
		case "mask_file_id":
			req.MaskFileID = value
			req.HasMask = value != ""
		case "model":
			req.Model = value
			req.ExplicitModel = value != ""
		case "prompt":
			req.Prompt = value
		case "size":
			req.Size = value
			req.ExplicitSize = value != ""
		case "response_format":
			req.ResponseFormat = strings.ToLower(value)
		case "stream":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("invalid stream field value")
			}
			req.Stream = parsed
		case "n":
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return fmt.Errorf("n must be a positive integer")
			}
			req.N = n
		case "quality":
			req.Quality = value
			req.HasNativeOptions = true
		case "background":
			req.Background = value
			req.HasNativeOptions = true
		case "output_format":
			req.OutputFormat = value
			req.HasNativeOptions = true
		case "moderation":
			req.Moderation = value
			req.HasNativeOptions = true
		case "input_fidelity":
			req.InputFidelity = value
			req.HasNativeOptions = true
		case "style":
			req.Style = value
			req.HasNativeOptions = true
		case "output_compression":
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid output_compression field value")
			}
			req.OutputCompression = &n
			req.HasNativeOptions = true
		case "partial_images":
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid partial_images field value")
			}
			req.PartialImages = &n
			req.HasNativeOptions = true
		default:
			if isOpenAINativeImageOption(name) && value != "" {
				req.HasNativeOptions = true
			}
		}
	}

	if len(req.Uploads) == 0 && req.IsEdits() {
		return fmt.Errorf("image file is required")
	}
	return nil
}

func readAllWithLimitDetection(r io.Reader, limit int64) ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("reader is nil")
	}
	if limit <= 0 {
		return io.ReadAll(r)
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("payload exceeds %d bytes", limit)
	}
	return data, nil
}

func parseOpenAIImageDimensions(_ textproto.MIMEHeader) (int, int) {
	return 0, 0
}

func applyOpenAIImagesDefaults(req *OpenAIImagesRequest) {
	if req == nil {
		return
	}
	if req.N <= 0 {
		req.N = 1
	}
	if strings.TrimSpace(req.Model) != "" {
		req.Model = strings.TrimSpace(req.Model)
		normalizeOpenAIImagesRequestForModel(req)
		return
	}
	req.Model = "gpt-image-2"
	normalizeOpenAIImagesRequestForModel(req)
}

func normalizeOpenAIImagesRequestForModel(req *OpenAIImagesRequest) {
	if req == nil {
		return
	}
	model := strings.ToLower(strings.TrimSpace(req.Model))
	background := strings.ToLower(strings.TrimSpace(req.Background))
	if model == "gpt-image-2" && background == "transparent" {
		req.Background = ""
	}
}

func isOpenAIImageGenerationModel(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "gpt-image-")
}

func validateOpenAIImagesModel(model string) error {
	model = strings.TrimSpace(model)
	if isOpenAIImageGenerationModel(model) {
		return nil
	}
	if model == "" {
		return fmt.Errorf("images endpoint requires an image model")
	}
	return fmt.Errorf("images endpoint requires an image model, got %q", model)
}

func normalizeOpenAIImagesEndpointPath(path string) string {
	trimmed := strings.TrimSpace(path)
	switch {
	case strings.Contains(trimmed, "/images/generations"):
		return openAIImagesGenerationsEndpoint
	case strings.Contains(trimmed, "/images/edits"):
		return openAIImagesEditsEndpoint
	default:
		return ""
	}
}

func isOpenAIImagesEditsEndpoint(endpoint string) bool {
	switch endpoint {
	case openAIImagesEditsEndpoint:
		return true
	default:
		return false
	}
}

func isOpenAIImages2APIEndpoint(endpoint string) bool {
	return false
}

func shouldUseLegacyOpenAIImagesBridge(account *Account, parsed *OpenAIImagesRequest) bool {
	return false
}

func applyOpenAIImagesRouteSelection(parsed *OpenAIImagesRequest, route string) RequestType {
	if parsed == nil {
		return RequestTypeUnknown
	}
	parsed.Endpoint = parsed.OriginalEndpoint
	return RequestTypeImage
}

func classifyOpenAIImagesCapability(req *OpenAIImagesRequest) OpenAIImagesCapability {
	if req == nil {
		return OpenAIImagesCapabilityNative
	}
	if req.ExplicitModel || req.ExplicitSize {
		return OpenAIImagesCapabilityNative
	}
	model := strings.ToLower(strings.TrimSpace(req.Model))
	if !strings.HasPrefix(model, "gpt-image-") {
		return OpenAIImagesCapabilityNative
	}
	if req.Stream || req.N != 1 || req.HasMask || req.HasNativeOptions {
		return OpenAIImagesCapabilityNative
	}
	if req.IsEdits() && !req.Multipart {
		return OpenAIImagesCapabilityNative
	}
	if req.ResponseFormat != "" && req.ResponseFormat != "b64_json" {
		return OpenAIImagesCapabilityNative
	}
	return OpenAIImagesCapabilityBasic
}

func hasOpenAINativeImageOptions(exists func(path string) bool) bool {
	for _, path := range []string{
		"background",
		"quality",
		"style",
		"output_format",
		"output_compression",
		"moderation",
		"input_fidelity",
		"partial_images",
	} {
		if exists(path) {
			return true
		}
	}
	return false
}

func isOpenAINativeImageOption(name string) bool {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "background", "quality", "style", "output_format", "output_compression", "moderation", "input_fidelity", "partial_images":
		return true
	default:
		return false
	}
}

func normalizeOpenAIImageSizeTier(size string) string {
	trimmed := strings.TrimSpace(size)
	normalized := strings.ToLower(trimmed)
	switch normalized {
	case "", "auto":
		return "2K"
	case "1024x1024":
		return "1K"
	case "1536x1024", "1024x1536", "1792x1024", "1024x1792", "2048x2048", "2048x1152", "1152x2048":
		return "2K"
	case "3840x2160", "2160x3840":
		return "4K"
	}
	width, height, ok := parseOpenAIImageSizeDimensions(trimmed)
	if !ok {
		return "2K"
	}
	return classifyUnknownOpenAIImageSizeTier(width, height)
}

const (
	openAIImage2KMaxPixels = 2560 * 1440
	openAIImageMinPixels   = 655360
	openAIImageMaxPixels   = 8294400
	openAIImageMaxEdge     = 3840
	openAIImageMaxRatio    = 3
	openAIImageMaxN        = 10
	openAIImageMaxParts    = 3
	openAIImageMaxQuality  = 100
)

func parseOpenAIImageSizeDimensions(size string) (int, int, bool) {
	trimmed := strings.TrimSpace(size)
	parts := strings.Split(strings.ToLower(trimmed), "x")
	if len(parts) != 2 {
		return 0, 0, false
	}
	width, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, false
	}
	height, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, false
	}
	if width <= 0 || height <= 0 {
		return 0, 0, false
	}
	return width, height, true
}

func validateOpenAIImageSize(size string) error {
	trimmed := strings.TrimSpace(size)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return nil
	}

	width, height, ok := parseOpenAIImageSizeDimensions(trimmed)
	if !ok {
		return fmt.Errorf("invalid size format: expected widthxheight, got %q", size)
	}
	if width%16 != 0 || height%16 != 0 {
		return fmt.Errorf("size must use widthxheight with both edges as multiples of 16")
	}
	if width > openAIImageMaxEdge || height > openAIImageMaxEdge {
		return fmt.Errorf("size exceeds the maximum edge length of 3840px")
	}
	longer, shorter := width, height
	if shorter > longer {
		longer, shorter = shorter, longer
	}
	if shorter == 0 || longer > shorter*openAIImageMaxRatio {
		return fmt.Errorf("size aspect ratio exceeds 3:1")
	}
	totalPixels := int64(width) * int64(height)
	if totalPixels < openAIImageMinPixels || totalPixels > openAIImageMaxPixels {
		return fmt.Errorf("size must contain between 655360 and 8294400 total pixels")
	}
	return nil
}

func validateOpenAIImageRequestLimits(req *OpenAIImagesRequest) error {
	if req == nil {
		return nil
	}
	if req.N <= 0 || req.N > openAIImageMaxN {
		return fmt.Errorf("n must be between 1 and 10")
	}
	if req.PartialImages != nil {
		if *req.PartialImages < 0 || *req.PartialImages > openAIImageMaxParts {
			return fmt.Errorf("partial_images must be between 0 and 3")
		}
	}
	if req.OutputCompression != nil {
		if *req.OutputCompression < 0 || *req.OutputCompression > openAIImageMaxQuality {
			return fmt.Errorf("output_compression must be between 0 and 100")
		}
	}
	switch req.ResponseFormat {
	case "", "b64_json":
	case "url":
		return fmt.Errorf("response_format=url is not supported for gpt-image models; use b64_json")
	default:
		return fmt.Errorf("unsupported response_format %q", req.ResponseFormat)
	}
	model := strings.ToLower(strings.TrimSpace(req.Model))
	if model == "gpt-image-2" && strings.TrimSpace(req.InputFidelity) != "" {
		return fmt.Errorf("input_fidelity is not supported for gpt-image-2")
	}
	outputFormat := strings.ToLower(strings.TrimSpace(req.OutputFormat))
	background := strings.ToLower(strings.TrimSpace(req.Background))
	if background == "transparent" && outputFormat != "" && outputFormat != "png" && outputFormat != "webp" {
		return fmt.Errorf("background=transparent requires output_format png or webp")
	}
	if req.OutputCompression != nil && outputFormat != "" && outputFormat != "jpeg" && outputFormat != "jpg" && outputFormat != "webp" {
		return fmt.Errorf("output_compression is only supported when output_format is jpeg or webp")
	}
	return nil
}

func classifyUnknownOpenAIImageSizeTier(width int, height int) string {
	if height > 0 && width > openAIImage2KMaxPixels/height {
		return "4K"
	}
	return "2K"
}

func (s *OpenAIGatewayService) ForwardImages(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	parsed *OpenAIImagesRequest,
	channelMappedModel string,
) (*OpenAIForwardResult, error) {
	if parsed == nil {
		return nil, fmt.Errorf("parsed images request is required")
	}
	imageRoute := GroupImageGenerationRouteCodex
	effectiveRequestType := applyOpenAIImagesRouteSelection(parsed, imageRoute)
	switch account.Type {
	case AccountTypeAPIKey:
		result, err := s.forwardOpenAIImagesAPIKey(ctx, c, account, body, parsed, channelMappedModel, imageRoute)
		if result != nil {
			result.EffectiveRequestType = effectiveRequestType
		}
		return result, err
	case AccountTypeOAuth:
		result, err := s.forwardOpenAIImagesOAuth(ctx, c, account, parsed, channelMappedModel, imageRoute)
		if result != nil {
			result.EffectiveRequestType = effectiveRequestType
		}
		return result, err
	default:
		return nil, fmt.Errorf("unsupported account type: %s", account.Type)
	}
}

func (s *OpenAIGatewayService) forwardOpenAIImagesAPIKey(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	parsed *OpenAIImagesRequest,
	channelMappedModel string,
	imageRoute string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	requestModel := strings.TrimSpace(parsed.Model)
	if mapped := strings.TrimSpace(channelMappedModel); mapped != "" {
		requestModel = mapped
	}
	if err := validateOpenAIImagesModel(requestModel); err != nil {
		return nil, err
	}
	upstreamModel := account.GetMappedModel(requestModel)
	if err := validateOpenAIImagesModel(upstreamModel); err != nil {
		return nil, err
	}
	logger.LegacyPrintf(
		"service.openai_gateway",
		"[OpenAI] Images request routing request_model=%s upstream_model=%s endpoint=%s account_type=%s",
		strings.TrimSpace(parsed.Model),
		upstreamModel,
		parsed.Endpoint,
		account.Type,
	)
	forwardBody, forwardContentType, err := normalizeOpenAIImagesForwardBody(body, parsed.ContentType, parsed)
	if err != nil {
		return nil, err
	}
	forwardBody, forwardContentType, err = rewriteOpenAIImagesModel(forwardBody, forwardContentType, upstreamModel)
	if err != nil {
		return nil, err
	}
	if !parsed.Multipart {
		setOpsUpstreamRequestBody(c, forwardBody)
	}

	upstreamCtx, releaseUpstreamCtx := detachStreamUpstreamContext(ctx, parsed.Stream)
	defer releaseUpstreamCtx()

	token, _, err := s.GetAccessToken(upstreamCtx, account)
	if err != nil {
		return nil, err
	}
	upstreamReq, err := s.buildOpenAIImagesRequest(upstreamCtx, c, account, forwardBody, forwardContentType, token, parsed.Endpoint)
	if err != nil {
		return nil, err
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	tlsRuntime := s.resolveOpenAITLSFingerprintRuntime(ctx, c, account)
	s.applyOpenAITLSFingerprintRuntime(ctx, upstreamReq, tlsRuntime, account.IsOpenAIPassthroughEnabled())
	upstreamStart := time.Now()
	resp, err := s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, tlsRuntime.Profile)
	SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
	if err != nil {
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		recordDetailedUpstreamTransportError(c, err)
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: 0,
			UpstreamURL:        safeUpstreamURL(upstreamReq.URL.String()),
			Kind:               "request_error",
			Message:            safeErr,
			Detail:             safeErr,
		})
		return nil, fmt.Errorf("upstream request failed: %s", safeErr)
	}
	if resp.StatusCode >= 400 {
		respBody := s.readUpstreamErrorBody(resp)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
		upstreamMsg := strings.TrimSpace(extractUpstreamErrorMessage(respBody))
		upstreamMsg = sanitizeUpstreamErrorMessage(upstreamMsg)
		if s.shouldFailoverOpenAIUpstreamResponse(resp.StatusCode, upstreamMsg, respBody) {
			appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
				Platform:           account.Platform,
				AccountID:          account.ID,
				AccountName:        account.Name,
				UpstreamStatusCode: resp.StatusCode,
				UpstreamRequestID:  resp.Header.Get("x-request-id"),
				UpstreamURL:        safeUpstreamURL(upstreamReq.URL.String()),
				Kind:               "failover",
				Message:            upstreamMsg,
			})
			if s.rateLimitService != nil {
				if !s.rateLimitService.handleOpenAIImageRoute429(upstreamCtx, account, imageRoute, resp.StatusCode, resp.Header, respBody, true) {
					s.handleFailoverSideEffects(upstreamCtx, resp, account, upstreamModel)
				}
			} else {
				s.handleFailoverSideEffects(upstreamCtx, resp, account, upstreamModel)
			}
			return nil, &UpstreamFailoverError{
				StatusCode:             resp.StatusCode,
				ResponseBody:           respBody,
				RetryableOnSameAccount: account.IsPoolMode() && account.IsPoolModeRetryableStatus(resp.StatusCode),
			}
		}
		return s.handleErrorResponse(upstreamCtx, resp, c, account, forwardBody, upstreamModel)
	}
	defer func() { _ = resp.Body.Close() }()

	var usage OpenAIUsage
	imageCount := parsed.N
	var firstTokenMs *int
	if parsed.Stream && isEventStreamResponse(resp.Header) {
		streamUsage, streamCount, ttft, err := s.handleOpenAIImagesStreamingResponse(upstreamCtx, resp, c, account, startTime)
		if err != nil {
			if streamCount > 0 {
				return &OpenAIForwardResult{
					RequestID:       resp.Header.Get("x-request-id"),
					Usage:           streamUsage,
					Model:           requestModel,
					UpstreamModel:   upstreamModel,
					Stream:          parsed.Stream,
					ResponseHeaders: resp.Header.Clone(),
					Duration:        time.Since(startTime),
					FirstTokenMs:    ttft,
					ImageCount:      streamCount,
					ImageSize:       parsed.SizeTier,
				}, err
			}
			return nil, err
		}
		usage = streamUsage
		imageCount = streamCount
		firstTokenMs = ttft
	} else {
		nonStreamUsage, nonStreamCount, err := s.handleOpenAIImagesNonStreamingResponse(upstreamCtx, resp, c, account)
		if err != nil {
			return nil, err
		}
		usage = nonStreamUsage
		if nonStreamCount > 0 {
			imageCount = nonStreamCount
		}
	}
	return &OpenAIForwardResult{
		RequestID:       resp.Header.Get("x-request-id"),
		Usage:           usage,
		Model:           requestModel,
		UpstreamModel:   upstreamModel,
		Stream:          parsed.Stream,
		ResponseHeaders: resp.Header.Clone(),
		Duration:        time.Since(startTime),
		FirstTokenMs:    firstTokenMs,
		ImageCount:      imageCount,
		ImageSize:       parsed.SizeTier,
	}, nil
}

func (s *OpenAIGatewayService) buildOpenAIImagesRequest(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	body []byte,
	contentType string,
	token string,
	endpoint string,
) (*http.Request, error) {
	targetURL := openAIImagesGenerationsURL
	if isOpenAIImagesEditsEndpoint(endpoint) {
		targetURL = openAIImagesEditsURL
	}
	baseURL := account.GetOpenAIBaseURL()
	if baseURL != "" {
		validatedURL, err := s.validateUpstreamBaseURL(baseURL)
		if err != nil {
			return nil, err
		}
		targetURL = buildOpenAIImagesURL(validatedURL, endpoint)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	req.Header.Set("Authorization", "Bearer "+token)
	for key, values := range c.Request.Header {
		if !shouldForwardOpenAIPassthroughHeader(account, strings.ToLower(key)) {
			continue
		}
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	customUA := account.GetOpenAIUserAgent()
	if customUA != "" {
		req.Header.Set("User-Agent", customUA)
	}
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func buildOpenAIImagesURL(base string, endpoint string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	relative := strings.TrimPrefix(strings.TrimSpace(endpoint), "/v1")
	if strings.HasSuffix(normalized, endpoint) || strings.HasSuffix(normalized, relative) {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + relative
	}
	return normalized + endpoint
}

func rewriteOpenAIImagesModel(body []byte, contentType string, model string) ([]byte, string, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return body, contentType, nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		rewrittenBody, rewrittenType, rewriteErr := rewriteOpenAIImagesMultipartModel(body, contentType, model)
		return rewrittenBody, rewrittenType, rewriteErr
	}
	rewritten, err := sjson.SetBytes(body, "model", model)
	if err != nil {
		return nil, "", fmt.Errorf("rewrite image request model: %w", err)
	}
	return rewritten, contentType, nil
}

func normalizeOpenAIImagesForwardBody(body []byte, contentType string, parsed *OpenAIImagesRequest) ([]byte, string, error) {
	if parsed == nil {
		return body, contentType, nil
	}
	if strings.ToLower(strings.TrimSpace(parsed.Model)) != "gpt-image-2" || strings.TrimSpace(parsed.Background) != "" {
		return body, contentType, nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, "multipart/form-data") {
		rewrittenBody, rewrittenType, rewriteErr := rewriteOpenAIImagesMultipartWithoutField(body, contentType, "background")
		return rewrittenBody, rewrittenType, rewriteErr
	}
	rewritten, err := sjson.DeleteBytes(body, "background")
	if err != nil {
		return nil, "", fmt.Errorf("rewrite image request background: %w", err)
	}
	return rewritten, contentType, nil
}

func rewriteOpenAIImagesMultipartModel(body []byte, contentType string, model string) ([]byte, string, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", fmt.Errorf("parse multipart content-type: %w", err)
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return nil, "", fmt.Errorf("multipart boundary is required")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	modelWritten := false

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", fmt.Errorf("read multipart body: %w", err)
		}

		formName := strings.TrimSpace(part.FormName())
		partHeader := cloneMultipartHeader(part.Header)
		target, err := writer.CreatePart(partHeader)
		if err != nil {
			_ = part.Close()
			return nil, "", fmt.Errorf("create multipart part: %w", err)
		}

		if formName == "model" && part.FileName() == "" {
			if _, err := target.Write([]byte(model)); err != nil {
				_ = part.Close()
				return nil, "", fmt.Errorf("rewrite multipart model: %w", err)
			}
			modelWritten = true
			_ = part.Close()
			continue
		}
		if _, err := io.Copy(target, part); err != nil {
			_ = part.Close()
			return nil, "", fmt.Errorf("copy multipart part: %w", err)
		}
		_ = part.Close()
	}

	if !modelWritten {
		if err := writer.WriteField("model", model); err != nil {
			return nil, "", fmt.Errorf("append multipart model field: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("finalize multipart body: %w", err)
	}
	return buffer.Bytes(), writer.FormDataContentType(), nil
}

func rewriteOpenAIImagesMultipartWithoutField(body []byte, contentType string, fieldName string) ([]byte, string, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", fmt.Errorf("parse multipart content-type: %w", err)
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return nil, "", fmt.Errorf("multipart boundary is required")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	skipField := strings.TrimSpace(fieldName)

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", fmt.Errorf("read multipart body: %w", err)
		}

		formName := strings.TrimSpace(part.FormName())
		if part.FileName() == "" && formName == skipField {
			_ = part.Close()
			continue
		}

		partHeader := cloneMultipartHeader(part.Header)
		target, err := writer.CreatePart(partHeader)
		if err != nil {
			_ = part.Close()
			return nil, "", fmt.Errorf("create multipart part: %w", err)
		}
		if _, err := io.Copy(target, part); err != nil {
			_ = part.Close()
			return nil, "", fmt.Errorf("copy multipart part: %w", err)
		}
		_ = part.Close()
	}

	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("finalize multipart body: %w", err)
	}
	return buffer.Bytes(), writer.FormDataContentType(), nil
}

func cloneMultipartHeader(src textproto.MIMEHeader) textproto.MIMEHeader {
	dst := make(textproto.MIMEHeader, len(src))
	for key, values := range src {
		copied := make([]string, len(values))
		copy(copied, values)
		dst[key] = copied
	}
	return dst
}

func (s *OpenAIGatewayService) handleOpenAIImagesNonStreamingResponse(ctx context.Context, resp *http.Response, c *gin.Context, account *Account) (OpenAIUsage, int, error) {
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		return OpenAIUsage{}, 0, err
	}
	_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	contentType := "application/json"
	if s.cfg != nil && !s.cfg.Security.ResponseHeaders.Enabled {
		if upstreamType := resp.Header.Get("Content-Type"); upstreamType != "" {
			contentType = upstreamType
		}
	}
	c.Data(resp.StatusCode, contentType, body)

	usage, _ := extractOpenAIUsageFromJSONBytes(body)
	return usage, extractOpenAIImageCountFromJSONBytes(body), nil
}

func (s *OpenAIGatewayService) handleOpenAIImagesStreamingResponse(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	startTime time.Time,
) (OpenAIUsage, int, *int, error) {
	responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "text/event-stream"
	}
	c.Status(resp.StatusCode)
	c.Header("Content-Type", contentType)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return OpenAIUsage{}, 0, nil, fmt.Errorf("streaming is not supported by response writer")
	}

	usage := OpenAIUsage{}
	imageCounter := newOpenAIImageOutputCounter()
	var firstTokenMs *int
	clientDisconnected := false
	lastDownstreamWriteAt := time.Now()
	var fallbackBody bytes.Buffer
	fallbackBytes := int64(0)
	fallbackLimit := resolveUpstreamResponseReadLimit(s.cfg)
	seenSSEData := false
	fallbackTooLarge := false
	var sseData openAISSEDataAccumulator

	processSSEData := func(dataBytes []byte) {
		seenSSEData = true
		fallbackBody.Reset()
		fallbackBytes = 0
		_ = s.markOpenAICyberPolicyIfDetected(ctx, account, dataBytes)
		mergeOpenAIUsage(&usage, dataBytes)
		imageCounter.AddSSEData(dataBytes)
	}

	flushSSEEvent := func() {
		sseData.Flush(processSSEData)
	}

	processLine := func(line []byte) {
		if len(line) == 0 {
			return
		}
		if firstTokenMs == nil {
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}
		if !clientDisconnected {
			if _, writeErr := c.Writer.Write(line); writeErr != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Images stream client disconnected, continue draining upstream for billing")
			} else {
				flusher.Flush()
				lastDownstreamWriteAt = time.Now()
			}
		}

		trimmedLine := strings.TrimRight(string(line), "\r\n")
		if _, ok := extractOpenAISSEDataLine(trimmedLine); ok || strings.TrimSpace(trimmedLine) == "" {
			sseData.AddLine(trimmedLine, processSSEData)
			return
		}
		if !seenSSEData && !fallbackTooLarge {
			fallbackBytes += int64(len(line))
			if fallbackBytes <= fallbackLimit {
				_, _ = fallbackBody.Write(line)
			} else {
				fallbackTooLarge = true
				fallbackBody.Reset()
			}
		}
	}

	finalizeFallbackBody := func() {
		if seenSSEData || fallbackBody.Len() == 0 {
			return
		}
		body := bytes.TrimSpace(fallbackBody.Bytes())
		if len(body) == 0 {
			return
		}
		_ = s.markOpenAICyberPolicyIfDetected(ctx, account, body)
		mergeOpenAIUsage(&usage, body)
		imageCounter.AddJSONResponse(body)
	}

	streamInterval := s.openAIImageStreamDataInterval()
	keepaliveInterval := s.openAIImageStreamKeepaliveInterval()
	if streamInterval <= 0 && keepaliveInterval <= 0 {
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			processLine(line)
			if err == io.EOF {
				break
			}
			if err != nil {
				flushSSEEvent()
				return usage, imageCounter.Count(), firstTokenMs, err
			}
		}
		flushSSEEvent()
		finalizeFallbackBody()
		return usage, imageCounter.Count(), firstTokenMs, nil
	}

	type readEvent struct {
		line []byte
		err  error
	}
	events := make(chan readEvent, 16)
	done := make(chan struct{})
	sendEvent := func(ev readEvent) bool {
		select {
		case events <- ev:
			return true
		case <-done:
			return false
		}
	}
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
	go func() {
		defer close(events)
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			}
			if len(line) > 0 && !sendEvent(readEvent{line: line}) {
				return
			}
			if err == io.EOF {
				return
			}
			if err != nil {
				_ = sendEvent(readEvent{err: err})
				return
			}
		}
	}()
	defer close(done)

	var intervalTicker *time.Ticker
	if streamInterval > 0 {
		intervalTicker = time.NewTicker(streamInterval)
		defer intervalTicker.Stop()
	}
	var intervalCh <-chan time.Time
	if intervalTicker != nil {
		intervalCh = intervalTicker.C
	}

	var keepaliveTicker *time.Ticker
	if keepaliveInterval > 0 {
		keepaliveTicker = time.NewTicker(keepaliveInterval)
		defer keepaliveTicker.Stop()
	}
	var keepaliveCh <-chan time.Time
	if keepaliveTicker != nil {
		keepaliveCh = keepaliveTicker.C
	}

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				flushSSEEvent()
				finalizeFallbackBody()
				return usage, imageCounter.Count(), firstTokenMs, nil
			}
			if ev.err != nil {
				flushSSEEvent()
				return usage, imageCounter.Count(), firstTokenMs, ev.err
			}
			processLine(ev.line)
		case <-intervalCh:
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < streamInterval {
				continue
			}
			if clientDisconnected {
				return usage, imageCounter.Count(), firstTokenMs, fmt.Errorf("image stream incomplete after timeout")
			}
			logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Images stream data interval timeout: interval=%s", streamInterval)
			_ = s.writeOpenAIImagesStreamEvent(c, flusher, "error", buildOpenAIImagesStreamErrorBody(fmt.Sprintf("upstream image stream idle for %s", streamInterval)))
			return usage, imageCounter.Count(), firstTokenMs, fmt.Errorf("image stream data interval timeout")
		case <-keepaliveCh:
			if clientDisconnected || time.Since(lastDownstreamWriteAt) < keepaliveInterval {
				continue
			}
			if _, writeErr := io.WriteString(c.Writer, ":\n\n"); writeErr != nil {
				clientDisconnected = true
				logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Images stream client disconnected during keepalive, continue draining upstream for billing")
				continue
			}
			flusher.Flush()
			lastDownstreamWriteAt = time.Now()
		}
	}
}

func (s *OpenAIGatewayService) openAIImageStreamDataInterval() time.Duration {
	if s == nil || s.cfg == nil || s.cfg.Gateway.ImageStreamDataIntervalTimeout <= 0 {
		return 0
	}
	return time.Duration(s.cfg.Gateway.ImageStreamDataIntervalTimeout) * time.Second
}

func (s *OpenAIGatewayService) openAIImageStreamKeepaliveInterval() time.Duration {
	if s == nil || s.cfg == nil || s.cfg.Gateway.ImageStreamKeepaliveInterval <= 0 {
		return 0
	}
	return time.Duration(s.cfg.Gateway.ImageStreamKeepaliveInterval) * time.Second
}

func extractOpenAIImagesBillableCountFromJSONBytes(body []byte) int {
	if count := extractOpenAIImageCountFromJSONBytes(body); count > 0 {
		return count
	}
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return 0
	}
	if count := int(gjson.GetBytes(body, "usage.images").Int()); count > 0 {
		return count
	}
	if count := int(gjson.GetBytes(body, "tool_usage.image_gen.images").Int()); count > 0 {
		return count
	}
	eventType := strings.TrimSpace(gjson.GetBytes(body, "type").String())
	if eventType == "" || !strings.HasSuffix(eventType, ".completed") {
		return 0
	}
	if gjson.GetBytes(body, "b64_json").Exists() || gjson.GetBytes(body, "url").Exists() {
		return 1
	}
	return 0
}

func mergeOpenAIUsage(dst *OpenAIUsage, body []byte) {
	if dst == nil {
		return
	}
	if parsed, ok := extractOpenAIUsageFromJSONBytes(body); ok {
		if parsed.InputTokens > 0 {
			dst.InputTokens = parsed.InputTokens
		}
		if parsed.OutputTokens > 0 {
			dst.OutputTokens = parsed.OutputTokens
		}
		if parsed.CacheReadInputTokens > 0 {
			dst.CacheReadInputTokens = parsed.CacheReadInputTokens
		}
		if parsed.ImageOutputTokens > 0 {
			dst.ImageOutputTokens = parsed.ImageOutputTokens
		}
	}
}

func extractOpenAIImageCountFromJSONBytes(body []byte) int {
	return countOpenAIResponseImageOutputsFromJSONBytes(body)
}

type openAIImagePointerInfo struct {
	Pointer     string
	DownloadURL string
	B64JSON     string
	MimeType    string
	Prompt      string
}

func collectOpenAIImagePointers(body []byte) []openAIImagePointerInfo {
	if len(body) == 0 {
		return nil
	}
	prompt := ""
	for _, path := range []string{
		"message.metadata.dalle.prompt",
		"metadata.dalle.prompt",
		"revised_prompt",
	} {
		if value := strings.TrimSpace(gjson.GetBytes(body, path).String()); value != "" {
			prompt = value
			break
		}
	}
	matches := openAIImagePointerMatches(body)
	out := make([]openAIImagePointerInfo, 0, len(matches))
	for _, pointer := range matches {
		out = append(out, openAIImagePointerInfo{Pointer: pointer, Prompt: prompt})
	}
	return mergeOpenAIImagePointerInfos(out, collectOpenAIImageInlineAssets(body, prompt))
}

func openAIImagePointerMatches(body []byte) []string {
	raw := string(body)
	matches := make([]string, 0, 4)
	for _, prefix := range []string{"file-service://", "sediment://"} {
		start := 0
		for {
			idx := strings.Index(raw[start:], prefix)
			if idx < 0 {
				break
			}
			idx += start
			end := idx + len(prefix)
			for end < len(raw) {
				ch := raw[end]
				if ch != '-' && ch != '_' &&
					(ch < '0' || ch > '9') &&
					(ch < 'a' || ch > 'z') &&
					(ch < 'A' || ch > 'Z') {
					break
				}
				end++
			}
			matches = append(matches, raw[idx:end])
			start = end
		}
	}
	return dedupeStrings(matches)
}

func mergeOpenAIImagePointerInfos(existing []openAIImagePointerInfo, next []openAIImagePointerInfo) []openAIImagePointerInfo {
	if len(next) == 0 {
		return existing
	}
	seen := make(map[string]openAIImagePointerInfo, len(existing)+len(next))
	out := make([]openAIImagePointerInfo, 0, len(existing)+len(next))
	for _, item := range existing {
		if key := item.identityKey(); key != "" {
			seen[key] = item
		}
		out = append(out, item)
	}
	for _, item := range next {
		key := item.identityKey()
		if key == "" {
			continue
		}
		if existingItem, ok := seen[key]; ok {
			merged := mergeOpenAIImagePointerInfo(existingItem, item)
			if merged != existingItem {
				for i := range out {
					if out[i].identityKey() == key {
						out[i] = merged
						break
					}
				}
				seen[key] = merged
			}
			continue
		}
		seen[key] = item
		out = append(out, item)
	}
	return out
}

func (i openAIImagePointerInfo) identityKey() string {
	switch {
	case strings.TrimSpace(i.Pointer) != "":
		return "pointer:" + strings.TrimSpace(i.Pointer)
	case strings.TrimSpace(i.DownloadURL) != "":
		return "download:" + strings.TrimSpace(i.DownloadURL)
	case strings.TrimSpace(i.B64JSON) != "":
		b64 := strings.TrimSpace(i.B64JSON)
		if len(b64) > 64 {
			b64 = b64[:64]
		}
		return "b64:" + b64
	default:
		return ""
	}
}

func mergeOpenAIImagePointerInfo(existing, next openAIImagePointerInfo) openAIImagePointerInfo {
	merged := existing
	if strings.TrimSpace(merged.Pointer) == "" {
		merged.Pointer = next.Pointer
	}
	if strings.TrimSpace(merged.DownloadURL) == "" {
		merged.DownloadURL = next.DownloadURL
	}
	if strings.TrimSpace(merged.B64JSON) == "" {
		merged.B64JSON = next.B64JSON
	}
	if strings.TrimSpace(merged.MimeType) == "" {
		merged.MimeType = next.MimeType
	}
	if strings.TrimSpace(merged.Prompt) == "" {
		merged.Prompt = next.Prompt
	}
	return merged
}

func resolveOpenAIImageBytes(
	ctx context.Context,
	client *req.Client,
	headers http.Header,
	profile *OpenAIWebProfile,
	conversationID string,
	pointer openAIImagePointerInfo,
	errorBodyReadLimit int64,
) ([]byte, error) {
	headers = applyOpenAIConversationPageReferer(headers, conversationID)
	if normalized := normalizeOpenAIImageBase64(pointer.B64JSON); normalized != "" {
		return base64.StdEncoding.DecodeString(normalized)
	}
	if downloadURL := strings.TrimSpace(pointer.DownloadURL); downloadURL != "" {
		return downloadOpenAIImageBytes(ctx, client, headers, profile, downloadURL, errorBodyReadLimit)
	}
	if strings.TrimSpace(pointer.Pointer) == "" {
		return nil, fmt.Errorf("image asset is missing pointer, url, and base64 data")
	}
	downloadURL, err := fetchOpenAIImageDownloadURL(ctx, client, headers, profile, conversationID, pointer.Pointer, errorBodyReadLimit)
	if err != nil {
		return nil, err
	}
	return downloadOpenAIImageBytes(ctx, client, headers, profile, downloadURL, errorBodyReadLimit)
}

func normalizeOpenAIImageBase64(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") {
		if idx := strings.Index(raw, ","); idx >= 0 && idx+1 < len(raw) {
			raw = raw[idx+1:]
		}
	}
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "=") + strings.Repeat("=", (4-len(raw)%4)%4)
	if raw == "" {
		return ""
	}
	if _, err := base64.StdEncoding.DecodeString(raw); err != nil {
		return ""
	}
	return raw
}

func collectOpenAIImageInlineAssets(body []byte, fallbackPrompt string) []openAIImagePointerInfo {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil
	}
	var out []openAIImagePointerInfo
	walkOpenAIImageInlineAssets(decoded, strings.TrimSpace(fallbackPrompt), &out)
	return out
}

func walkOpenAIImageInlineAssets(node any, prompt string, out *[]openAIImagePointerInfo) {
	switch value := node.(type) {
	case map[string]any:
		localPrompt := prompt
		for _, key := range []string{"revised_prompt", "image_gen_title", "prompt"} {
			if v, ok := value[key].(string); ok && strings.TrimSpace(v) != "" {
				localPrompt = strings.TrimSpace(v)
				break
			}
		}
		item := openAIImagePointerInfo{
			Prompt:      localPrompt,
			Pointer:     firstNonEmptyString(value["asset_pointer"], value["pointer"]),
			DownloadURL: firstNonEmptyString(value["download_url"], value["url"], value["image_url"]),
			B64JSON:     firstNonEmptyString(value["b64_json"], value["base64"], value["image_base64"]),
			MimeType:    firstNonEmptyString(value["mime_type"], value["mimeType"], value["content_type"]),
		}
		switch {
		case strings.HasPrefix(strings.TrimSpace(item.Pointer), "file-service://"),
			strings.HasPrefix(strings.TrimSpace(item.Pointer), "sediment://"),
			isLikelyOpenAIImageDownloadURL(item.DownloadURL),
			normalizeOpenAIImageBase64(item.B64JSON) != "":
			*out = append(*out, item)
		}
		for _, child := range value {
			walkOpenAIImageInlineAssets(child, localPrompt, out)
		}
	case []any:
		for _, child := range value {
			walkOpenAIImageInlineAssets(child, prompt, out)
		}
	}
}

func firstNonEmptyString(values ...any) string {
	for _, value := range values {
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func isLikelyOpenAIImageDownloadURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:image/") {
		return true
	}
	if !strings.HasPrefix(strings.ToLower(raw), "http://") && !strings.HasPrefix(strings.ToLower(raw), "https://") {
		return false
	}
	lower := strings.ToLower(raw)
	return strings.Contains(lower, "/download") ||
		strings.Contains(lower, ".png") ||
		strings.Contains(lower, ".jpg") ||
		strings.Contains(lower, ".jpeg") ||
		strings.Contains(lower, ".webp")
}

func fetchOpenAIImageDownloadURL(
	ctx context.Context,
	client *req.Client,
	headers http.Header,
	profile *OpenAIWebProfile,
	conversationID string,
	pointer string,
	errorBodyReadLimit int64,
) (string, error) {
	url := ""
	allowConversationRetry := false
	targetRoute := ""
	switch {
	case strings.HasPrefix(pointer, "file-service://"):
		fileID := strings.TrimPrefix(pointer, "file-service://")
		url = fmt.Sprintf("%s/%s/download", openAIChatGPTFilesURL, fileID)
		targetRoute = "/backend-api/files/{file_id}/download"
	case strings.HasPrefix(pointer, "sediment://"):
		attachmentID := strings.TrimPrefix(pointer, "sediment://")
		url = fmt.Sprintf("https://chatgpt.com/backend-api/conversation/%s/attachment/%s/download", conversationID, attachmentID)
		targetRoute = "/backend-api/conversation/{conversation_id}/attachment/{attachment_id}/download"
		allowConversationRetry = true
	default:
		return "", fmt.Errorf("unsupported image pointer: %s", pointer)
	}

	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		var result struct {
			DownloadURL string `json:"download_url"`
		}
		resp, err := client.R().
			SetContext(ctx).
			SetHeaders(headerToMap(func() http.Header {
				requestHeaders := cloneHTTPHeader(headers)
				setOpenAIBackendAPIRequestTarget(requestHeaders, url, targetRoute)
				setOpenAIBackendAPIRequestCookieHeader(requestHeaders, profile, url)
				return requestHeaders
			}())).
			SetSuccessResult(&result).
			Get(url)
		if err != nil {
			lastErr = err
		} else if resp.IsSuccessState() && strings.TrimSpace(result.DownloadURL) != "" {
			return strings.TrimSpace(result.DownloadURL), nil
		} else {
			statusErr := newOpenAIImageStatusError(resp, "fetch image download url failed", errorBodyReadLimit)
			if !allowConversationRetry || !isOpenAIImageTransientConversationNotFoundError(statusErr) {
				return "", statusErr
			}
			lastErr = statusErr
		}
		if attempt == 7 {
			break
		}
		timer := time.NewTimer(750 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("fetch image download url failed")
	}
	return "", lastErr
}

func downloadOpenAIImageBytes(ctx context.Context, client *req.Client, headers http.Header, profile *OpenAIWebProfile, downloadURL string, errorBodyReadLimit int64) ([]byte, error) {
	request := client.R().
		SetContext(ctx).
		DisableAutoReadResponse()

	if strings.HasPrefix(downloadURL, openAIChatGPTStartURL) {
		downloadHeaders := cloneHTTPHeader(headers)
		setOpenAIBackendAPIRequestCookieHeader(downloadHeaders, profile, downloadURL)
		downloadHeaders.Set("Accept", "image/*,*/*;q=0.8")
		downloadHeaders.Del("Content-Type")
		if strings.Contains(downloadURL, "/backend-api/conversation/") && strings.Contains(downloadURL, "/attachment/") {
			setOpenAIBackendAPIRequestTarget(downloadHeaders, downloadURL, "/backend-api/conversation/{conversation_id}/attachment/{attachment_id}/download")
		} else if strings.Contains(downloadURL, "/backend-api/files/") {
			setOpenAIBackendAPIRequestTarget(downloadHeaders, downloadURL, "/backend-api/files/{file_id}/download")
		}
		request.SetHeaders(headerToMap(downloadHeaders))
	} else {
		userAgent := strings.TrimSpace(headers.Get("User-Agent"))
		if userAgent == "" {
			userAgent = openAIImageBackendUserAgent
		}
		request.SetHeader("User-Agent", userAgent)
	}

	resp, err := request.Get(downloadURL)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newOpenAIImageStatusError(resp, "download image bytes failed", errorBodyReadLimit)
	}
	data, err := readAllWithLimitDetection(resp.Body, int64(openAIImageMaxDownloadBytes))
	if err != nil {
		return nil, fmt.Errorf("download image bytes failed: %w", err)
	}
	return data, nil
}

func downloadOpenAIImageReferenceBytes(ctx context.Context, client *req.Client, headers http.Header, profile *OpenAIWebProfile, imageURL string) ([]byte, error) {
	trimmed := strings.TrimSpace(imageURL)
	if trimmed == "" {
		return nil, fmt.Errorf("image url is required")
	}
	if strings.HasPrefix(trimmed, openAIChatGPTStartURL) {
		return downloadOpenAIImageBytes(ctx, client, headers, profile, trimmed, openAIUpstreamErrorBodyReadLimit)
	}

	requestHeaders := cloneHTTPHeader(headers)
	requestHeaders.Set("Accept", "image/*,*/*;q=0.8")
	requestHeaders.Del("Content-Type")
	userAgent := strings.TrimSpace(requestHeaders.Get("User-Agent"))
	if userAgent == "" {
		userAgent = openAIImageBackendUserAgent
	}
	requestHeaders.Set("User-Agent", userAgent)

	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(requestHeaders)).
		Get(trimmed)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newOpenAIImageStatusError(resp, "download image bytes failed", openAIUpstreamErrorBodyReadLimit)
	}
	data, err := readAllWithLimitDetection(resp.Body, int64(openAIImageMaxDownloadBytes))
	if err != nil {
		return nil, fmt.Errorf("download image bytes failed: %w", err)
	}
	return data, nil
}

func resolveOpenAILegacyBridgeInputImages(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, inputImages []OpenAIImagesInputRef) ([]openAIUploadedImage, error) {
	if len(inputImages) == 0 {
		return nil, nil
	}
	resolved := make([]openAIUploadedImage, 0, len(inputImages))
	for index, inputImage := range inputImages {
		if trimmed := strings.TrimSpace(inputImage.FileID); trimmed != "" {
			resolved = append(resolved, openAIUploadedImage{
				FileID:        trimmed,
				LibraryFileID: trimmed,
				FileName:      trimmed,
				MimeType:      "application/octet-stream",
				Source:        "library",
			})
			continue
		}
		if trimmed := strings.TrimSpace(inputImage.ImageURL); trimmed != "" {
			data, err := downloadOpenAIImageReferenceBytes(ctx, client, headers, profile, trimmed)
			if err != nil {
				return nil, fmt.Errorf("download legacy image input %d failed: %w", index+1, err)
			}
			uploaded, err := uploadOpenAIImageFiles(ctx, client, headers, account, profile, nil, "", "", []OpenAIImagesUpload{{
				FieldName:   "image",
				FileName:    fmt.Sprintf("legacy-image-%d.png", index+1),
				ContentType: http.DetectContentType(data),
				Data:        data,
			}})
			if err != nil {
				return nil, err
			}
			if len(uploaded) == 0 {
				return nil, fmt.Errorf("legacy image input %d upload returned no file id", index+1)
			}
			resolved = append(resolved, uploaded[0])
		}
	}
	return resolved, nil
}

type openAIImageStatusError struct {
	StatusCode      int
	Message         string
	ResponseBody    []byte
	ResponseHeaders http.Header
	RequestID       string
	URL             string
	Synthetic       bool
}

func (e *openAIImageStatusError) Error() string {
	if e == nil {
		return "openai image backend request failed"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("openai image backend request failed: status %d", e.StatusCode)
	}
	return "openai image backend request failed"
}

func newOpenAIImageStatusError(resp *req.Response, fallback string, errorBodyReadLimit int64) error {
	if resp == nil {
		if strings.TrimSpace(fallback) == "" {
			fallback = "openai image backend request failed"
		}
		return fmt.Errorf("%s", fallback)
	}

	statusCode := resp.StatusCode
	headers := http.Header(nil)
	requestID := ""
	requestURL := ""
	body := []byte(nil)

	if resp.Response != nil {
		headers = resp.Header.Clone()
		requestID = strings.TrimSpace(resp.Header.Get("x-request-id"))
		if resp.Request != nil && resp.Request.URL != nil {
			requestURL = resp.Request.URL.String()
		}
		if resp.Body != nil {
			if errorBodyReadLimit <= 0 {
				errorBodyReadLimit = openAIUpstreamErrorBodyReadLimit
			}
			body, _ = io.ReadAll(io.LimitReader(resp.Body, errorBodyReadLimit))
			_ = resp.Body.Close()
		}
	}

	message := sanitizeUpstreamErrorMessage(extractUpstreamErrorMessage(body))
	if message == "" {
		prefix := strings.TrimSpace(fallback)
		if prefix == "" {
			prefix = "openai image backend request failed"
		}
		message = fmt.Sprintf("%s: status %d", prefix, statusCode)
	}

	return &openAIImageStatusError{
		StatusCode:      statusCode,
		Message:         message,
		ResponseBody:    body,
		ResponseHeaders: headers,
		RequestID:       requestID,
		URL:             requestURL,
		Synthetic:       false,
	}
}

func isOpenAIImageTransientConversationNotFoundError(err error) bool {
	statusErr, ok := err.(*openAIImageStatusError)
	if !ok || statusErr == nil || statusErr.StatusCode != http.StatusNotFound {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(statusErr.Message))
	if strings.Contains(msg, "conversation_not_found") {
		return true
	}
	if strings.Contains(msg, "conversation") && strings.Contains(msg, "not found") {
		return true
	}
	bodyMsg := strings.ToLower(strings.TrimSpace(extractUpstreamErrorMessage(statusErr.ResponseBody)))
	if strings.Contains(bodyMsg, "conversation_not_found") {
		return true
	}
	return strings.Contains(bodyMsg, "conversation") && strings.Contains(bodyMsg, "not found")
}

func cloneHTTPHeader(src http.Header) http.Header {
	dst := make(http.Header, len(src))
	for key, values := range src {
		copied := make([]string, len(values))
		copy(copied, values)
		dst[key] = copied
	}
	return dst
}

func headerToMap(header http.Header) map[string]string {
	if len(header) == 0 {
		return nil
	}
	result := make(map[string]string, len(header))
	for key, values := range header {
		if len(values) == 0 {
			continue
		}
		result[key] = values[0]
	}
	return result
}

func resolveOpenAIProxyURL(account *Account) string {
	if account != nil && account.ProxyID != nil && account.Proxy != nil {
		return account.Proxy.URL()
	}
	return ""
}

func newOpenAIBackendAPIClient(proxyURL string) (*req.Client, error) {
	client := req.C().
		SetTimeout(180 * time.Second).
		ImpersonateChrome()
	trimmed, _, err := proxyurl.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	if trimmed != "" {
		client.SetProxyURL(trimmed)
	}
	return client, nil
}

func (s *OpenAIGatewayService) buildOpenAIBackendAPIHeaders(account *Account, token string) (http.Header, error) {
	deviceID, sessionID := s.ensureOpenAIImageSessionCredentials(context.Background(), account)
	profile := ResolveOpenAIImageWebProfile(account)
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+token)
	headers.Set("Accept", "application/json")
	headers.Set("Accept-Language", "en-US,en;q=0.9")
	headers.Set("Origin", "https://chatgpt.com")
	headers.Set("Referer", "https://chatgpt.com/")
	headers.Set("Sec-Fetch-Dest", "empty")
	headers.Set("Sec-Fetch-Mode", "cors")
	headers.Set("Sec-Fetch-Site", "same-origin")
	headers.Set("User-Agent", openAIImageBackendUserAgent)
	if customUA := strings.TrimSpace(account.GetOpenAIUserAgent()); customUA != "" {
		headers.Set("User-Agent", customUA)
	}
	ApplyOpenAIWebProfileHeaders(headers, profile)
	if chatgptAccountID := strings.TrimSpace(account.GetChatGPTAccountID()); chatgptAccountID != "" {
		headers.Set("ChatGPT-Account-ID", chatgptAccountID)
	}
	if profile != nil && strings.TrimSpace(profile.OAIDeviceID) != "" {
		deviceID = strings.TrimSpace(profile.OAIDeviceID)
	}
	if profile != nil && strings.TrimSpace(profile.OAISessionID) != "" {
		sessionID = strings.TrimSpace(profile.OAISessionID)
	}
	if deviceID != "" {
		headers.Set("OAI-Device-Id", deviceID)
	}
	if sessionID != "" {
		headers.Set("OAI-Session-Id", sessionID)
	}
	cookieHeader := ""
	if profile != nil {
		cookieHeader = profile.CookieHeaderForHost(openAIChatGPTConversationURL)
	}
	if deviceID != "" {
		cookieHeader = mergeOpenAIImageCookieHeader(cookieHeader, "oai-did", deviceID)
	}
	if cookieHeader != "" {
		headers.Set("Cookie", cookieHeader)
	}
	return headers, nil
}

func setOpenAIBackendAPIRequestTarget(headers http.Header, targetPath string, targetRoute string) {
	if headers == nil {
		return
	}
	if trimmed := normalizeOpenAIBackendAPITargetPath(targetPath); trimmed != "" {
		headers.Set("X-OpenAI-Target-Path", trimmed)
	}
	if trimmed := strings.TrimSpace(targetRoute); trimmed != "" {
		headers.Set("X-OpenAI-Target-Route", trimmed)
	}
}

func normalizeOpenAIBackendAPITargetPath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := neturl.Parse(trimmed)
	if err != nil || parsed == nil {
		return trimmed
	}
	if parsed.Scheme == "" && parsed.Host == "" {
		return trimmed
	}
	if path := strings.TrimSpace(parsed.Path); path != "" {
		return path
	}
	return "/"
}

func setOpenAIBackendAPIRequestCookieHeader(headers http.Header, profile *OpenAIWebProfile, requestURL string) {
	if headers == nil || profile == nil {
		return
	}
	cookieHeader := profile.CookieHeaderForHost(requestURL)
	if deviceID := strings.TrimSpace(profile.OAIDeviceID); deviceID != "" {
		cookieHeader = mergeOpenAIImageCookieHeader(cookieHeader, "oai-did", deviceID)
	}
	if cookieHeader != "" {
		headers.Set("Cookie", cookieHeader)
		return
	}
	headers.Del("Cookie")
}

func openAIBackendAPIResponseRequestURL(resp *http.Response) string {
	if resp != nil && resp.Request != nil && resp.Request.URL != nil {
		return resp.Request.URL.String()
	}
	return openAIChatGPTConversationURL
}

func (s *OpenAIGatewayService) applyOpenAIBackendAPIResponseState(ctx context.Context, account *Account, profile *OpenAIWebProfile, headers http.Header, resp *http.Response) {
	if profile == nil || resp == nil {
		return
	}
	merged, changed := MergeOpenAIWebProfileResponseState(profile, resp)
	if merged == nil {
		return
	}
	*profile = *merged
	if headers != nil {
		headers.Set("X-OAI-IS", profile.XOAIIS)
		headers.Set("OAI-Client-Version", profile.OAIClientVersion)
		headers.Set("OAI-Client-Build-Number", profile.OAIClientBuildNumber)
		setOpenAIBackendAPIRequestCookieHeader(headers, profile, openAIBackendAPIResponseRequestURL(resp))
	}
	if !changed {
		return
	}
	if profile.Version == "" {
		profile.Version = "1"
	}
	if profile.Source == "" {
		profile.Source = "sub2api-images2api"
	}
	profile.CapturedAt = time.Now().UTC().Format(time.RFC3339)
	if account != nil && account.Extra != nil {
		account.Extra[openAIWebProfileExtraKey] = profile.ToExtraMap()
	}
	if s == nil || s.accountRepo == nil || account == nil || account.ID == 0 {
		return
	}
	updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(updateCtx, account.ID, map[string]any{openAIWebProfileExtraKey: profile.ToExtraMap()}); err != nil {
		logger.LegacyPrintf("service.openai_gateway", "persist openai web profile response state failed: account=%d err=%v", account.ID, err)
	}
}

func (s *OpenAIGatewayService) openAIOAuthImageBridgeUpstreamOptions(ctx context.Context) HTTPUpstreamRequestOptions {
	settings := s.openAIOAuthImageBridgeTransportSettings(ctx)
	return HTTPUpstreamRequestOptions{
		FreshClient:       settings.freshClient,
		DisableKeepAlives: settings.disableKeepAlives,
	}
}

func (s *OpenAIGatewayService) applyOpenAIOAuthImageBridgeUpstreamOptions(req *http.Request) *http.Request {
	if req == nil {
		return nil
	}
	opts := s.openAIOAuthImageBridgeUpstreamOptions(req.Context())
	if !opts.HasOverrides() {
		return req
	}
	return req.WithContext(WithHTTPUpstreamRequestOptions(req.Context(), opts))
}

func mergeOpenAIImageCookieHeader(cookieHeader string, name string, value string) string {
	name = strings.TrimSpace(name)
	value = strings.TrimSpace(value)
	if name == "" || value == "" {
		return strings.TrimSpace(cookieHeader)
	}
	parts := strings.Split(cookieHeader, ";")
	merged := make([]string, 0, len(parts)+1)
	found := false
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		cookieName := trimmed
		if idx := strings.Index(trimmed, "="); idx >= 0 {
			cookieName = strings.TrimSpace(trimmed[:idx])
		}
		if cookieName == name {
			merged = append(merged, name+"="+value)
			found = true
			continue
		}
		merged = append(merged, trimmed)
	}
	if !found {
		merged = append(merged, name+"="+value)
	}
	return strings.Join(merged, "; ")
}

func (s *OpenAIGatewayService) recordOpenAIImagesLegacyBridgeTelemetry(c *gin.Context, account *Account, profile *OpenAIWebProfile, proxyURL string) {
	if c == nil {
		return
	}
	profileTime := parseOpenAIWebProfileCapturedAt(profile)
	cookieNames := openAIWebProfileCookieNames(profile)
	proxyID := ""
	proxyMatch := profile == nil
	if account != nil && account.ProxyID != nil {
		proxyID = strconv.FormatInt(*account.ProxyID, 10)
		if profile != nil && profile.ProxyID > 0 {
			proxyMatch = profile.ProxyID == *account.ProxyID
		}
	} else if profile != nil && profile.ProxyID == 0 {
		proxyMatch = true
	}
	SetOpenAIImagesTelemetryProfile(c, NewOpenAIImagesTelemetryProfileState(OpenAIImagesTelemetryProfileInput{
		HasWebProfile:   profile != nil,
		ProfileSource:   openAIWebProfileSource(profile),
		ProfileTime:     profileTime,
		CookieNames:     cookieNames,
		ProxyMatch:      proxyMatch,
		UserAgent:       openAIWebProfileUserAgent(profile),
		SecCHUAPresent:  profile != nil && strings.TrimSpace(profile.SecCHUA) != "",
		CookieJarExists: profile != nil && len(profile.Cookies) > 0,
	}))
	SetOpenAIImagesTelemetryNetwork(c, NewOpenAIImagesTelemetryNetworkState(
		proxyID,
		proxyURL,
		"req_impersonate",
		openAIWebProfileImpersonate(profile),
		"",
		openAITLSProfileIDForTelemetry(account),
		"not_applied",
	))
}

func openAIImagesChallengeTelemetryState(reqs *openAIChatRequirements, unsupported string) OpenAIImagesTelemetryChallengeState {
	return openAIImagesChallengeTelemetryStateWithProof(reqs, "", unsupported)
}

func openAIImagesChallengeTelemetryStateWithProof(reqs *openAIChatRequirements, proofToken string, unsupported string) OpenAIImagesTelemetryChallengeState {
	if reqs == nil {
		return OpenAIImagesTelemetryChallengeState{UnsupportedChallenge: strings.TrimSpace(unsupported)}
	}
	return OpenAIImagesTelemetryChallengeState{
		ArkoseRequired:           reqs.Arkose.Required,
		TurnstileRequired:        reqs.Turnstile.Required,
		PoWRequired:              reqs.ProofOfWork.Required,
		RequirementsTokenPresent: strings.TrimSpace(reqs.Token) != "",
		ProofTokenPresent:        strings.TrimSpace(proofToken) != "",
		UnsupportedChallenge:     strings.TrimSpace(unsupported),
	}
}

func parseOpenAIWebProfileCapturedAt(profile *OpenAIWebProfile) time.Time {
	if profile == nil || strings.TrimSpace(profile.CapturedAt) == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(profile.CapturedAt))
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func openAIWebProfileCookieNames(profile *OpenAIWebProfile) []string {
	if profile == nil || len(profile.Cookies) == 0 {
		return nil
	}
	names := make([]string, 0, len(profile.Cookies))
	for _, cookie := range profile.Cookies {
		if name := strings.TrimSpace(cookie.Name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func openAIWebProfileSource(profile *OpenAIWebProfile) string {
	if profile == nil {
		return ""
	}
	return strings.TrimSpace(profile.Source)
}

func openAIWebProfileUserAgent(profile *OpenAIWebProfile) string {
	if profile == nil {
		return openAIImageBackendUserAgent
	}
	if ua := strings.TrimSpace(profile.UserAgent); ua != "" {
		return ua
	}
	return openAIImageBackendUserAgent
}

func openAIWebProfileImpersonate(profile *OpenAIWebProfile) string {
	if profile == nil || strings.TrimSpace(profile.Impersonate) == "" {
		return "chrome"
	}
	return strings.TrimSpace(profile.Impersonate)
}

func openAITLSProfileIDForTelemetry(account *Account) string {
	if account == nil {
		return ""
	}
	id := account.GetTLSFingerprintProfileID()
	if id == 0 {
		return ""
	}
	return strconv.FormatInt(id, 10)
}

func (s *OpenAIGatewayService) ensureOpenAIImageSessionCredentials(ctx context.Context, account *Account) (string, string) {
	if account == nil {
		return "", ""
	}
	deviceID := account.GetOpenAIDeviceID()
	sessionID := account.GetOpenAISessionID()
	if deviceID != "" && sessionID != "" {
		return deviceID, sessionID
	}

	updates := map[string]any{}
	if deviceID == "" {
		deviceID = uuid.NewString()
		updates["openai_device_id"] = deviceID
	}
	if sessionID == "" {
		sessionID = uuid.NewString()
		updates["openai_session_id"] = sessionID
	}
	if account.Extra == nil {
		account.Extra = map[string]any{}
	}
	for key, value := range updates {
		account.Extra[key] = value
	}
	if len(updates) == 0 || s == nil || s.accountRepo == nil {
		return deviceID, sessionID
	}

	updateCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(updateCtx, account.ID, updates); err != nil {
		logger.LegacyPrintf("service.openai_gateway", "persist openai image session creds failed: account=%d err=%v", account.ID, err)
	}
	return deviceID, sessionID
}

func bootstrapOpenAIBackendAPI(ctx context.Context, client *req.Client, headers http.Header) error {
	requestHeaders := cloneHTTPHeader(headers)
	setOpenAIBackendAPIRequestTarget(requestHeaders, openAIChatGPTSentinelPingURL, "/backend-api/sentinel/ping")
	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(requestHeaders)).
		Post(openAIChatGPTSentinelPingURL)
	if err != nil {
		return err
	}
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	return nil
}

func initializeOpenAIImageConversation(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, service *OpenAIGatewayService) error {
	payload := map[string]any{
		"gizmo_id":                nil,
		"requested_default_model": nil,
		"conversation_id":         nil,
		"timezone_offset_min":     openAITimezoneOffsetMinutes(),
		"system_hints":            []string{"picture_v2"},
	}
	requestHeaders := cloneHTTPHeader(headers)
	setOpenAIBackendAPIRequestTarget(requestHeaders, openAIChatGPTConversationInitURL, "/backend-api/conversation/init")
	setOpenAIBackendAPIRequestCookieHeader(requestHeaders, profile, openAIChatGPTConversationInitURL)
	requestHeaders.Set("Content-Type", "application/json")
	resp, err := client.R().
		SetContext(ctx).
		SetHeaders(headerToMap(requestHeaders)).
		SetBodyJsonMarshal(payload).
		Post(openAIChatGPTConversationInitURL)
	if err != nil {
		return err
	}
	if !resp.IsSuccessState() {
		return newOpenAIImageStatusError(resp, "conversation init failed", openAIUpstreamErrorBodyReadLimit)
	}
	if service != nil {
		service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	}
	return nil
}

type openAIChatRequirements struct {
	Persona      string `json:"persona"`
	PrepareToken string `json:"prepare_token"`
	Token        string `json:"token"`
	Turnstile    struct {
		Required bool `json:"required"`
	} `json:"turnstile"`
	Arkose struct {
		Required bool `json:"required"`
	} `json:"arkose"`
	ProofOfWork struct {
		Required   bool   `json:"required"`
		Seed       string `json:"seed"`
		Difficulty string `json:"difficulty"`
	} `json:"proofofwork"`
}

func fetchOpenAIChatRequirements(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, service *OpenAIGatewayService) (*openAIChatRequirements, error) {
	preparePayload := map[string]any{
		"p": generateOpenAIRequirementsToken(headers.Get("User-Agent")),
	}
	var prepareResult openAIChatRequirements
	prepareHeaders := cloneHTTPHeader(headers)
	setOpenAIBackendAPIRequestTarget(prepareHeaders, openAIChatGPTChatRequirementsPrepareURL, "/backend-api/sentinel/chat-requirements/prepare")
	setOpenAIBackendAPIRequestCookieHeader(prepareHeaders, profile, openAIChatGPTChatRequirementsPrepareURL)
	prepareHeaders.Set("Content-Type", "application/json")
	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(prepareHeaders)).
		SetBodyJsonMarshal(preparePayload).
		Post(openAIChatGPTChatRequirementsPrepareURL)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		_ = json.Unmarshal(body, &prepareResult)
	}
	if !resp.IsSuccessState() {
		return nil, newOpenAIImageStatusError(resp, "chat-requirements prepare failed", openAIUpstreamErrorBodyReadLimit)
	}
	if service != nil {
		service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	}
	if strings.TrimSpace(prepareResult.PrepareToken) == "" {
		return nil, fmt.Errorf("chat-requirements prepare token missing")
	}

	proofToken := generateOpenAIProofToken(
		prepareResult.ProofOfWork.Required,
		prepareResult.ProofOfWork.Seed,
		prepareResult.ProofOfWork.Difficulty,
		headers.Get("User-Agent"),
	)
	finalizePayload := map[string]any{
		"prepare_token": strings.TrimSpace(prepareResult.PrepareToken),
	}
	if trimmed := strings.TrimSpace(prepareResult.Token); trimmed != "" {
		finalizePayload["turnstile"] = trimmed
	}
	if trimmed := strings.TrimSpace(prepareResult.Persona); trimmed != "" {
		finalizePayload["persona"] = trimmed
	}
	if trimmed := strings.TrimSpace(proofToken); trimmed != "" {
		finalizePayload["proofofwork"] = trimmed
	}
	var finalResult openAIChatRequirements
	finalizeHeaders := cloneHTTPHeader(headers)
	setOpenAIBackendAPIRequestTarget(finalizeHeaders, openAIChatGPTChatRequirementsFinalizeURL, "/backend-api/sentinel/chat-requirements/finalize")
	setOpenAIBackendAPIRequestCookieHeader(finalizeHeaders, profile, openAIChatGPTChatRequirementsFinalizeURL)
	finalizeHeaders.Set("Content-Type", "application/json")
	resp, err = client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(finalizeHeaders)).
		SetBodyJsonMarshal(finalizePayload).
		SetSuccessResult(&finalResult).
		Post(openAIChatGPTChatRequirementsFinalizeURL)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if !resp.IsSuccessState() {
		return nil, newOpenAIImageStatusError(resp, "chat-requirements finalize failed", openAIUpstreamErrorBodyReadLimit)
	}
	if service != nil {
		service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	}
	if strings.TrimSpace(finalResult.Token) == "" {
		return nil, fmt.Errorf("chat-requirements token missing")
	}
	finalResult.Persona = strings.TrimSpace(prepareResult.Persona)
	finalResult.PrepareToken = strings.TrimSpace(prepareResult.PrepareToken)
	finalResult.Turnstile = prepareResult.Turnstile
	finalResult.Arkose = prepareResult.Arkose
	finalResult.ProofOfWork = prepareResult.ProofOfWork
	return &finalResult, nil
}

func prepareOpenAIImageConversation(
	ctx context.Context,
	client *req.Client,
	headers http.Header,
	account *Account,
	profile *OpenAIWebProfile,
	service *OpenAIGatewayService,
	prompt string,
	parentMessageID string,
	chatToken string,
	proofToken string,
) (string, error) {
	messageID := uuid.NewString()
	payload := map[string]any{
		"action":                "next",
		"client_prepare_state":  "success",
		"fork_from_shared_post": false,
		"parent_message_id":     parentMessageID,
		"model":                 openAIChatGPTConversationModelAuto,
		"timezone_offset_min":   openAITimezoneOffsetMinutes(),
		"timezone":              openAITimezoneName(),
		"conversation_mode":     map[string]any{"kind": "primary_assistant"},
		"system_hints":          []string{"picture_v2"},
		"supports_buffering":    true,
		"supported_encodings":   []string{"v1"},
		"partial_query": map[string]any{
			"id":     messageID,
			"author": map[string]any{"role": "user"},
			"content": map[string]any{
				"content_type": "text",
				"parts":        []string{coalesceOpenAIFileName(prompt, "Generate an image.")},
			},
		},
		"client_contextual_info": map[string]any{
			"app_name": "chatgpt.com",
		},
	}
	prepareHeaders := buildOpenAIImageConversationPrepareHeaders(headers)
	setOpenAIBackendAPIRequestCookieHeader(prepareHeaders, profile, openAIChatGPTConversationPrepareURL)
	var result struct {
		ConduitToken string `json:"conduit_token"`
	}
	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(prepareHeaders)).
		SetBodyJsonMarshal(payload).
		SetSuccessResult(&result).
		Post(openAIChatGPTConversationPrepareURL)
	if err != nil {
		return "", err
	}
	if !resp.IsSuccessState() {
		return "", newOpenAIImageStatusError(resp, "conversation prepare failed", openAIUpstreamErrorBodyReadLimit)
	}
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if service != nil {
		service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	}
	return strings.TrimSpace(result.ConduitToken), nil
}

type openAIUploadedImage struct {
	FileID        string
	LibraryFileID string
	FileName      string
	FileSize      int
	MimeType      string
	Width         int
	Height        int
	Source        string
	IsBigPaste    bool
}

func uploadOpenAIImageFiles(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, service *OpenAIGatewayService, parentMessageID string, messageID string, uploads []OpenAIImagesUpload) ([]openAIUploadedImage, error) {
	if len(uploads) == 0 {
		return nil, nil
	}
	results := make([]openAIUploadedImage, 0, len(uploads))
	for i := range uploads {
		item := uploads[i]
		fileName := coalesceOpenAIFileName(item.FileName, "image.png")
		payload := map[string]any{
			"file_name":                fileName,
			"file_size":                len(item.Data),
			"use_case":                 "multimodal",
			"timezone_offset_min":      openAITimezoneOffsetMinutes(),
			"reset_rate_limits":        false,
			"store_in_library":         true,
			"library_persistence_mode": "opportunistic",
		}
		createHeaders := cloneHTTPHeader(headers)
		setOpenAIBackendAPIRequestTarget(createHeaders, openAIChatGPTFilesURL, "/backend-api/files")
		setOpenAIBackendAPIRequestCookieHeader(createHeaders, profile, openAIChatGPTFilesURL)
		createHeaders.Set("Content-Type", "application/json")
		createResp, err := client.R().
			SetContext(ctx).
			DisableAutoReadResponse().
			SetHeaders(headerToMap(createHeaders)).
			SetBodyJsonMarshal(payload).
			Post(openAIChatGPTFilesURL)
		if err != nil {
			return nil, err
		}
		if createResp != nil && createResp.Body != nil {
			body, readErr := io.ReadAll(createResp.Body)
			_ = createResp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			if service != nil {
				service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, createResp.Response)
			}
			if !createResp.IsSuccessState() {
				return nil, newOpenAIImageStatusError(createResp, "create upload slot failed", openAIUpstreamErrorBodyReadLimit)
			}
			var created struct {
				Status    string `json:"status"`
				FileID    string `json:"file_id"`
				UploadURL string `json:"upload_url"`
			}
			if err := json.Unmarshal(body, &created); err != nil {
				return nil, fmt.Errorf("parse upload slot response failed: %w", err)
			}
			if strings.TrimSpace(created.FileID) == "" || strings.TrimSpace(created.UploadURL) == "" {
				return nil, fmt.Errorf("create upload slot response missing file_id or upload_url")
			}
			uploadHeaders := map[string]string{
				"Content-Type":   coalesceOpenAIFileName(item.ContentType, "application/octet-stream"),
				"Origin":         "https://chatgpt.com",
				"x-ms-blob-type": "BlockBlob",
				"x-ms-version":   "2020-04-08",
				"User-Agent":     headers.Get("User-Agent"),
			}
			putResp, err := client.R().
				SetContext(ctx).
				SetHeaders(uploadHeaders).
				SetBody(item.Data).
				DisableAutoReadResponse().
				Put(created.UploadURL)
			if err != nil {
				return nil, err
			}
			if putResp.Response != nil && putResp.Body != nil {
				_, _ = io.Copy(io.Discard, putResp.Body)
				_ = putResp.Body.Close()
			}
			if putResp.StatusCode < 200 || putResp.StatusCode >= 300 {
				return nil, newOpenAIImageStatusError(putResp, "upload image bytes failed", openAIUpstreamErrorBodyReadLimit)
			}

			processPayload := map[string]any{
				"file_id":                  created.FileID,
				"use_case":                 "multimodal",
				"index_for_retrieval":      false,
				"file_name":                fileName,
				"library_persistence_mode": "opportunistic",
				"metadata": map[string]any{
					"store_in_library": true,
					"library_file_info": map[string]any{
						"origination_message_id": messageID,
						"origination_thread_id":  parentMessageID,
					},
				},
				"entry_surface": "chat_composer",
			}
			processHeaders := cloneHTTPHeader(headers)
			setOpenAIBackendAPIRequestTarget(processHeaders, openAIChatGPTFilesProcessUploadURL, "/backend-api/files/process_upload_stream")
			setOpenAIBackendAPIRequestCookieHeader(processHeaders, profile, openAIChatGPTFilesProcessUploadURL)
			processHeaders.Set("Content-Type", "application/json")
			processResp, err := client.R().
				SetContext(ctx).
				DisableAutoReadResponse().
				SetHeaders(headerToMap(processHeaders)).
				SetBodyJsonMarshal(processPayload).
				Post(openAIChatGPTFilesProcessUploadURL)
			if err != nil {
				return nil, err
			}
			if processResp != nil && processResp.Body != nil {
				body, readErr := io.ReadAll(processResp.Body)
				_ = processResp.Body.Close()
				if readErr != nil {
					return nil, readErr
				}
				if service != nil {
					service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, processResp.Response)
				}
				if !processResp.IsSuccessState() {
					return nil, newOpenAIImageStatusError(processResp, "process upload stream failed", openAIUpstreamErrorBodyReadLimit)
				}
				libraryFileID, err := parseOpenAIProcessedUploadLibraryFileID(body)
				if err != nil {
					return nil, err
				}
				results = append(results, openAIUploadedImage{
					FileID:        created.FileID,
					LibraryFileID: libraryFileID,
					FileName:      fileName,
					FileSize:      len(item.Data),
					MimeType:      coalesceOpenAIFileName(item.ContentType, "application/octet-stream"),
					Width:         item.Width,
					Height:        item.Height,
					Source:        "library",
					IsBigPaste:    false,
				})
				continue
			}
			return nil, fmt.Errorf("process upload stream response missing body")
		}
	}
	return results, nil
}

func uploadOpenAIImageMaskFile(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, service *OpenAIGatewayService, mask OpenAIImagesUpload) (string, error) {
	if len(mask.Data) == 0 {
		return "", nil
	}
	fileName := coalesceOpenAIFileName(mask.FileName, "mask.png")
	createPayload := map[string]any{
		"file_name":           fileName,
		"file_size":           len(mask.Data),
		"use_case":            "dalle_agent",
		"timezone_offset_min": openAITimezoneOffsetMinutes(),
		"reset_rate_limits":   false,
	}
	createHeaders := cloneHTTPHeader(headers)
	setOpenAIBackendAPIRequestTarget(createHeaders, openAIChatGPTFilesURL, "/backend-api/files")
	setOpenAIBackendAPIRequestCookieHeader(createHeaders, profile, openAIChatGPTFilesURL)
	createHeaders.Set("Content-Type", "application/json")
	createResp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(createHeaders)).
		SetBodyJsonMarshal(createPayload).
		Post(openAIChatGPTFilesURL)
	if err != nil {
		return "", err
	}
	if createResp != nil && createResp.Body != nil {
		body, readErr := io.ReadAll(createResp.Body)
		_ = createResp.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if service != nil {
			service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, createResp.Response)
		}
		if !createResp.IsSuccessState() {
			return "", newOpenAIImageStatusError(createResp, "create mask upload slot failed", openAIUpstreamErrorBodyReadLimit)
		}
		var created struct {
			FileID    string `json:"file_id"`
			UploadURL string `json:"upload_url"`
		}
		if err := json.Unmarshal(body, &created); err != nil {
			return "", fmt.Errorf("parse mask upload slot response failed: %w", err)
		}
		if strings.TrimSpace(created.FileID) == "" || strings.TrimSpace(created.UploadURL) == "" {
			return "", fmt.Errorf("create mask upload slot response missing file_id or upload_url")
		}
		uploadHeaders := map[string]string{
			"Content-Type":   coalesceOpenAIFileName(mask.ContentType, "application/octet-stream"),
			"Origin":         "https://chatgpt.com",
			"x-ms-blob-type": "BlockBlob",
			"x-ms-version":   "2020-04-08",
			"User-Agent":     headers.Get("User-Agent"),
		}
		putResp, err := client.R().
			SetContext(ctx).
			SetHeaders(uploadHeaders).
			SetBody(mask.Data).
			DisableAutoReadResponse().
			Put(created.UploadURL)
		if err != nil {
			return "", err
		}
		if putResp.Response != nil && putResp.Body != nil {
			_, _ = io.Copy(io.Discard, putResp.Body)
			_ = putResp.Body.Close()
		}
		if putResp.StatusCode < 200 || putResp.StatusCode >= 300 {
			return "", newOpenAIImageStatusError(putResp, "upload mask bytes failed", openAIUpstreamErrorBodyReadLimit)
		}
		processPayload := map[string]any{
			"file_id":             created.FileID,
			"use_case":            "dalle_agent",
			"index_for_retrieval": false,
			"file_name":           fileName,
		}
		processHeaders := cloneHTTPHeader(headers)
		setOpenAIBackendAPIRequestTarget(processHeaders, openAIChatGPTFilesProcessUploadURL, "/backend-api/files/process_upload_stream")
		setOpenAIBackendAPIRequestCookieHeader(processHeaders, profile, openAIChatGPTFilesProcessUploadURL)
		processHeaders.Set("Content-Type", "application/json")
		processResp, err := client.R().
			SetContext(ctx).
			DisableAutoReadResponse().
			SetHeaders(headerToMap(processHeaders)).
			SetBodyJsonMarshal(processPayload).
			Post(openAIChatGPTFilesProcessUploadURL)
		if err != nil {
			return "", err
		}
		if processResp != nil && processResp.Body != nil {
			_, _ = io.Copy(io.Discard, processResp.Body)
			_ = processResp.Body.Close()
		}
		if service != nil {
			service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, processResp.Response)
		}
		if !processResp.IsSuccessState() {
			return "", newOpenAIImageStatusError(processResp, "process mask upload failed", openAIUpstreamErrorBodyReadLimit)
		}
		return created.FileID, nil
	}
	return "", fmt.Errorf("process mask upload response missing body")
}

func parseOpenAIProcessedUploadLibraryFileID(body []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	var libraryFileID string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if !gjson.ValidBytes([]byte(line)) {
			continue
		}
		event := strings.TrimSpace(gjson.Get(line, "event").String())
		switch event {
		case "file.indexing.completed":
			if candidate := strings.TrimSpace(gjson.Get(line, "extra.metadata_object_id").String()); candidate != "" {
				libraryFileID = candidate
			}
		case "file.processing.completed":
			// keep scanning to capture indexing metadata if it appears later
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if libraryFileID == "" {
		return "", fmt.Errorf("process upload stream did not return a library file id")
	}
	return libraryFileID, nil
}

func coalesceOpenAIFileName(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func buildOpenAIImageConversationInpaintingOperation(parsed *OpenAIImagesRequest, maskFileID string) map[string]any {
	if parsed == nil {
		return nil
	}
	originalFileID := parsed.LegacyOriginalFileID()
	maskFileID = strings.TrimSpace(firstNonEmpty(maskFileID, parsed.LegacyMaskFileID()))
	originalGenID := strings.TrimSpace(parsed.OriginalGenID)
	if originalFileID == "" || maskFileID == "" || originalGenID == "" {
		return nil
	}
	return map[string]any{
		"type":             "inpainting",
		"original_file_id": originalFileID,
		"mask_file_id":     maskFileID,
		"original_gen_id":  originalGenID,
	}
}

func buildOpenAIConversationAsyncStatusRequestTarget(conversationID string) (url string, targetPath string, targetRoute string, referer string) {
	conversationID = strings.TrimSpace(conversationID)
	url = fmt.Sprintf(openAIChatGPTConversationAsyncURL, conversationID)
	targetPath = "/backend-api/conversation/" + conversationID + "/async-status"
	targetRoute = "/backend-api/conversation/{conversation_id}/async-status"
	referer = "https://chatgpt.com/c/" + conversationID
	return url, targetPath, targetRoute, referer
}

func applyOpenAIConversationPageReferer(headers http.Header, conversationID string) http.Header {
	updated := cloneHTTPHeader(headers)
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return updated
	}
	updated.Set("Referer", "https://chatgpt.com/c/"+conversationID)
	return updated
}

func resolveOpenAIConversationParentMessageIDFromBody(body []byte) string {
	currentNode := strings.TrimSpace(gjson.GetBytes(body, "current_node").String())
	if currentNode != "" {
		return currentNode
	}
	mapping := gjson.GetBytes(body, "mapping")
	if !mapping.Exists() || !mapping.IsObject() {
		return ""
	}
	bestID := ""
	bestTime := float64(-1)
	for key, value := range mapping.Map() {
		node := value.Get("message")
		if !node.Exists() {
			continue
		}
		createTime := node.Get("create_time").Float()
		if createTime >= bestTime {
			bestTime = createTime
			bestID = strings.TrimSpace(firstNonEmpty(node.Get("id").String(), key))
		}
	}
	return bestID
}

func fetchOpenAIConversationParentMessageID(
	ctx context.Context,
	client *req.Client,
	headers http.Header,
	account *Account,
	profile *OpenAIWebProfile,
	service *OpenAIGatewayService,
	conversationID string,
) (string, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return "", nil
	}
	conversationURL := "https://chatgpt.com/backend-api/conversation/" + conversationID
	requestHeaders := applyOpenAIConversationPageReferer(headers, conversationID)
	setOpenAIBackendAPIRequestTarget(requestHeaders, conversationURL, "/backend-api/conversation/{conversation_id}")
	setOpenAIBackendAPIRequestCookieHeader(requestHeaders, profile, conversationURL)
	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(requestHeaders)).
		Get(conversationURL)
	if err != nil {
		return "", err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	if service != nil {
		service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	}
	if !resp.IsSuccessState() {
		return "", newOpenAIImageStatusError(resp, "fetch conversation state failed", openAIUpstreamErrorBodyReadLimit)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	parentMessageID := resolveOpenAIConversationParentMessageIDFromBody(body)
	if parentMessageID == "" {
		return "", fmt.Errorf("conversation state missing current parent message id")
	}
	return parentMessageID, nil
}

func buildOpenAIImageConversationPrepareHeaders(headers http.Header) http.Header {
	prepareHeaders := cloneHTTPHeader(headers)
	prepareHeaders.Set("Accept", "*/*")
	prepareHeaders.Set("Content-Type", "application/json")
	setOpenAIBackendAPIRequestTarget(prepareHeaders, openAIChatGPTConversationPrepareURL, "/backend-api/f/conversation/prepare")
	prepareHeaders.Del("OpenAI-Sentinel-Chat-Requirements-Token")
	prepareHeaders.Del("OpenAI-Sentinel-Proof-Token")
	prepareHeaders.Set("x-conduit-token", "no-token")
	return prepareHeaders
}

func resolveOpenAIImageConversationModel(ctx context.Context, settingService *SettingService, account *Account) string {
	settings := DefaultOpenAIImageWebConversationSettings()
	if account == nil {
		return openAIChatGPTConversationModelAuto
	}
	switch strings.ToLower(strings.TrimSpace(account.GetCredential("plan_type"))) {
	case "free":
		return settings.FreeModel
	case "plus", "pro", "team":
		return settings.PaidModel
	default:
		return settings.PaidModel
	}
}

func buildOpenAIImageConversationRequest(conversationModel string, parsed *OpenAIImagesRequest, parentMessageID string, messageID string, uploads []openAIUploadedImage, maskFileID string) map[string]any {
	parts := []any{coalesceOpenAIFileName(parsed.Prompt, "Generate an image.")}
	attachments := make([]map[string]any, 0, len(uploads))
	inpaintingOperation := buildOpenAIImageConversationInpaintingOperation(parsed, maskFileID)
	if len(uploads) > 0 && inpaintingOperation == nil {
		parts = make([]any, 0, len(uploads)+1)
		for _, upload := range uploads {
			parts = append(parts, map[string]any{
				"content_type":  "image_asset_pointer",
				"asset_pointer": "sediment://" + upload.FileID,
				"size_bytes":    upload.FileSize,
				"width":         upload.Width,
				"height":        upload.Height,
			})
			attachment := map[string]any{
				"id":              upload.FileID,
				"mime_type":       upload.MimeType,
				"name":            upload.FileName,
				"size":            upload.FileSize,
				"source":          upload.Source,
				"library_file_id": upload.LibraryFileID,
				"is_big_paste":    upload.IsBigPaste,
			}
			if upload.Width > 0 {
				attachment["width"] = upload.Width
			}
			if upload.Height > 0 {
				attachment["height"] = upload.Height
			}
			attachments = append(attachments, attachment)
		}
		parts = append(parts, coalesceOpenAIFileName(parsed.Prompt, "Edit this image."))
	}

	contentType := "text"
	if len(uploads) > 0 {
		contentType = "multimodal_text"
	}
	metadata := map[string]any{
		"developer_mode_connector_ids": []any{},
		"selected_sources":             []any{},
		"selected_github_repos":        []any{},
		"selected_all_github_repos":    false,
		"system_hints":                 []string{"picture_v2"},
		"serialization_metadata": map[string]any{
			"custom_symbol_offsets": []any{},
		},
	}
	if inpaintingOperation != nil {
		metadata["dalle"] = map[string]any{
			"from_client": map[string]any{
				"operation": inpaintingOperation,
			},
		}
	}
	message := map[string]any{
		"id":     messageID,
		"author": map[string]any{"role": "user"},
		"content": map[string]any{
			"content_type": contentType,
			"parts":        parts,
		},
		"metadata":    metadata,
		"create_time": float64(time.Now().UnixMilli()) / 1000,
	}
	if len(attachments) > 0 {
		metadata["attachments"] = attachments
	}

	req := map[string]any{
		"action": "next",
		"client_prepare_state": func() string {
			if len(uploads) > 0 && inpaintingOperation == nil {
				return "sent"
			}
			return "success"
		}(),
		"parent_message_id":                    parentMessageID,
		"model":                                strings.TrimSpace(conversationModel),
		"timezone_offset_min":                  openAITimezoneOffsetMinutes(),
		"timezone":                             openAITimezoneName(),
		"conversation_mode":                    map[string]any{"kind": "primary_assistant"},
		"enable_message_followups":             true,
		"system_hints":                         []string{"picture_v2"},
		"supports_buffering":                   true,
		"supported_encodings":                  []string{"v1"},
		"paragen_cot_summary_display_override": "allow",
		"force_parallel_switch":                "auto",
		"thinking_effort":                      "standard",
		"client_contextual_info": map[string]any{
			"is_dark_mode":      false,
			"time_since_loaded": 200,
			"page_height":       932,
			"page_width":        1200,
			"pixel_ratio":       2,
			"screen_height":     1243,
			"screen_width":      1920,
			"app_name":          "chatgpt.com",
		},
		"messages": []any{message},
	}
	if conversationID := strings.TrimSpace(parsed.ConversationID); conversationID != "" {
		req["conversation_id"] = conversationID
	}
	return req
}

func readOpenAIImageConversationStream(resp *req.Response, startTime time.Time) (string, []openAIImagePointerInfo, OpenAIUsage, *int, error) {
	if resp == nil || resp.Body == nil {
		return "", nil, OpenAIUsage{}, nil, fmt.Errorf("empty conversation response")
	}
	reader := bufio.NewReader(resp.Body)
	var (
		conversationID string
		firstTokenMs   *int
		usage          OpenAIUsage
		pointers       []openAIImagePointerInfo
	)

	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSpace(line) != "" && firstTokenMs == nil {
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}
		if data, ok := extractOpenAISSEDataLine(strings.TrimRight(line, "\r\n")); ok && data != "" && data != "[DONE]" {
			dataBytes := []byte(data)
			if conversationID == "" {
				conversationID = strings.TrimSpace(gjson.GetBytes(dataBytes, "v.conversation_id").String())
				if conversationID == "" {
					conversationID = strings.TrimSpace(gjson.GetBytes(dataBytes, "conversation_id").String())
				}
			}
			mergeOpenAIUsage(&usage, dataBytes)
			pointers = mergeOpenAIImagePointerInfos(pointers, collectOpenAIImagePointers(dataBytes))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, OpenAIUsage{}, firstTokenMs, err
		}
	}
	return conversationID, pointers, usage, firstTokenMs, nil
}

type openAIImageToolMessage struct {
	MessageID    string
	CreateTime   float64
	PointerInfos []openAIImagePointerInfo
}

func extractOpenAIImageToolMessages(mapping map[string]any) []openAIImageToolMessage {
	if len(mapping) == 0 {
		return nil
	}
	out := make([]openAIImageToolMessage, 0, 4)
	for messageID, raw := range mapping {
		node, _ := raw.(map[string]any)
		if node == nil {
			continue
		}
		message, _ := node["message"].(map[string]any)
		author, _ := message["author"].(map[string]any)
		metadata, _ := message["metadata"].(map[string]any)
		content, _ := message["content"].(map[string]any)
		if author == nil || metadata == nil || content == nil {
			continue
		}
		if role, _ := author["role"].(string); role != "tool" {
			continue
		}
		if asyncTaskType, _ := metadata["async_task_type"].(string); asyncTaskType != "image_gen" {
			continue
		}
		if contentType, _ := content["content_type"].(string); contentType != "multimodal_text" {
			continue
		}
		prompt := ""
		if title, _ := metadata["image_gen_title"].(string); strings.TrimSpace(title) != "" {
			prompt = strings.TrimSpace(title)
		}
		item := openAIImageToolMessage{MessageID: messageID}
		if createTime, ok := message["create_time"].(float64); ok {
			item.CreateTime = createTime
		}
		parts, _ := content["parts"].([]any)
		for _, part := range parts {
			switch value := part.(type) {
			case map[string]any:
				if assetPointer, _ := value["asset_pointer"].(string); strings.TrimSpace(assetPointer) != "" {
					for _, pointer := range openAIImagePointerMatches([]byte(assetPointer)) {
						item.PointerInfos = append(item.PointerInfos, openAIImagePointerInfo{
							Pointer: pointer,
							Prompt:  prompt,
						})
					}
				}
			case string:
				for _, pointer := range openAIImagePointerMatches([]byte(value)) {
					item.PointerInfos = append(item.PointerInfos, openAIImagePointerInfo{
						Pointer: pointer,
						Prompt:  prompt,
					})
				}
			}
		}
		if len(item.PointerInfos) == 0 {
			continue
		}
		item.PointerInfos = mergeOpenAIImagePointerInfos(nil, item.PointerInfos)
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreateTime < out[j].CreateTime
	})
	return out
}

func hasOpenAIFileServicePointerInfos(items []openAIImagePointerInfo) bool {
	for _, item := range items {
		if strings.HasPrefix(item.Pointer, "file-service://") {
			return true
		}
	}
	return false
}

func preferOpenAIFileServicePointerInfos(items []openAIImagePointerInfo) []openAIImagePointerInfo {
	if !hasOpenAIFileServicePointerInfos(items) {
		return items
	}
	out := make([]openAIImagePointerInfo, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(item.Pointer, "file-service://") {
			out = append(out, item)
		}
	}
	return out
}

func pollOpenAIImageConversation(ctx context.Context, client *req.Client, headers http.Header, account *Account, profile *OpenAIWebProfile, service *OpenAIGatewayService, conversationID string) ([]openAIImagePointerInfo, error) {
	conversationID = strings.TrimSpace(conversationID)
	if conversationID == "" {
		return nil, nil
	}
	deadline := time.Now().Add(90 * time.Second)
	interval := 3 * time.Second
	previewWait := 15 * time.Second
	var (
		lastErr     error
		firstToolAt time.Time
	)
	for time.Now().Before(deadline) {
		pollURL, targetPath, targetRoute, referer := buildOpenAIConversationAsyncStatusRequestTarget(conversationID)
		pollHeaders := cloneHTTPHeader(headers)
		pollHeaders.Set("Content-Type", "application/json")
		pollHeaders.Set("Referer", referer)
		setOpenAIBackendAPIRequestTarget(pollHeaders, targetPath, targetRoute)
		setOpenAIBackendAPIRequestCookieHeader(pollHeaders, profile, pollURL)
		resp, err := client.R().
			SetContext(ctx).
			SetHeaders(headerToMap(pollHeaders)).
			DisableAutoReadResponse().
			SetBodyJsonMarshal(map[string]any{"status": 4}).
			Post(pollURL)
		if err != nil {
			lastErr = err
		} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if service != nil {
				service.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
			}
			if readErr != nil {
				lastErr = readErr
				goto waitNextPoll
			}
			pointers := mergeOpenAIImagePointerInfos(nil, collectOpenAIImagePointers(body))
			var decoded map[string]any
			if err := json.Unmarshal(body, &decoded); err == nil {
				if mapping, _ := decoded["mapping"].(map[string]any); len(mapping) > 0 {
					toolMessages := extractOpenAIImageToolMessages(mapping)
					if len(toolMessages) > 0 && firstToolAt.IsZero() {
						firstToolAt = time.Now()
					}
					for _, msg := range toolMessages {
						pointers = mergeOpenAIImagePointerInfos(pointers, msg.PointerInfos)
					}
				}
			}
			if hasOpenAIFileServicePointerInfos(pointers) {
				return preferOpenAIFileServicePointerInfos(pointers), nil
			}
			if len(pointers) > 0 && !firstToolAt.IsZero() && time.Since(firstToolAt) >= previewWait {
				return pointers, nil
			}
		} else {
			statusErr := newOpenAIImageStatusError(resp, "conversation poll failed", openAIUpstreamErrorBodyReadLimit)
			if isOpenAIImageTransientConversationNotFoundError(statusErr) {
				lastErr = statusErr
				goto waitNextPoll
			}
			return nil, statusErr
		}

	waitNextPoll:
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func buildOpenAIImageResponse(
	ctx context.Context,
	client *req.Client,
	headers http.Header,
	profile *OpenAIWebProfile,
	conversationID string,
	pointers []openAIImagePointerInfo,
	responseFormat string,
	meta openAIResponsesImageResult,
	usage OpenAIUsage,
) ([]byte, int, error) {
	results := make([]openAIResponsesImageResult, 0, len(pointers))
	for _, pointer := range pointers {
		data, err := resolveOpenAIImageBytes(ctx, client, headers, profile, conversationID, pointer, openAIUpstreamErrorBodyReadLimit)
		if err != nil {
			return nil, 0, err
		}
		results = append(results, openAIResponsesImageResult{
			Result:        base64.StdEncoding.EncodeToString(data),
			RevisedPrompt: pointer.Prompt,
			OutputFormat:  meta.OutputFormat,
			Size:          meta.Size,
			Background:    meta.Background,
			Quality:       meta.Quality,
			Model:         meta.Model,
		})
	}
	if len(results) == 0 {
		return nil, 0, fmt.Errorf("no image output resolved from conversation")
	}
	usageRaw := buildOpenAIImagesUsageJSON(usage, len(results))
	format := normalizeOpenAIImagesResponseFormat(responseFormat)
	body, err := buildOpenAIImagesAPIResponse(results, time.Now().Unix(), usageRaw, results[0], format)
	if err != nil {
		return nil, 0, err
	}
	if format == "url" {
		for i, img := range results {
			body, _ = sjson.SetBytes(body, fmt.Sprintf("data.%d.b64_json", i), img.Result)
		}
	}
	return body, len(results), nil
}

func (s *OpenAIGatewayService) writeOpenAIImagesLegacyBridgeStreamingResponse(
	c *gin.Context,
	results []openAIResponsesImageResult,
	responseFormat string,
	usage OpenAIUsage,
	createdAt int64,
) error {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return fmt.Errorf("streaming is not supported by response writer")
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)

	streamPrefix := "image_generation"
	usageRaw := buildOpenAIImagesUsageJSON(usage, len(results))
	format := normalizeOpenAIImagesResponseFormat(responseFormat)
	for _, img := range results {
		payload := buildOpenAIImagesStreamCompletedPayload(streamPrefix+".completed", img, format, createdAt, usageRaw)
		if err := s.writeOpenAIImagesStreamEvent(c, flusher, streamPrefix+".completed", payload); err != nil {
			return err
		}
	}
	return nil
}

func handleOpenAIImageBackendError(resp *req.Response) error {
	return newOpenAIImageStatusError(resp, "backend-api request failed", openAIUpstreamErrorBodyReadLimit)
}

func newOpenAIImageSyntheticStatusError(statusCode int, message string, requestURL string) *openAIImageStatusError {
	message = sanitizeUpstreamErrorMessage(strings.TrimSpace(message))
	if message == "" {
		message = "openai image backend request failed"
	}
	var body []byte
	if payload, err := json.Marshal(map[string]string{"detail": message}); err == nil {
		body = payload
	}
	return &openAIImageStatusError{
		StatusCode:   statusCode,
		Message:      message,
		ResponseBody: body,
		URL:          strings.TrimSpace(requestURL),
		Synthetic:    true,
	}
}

func (s *OpenAIGatewayService) wrapOpenAIImageBackendError(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	imageRoute string,
	err error,
) error {
	var statusErr *openAIImageStatusError
	if !errors.As(err, &statusErr) || statusErr == nil {
		return err
	}

	upstreamMsg := appendOpenAIImagesTelemetrySummaryToMessage(sanitizeUpstreamErrorMessage(statusErr.Message), GetOpenAIImagesTelemetry(c))
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: statusErr.StatusCode,
		UpstreamRequestID:  statusErr.RequestID,
		UpstreamURL:        safeUpstreamURL(statusErr.URL),
		Kind:               "request_error",
		Message:            upstreamMsg,
	})
	setOpsUpstreamError(c, statusErr.StatusCode, upstreamMsg, "")

	if s.shouldFailoverOpenAIUpstreamResponse(statusErr.StatusCode, upstreamMsg, statusErr.ResponseBody) {
		if statusErr.Synthetic {
			return statusErr
		}
		if s.rateLimitService != nil {
			if !s.rateLimitService.handleOpenAIImageRoute429(ctx, account, imageRoute, statusErr.StatusCode, statusErr.ResponseHeaders, statusErr.ResponseBody, true) {
				s.rateLimitService.HandleUpstreamError(ctx, account, statusErr.StatusCode, statusErr.ResponseHeaders, statusErr.ResponseBody)
			}
		}
		appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
			Platform:           account.Platform,
			AccountID:          account.ID,
			AccountName:        account.Name,
			UpstreamStatusCode: statusErr.StatusCode,
			UpstreamRequestID:  statusErr.RequestID,
			UpstreamURL:        safeUpstreamURL(statusErr.URL),
			Kind:               "failover",
			Message:            upstreamMsg,
		})
		retryableOnSameAccount := account.IsPoolMode() && isPoolModeRetryableStatus(statusErr.StatusCode)
		if strings.Contains(strings.ToLower(statusErr.Message), "unsupported challenge") {
			retryableOnSameAccount = false
		}
		return &UpstreamFailoverError{
			StatusCode:             statusErr.StatusCode,
			ResponseBody:           statusErr.ResponseBody,
			RetryableOnSameAccount: retryableOnSameAccount,
		}
	}

	return statusErr
}

func appendOpenAIImagesTelemetrySummaryToMessage(message string, telemetry *OpenAIImagesTelemetry) string {
	message = strings.TrimSpace(message)
	summary := FormatOpenAIImagesTelemetrySummary(telemetry)
	if summary == "" {
		return message
	}
	if message == "" {
		return "openai_images_telemetry: " + summary
	}
	return message + " | openai_images_telemetry: " + summary
}

func openAITimezoneOffsetMinutes() int {
	_, offset := time.Now().Zone()
	return offset / 60
}

func openAITimezoneName() string {
	return time.Now().Location().String()
}

func generateOpenAIRequirementsToken(userAgent string) string {
	config := []any{
		"core" + strconv.Itoa(3008),
		time.Now().UTC().Format(time.RFC1123),
		nil,
		0.123456,
		coalesceOpenAIFileName(strings.TrimSpace(userAgent), openAIImageBackendUserAgent),
		nil,
		"prod-openai-images",
		"en-US",
		"en-US,en",
		0,
		"navigator.webdriver",
		"location",
		"document.body",
		float64(time.Now().UnixMilli()) / 1000,
		uuid.NewString(),
		"",
		8,
		time.Now().Unix(),
	}
	answer, solved := generateOpenAIChallengeAnswer(strconv.FormatInt(time.Now().UnixNano(), 10), openAIImageRequirementsDiff, config)
	if solved {
		return "gAAAAAC" + answer
	}
	return ""
}

func generateOpenAIChallengeAnswer(seed string, difficulty string, config []any) (string, bool) {
	diffBytes, err := hex.DecodeString(difficulty)
	if err != nil {
		return "", false
	}
	p1 := []byte(jsonCompactSlice(config[:3], true))
	p2 := []byte(jsonCompactSlice(config[4:9], false))
	p3 := []byte(jsonCompactSlice(config[10:], false))
	seedBytes := []byte(seed)

	for i := 0; i < 100000; i++ {
		payload := fmt.Sprintf("%s%d,%s,%d,%s", p1, i, p2, i>>1, p3)
		encoded := base64.StdEncoding.EncodeToString([]byte(payload))
		sum := sha3.Sum512(append(seedBytes, []byte(encoded)...))
		if bytes.Compare(sum[:len(diffBytes)], diffBytes) <= 0 {
			return encoded, true
		}
	}
	return "", false
}

func jsonCompactSlice(values []any, trimSuffixComma bool) string {
	raw, _ := json.Marshal(values)
	text := string(raw)
	if trimSuffixComma {
		return strings.TrimSuffix(text, "]")
	}
	return strings.TrimPrefix(text, "[")
}

func generateOpenAIProofToken(required bool, seed string, difficulty string, userAgent string) string {
	if !required || strings.TrimSpace(seed) == "" || strings.TrimSpace(difficulty) == "" {
		return ""
	}
	screen := 3008
	if len(seed)%2 == 0 {
		screen = 4010
	}
	proofToken := []any{
		screen,
		time.Now().UTC().Format(time.RFC1123),
		nil,
		0,
		coalesceOpenAIFileName(strings.TrimSpace(userAgent), openAIImageBackendUserAgent),
		"https://chatgpt.com/",
		"dpl=openai-images",
		"en",
		"en-US",
		nil,
		"plugins[object PluginArray]",
		"_reactListening",
		"alert",
	}
	diffLen := len(difficulty)
	for i := 0; i < 100000; i++ {
		proofToken[3] = i
		raw, _ := json.Marshal(proofToken)
		encoded := base64.StdEncoding.EncodeToString(raw)
		sum := sha3.Sum512([]byte(seed + encoded))
		if strings.Compare(hex.EncodeToString(sum[:])[:diffLen], difficulty) <= 0 {
			return "gAAAAAB" + encoded
		}
	}
	fallbackBase := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%q", seed)))
	return "gAAAAABwQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D" + fallbackBase
}

func (s *OpenAIGatewayService) forwardOpenAIImagesLegacyBridge(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	parsed *OpenAIImagesRequest,
	channelMappedModel string,
) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	profile := ResolveOpenAIImageWebProfile(account)
	proxyURL := resolveOpenAIProxyURL(account)
	s.recordOpenAIImagesLegacyBridgeTelemetry(c, account, profile, proxyURL)
	requestModel := strings.TrimSpace(parsed.Model)
	if mapped := strings.TrimSpace(channelMappedModel); mapped != "" {
		requestModel = mapped
	}
	if requestModel == "" {
		requestModel = strings.TrimSpace(parsed.Model)
	}
	responseMeta := openAIResponsesImageResult{
		Model:        requestModel,
		OutputFormat: strings.TrimSpace(parsed.OutputFormat),
		Background:   strings.TrimSpace(parsed.Background),
		Quality:      strings.TrimSpace(parsed.Quality),
		Size:         strings.TrimSpace(parsed.Size),
	}

	if parsed.IsLegacyBridge() && parsed.IsEdits() {
		if len(parsed.InputImages) > 0 && !parsed.UsesLegacyInpainting() {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, fmt.Errorf("legacy bridge does not support JSON image_url/file_id edits"))
		}
		if parsed.HasAnyLegacyInpaintingInput() && !parsed.UsesLegacyInpainting() {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, fmt.Errorf("legacy inpainting requires original_file_id, original_gen_id, and mask inputs"))
		}
	}

	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	client, err := newOpenAIBackendAPIClient(proxyURL)
	if err != nil {
		return nil, err
	}
	headers, err := s.buildOpenAIBackendAPIHeaders(account, token)
	if err != nil {
		return nil, err
	}
	headers = applyOpenAIConversationPageReferer(headers, parsed.ConversationID)
	bootstrapStart := time.Now()
	if bootstrapErr := bootstrapOpenAIBackendAPI(ctx, client, headers); bootstrapErr != nil {
		logger.LegacyPrintf("service.openai_gateway", "OpenAI image bootstrap failed: %v", bootstrapErr)
	}
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageBootstrap, OpenAIImagesTelemetryStageOption{At: bootstrapStart, LatencyMs: time.Since(bootstrapStart).Milliseconds()})

	requirementsStart := time.Now()
	chatReqs, err := fetchOpenAIChatRequirements(ctx, client, headers, account, profile, s)
	if err != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, err)
	}
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageChatRequirements, OpenAIImagesTelemetryStageOption{At: requirementsStart, LatencyMs: time.Since(requirementsStart).Milliseconds()})
	if chatReqs.Arkose.Required {
		SetOpenAIImagesTelemetryChallenge(c, openAIImagesChallengeTelemetryState(chatReqs, "arkose"))
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, newOpenAIImageSyntheticStatusError(
			http.StatusForbidden,
			"chat-requirements requires unsupported challenge (arkose)",
			openAIChatGPTChatRequirementsPrepareURL,
		))
	}
	if chatReqs.Turnstile.Required {
		SetOpenAIImagesTelemetryChallenge(c, openAIImagesChallengeTelemetryState(chatReqs, "turnstile"))
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, newOpenAIImageSyntheticStatusError(
			http.StatusForbidden,
			"chat-requirements requires unsupported challenge (turnstile)",
			openAIChatGPTChatRequirementsPrepareURL,
		))
	}

	parentMessageID := strings.TrimSpace(parsed.ParentMessageID)
	if strings.TrimSpace(parsed.ConversationID) == "" && parentMessageID == "" {
		parentMessageID = uuid.NewString()
	}
	if strings.TrimSpace(parsed.ConversationID) != "" && parentMessageID == "" {
		resolvedParentMessageID, resolveErr := fetchOpenAIConversationParentMessageID(ctx, client, headers, account, profile, s, parsed.ConversationID)
		if resolveErr != nil {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, resolveErr)
		}
		parentMessageID = resolvedParentMessageID
	}
	proofToken := generateOpenAIProofToken(chatReqs.ProofOfWork.Required, chatReqs.ProofOfWork.Seed, chatReqs.ProofOfWork.Difficulty, headers.Get("User-Agent"))
	SetOpenAIImagesTelemetryChallenge(c, openAIImagesChallengeTelemetryStateWithProof(chatReqs, proofToken, ""))
	initStart := time.Now()
	if initErr := initializeOpenAIImageConversation(ctx, client, headers, account, profile, s); initErr != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, initErr)
	}
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageConversationInit, OpenAIImagesTelemetryStageOption{At: initStart, LatencyMs: time.Since(initStart).Milliseconds()})
	prepareStart := time.Now()
	conduitToken, err := prepareOpenAIImageConversation(ctx, client, headers, account, profile, s, parsed.Prompt, parentMessageID, chatReqs.Token, proofToken)
	if err != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, err)
	}
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStagePrepare, OpenAIImagesTelemetryStageOption{At: prepareStart, LatencyMs: time.Since(prepareStart).Milliseconds()})

	messageID := uuid.NewString()
	uploadStart := time.Now()
	uploads, err := uploadOpenAIImageFiles(ctx, client, headers, account, profile, s, parentMessageID, messageID, parsed.Uploads)
	if err != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, err)
	}
	if len(parsed.Uploads) > 0 {
		AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageUploadCreate, OpenAIImagesTelemetryStageOption{At: uploadStart, LatencyMs: time.Since(uploadStart).Milliseconds()})
		AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageUploadPut, OpenAIImagesTelemetryStageOption{At: uploadStart, LatencyMs: time.Since(uploadStart).Milliseconds()})
		AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageUploadUploaded, OpenAIImagesTelemetryStageOption{At: uploadStart, LatencyMs: time.Since(uploadStart).Milliseconds()})
	}
	if parsed.IsLegacyBridge() && parsed.IsEdits() && len(parsed.InputImages) > 0 {
		legacyUploads, legacyErr := resolveOpenAILegacyBridgeInputImages(ctx, client, headers, account, profile, parsed.InputImages)
		if legacyErr != nil {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, legacyErr)
		}
		uploads = append(uploads, legacyUploads...)
	}
	maskFileID := strings.TrimSpace(parsed.MaskFileID)
	if maskFileID == "" && parsed.UsesLegacyInpainting() && parsed.MaskUpload != nil {
		maskFileID, err = uploadOpenAIImageMaskFile(ctx, client, headers, account, profile, s, *parsed.MaskUpload)
		if err != nil {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, err)
		}
	}

	conversationModel := resolveOpenAIImageConversationModel(ctx, s.settingService, account)
	convReq := buildOpenAIImageConversationRequest(conversationModel, parsed, parentMessageID, messageID, uploads, maskFileID)
	if parsedContent, err := json.Marshal(convReq); err == nil {
		setOpsUpstreamRequestBody(c, parsedContent)
	}
	convHeaders := cloneHTTPHeader(headers)
	convHeaders.Set("Accept", "text/event-stream")
	convHeaders.Set("Content-Type", "application/json")
	setOpenAIBackendAPIRequestTarget(convHeaders, openAIChatGPTConversationURL, "/backend-api/f/conversation")
	setOpenAIBackendAPIRequestCookieHeader(convHeaders, profile, openAIChatGPTConversationURL)
	convHeaders.Set("openai-sentinel-chat-requirements-token", chatReqs.Token)
	if conduitToken != "" {
		convHeaders.Set("x-conduit-token", conduitToken)
	}
	if proofToken != "" {
		convHeaders.Set("openai-sentinel-proof-token", proofToken)
	}

	conversationStart := time.Now()
	resp, err := client.R().
		SetContext(ctx).
		DisableAutoReadResponse().
		SetHeaders(headerToMap(convHeaders)).
		SetBodyJsonMarshal(convReq).
		Post(openAIChatGPTConversationURL)
	if err != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, fmt.Errorf("openai image conversation request failed: %w", err))
	}
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageConversation, OpenAIImagesTelemetryStageOption{At: conversationStart, LatencyMs: time.Since(conversationStart).Milliseconds()})
	s.applyOpenAIBackendAPIResponseState(ctx, account, profile, headers, resp.Response)
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode >= 400 {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, handleOpenAIImageBackendError(resp))
	}

	conversationID, pointerInfos, usage, firstTokenMs, err := readOpenAIImageConversationStream(resp, startTime)
	if err != nil {
		return nil, err
	}
	if conversationID != "" && !hasOpenAIFileServicePointerInfos(pointerInfos) {
		pollStart := time.Now()
		polledPointers, pollErr := pollOpenAIImageConversation(ctx, client, headers, account, profile, s, conversationID)
		AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStagePoll, OpenAIImagesTelemetryStageOption{At: pollStart, LatencyMs: time.Since(pollStart).Milliseconds()})
		if pollErr != nil {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, pollErr)
		}
		pointerInfos = mergeOpenAIImagePointerInfos(pointerInfos, polledPointers)
	}
	pointerInfos = preferOpenAIFileServicePointerInfos(pointerInfos)
	if len(pointerInfos) == 0 {
		return nil, fmt.Errorf("openai image conversation returned no downloadable images")
	}

	downloadStart := time.Now()
	responseBody, imageCount, err := buildOpenAIImageResponse(ctx, client, headers, profile, conversationID, pointerInfos, parsed.ResponseFormat, responseMeta, usage)
	AppendOpenAIImagesTelemetryStage(c, OpenAIImagesTelemetryStageDownloadBytes, OpenAIImagesTelemetryStageOption{At: downloadStart, LatencyMs: time.Since(downloadStart).Milliseconds()})
	if err != nil {
		return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, err)
	}
	if parsed.Stream {
		results, createdAt, usageRaw, firstMeta, _, collectErr := collectOpenAIImagesFromResponsesBody(responseBody)
		if collectErr != nil {
			return nil, s.wrapOpenAIImageBackendError(ctx, c, account, GroupImageGenerationRouteWeb2API, collectErr)
		}
		if len(results) == 0 {
			if gjson.ValidBytes(responseBody) {
				for _, item := range gjson.GetBytes(responseBody, "data").Array() {
					result := strings.TrimSpace(item.Get("b64_json").String())
					if result == "" {
						result = normalizeOpenAIImageBase64(item.Get("url").String())
					}
					if result == "" {
						continue
					}
					results = append(results, openAIResponsesImageResult{
						Result:        result,
						RevisedPrompt: strings.TrimSpace(item.Get("revised_prompt").String()),
						OutputFormat:  strings.TrimSpace(gjson.GetBytes(responseBody, "output_format").String()),
						Background:    strings.TrimSpace(gjson.GetBytes(responseBody, "background").String()),
						Quality:       strings.TrimSpace(gjson.GetBytes(responseBody, "quality").String()),
						Size:          strings.TrimSpace(gjson.GetBytes(responseBody, "size").String()),
						Model:         strings.TrimSpace(gjson.GetBytes(responseBody, "model").String()),
					})
				}
			}
			firstMeta = responseMeta
		}
		if len(results) == 0 {
			return nil, fmt.Errorf("openai image conversation returned no streamable images")
		}
		if createdAt <= 0 {
			createdAt = time.Now().Unix()
		}
		if len(usageRaw) > 0 && gjson.ValidBytes(usageRaw) {
			if parsedUsage, ok := extractOpenAIUsageFromJSONBytes(usageRaw); ok {
				usage = parsedUsage
			}
		}
		for i := range results {
			mergeOpenAIResponsesImageMeta(&results[i], firstMeta)
			mergeOpenAIResponsesImageMeta(&results[i], responseMeta)
		}
		if err := s.writeOpenAIImagesLegacyBridgeStreamingResponse(c, results, parsed.ResponseFormat, usage, createdAt); err != nil {
			return nil, err
		}
	} else {
		c.Data(http.StatusOK, "application/json; charset=utf-8", responseBody)
	}
	return &OpenAIForwardResult{
		RequestID:     resp.Header.Get("x-request-id"),
		Usage:         usage,
		Model:         requestModel,
		UpstreamModel: requestModel,
		Stream:        parsed.Stream,
		Duration:      time.Since(startTime),
		FirstTokenMs:  firstTokenMs,
		ImageCount:    imageCount,
		ImageSize:     parsed.SizeTier,
	}, nil
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
