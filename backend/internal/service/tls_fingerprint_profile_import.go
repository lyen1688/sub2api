package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/model"
	"gopkg.in/yaml.v3"
)

// TLSFingerprintCaptureImportRequest imports raw collector payloads.
// Each profile payload may be JSON or YAML and must contain the full set of
// fields supported by tls_fingerprint_profiles.
type TLSFingerprintCaptureImportRequest struct {
	Platform string   `json:"platform"`
	Profiles []string `json:"profiles"`
}

// TLSFingerprintCaptureImportResult reports created and duplicate profiles.
type TLSFingerprintCaptureImportResult struct {
	Imported   int                                 `json:"imported"`
	Duplicates int                                 `json:"duplicates"`
	Profiles   []TLSFingerprintCaptureImportRecord `json:"profiles"`
}

// TLSFingerprintCaptureImportRecord is one import attempt result.
type TLSFingerprintCaptureImportRecord struct {
	Profile         *model.TLSFingerprintProfile `json:"profile"`
	Duplicate       bool                         `json:"duplicate"`
	FingerprintHash string                       `json:"fingerprint_hash"`
}

type tlsFingerprintHashInput struct {
	EnableGREASE                   bool     `json:"enable_grease"`
	CipherSuites                   []uint16 `json:"cipher_suites"`
	Curves                         []uint16 `json:"curves"`
	PointFormats                   []uint16 `json:"point_formats"`
	SignatureAlgorithms            []uint16 `json:"signature_algorithms"`
	ALPNProtocols                  []string `json:"alpn_protocols"`
	SupportedVersions              []uint16 `json:"supported_versions"`
	KeyShareGroups                 []uint16 `json:"key_share_groups"`
	PSKModes                       []uint16 `json:"psk_modes"`
	Extensions                     []uint16 `json:"extensions"`
	CompressCertAlgos              []uint16 `json:"compress_cert_algos"`
	DelegatedCredentialsAlgorithms []uint16 `json:"delegated_credentials_algorithms"`
	ApplicationSettingsProtocols   []string `json:"application_settings_protocols"`
}

// ImportTLSFingerprintCaptures imports real collector payloads and deduplicates
// by the replayable fingerprint fields, not by profile name.
func (s *TLSFingerprintProfileService) ImportTLSFingerprintCaptures(ctx context.Context, req TLSFingerprintCaptureImportRequest) (*TLSFingerprintCaptureImportResult, error) {
	if len(req.Profiles) == 0 {
		return nil, &model.ValidationError{Field: "profiles", Message: "at least one captured profile is required"}
	}

	existing, err := s.repo.List(ctx)
	if err != nil {
		return nil, err
	}

	byHash := make(map[string]*model.TLSFingerprintProfile, len(existing))
	usedNames := make(map[string]struct{}, len(existing))
	for _, profile := range existing {
		if profile == nil {
			continue
		}
		usedNames[profile.Name] = struct{}{}
		hash, err := TLSFingerprintProfileReplayHash(profile)
		if err != nil {
			return nil, err
		}
		if _, ok := byHash[hash]; !ok {
			byHash[hash] = profile
		}
	}

	result := &TLSFingerprintCaptureImportResult{
		Profiles: make([]TLSFingerprintCaptureImportRecord, 0, len(req.Profiles)),
	}
	var imported bool

	for i, raw := range req.Profiles {
		profile, err := ParseTLSFingerprintCaptureProfile(raw)
		if err != nil {
			return nil, &model.ValidationError{Field: fmt.Sprintf("profiles[%d]", i), Message: err.Error()}
		}
		profile.Platform = strings.TrimSpace(req.Platform)
		hash, err := TLSFingerprintProfileReplayHash(profile)
		if err != nil {
			return nil, err
		}

		if existingProfile, ok := byHash[hash]; ok {
			result.Duplicates++
			result.Profiles = append(result.Profiles, TLSFingerprintCaptureImportRecord{
				Profile:         existingProfile,
				Duplicate:       true,
				FingerprintHash: hash,
			})
			continue
		}

		profile.Name = uniqueTLSFingerprintProfileName(profile.Name, usedNames, hash)
		created, err := s.repo.Create(ctx, profile)
		if err != nil {
			return nil, err
		}
		imported = true
		result.Imported++
		result.Profiles = append(result.Profiles, TLSFingerprintCaptureImportRecord{
			Profile:         created,
			Duplicate:       false,
			FingerprintHash: hash,
		})
		byHash[hash] = created
		usedNames[created.Name] = struct{}{}
	}

	if imported {
		refreshCtx, cancel := s.newCacheRefreshContext()
		defer cancel()
		s.invalidateAndNotify(refreshCtx)
	}

	return result, nil
}

// ParseTLSFingerprintCaptureProfile parses one collector JSON/YAML payload.
func ParseTLSFingerprintCaptureProfile(raw string) (*model.TLSFingerprintProfile, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("payload is required")
	}

	var decoded any
	if err := yaml.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("parse collector payload: %w", err)
	}

	payload, ok := findTLSFingerprintPayload(decoded)
	if !ok {
		return nil, fmt.Errorf("collector payload must be an object with TLS fingerprint fields")
	}

	profile := &model.TLSFingerprintProfile{
		Name:                           stringField(payload, "name"),
		UserAgent:                      stringField(payload, "user_agent"),
		EnableGREASE:                   boolField(payload, "enable_grease"),
		CipherSuites:                   uint16SliceField(payload, "cipher_suites"),
		Curves:                         uint16SliceField(payload, "curves"),
		PointFormats:                   uint16SliceField(payload, "point_formats"),
		SignatureAlgorithms:            uint16SliceField(payload, "signature_algorithms"),
		ALPNProtocols:                  stringSliceField(payload, "alpn_protocols"),
		SupportedVersions:              uint16SliceField(payload, "supported_versions"),
		KeyShareGroups:                 uint16SliceField(payload, "key_share_groups"),
		PSKModes:                       uint16SliceField(payload, "psk_modes"),
		Extensions:                     uint16SliceField(payload, "extensions"),
		CompressCertAlgos:              uint16SliceField(payload, "compress_cert_algos"),
		DelegatedCredentialsAlgorithms: uint16SliceField(payload, "delegated_credentials_algorithms"),
		ApplicationSettingsProtocols:   stringSliceField(payload, "application_settings_protocols"),
	}
	if desc := stringField(payload, "description"); desc != "" {
		profile.Description = &desc
	}
	if profile.Name == "" {
		profile.Name = deriveTLSFingerprintProfileName(payload)
	}
	if err := validateCompleteTLSFingerprintProfile(profile); err != nil {
		return nil, err
	}
	return profile, nil
}

// TLSFingerprintProfileReplayHash returns a stable hash over the fields this
// service can replay through uTLS. Metadata such as name, description, JA3, JA4,
// and user-agent is intentionally excluded.
func TLSFingerprintProfileReplayHash(profile *model.TLSFingerprintProfile) (string, error) {
	if profile == nil {
		return "", fmt.Errorf("profile is required")
	}
	input := tlsFingerprintHashInput{
		EnableGREASE:                   profile.EnableGREASE,
		CipherSuites:                   append([]uint16(nil), profile.CipherSuites...),
		Curves:                         append([]uint16(nil), profile.Curves...),
		PointFormats:                   append([]uint16(nil), profile.PointFormats...),
		SignatureAlgorithms:            append([]uint16(nil), profile.SignatureAlgorithms...),
		ALPNProtocols:                  append([]string(nil), profile.ALPNProtocols...),
		SupportedVersions:              append([]uint16(nil), profile.SupportedVersions...),
		KeyShareGroups:                 append([]uint16(nil), profile.KeyShareGroups...),
		PSKModes:                       append([]uint16(nil), profile.PSKModes...),
		Extensions:                     append([]uint16(nil), profile.Extensions...),
		CompressCertAlgos:              append([]uint16(nil), profile.CompressCertAlgos...),
		DelegatedCredentialsAlgorithms: append([]uint16(nil), profile.DelegatedCredentialsAlgorithms...),
		ApplicationSettingsProtocols:   append([]string(nil), profile.ApplicationSettingsProtocols...),
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func validateCompleteTLSFingerprintProfile(profile *model.TLSFingerprintProfile) error {
	if profile.Name == "" {
		return fmt.Errorf("name is required")
	}
	required := []struct {
		name string
		ok   bool
	}{
		{name: "cipher_suites", ok: len(profile.CipherSuites) > 0},
		{name: "curves", ok: len(profile.Curves) > 0},
		{name: "point_formats", ok: len(profile.PointFormats) > 0},
		{name: "signature_algorithms", ok: len(profile.SignatureAlgorithms) > 0},
		{name: "alpn_protocols", ok: len(profile.ALPNProtocols) > 0},
		{name: "supported_versions", ok: len(profile.SupportedVersions) > 0},
		{name: "key_share_groups", ok: len(profile.KeyShareGroups) > 0},
		{name: "psk_modes", ok: len(profile.PSKModes) > 0},
		{name: "extensions", ok: len(profile.Extensions) > 0},
	}
	for _, field := range required {
		if !field.ok {
			return fmt.Errorf("%s is required for a complete replayable TLS fingerprint", field.name)
		}
	}
	if containsUint16(profile.Extensions, 27) && len(profile.CompressCertAlgos) == 0 {
		return fmt.Errorf("compress_cert_algos is required for a complete replayable TLS fingerprint when extension 27 is present")
	}
	if containsUint16(profile.Extensions, 34) && len(profile.DelegatedCredentialsAlgorithms) == 0 {
		return fmt.Errorf("delegated_credentials_algorithms is required for a complete replayable TLS fingerprint when extension 34 is present")
	}
	if (containsUint16(profile.Extensions, 17513) || containsUint16(profile.Extensions, 17613)) && len(profile.ApplicationSettingsProtocols) == 0 {
		return fmt.Errorf("application_settings_protocols is required for a complete replayable TLS fingerprint when application_settings is present")
	}
	return nil
}

func containsUint16(values []uint16, target uint16) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func findTLSFingerprintPayload(value any) (map[string]any, bool) {
	obj, ok := asStringAnyMap(value)
	if !ok {
		return nil, false
	}
	if hasTLSFingerprintFields(obj) {
		return obj, true
	}
	for _, key := range []string{"profile", "fingerprint", "tls_fingerprint"} {
		if nested, ok := asStringAnyMap(obj[key]); ok && hasTLSFingerprintFields(nested) {
			return nested, true
		}
	}
	if len(obj) == 1 {
		for _, nestedValue := range obj {
			if nested, ok := findTLSFingerprintPayload(nestedValue); ok {
				return nested, true
			}
		}
	}
	return nil, false
}

func hasTLSFingerprintFields(obj map[string]any) bool {
	if _, ok := obj["cipher_suites"]; ok {
		return true
	}
	if _, ok := obj["extensions"]; ok {
		return true
	}
	_, ok := obj["signature_algorithms"]
	return ok
}

func asStringAnyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, val := range typed {
			keyString, ok := key.(string)
			if !ok {
				continue
			}
			out[keyString] = val
		}
		return out, true
	default:
		return nil, false
	}
}

func stringField(obj map[string]any, key string) string {
	switch val := obj[key].(type) {
	case string:
		return strings.TrimSpace(val)
	case fmt.Stringer:
		return strings.TrimSpace(val.String())
	default:
		return ""
	}
}

func boolField(obj map[string]any, key string) bool {
	switch val := obj[key].(type) {
	case bool:
		return val
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(val))
		return err == nil && parsed
	default:
		return false
	}
}

func uint16SliceField(obj map[string]any, key string) []uint16 {
	return toUint16Slice(obj[key])
}

func toUint16Slice(value any) []uint16 {
	switch typed := value.(type) {
	case []any:
		out := make([]uint16, 0, len(typed))
		for _, item := range typed {
			if parsed, ok := toUint16(item); ok {
				out = append(out, parsed)
			}
		}
		return out
	case []int:
		out := make([]uint16, 0, len(typed))
		for _, item := range typed {
			if parsed, ok := toUint16(item); ok {
				out = append(out, parsed)
			}
		}
		return out
	case []uint16:
		return append([]uint16(nil), typed...)
	case string:
		trimmed := strings.TrimSpace(typed)
		trimmed = strings.TrimPrefix(strings.TrimSuffix(trimmed, "]"), "[")
		if trimmed == "" {
			return nil
		}
		parts := strings.Split(trimmed, ",")
		out := make([]uint16, 0, len(parts))
		for _, part := range parts {
			if parsed, ok := parseUint16String(part); ok {
				out = append(out, parsed)
			}
		}
		return out
	default:
		return nil
	}
}

func toUint16(value any) (uint16, bool) {
	switch typed := value.(type) {
	case int:
		return uint16FromInt64(int64(typed))
	case int64:
		return uint16FromInt64(typed)
	case uint64:
		if typed > 65535 {
			return 0, false
		}
		return uint16(typed), true
	case float64:
		if typed != float64(uint16(typed)) {
			return 0, false
		}
		return uint16(typed), true
	case string:
		return parseUint16String(typed)
	default:
		return 0, false
	}
}

func uint16FromInt64(value int64) (uint16, bool) {
	if value < 0 || value > 65535 {
		return 0, false
	}
	return uint16(value), true
}

func parseUint16String(raw string) (uint16, bool) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.Trim(trimmed, `"'`)
	if trimmed == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(trimmed, 0, 16)
	if err != nil {
		return 0, false
	}
	return uint16(parsed), true
}

func stringSliceField(obj map[string]any, key string) []string {
	switch typed := obj[key].(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if val, ok := item.(string); ok && strings.TrimSpace(val) != "" {
				out = append(out, strings.TrimSpace(val))
			}
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case string:
		trimmed := strings.TrimSpace(typed)
		trimmed = strings.TrimPrefix(strings.TrimSuffix(trimmed, "]"), "[")
		if trimmed == "" {
			return nil
		}
		parts := strings.Split(trimmed, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.Trim(strings.TrimSpace(part), `"'`)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return nil
	}
}

func deriveTLSFingerprintProfileName(payload map[string]any) string {
	for _, key := range []string{"client", "client_name", "user_agent"} {
		if value := stringField(payload, key); value != "" {
			return trimTLSFingerprintProfileName(value)
		}
	}
	return ""
}

func uniqueTLSFingerprintProfileName(name string, used map[string]struct{}, hash string) string {
	name = trimTLSFingerprintProfileName(name)
	if name == "" {
		name = "TLS capture " + hash[:12]
	}
	if _, ok := used[name]; !ok {
		used[name] = struct{}{}
		return name
	}

	suffix := " " + hash[:8]
	base := name
	if len(base)+len(suffix) > 100 {
		base = strings.TrimSpace(base[:100-len(suffix)])
	}
	candidate := base + suffix
	for i := 2; ; i++ {
		if _, ok := used[candidate]; !ok {
			used[candidate] = struct{}{}
			return candidate
		}
		ordinalSuffix := fmt.Sprintf(" %s-%d", hash[:8], i)
		base = name
		if len(base)+len(ordinalSuffix) > 100 {
			base = strings.TrimSpace(base[:100-len(ordinalSuffix)])
		}
		candidate = base + ordinalSuffix
	}
}

func trimTLSFingerprintProfileName(name string) string {
	name = strings.Join(strings.Fields(strings.TrimSpace(name)), " ")
	if len(name) <= 100 {
		return name
	}
	return strings.TrimSpace(name[:100])
}
