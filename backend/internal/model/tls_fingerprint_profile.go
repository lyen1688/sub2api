// Package model 定义服务层使用的数据模型。
package model

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// TLSFingerprintProfile TLS 指纹配置模板
// 包含完整的 ClientHello 参数，用于模拟特定客户端的 TLS 握手特征
type TLSFingerprintProfile struct {
	ID                             int64     `json:"id"`
	Platform                       string    `json:"platform"`
	Name                           string    `json:"name"`
	Description                    *string   `json:"description"`
	UserAgent                      string    `json:"user_agent"`
	EnableGREASE                   bool      `json:"enable_grease"`
	CipherSuites                   []uint16  `json:"cipher_suites"`
	Curves                         []uint16  `json:"curves"`
	PointFormats                   []uint16  `json:"point_formats"`
	SignatureAlgorithms            []uint16  `json:"signature_algorithms"`
	ALPNProtocols                  []string  `json:"alpn_protocols"`
	SupportedVersions              []uint16  `json:"supported_versions"`
	KeyShareGroups                 []uint16  `json:"key_share_groups"`
	PSKModes                       []uint16  `json:"psk_modes"`
	Extensions                     []uint16  `json:"extensions"`
	CompressCertAlgos              []uint16  `json:"compress_cert_algos"`
	DelegatedCredentialsAlgorithms []uint16  `json:"delegated_credentials_algorithms"`
	ApplicationSettingsProtocols   []string  `json:"application_settings_protocols"`
	CreatedAt                      time.Time `json:"created_at"`
	UpdatedAt                      time.Time `json:"updated_at"`
}

// Validate 验证模板配置的有效性
func (p *TLSFingerprintProfile) Validate() error {
	if p.Name == "" {
		return &ValidationError{Field: "name", Message: "name is required"}
	}
	if len(p.Platform) > 50 {
		return &ValidationError{Field: "platform", Message: "platform is too long"}
	}
	return nil
}

// ToTLSProfile 将领域模型转换为运行时使用的 tlsfingerprint.Profile
// 空切片字段会在 dialer 中 fallback 到内置默认值
func (p *TLSFingerprintProfile) ToTLSProfile() *tlsfingerprint.Profile {
	return &tlsfingerprint.Profile{
		Name:                           p.Name,
		UserAgent:                      p.UserAgent,
		EnableGREASE:                   p.EnableGREASE,
		CipherSuites:                   p.CipherSuites,
		Curves:                         p.Curves,
		PointFormats:                   p.PointFormats,
		SignatureAlgorithms:            p.SignatureAlgorithms,
		ALPNProtocols:                  p.ALPNProtocols,
		SupportedVersions:              p.SupportedVersions,
		KeyShareGroups:                 p.KeyShareGroups,
		PSKModes:                       p.PSKModes,
		Extensions:                     p.Extensions,
		CompressCertAlgos:              p.CompressCertAlgos,
		DelegatedCredentialsAlgorithms: p.DelegatedCredentialsAlgorithms,
		ApplicationSettingsProtocols:   p.ApplicationSettingsProtocols,
	}
}
