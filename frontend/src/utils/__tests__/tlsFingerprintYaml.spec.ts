import { describe, expect, it } from 'vitest'
import { parseTLSFingerprintYaml } from '../tlsFingerprintYaml'

describe('parseTLSFingerprintYaml', () => {
  it('extracts user agent and extensions from capture comments', () => {
    const parsed = parseTLSFingerprintYaml(`# TLS Fingerprint Profile for Sub2API
# User-Agent: codex_exec/0.141.0 (Ubuntu 24.4.0; x86_64) WindowsTerminal (codex_exec; 0.141.0)
codex_exec:
  name: "Codex Exec"
  enable_grease: false
  cipher_suites: [4865, 4866]
  curves: [29, 23]
  point_formats: [0]
  signature_algorithms: [1027]
  alpn_protocols: ["http/1.1"]
  supported_versions: [772, 771]
  key_share_groups: [29]
  psk_modes: [1]
  # extensions: [65281, 0, 11]
`)

    expect(parsed.name).toBe('Codex Exec')
    expect(parsed.user_agent).toBe('codex_exec/0.141.0 (Ubuntu 24.4.0; x86_64) WindowsTerminal (codex_exec; 0.141.0)')
    expect(parsed.enable_grease).toBe(false)
    expect(parsed.cipher_suites).toBe('4865, 4866')
    expect(parsed.alpn_protocols).toBe('http/1.1')
    expect(parsed.extensions).toBe('65281, 0, 11')
  })

  it('prefers explicit user_agent fields when present', () => {
    const parsed = parseTLSFingerprintYaml(`profile:
  name: "Codex TUI"
  user_agent: "codex-tui/0.142.0"
  extensions: [0, 11, 10]
`)

    expect(parsed.user_agent).toBe('codex-tui/0.142.0')
    expect(parsed.extensions).toBe('0, 11, 10')
  })
})
