export interface ParsedTLSFingerprintYaml {
  platform?: string
  name?: string
  description?: string | null
  user_agent?: string
  enable_grease?: boolean
  cipher_suites?: string
  curves?: string
  point_formats?: string
  signature_algorithms?: string
  alpn_protocols?: string
  supported_versions?: string
  key_share_groups?: string
  psk_modes?: string
  extensions?: string
  compress_cert_algos?: string
  delegated_credentials_algorithms?: string
  application_settings_protocols?: string
}

const numericArrayKeys = new Set([
  'cipher_suites',
  'curves',
  'point_formats',
  'signature_algorithms',
  'supported_versions',
  'key_share_groups',
  'psk_modes',
  'extensions',
  'compress_cert_algos',
  'delegated_credentials_algorithms'
])

const stringArrayKeys = new Set([
  'alpn_protocols',
  'application_settings_protocols'
])

function stripQuotes(value: string): string {
  return value.trim().replace(/^["']|["']$/g, '')
}

function parseArrayInner(raw: string, stripItemQuotes = false): string {
  const match = raw.trim().match(/^\[(.*)?\]$/)
  if (!match) return ''
  return (match[1] || '')
    .split(',')
    .map(item => stripItemQuotes ? stripQuotes(item) : item.trim())
    .filter(Boolean)
    .join(', ')
}

export function parseTLSFingerprintYaml(text: string): ParsedTLSFingerprintYaml {
  const parsed: ParsedTLSFingerprintYaml = {}

  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue

    if (trimmed.startsWith('#')) {
      const comment = trimmed.replace(/^#+\s*/, '')
      const userAgentMatch = comment.match(/^User-Agent:\s*(.+)$/i)
      if (userAgentMatch) {
        parsed.user_agent = userAgentMatch[1].trim()
      }
      const extensionsMatch = comment.match(/^extensions:\s*(\[.*\])$/i)
      if (extensionsMatch && !parsed.extensions) {
        parsed.extensions = parseArrayInner(extensionsMatch[1])
      }
      continue
    }

    const match = trimmed.match(/^([A-Za-z0-9_]+):\s*(.*)$/)
    if (!match) continue

    const [, key, rawValue] = match
    const value = rawValue.trim()
    if (value === '') continue

    if (key === 'platform') parsed.platform = stripQuotes(value)
    if (key === 'name') parsed.name = stripQuotes(value)
    if (key === 'description') parsed.description = stripQuotes(value) || null
    if (key === 'user_agent') parsed.user_agent = stripQuotes(value)
    if (key === 'enable_grease') parsed.enable_grease = value === 'true'
    if (numericArrayKeys.has(key)) {
      const arrayValue = parseArrayInner(value)
      if (arrayValue) parsed[key as keyof ParsedTLSFingerprintYaml] = arrayValue as never
    }
    if (stringArrayKeys.has(key)) {
      const arrayValue = parseArrayInner(value, true)
      if (arrayValue) parsed[key as keyof ParsedTLSFingerprintYaml] = arrayValue as never
    }
  }

  return parsed
}
