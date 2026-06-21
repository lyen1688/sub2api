/**
 * Admin TLS Fingerprint Profile API endpoints
 * Handles TLS fingerprint profile CRUD for administrators
 */

import { apiClient } from '../client'

/**
 * TLS fingerprint profile interface
 */
export interface TLSFingerprintProfile {
  id: number
  platform: string
  name: string
  description: string | null
  user_agent: string
  enable_grease: boolean
  cipher_suites: number[]
  curves: number[]
  point_formats: number[]
  signature_algorithms: number[]
  alpn_protocols: string[]
  supported_versions: number[]
  key_share_groups: number[]
  psk_modes: number[]
  extensions: number[]
  compress_cert_algos: number[]
  delegated_credentials_algorithms: number[]
  application_settings_protocols: string[]
  created_at: string
  updated_at: string
}

/**
 * Create profile request
 */
export interface CreateProfileRequest {
  platform?: string
  name: string
  description?: string | null
  user_agent?: string
  enable_grease?: boolean
  cipher_suites?: number[]
  curves?: number[]
  point_formats?: number[]
  signature_algorithms?: number[]
  alpn_protocols?: string[]
  supported_versions?: number[]
  key_share_groups?: number[]
  psk_modes?: number[]
  extensions?: number[]
  compress_cert_algos?: number[]
  delegated_credentials_algorithms?: number[]
  application_settings_protocols?: string[]
}

/**
 * Update profile request
 */
export interface UpdateProfileRequest {
  platform?: string
  name?: string
  description?: string | null
  user_agent?: string
  enable_grease?: boolean
  cipher_suites?: number[]
  curves?: number[]
  point_formats?: number[]
  signature_algorithms?: number[]
  alpn_protocols?: string[]
  supported_versions?: number[]
  key_share_groups?: number[]
  psk_modes?: number[]
  extensions?: number[]
  compress_cert_algos?: number[]
  delegated_credentials_algorithms?: number[]
  application_settings_protocols?: string[]
}

export interface TLSFingerprintCaptureTask {
  id: number
  name: string
  status: 'running' | 'completed' | 'stopped' | string
  token?: string
  targets: Record<string, number>
  counts: Record<string, number>
  ua_keywords: string[]
  created_at: string
  updated_at: string
  completed_at?: string | null
}

export interface TLSFingerprintCaptureSample {
  id: number
  task_id: number
  platform: string
  user_agent: string
  fingerprint_hash: string
  profile: TLSFingerprintProfile
  raw_payload: string
  created_at: string
}

export interface StartCaptureTaskRequest {
  name?: string
  targets: Record<string, number>
  ua_keywords?: string[]
}

export interface SubmitCaptureRequest {
  token: string
  platform: string
  user_agent: string
  payload: string
}

export interface CaptureSubmitResult {
  accepted: boolean
  duplicate: boolean
  ignored_reason?: string
  fingerprint_hash?: string
  task: TLSFingerprintCaptureTask
  sample?: TLSFingerprintCaptureSample
  counts: Record<string, number>
}

export interface ImportCaptureTaskSamplesRequest {
  sample_ids?: number[]
}

export interface ImportCaptureRecord {
  profile: TLSFingerprintProfile
  duplicate: boolean
  fingerprint_hash: string
}

export interface ImportCaptureResult {
  imported: number
  duplicates: number
  profiles: ImportCaptureRecord[]
}

export async function list(): Promise<TLSFingerprintProfile[]> {
  const { data } = await apiClient.get<TLSFingerprintProfile[]>('/admin/tls-fingerprint-profiles')
  return data
}

export async function getById(id: number): Promise<TLSFingerprintProfile> {
  const { data } = await apiClient.get<TLSFingerprintProfile>(`/admin/tls-fingerprint-profiles/${id}`)
  return data
}

export async function create(profileData: CreateProfileRequest): Promise<TLSFingerprintProfile> {
  const { data } = await apiClient.post<TLSFingerprintProfile>('/admin/tls-fingerprint-profiles', profileData)
  return data
}

export async function update(id: number, updates: UpdateProfileRequest): Promise<TLSFingerprintProfile> {
  const { data } = await apiClient.put<TLSFingerprintProfile>(`/admin/tls-fingerprint-profiles/${id}`, updates)
  return data
}

export async function deleteProfile(id: number): Promise<{ message: string }> {
  const { data } = await apiClient.delete<{ message: string }>(`/admin/tls-fingerprint-profiles/${id}`)
  return data
}

export async function listCaptureTasks(): Promise<TLSFingerprintCaptureTask[]> {
  const { data } = await apiClient.get<TLSFingerprintCaptureTask[]>('/admin/tls-fingerprint-profiles/capture-tasks')
  return data
}

export async function startCaptureTask(request: StartCaptureTaskRequest): Promise<TLSFingerprintCaptureTask> {
  const { data } = await apiClient.post<TLSFingerprintCaptureTask>('/admin/tls-fingerprint-profiles/capture-tasks', request)
  return data
}

export async function getCaptureTask(id: number): Promise<TLSFingerprintCaptureTask> {
  const { data } = await apiClient.get<TLSFingerprintCaptureTask>(`/admin/tls-fingerprint-profiles/capture-tasks/${id}`)
  return data
}

export async function stopCaptureTask(id: number): Promise<TLSFingerprintCaptureTask> {
  const { data } = await apiClient.post<TLSFingerprintCaptureTask>(`/admin/tls-fingerprint-profiles/capture-tasks/${id}/stop`)
  return data
}

export async function listCaptureSamples(taskId: number): Promise<TLSFingerprintCaptureSample[]> {
  const { data } = await apiClient.get<TLSFingerprintCaptureSample[]>(`/admin/tls-fingerprint-profiles/capture-tasks/${taskId}/samples`)
  return data
}

export async function importCaptureTaskSamples(
  taskId: number,
  request: ImportCaptureTaskSamplesRequest = {}
): Promise<ImportCaptureResult> {
  const { data } = await apiClient.post<ImportCaptureResult>(
    `/admin/tls-fingerprint-profiles/capture-tasks/${taskId}/import`,
    request
  )
  return data
}

export async function submitCapture(request: SubmitCaptureRequest): Promise<CaptureSubmitResult> {
  const { data } = await apiClient.post<CaptureSubmitResult>('/tls-fingerprint-captures/submit', request)
  return data
}

export async function submitCaptureToEndpoint(
  endpoint: string,
  request: SubmitCaptureRequest
): Promise<CaptureSubmitResult> {
  const { data } = await apiClient.post<CaptureSubmitResult>(endpoint, request)
  return data
}

export const tlsFingerprintProfileAPI = {
  list,
  getById,
  create,
  update,
  delete: deleteProfile,
  listCaptureTasks,
  startCaptureTask,
  getCaptureTask,
  stopCaptureTask,
  listCaptureSamples,
  importCaptureTaskSamples,
  submitCapture,
  submitCaptureToEndpoint
}

export default tlsFingerprintProfileAPI
