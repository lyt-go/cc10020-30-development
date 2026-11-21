import { request } from './client'

export type ProgressImportResult = {
  ok: boolean
  message: string
  duplicate?: boolean
  confirmed?: boolean
  receipt_id?: number
  凭证编号?: string
  endpoint_id?: string
  status?: string
  coverage_range?: string
  coverage_dates?: string[]
  confirmed_dates?: string[]
  unconfirmed_dates?: string[]
  pending_dates?: string[]
  errors?: string[]
}

export type CoverageSource = {
  type: string
  receipt_id: number | null
  receipt_no: string
  coverage_id: number
  write_result: string
  acquired_date: string
  location: string
  confirmed: boolean
}

export type CoverageDay = {
  date: string
  confirmed: boolean
  sources: CoverageSource[]
}

/** 上传离线采集进度文件；后端按 (端点, 文件摘要) 去重，只确认一次凭证。 */
export async function uploadProgressReceipt(
  endpointId: string,
  file: File,
  acquiredDate = '',
): Promise<ProgressImportResult> {
  const form = new FormData()
  form.append('file', file)
  form.append('endpoint_id', endpointId)
  if (acquiredDate) form.append('acquired_date', acquiredDate)
  const response = await request('/api/progress_receipt/import', {
    method: 'POST',
    body: form,
    headers: {}, // 交给浏览器带 multipart boundary
  })
  return await response.json()
}

export async function loadCoverage(
  endpointId: string,
  startDate = '',
  endDate = '',
): Promise<{ total: number; sources: CoverageDay[] }> {
  const query = new URLSearchParams({ endpoint_id: endpointId })
  if (startDate) query.set('start_date', startDate)
  if (endDate) query.set('end_date', endDate)
  const response = await request(`/api/progress_receipt/coverage?${query.toString()}`)
  if (!response.ok) throw new Error('采集进度覆盖读取失败')
  return await response.json()
}

export async function resumeReceipt(receiptId: number): Promise<ProgressImportResult> {
  const response = await request(`/api/progress_receipt/${receiptId}/confirm`, { method: 'POST' })
  return await response.json()
}
