import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 来源批次文件接入与融合取数 ----

export type SourceBatchRecord = {
  id: number
  批次编号: string
  来源编号: string
  批次名称: string
  数据版本: string
  文件名: string
  内容摘要: string
  数据行数: string
  有效行数: string
  无效行数: string
  校验结果: string
  受理状态: string
  融合确认: string
  接收时间: string
  重传次数: string
}

export type SourceBatchDetail = SourceBatchRecord & {
  columns: string[]
  errors: string[]
  rows: Record<string, string | number>[]
}

export type UploadResult = {
  ok: boolean
  accepted: boolean
  duplicate: boolean
  message: string
  batch?: SourceBatchRecord
  detail?: SourceBatchDetail
}

export type FusionReceipt = {
  回执编号: string
  来源编号: string
  受理批次: string
  批次名称: string
  数据版本: string
  来源文件: string
  内容摘要: string
  有效行数: string
  已取行数: string
  固定时间: string
}

export type FusionRunState = {
  ok: boolean
  job_id: number
  run_status: string
  receipts: FusionReceipt[]
  blocked_sources: string[]
  records?: {
    来源编号: string
    受理批次: string
    来源文件: string
    原数据行: string
    内容摘要: string
    数据: Record<string, string | number>
  }[]
}

export type FusionContinueResult = {
  ok: boolean
  message?: string
  already_completed?: boolean
  run_status?: string
  raw_records?: number
  blocked_sources?: string[]
  receipts?: FusionReceipt[]
  records?: Record<string, string | number>[]
  resolved_records?: Record<string, string | number>[]
  confirmation?: { batches: Record<string, string | number>[]; blocked_sources: string[] }
}

export async function listBatches(
  sourceId: number,
  batchName = '',
): Promise<{ items: SourceBatchRecord[]; total: number }> {
  const query = new URLSearchParams(batchName ? { batch_name: batchName } : {}).toString()
  const response = await request(`/api/fusion/sources/${sourceId}/batches?${query}`)
  if (!response.ok) throw new Error('批次列表读取失败，请确认来源已登记')
  return await response.json()
}

export async function uploadBatch(
  sourceId: number,
  file: File,
  batchName: string,
): Promise<UploadResult> {
  const query = new URLSearchParams({
    batch_name: batchName || file.name,
    file_name: file.name,
    identifier_fields: 'id',
  }).toString()
  const response = await request(`/api/fusion/sources/${sourceId}/batches?${query}`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/csv' },
    body: await file.arrayBuffer(),
  })
  if (!response.ok) throw new Error(`上传未受理：接口返回 ${response.status}`)
  return await response.json()
}

export async function startFusionRun(jobId: number): Promise<FusionRunState & { message?: string }> {
  const response = await request(`/api/fusion/jobs/${jobId}/start`, { method: 'POST' })
  if (!response.ok) throw new Error('融合作业不存在或已开始取数')
  return await response.json()
}

export async function readFusionBatch(jobId: number, rowsPerSource: number | null): Promise<FusionRunState> {
  const query = rowsPerSource ? `?rows_per_source=${rowsPerSource}` : ''
  const response = await request(`/api/fusion/jobs/${jobId}/read${query}`, { method: 'POST' })
  if (!response.ok) throw new Error('取数失败，请先固定批次回执')
  return await response.json()
}

export async function continueFusionJob(jobId: number): Promise<FusionContinueResult> {
  const response = await request(`/api/fusion/jobs/${jobId}/continue`, { method: 'POST' })
  if (!response.ok) throw new Error('继续作业失败，请确认融合作业已登记')
  return await response.json()
}

export async function loadFusionRun(jobId: number): Promise<FusionRunState> {
  const response = await request(`/api/fusion/jobs/${jobId}/run`)
  if (!response.ok) throw new Error('取数状态读取失败')
  return await response.json()
}
