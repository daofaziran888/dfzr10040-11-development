<template>
  <section class="page" data-module="batch_ingest">
    <header class="page-head">
      <div>
        <h2>来源批次接入</h2>
        <p class="page-desc">
          为已登记来源上传批次文件；相同字节重传只确认一次，内容变化生成新数据版本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="refresh">刷新批次</button>
      </div>
    </header>

    <form class="analysis-form" @submit.prevent="submitUpload">
      <label>
        <span>数据源</span>
        <select v-model.number="sourceId" data-testid="select-source" required>
          <option v-for="source in sources" :key="source.id" :value="source.id">
            {{ source.sourceCode }} · {{ source.sourceName }}
          </option>
        </select>
      </label>
      <label>
        <span>批次名称</span>
        <input v-model="batchName" data-testid="input-batch-name" placeholder="如 orders.csv" />
      </label>
      <label>
        <span>批次文件（CSV，含 id 标识字段）</span>
        <input data-testid="input-file" type="file" accept=".csv,text/csv" @change="pickFile" />
      </label>
      <button class="btn primary" data-testid="submit-upload" type="submit" :disabled="uploading || !file">
        {{ uploading ? '上传校验中…' : '上传并校验' }}
      </button>
    </form>

    <p v-if="message" class="status-legend" :class="{ 'error-text': !lastAccepted }" data-testid="upload-message">
      {{ message }}
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次编号</th>
          <th>来源编号</th>
          <th>批次名称</th>
          <th>数据版本</th>
          <th>有效行数</th>
          <th>校验结果</th>
          <th>受理状态</th>
          <th>融合确认</th>
          <th>重传次数</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="batch.id" :data-testid="`batch-${batch.批次编号}`">
          <td>{{ batch.批次编号 }}</td>
          <td>{{ batch.来源编号 }}</td>
          <td>{{ batch.批次名称 }}</td>
          <td>{{ batch.数据版本 }}</td>
          <td>{{ batch.有效行数 }}</td>
          <td>{{ batch.校验结果 }}</td>
          <td>{{ batch.受理状态 }}</td>
          <td>{{ batch.融合确认 }}</td>
          <td>{{ batch.重传次数 }}</td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="9" class="empty-state">该来源还没有上传过批次文件</td>
        </tr>
      </tbody>
    </table>

    <section v-if="lastDetail" class="ingest-detail">
      <h3>{{ lastDetail.批次编号 }} 批次明细</h3>
      <p v-if="lastDetail.errors.length" class="error-text">
        未受理原因：{{ lastDetail.errors.join('；') }}
      </p>
      <p v-else>共 {{ lastDetail.有效行数 }} 行有效数据，可被融合作业按回执消费。</p>
      <table v-if="lastDetail.rows.length" class="data-table">
        <thead>
          <tr>
            <th>来源行号</th>
            <th v-for="column in lastDetail.columns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in lastDetail.rows" :key="String(row.__row_number__)">
            <td>{{ row.__row_number__ }}</td>
            <td v-for="column in lastDetail.columns" :key="column">{{ row[column] }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="ingest-runs">
      <h3>融合作业取数与批次回执</h3>
      <form class="analysis-form" @submit.prevent="loadRun">
        <label>
          <span>融合作业</span>
          <select v-model.number="jobId" data-testid="select-job">
            <option v-for="job in jobs" :key="job.id" :value="job.id">
              {{ job.jobCode }} · {{ job.targetWideTable }}
            </option>
          </select>
        </label>
        <label>
          <span>本次每源读取行数</span>
          <input v-model.number="rowsPerSource" data-testid="input-rows" min="1" type="number" placeholder="留空读取全部" />
        </label>
        <button class="btn" data-testid="start-run" type="button" @click="startRun">固定批次回执</button>
        <button class="btn" data-testid="read-run" type="button" @click="readRun">读取一批</button>
        <button class="btn primary" data-testid="continue-run" type="button" @click="continueRun">继续作业</button>
      </form>

      <p class="status-legend">
        取数状态：{{ runState?.run_status ?? '未开始' }}
        <template v-if="runState?.blocked_sources?.length">
          ｜未就绪来源：{{ runState.blocked_sources.join('、') }}
        </template>
      </p>

      <table v-if="runState?.receipts?.length" class="data-table">
        <thead>
          <tr>
            <th>回执编号</th>
            <th>来源编号</th>
            <th>受理批次</th>
            <th>数据版本</th>
            <th>来源文件</th>
            <th>有效行数</th>
            <th>已取行数</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="receipt in runState.receipts" :key="receipt.回执编号">
            <td>{{ receipt.回执编号 }}</td>
            <td>{{ receipt.来源编号 }}</td>
            <td>{{ receipt.受理批次 }}</td>
            <td>{{ receipt.数据版本 }}</td>
            <td>{{ receipt.来源文件 }}</td>
            <td>{{ receipt.有效行数 }}</td>
            <td>{{ receipt.已取行数 }}</td>
          </tr>
        </tbody>
      </table>

      <table v-if="outputRows.length" class="data-table">
        <thead>
          <tr>
            <th>来源文件</th>
            <th>受理批次</th>
            <th>原数据行</th>
            <th>关联键</th>
            <th>金额</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in outputRows" :key="index">
            <td>{{ row._file_name }}</td>
            <td>{{ row._batch_code }}</td>
            <td>{{ row._row_number }}</td>
            <td>{{ row.id ?? row.order_id ?? '—' }}</td>
            <td>{{ row.amount ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>来源批次接入与融合取数只走本页接口，列表数据仍由各业务模块维护。</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  continueFusionJob,
  listBatches,
  listEntries,
  loadFusionRun,
  readFusionBatch,
  startFusionRun,
  uploadBatch,
  type FusionContinueResult,
  type FusionRunState,
  type SourceBatchDetail,
  type SourceBatchRecord,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const sources = ref<{ id: number; sourceCode: string; sourceName: string }[]>([])
const jobs = ref<{ id: number; jobCode: string; targetWideTable: string }[]>([])
const sourceId = ref(0)
const jobId = ref(0)
const batchName = ref('')
const file = ref<File | null>(null)
const uploading = ref(false)
const message = ref('')
const lastAccepted = ref(true)
const batches = ref<SourceBatchRecord[]>([])
const lastDetail = ref<SourceBatchDetail | null>(null)
const rowsPerSource = ref<number | null>(null)
const runState = ref<(FusionRunState & { message?: string }) | null>(null)
const continueResult = ref<FusionContinueResult | null>(null)
const errorMessage = ref('')

const outputRows = computed<Record<string, string | number>[]>(
  () => continueResult.value?.resolved_records ?? [],
)

function pickFile(event: Event) {
  const input = event.target as HTMLInputElement
  file.value = input.files?.[0] ?? null
  if (file.value && !batchName.value) batchName.value = file.value.name
}

function describeError(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

async function loadSources() {
  try {
    const payload = await listEntries('source_def')
    sources.value = payload.items.map((row: EntryRow) => ({
      id: Number(row.id),
      sourceCode: String(row['源编号'] ?? ''),
      sourceName: String(row['源名称'] ?? ''),
    }))
    if (!sourceId.value && sources.value.length) sourceId.value = sources.value[0].id
  } catch (error) {
    errorMessage.value = describeError(error, '数据源列表读取失败')
  }
}

async function loadJobs() {
  try {
    const payload = await listEntries('fusion_job')
    jobs.value = payload.items.map((row: EntryRow) => ({
      id: Number(row.id),
      jobCode: String(row['作业编号'] ?? ''),
      targetWideTable: String(row['目标宽表'] ?? ''),
    }))
    if (!jobId.value && jobs.value.length) jobId.value = jobs.value[0].id
  } catch (error) {
    errorMessage.value = describeError(error, '融合作业列表读取失败')
  }
}

async function refresh() {
  if (!sourceId.value) return
  try {
    const payload = await listBatches(sourceId.value, batchName.value)
    batches.value = payload.items
  } catch (error) {
    errorMessage.value = describeError(error, '批次列表读取失败')
  }
}

async function submitUpload() {
  if (!file.value || !sourceId.value) return
  uploading.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await uploadBatch(sourceId.value, file.value, batchName.value)
    lastAccepted.value = result.accepted
    message.value = result.message
    lastDetail.value = result.detail ?? null
    await refresh()
  } catch (error) {
    lastAccepted.value = false
    errorMessage.value = describeError(error, '批次上传失败')
  } finally {
    uploading.value = false
  }
}

async function loadRun() {
  if (!jobId.value) return
  try {
    runState.value = await loadFusionRun(jobId.value)
  } catch (error) {
    errorMessage.value = describeError(error, '取数状态读取失败')
  }
}

async function startRun() {
  errorMessage.value = ''
  try {
    runState.value = await startFusionRun(jobId.value)
    continueResult.value = null
    message.value = runState.value.message ?? ''
  } catch (error) {
    errorMessage.value = describeError(error, '固定批次回执失败')
  }
}

async function readRun() {
  errorMessage.value = ''
  try {
    runState.value = await readFusionBatch(jobId.value, rowsPerSource.value)
  } catch (error) {
    errorMessage.value = describeError(error, '读取批次失败')
  }
}

async function continueRun() {
  errorMessage.value = ''
  try {
    const result = await continueFusionJob(jobId.value)
    continueResult.value = result
    message.value = result.message ?? `本次取数 ${result.raw_records ?? 0} 行`
    await loadRun()
    await refresh()
  } catch (error) {
    errorMessage.value = describeError(error, '继续作业失败')
  }
}

watch(sourceId, () => {
  lastDetail.value = null
  void refresh()
})

onMounted(async () => {
  await Promise.all([loadSources(), loadJobs()])
  await refresh()
})
</script>
