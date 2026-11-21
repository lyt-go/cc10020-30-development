<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <p v-if="meta.key === 'progress_receipt'" class="status-legend" data-testid="progress-hint">
      离线采集进度文件导入后会登记原文件；同一端点重复上传同一文件只确认一次凭证。
    </p>

    <form
      v-if="meta.key === 'progress_receipt'"
      class="filter-bar"
      data-testid="progress-import"
      @submit.prevent="submitImport"
    >
      <label class="filter-item">
        <span>关联端点</span>
        <input v-model="importForm.endpoint" data-testid="import-endpoint" placeholder="例如 7" required />
      </label>
      <label class="filter-item">
        <span>取得日期</span>
        <input v-model="importForm.acquiredDate" data-testid="import-acquired" placeholder="YYYY-MM-DD" />
      </label>
      <label class="filter-item">
        <span>进度文件</span>
        <input
          data-testid="import-file"
          type="file"
          accept=".csv,.json,.jsonl,.ndjson,text/csv,application/json"
          @change="onFileChange"
        />
      </label>
      <button class="btn primary" data-testid="import-submit" type="submit" :disabled="importing">
        {{ importing ? '导入中…' : '导入进度凭证' }}
      </button>
    </form>

    <p v-if="importMessage" class="status-legend" data-testid="import-result">{{ importMessage }}</p>

    <table v-if="meta.key === 'progress_receipt' && coverageDays.length" class="data-table" data-testid="coverage-table">
      <thead>
        <tr><th>日期</th><th>确认状态</th><th>来源依据</th><th>写入结果</th><th>取得日期</th></tr>
      </thead>
      <tbody>
        <tr v-for="day in coverageDays" :key="day.date" :data-testid="`coverage-${day.date}`">
          <td>{{ day.date }}</td>
          <td>{{ day.confirmed ? '已确认' : '未确认' }}</td>
          <td>{{ day.sources.map((item) => `${item.type}:${item.receipt_no}`).join(' / ') }}</td>
          <td>{{ day.sources.map((item) => item.write_result).join(' / ') }}</td>
          <td>{{ day.sources.map((item) => item.acquired_date).filter(Boolean).join(' / ') || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import { loadCoverage, uploadProgressReceipt } from '@/api/progress'
import type { CoverageDay } from '@/api/progress'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const importForm = ref({ endpoint: '', acquiredDate: '', file: null as File | null })
const importing = ref(false)
const importMessage = ref('')
const coverageDays = ref<CoverageDay[]>([])
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  importForm.value.file = input.files?.[0] ?? null
}

async function submitImport() {
  errorMessage.value = ''
  importMessage.value = ''
  const { endpoint, acquiredDate, file } = importForm.value
  if (!file) {
    importMessage.value = '请选择需要导入的进度文件'
    return
  }
  importing.value = true
  try {
    const result = await uploadProgressReceipt(endpoint, file, acquiredDate)
    importMessage.value = result.message
    if (!result.ok && !result.confirmed) {
      throw new Error(result.message)
    }
    await reload()
    await refreshCoverage()
  } catch (error) {
    importMessage.value = error instanceof Error ? error.message : '进度凭证导入失败'
  } finally {
    importing.value = false
  }
}

async function refreshCoverage() {
  if (meta.key !== 'progress_receipt' || !importForm.value.endpoint) {
    coverageDays.value = []
    return
  }
  try {
    const payload = await loadCoverage(importForm.value.endpoint)
    coverageDays.value = payload.sources ?? []
  } catch (error) {
    coverageDays.value = []
    errorMessage.value = error instanceof Error ? error.message : '采集进度覆盖读取失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
