<script setup>
import { ref, nextTick } from 'vue'

const token = ref('')
const oldLink = ref('')
const newLink = ref('')
const communities = ref('')
const logLines = ref([])
const running = ref(false)
const errorMessage = ref('')

const logRef = ref(null)

function appendLog(text) {
  if (typeof text !== 'string') return
  const lines = text.split('\n').filter(Boolean)
  logLines.value.push(...lines)
  nextTick(() => {
    if (logRef.value) logRef.value.scrollTop = logRef.value.scrollHeight
  })
}

function start() {
  const commList = communities.value.split('\n').map(s => s.trim()).filter(Boolean)
  if (!token.value.trim()) {
    errorMessage.value = 'Укажите VK токен.'
    return
  }
  if (!oldLink.value.trim() || !newLink.value.trim()) {
    errorMessage.value = 'Старая и новая ссылки не должны быть пустыми.'
    return
  }
  if (!commList.length) {
    errorMessage.value = 'Укажите хотя бы одно сообщество.'
    return
  }

  errorMessage.value = ''
  logLines.value = []
  running.value = true
  appendLog('Запуск скрипта vk_link_rewriter…')
  appendLog('Используйте интерфейс: python app.py → http://127.0.0.1:5000')
  running.value = false
}

function stop() {
  // Остановка только в интерфейсе по адресу http://127.0.0.1:5000
}
</script>

<template>
  <div class="app">
    <header class="header">
      <h1 class="title">VK Link Rewriter</h1>
      <p class="subtitle">Массовая замена ссылок в постах и комментариях сообществ ВКонтакте</p>
    </header>

    <div class="card form-card">
      <div class="field">
        <label for="token">VK токен <span class="required">*</span></label>
        <input
          id="token"
          v-model="token"
          type="password"
          placeholder="Токен с правами wall, groups, offline"
          :disabled="running"
          autocomplete="off"
        />
      </div>
      <div class="row">
        <div class="field half">
          <label for="old">Старая ссылка (что искать)</label>
          <input
            id="old"
            v-model="oldLink"
            type="text"
            placeholder="https://..."
            :disabled="running"
          />
        </div>
        <div class="field half">
          <label for="new">Новая ссылка (на что заменить)</label>
          <input
            id="new"
            v-model="newLink"
            type="text"
            placeholder="https://..."
            :disabled="running"
          />
        </div>
      </div>
      <div class="field">
        <label for="communities">Ссылки или ID сообществ (по одной в строке)</label>
        <textarea
          id="communities"
          v-model="communities"
          rows="4"
          placeholder="https://vk.com/public123&#10;https://vk.com/club123&#10;my_public_name"
          :disabled="running"
        />
      </div>
      <div v-if="errorMessage" class="error-banner">
        {{ errorMessage }}
      </div>
      <div class="actions">
        <button
          type="button"
          class="btn btn-primary"
          :disabled="running"
          @click="start"
        >
          {{ running ? 'Выполняется…' : 'Запустить' }}
        </button>
        <button
          type="button"
          class="btn btn-secondary"
          :disabled="!running"
          @click="stop"
        >
          Остановить
        </button>
      </div>
    </div>

    <div class="card log-card">
      <div class="log-header">
        <span class="log-title">Лог</span>
      </div>
      <div ref="logRef" class="log-view">
        <div v-if="!logLines.length" class="log-placeholder">
          Лог появится после запуска задачи…
        </div>
        <div
          v-for="(line, i) in logLines"
          :key="i"
          class="log-line"
        >{{ line }}</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.app {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.header {
  text-align: center;
  margin-bottom: 0.5rem;
}

.title {
  font-family: var(--font-sans);
  font-size: 1.75rem;
  font-weight: 600;
  letter-spacing: -0.02em;
  margin: 0 0 0.25rem 0;
  background: linear-gradient(135deg, #fff 0%, var(--vk-blue) 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.subtitle {
  color: var(--text-muted);
  font-size: 0.9rem;
  margin: 0;
  font-weight: 400;
}

.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 1.25rem 1.5rem;
}

.form-card {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.field {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.field label {
  font-size: 0.8rem;
  font-weight: 500;
  color: var(--text-muted);
}

.required {
  color: var(--error);
}

.row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}

.half {
  min-width: 0;
}

input,
textarea {
  background: var(--bg-input);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0.6rem 0.75rem;
  color: var(--text);
  font-size: 0.95rem;
  transition: border-color 0.2s, box-shadow 0.2s;
}

input:focus,
textarea:focus {
  outline: none;
  border-color: var(--vk-blue);
  box-shadow: 0 0 0 3px var(--vk-blue-dim);
}

input:disabled,
textarea:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

textarea {
  resize: vertical;
  min-height: 88px;
  font-family: var(--font-mono);
  font-size: 0.85rem;
}

.error-banner {
  background: rgba(239, 68, 68, 0.12);
  border: 1px solid rgba(239, 68, 68, 0.3);
  color: var(--error);
  padding: 0.5rem 0.75rem;
  border-radius: var(--radius-sm);
  font-size: 0.9rem;
}

.actions {
  display: flex;
  gap: 0.75rem;
  margin-top: 0.25rem;
}

.btn {
  padding: 0.6rem 1.25rem;
  border-radius: var(--radius-sm);
  font-weight: 500;
  font-size: 0.95rem;
  cursor: pointer;
  transition: background 0.2s, transform 0.1s;
  border: none;
  font-family: var(--font-sans);
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-primary {
  background: var(--vk-blue);
  color: #fff;
}

.btn-primary:hover:not(:disabled) {
  background: var(--vk-blue-hover);
}

.btn-primary:active:not(:disabled) {
  transform: scale(0.98);
}

.btn-secondary {
  background: var(--bg-input);
  color: var(--text);
  border: 1px solid var(--border);
}

.btn-secondary:hover:not(:disabled) {
  background: var(--border);
}

.log-card {
  display: flex;
  flex-direction: column;
  min-height: 280px;
}

.log-header {
  margin-bottom: 0.75rem;
}

.log-title {
  font-size: 0.8rem;
  font-weight: 500;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.log-view {
  flex: 1;
  background: #0a0a0c;
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 1rem;
  overflow: auto;
  font-family: var(--font-mono);
  font-size: 0.8rem;
  line-height: 1.6;
  min-height: 220px;
}

.log-placeholder {
  color: var(--text-muted);
}

.log-line {
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
