<template>
  <BaseDialog
    :show="show"
    :title="t('admin.tlsFingerprintProfiles.title')"
    width="wide"
    @close="$emit('close')"
  >
    <div class="space-y-5">
      <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <p class="text-sm text-gray-500 dark:text-gray-400">
          {{ t('admin.tlsFingerprintProfiles.description') }}
        </p>
        <div class="flex flex-wrap items-center gap-2">
          <button @click="refreshAll" :disabled="loading || captureLoading" class="btn btn-secondary btn-sm">
            <Icon
              name="refresh"
              size="sm"
              :class="['mr-1', (loading || captureLoading) ? 'animate-spin' : '']"
            />
            {{ t('common.refresh') }}
          </button>
          <button @click="showCreateModal = true" class="btn btn-primary btn-sm">
            <Icon name="plus" size="sm" class="mr-1" />
            {{ t('admin.tlsFingerprintProfiles.createProfile') }}
          </button>
        </div>
      </div>

      <section class="rounded-xl border border-blue-200 bg-blue-50/70 p-4 dark:border-blue-900/60 dark:bg-blue-950/20">
        <div class="mb-4 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h4 class="text-sm font-semibold text-blue-950 dark:text-blue-100">
              {{ t('admin.tlsFingerprintProfiles.capture.title') }}
            </h4>
            <p class="mt-1 text-xs text-blue-700 dark:text-blue-300">
              {{ t('admin.tlsFingerprintProfiles.capture.description') }}
            </p>
          </div>
          <div class="flex flex-wrap gap-2">
            <button @click="startCaptureTask" :disabled="captureSubmitting" class="btn btn-primary btn-sm">
              <Icon v-if="captureSubmitting" name="refresh" size="sm" class="mr-1 animate-spin" />
              <Icon v-else name="play" size="sm" class="mr-1" />
              {{ t('admin.tlsFingerprintProfiles.capture.start') }}
            </button>
            <button
              v-if="selectedTask?.status === 'running'"
              @click="stopSelectedTask"
              :disabled="captureSubmitting"
              class="btn btn-secondary btn-sm"
            >
              {{ t('admin.tlsFingerprintProfiles.capture.stop') }}
            </button>
          </div>
        </div>

        <div class="grid gap-4 lg:grid-cols-[1.1fr_1.4fr]">
          <div class="space-y-3">
            <div>
              <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.capture.taskName') }}</label>
              <input
                v-model="captureForm.name"
                type="text"
                class="input"
                :placeholder="t('admin.tlsFingerprintProfiles.capture.taskNamePlaceholder')"
              />
            </div>

            <div>
              <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.capture.uaKeywords') }}</label>
              <textarea
                v-model="captureForm.uaKeywords"
                rows="2"
                class="input font-mono text-xs"
                :placeholder="t('admin.tlsFingerprintProfiles.capture.uaKeywordsPlaceholder')"
              />
              <p class="input-hint text-xs">{{ t('admin.tlsFingerprintProfiles.capture.uaKeywordsHint') }}</p>
            </div>

            <div>
              <div class="mb-2 flex items-center justify-between">
                <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.capture.targets') }}</label>
                <span class="text-xs text-gray-500 dark:text-gray-400">
                  {{ t('admin.tlsFingerprintProfiles.capture.targetsHint') }}
                </span>
              </div>
              <div class="grid grid-cols-2 gap-2 sm:grid-cols-3">
                <label
                  v-for="target in captureTargets"
                  :key="target.platform"
                  class="rounded-lg border border-gray-200 bg-white p-2 dark:border-dark-600 dark:bg-dark-800"
                >
                  <span class="mb-1 block text-xs font-medium text-gray-700 dark:text-gray-300">
                    {{ target.label }}
                  </span>
                  <input
                    v-model.number="target.count"
                    type="number"
                    min="0"
                    max="500"
                    class="input text-sm"
                  />
                </label>
              </div>
              <div class="mt-2 grid grid-cols-[1fr_90px] gap-2">
                <input
                  v-model="customCaptureTarget.platform"
                  type="text"
                  class="input text-sm"
                  :placeholder="t('admin.tlsFingerprintProfiles.capture.customPlatform')"
                />
                <input
                  v-model.number="customCaptureTarget.count"
                  type="number"
                  min="0"
                  max="500"
                  class="input text-sm"
                />
              </div>
            </div>
          </div>

          <div class="space-y-3">
            <div v-if="captureLoading" class="flex items-center justify-center rounded-lg bg-white py-8 dark:bg-dark-800">
              <Icon name="refresh" size="lg" class="animate-spin text-gray-400" />
            </div>

            <div v-else-if="captureTasks.length === 0" class="rounded-lg bg-white p-4 text-sm text-gray-500 dark:bg-dark-800 dark:text-gray-400">
              {{ t('admin.tlsFingerprintProfiles.capture.noTasks') }}
            </div>

            <div v-else class="grid gap-2 sm:grid-cols-2">
              <button
                v-for="task in captureTasks"
                :key="task.id"
                type="button"
                :class="[
                  'rounded-lg border p-3 text-left transition',
                  selectedTask?.id === task.id
                    ? 'border-primary-500 bg-primary-50 dark:border-primary-500 dark:bg-primary-900/20'
                    : 'border-gray-200 bg-white hover:border-primary-300 dark:border-dark-600 dark:bg-dark-800'
                ]"
                @click="selectTask(task.id)"
              >
                <div class="flex items-start justify-between gap-2">
                  <div class="min-w-0">
                    <div class="truncate text-sm font-semibold text-gray-900 dark:text-white">{{ task.name }}</div>
                    <div class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ formatDateTime(task.created_at) }}</div>
                  </div>
                  <span :class="['badge text-xs', captureStatusClass(task.status)]">
                    {{ t(`admin.tlsFingerprintProfiles.capture.status.${task.status}`) }}
                  </span>
                </div>
                <div class="mt-2 space-y-1">
                  <div
                    v-for="platform in Object.keys(task.targets || {})"
                    :key="platform"
                    class="flex items-center justify-between text-xs text-gray-600 dark:text-gray-300"
                  >
                    <span>{{ platform }}</span>
                    <span>{{ task.counts?.[platform] || 0 }} / {{ task.targets?.[platform] || 0 }}</span>
                  </div>
                </div>
              </button>
            </div>

            <div v-if="selectedTask" class="rounded-lg border border-gray-200 bg-white p-3 dark:border-dark-600 dark:bg-dark-800">
              <div class="mb-3 flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <div class="text-sm font-semibold text-gray-900 dark:text-white">{{ selectedTask.name }}</div>
                  <div class="text-xs text-gray-500 dark:text-gray-400">
                    {{ t('admin.tlsFingerprintProfiles.capture.selectedTaskHint') }}
                  </div>
                </div>
                <div class="flex flex-wrap gap-2">
                  <button @click="loadSelectedTaskSamples" :disabled="samplesLoading" class="btn btn-secondary btn-sm">
                    <Icon
                      name="refresh"
                      size="sm"
                      :class="['mr-1', samplesLoading ? 'animate-spin' : '']"
                    />
                    {{ t('common.refresh') }}
                  </button>
                  <button @click="importSelectedSamples" :disabled="importingSamples || selectedSamples.length === 0" class="btn btn-primary btn-sm">
                    <Icon v-if="importingSamples" name="refresh" size="sm" class="mr-1 animate-spin" />
                    {{ t('admin.tlsFingerprintProfiles.capture.importSelected', { count: selectedSamples.length }) }}
                  </button>
                  <button @click="importAllSamples" :disabled="importingSamples || captureSamples.length === 0" class="btn btn-secondary btn-sm">
                    {{ t('admin.tlsFingerprintProfiles.capture.importAll') }}
                  </button>
                </div>
              </div>

              <div class="mb-3 grid gap-2 text-xs lg:grid-cols-[1fr_auto]">
                <div class="rounded-md bg-gray-50 p-2 font-mono text-gray-700 dark:bg-dark-700 dark:text-gray-200">
                  <div class="truncate">{{ submitURL }}</div>
                  <div class="mt-1 truncate">{{ selectedTask.token }}</div>
                  <div class="mt-1 truncate">{{ collectorURL }}</div>
                </div>
                <div class="flex flex-wrap gap-2 lg:flex-col">
                  <a
                    :href="collectorURL"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="btn btn-secondary btn-sm"
                  >
                    {{ t('admin.tlsFingerprintProfiles.form.openCollector') }}
                  </a>
                  <button @click="copyCaptureConfig" class="btn btn-secondary btn-sm">
                    {{ t('admin.tlsFingerprintProfiles.capture.copyConfig') }}
                  </button>
                </div>
              </div>

              <div v-if="samplesLoading" class="flex items-center justify-center py-6">
                <Icon name="refresh" size="lg" class="animate-spin text-gray-400" />
              </div>

              <div v-else-if="captureSamples.length === 0" class="py-4 text-center text-sm text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.capture.noSamples') }}
              </div>

              <div v-else class="max-h-80 overflow-auto rounded-lg border border-gray-200 dark:border-dark-600">
                <table class="min-w-full divide-y divide-gray-200 dark:divide-dark-700">
                  <thead class="sticky top-0 bg-gray-50 dark:bg-dark-700">
                    <tr>
                      <th class="w-8 px-2 py-2">
                        <input
                          type="checkbox"
                          :checked="allSamplesSelected"
                          @change="toggleAllSamples(($event.target as HTMLInputElement).checked)"
                        />
                      </th>
                      <th class="px-2 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                        {{ t('admin.tlsFingerprintProfiles.columns.platform') }}
                      </th>
                      <th class="px-2 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                        {{ t('admin.tlsFingerprintProfiles.capture.userAgent') }}
                      </th>
                      <th class="px-2 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                        {{ t('admin.tlsFingerprintProfiles.capture.hash') }}
                      </th>
                      <th class="px-2 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                        {{ t('admin.tlsFingerprintProfiles.capture.details') }}
                      </th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-200 bg-white dark:divide-dark-700 dark:bg-dark-800">
                    <tr v-for="sample in captureSamples" :key="sample.id">
                      <td class="px-2 py-2">
                        <input v-model="selectedSampleIDs" type="checkbox" :value="sample.id" />
                      </td>
                      <td class="px-2 py-2 text-xs text-gray-700 dark:text-gray-300">{{ sample.platform || 'shared' }}</td>
                      <td class="px-2 py-2">
                        <div class="max-w-sm truncate text-xs text-gray-700 dark:text-gray-300">{{ sample.user_agent || '—' }}</div>
                      </td>
                      <td class="px-2 py-2">
                        <code class="text-xs text-gray-500 dark:text-gray-400">{{ sample.fingerprint_hash.slice(0, 12) }}</code>
                      </td>
                      <td class="px-2 py-2">
                        <details class="text-xs">
                          <summary class="cursor-pointer text-primary-600 dark:text-primary-400">
                            {{ t('admin.tlsFingerprintProfiles.capture.viewDetails') }}
                          </summary>
                          <pre class="mt-2 max-h-56 overflow-auto rounded bg-gray-950 p-2 text-[11px] text-gray-100">{{ formatSampleDetail(sample) }}</pre>
                        </details>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </section>

      <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex items-center gap-2">
          <label class="text-xs font-medium text-gray-600 dark:text-gray-300">
            {{ t('admin.tlsFingerprintProfiles.filterPlatform') }}
          </label>
          <select v-model="profilePlatformFilter" class="input w-44 text-sm">
            <option value="">{{ t('admin.tlsFingerprintProfiles.allPlatforms') }}</option>
            <option v-for="platform in profilePlatforms" :key="platform" :value="platform">
              {{ platform || 'shared' }}
            </option>
          </select>
        </div>
        <div class="text-xs text-gray-500 dark:text-gray-400">
          {{ t('admin.tlsFingerprintProfiles.profileCount', { count: filteredProfiles.length }) }}
        </div>
      </div>

      <div v-if="loading" class="flex items-center justify-center py-8">
        <Icon name="refresh" size="lg" class="animate-spin text-gray-400" />
      </div>

      <div v-else-if="filteredProfiles.length === 0" class="py-8 text-center">
        <div class="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-gray-100 dark:bg-dark-700">
          <Icon name="shield" size="lg" class="text-gray-400" />
        </div>
        <h4 class="mb-1 text-sm font-medium text-gray-900 dark:text-white">
          {{ t('admin.tlsFingerprintProfiles.noProfiles') }}
        </h4>
        <p class="text-sm text-gray-500 dark:text-gray-400">
          {{ t('admin.tlsFingerprintProfiles.createFirstProfile') }}
        </p>
      </div>

      <div v-else class="max-h-96 overflow-auto rounded-lg border border-gray-200 dark:border-dark-600">
        <table class="min-w-full divide-y divide-gray-200 dark:divide-dark-700">
          <thead class="sticky top-0 bg-gray-50 dark:bg-dark-700">
            <tr>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.platform') }}
              </th>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.name') }}
              </th>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.description') }}
              </th>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.grease') }}
              </th>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.alpn') }}
              </th>
              <th class="px-3 py-2 text-left text-xs font-medium uppercase text-gray-500 dark:text-gray-400">
                {{ t('admin.tlsFingerprintProfiles.columns.actions') }}
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200 bg-white dark:divide-dark-700 dark:bg-dark-800">
            <tr v-for="profile in filteredProfiles" :key="profile.id" class="hover:bg-gray-50 dark:hover:bg-dark-700">
              <td class="px-3 py-2">
                <span class="badge badge-gray text-xs">{{ profile.platform || 'shared' }}</span>
              </td>
              <td class="px-3 py-2">
                <div class="text-sm font-medium text-gray-900 dark:text-white">{{ profile.name }}</div>
              </td>
              <td class="px-3 py-2">
                <div v-if="profile.description" class="max-w-xs truncate text-sm text-gray-500 dark:text-gray-400">
                  {{ profile.description }}
                </div>
                <div v-else class="text-xs text-gray-400 dark:text-gray-600">—</div>
              </td>
              <td class="px-3 py-2">
                <Icon
                  :name="profile.enable_grease ? 'check' : 'lock'"
                  size="sm"
                  :class="profile.enable_grease ? 'text-green-500' : 'text-gray-400'"
                />
              </td>
              <td class="px-3 py-2">
                <div v-if="profile.alpn_protocols?.length" class="flex flex-wrap gap-1">
                  <span
                    v-for="proto in profile.alpn_protocols.slice(0, 3)"
                    :key="proto"
                    class="badge badge-primary text-xs"
                  >
                    {{ proto }}
                  </span>
                  <span v-if="profile.alpn_protocols.length > 3" class="text-xs text-gray-500">
                    +{{ profile.alpn_protocols.length - 3 }}
                  </span>
                </div>
                <div v-else class="text-xs text-gray-400 dark:text-gray-600">—</div>
              </td>
              <td class="px-3 py-2">
                <div class="flex items-center gap-1">
                  <button
                    @click="handleEdit(profile)"
                    class="p-1 text-gray-500 hover:text-primary-600 dark:hover:text-primary-400"
                    :title="t('common.edit')"
                  >
                    <Icon name="edit" size="sm" />
                  </button>
                  <button
                    @click="handleDelete(profile)"
                    class="p-1 text-gray-500 hover:text-red-600 dark:hover:text-red-400"
                    :title="t('common.delete')"
                  >
                    <Icon name="trash" size="sm" />
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <template #footer>
      <div class="flex justify-end">
        <button @click="$emit('close')" class="btn btn-secondary">
          {{ t('common.close') }}
        </button>
      </div>
    </template>

    <BaseDialog
      :show="showCreateModal || showEditModal"
      :title="showEditModal ? t('admin.tlsFingerprintProfiles.editProfile') : t('admin.tlsFingerprintProfiles.createProfile')"
      width="wide"
      :z-index="60"
      @close="closeFormModal"
    >
      <form @submit.prevent="handleSubmit" class="space-y-4">
        <div>
          <label class="input-label">{{ t('admin.tlsFingerprintProfiles.form.pasteYaml') }}</label>
          <textarea
            v-model="yamlInput"
            rows="4"
            class="input font-mono text-xs"
            :placeholder="t('admin.tlsFingerprintProfiles.form.pasteYamlPlaceholder')"
            @paste="handleYamlPaste"
          />
          <div class="mt-1 flex items-center gap-2">
            <button type="button" @click="parseYamlInput" class="btn btn-secondary btn-sm">
              {{ t('admin.tlsFingerprintProfiles.form.parseYaml') }}
            </button>
            <p class="text-xs text-gray-500 dark:text-gray-400">
              {{ t('admin.tlsFingerprintProfiles.form.pasteYamlHint') }}
              <a :href="collectorURL" target="_blank" rel="noopener noreferrer" class="text-primary-600 underline hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300">{{ t('admin.tlsFingerprintProfiles.form.openCollector') }}</a>
            </p>
          </div>
        </div>

        <hr class="border-gray-200 dark:border-dark-600" />

        <div class="grid grid-cols-2 gap-4">
          <div>
            <label class="input-label">{{ t('admin.tlsFingerprintProfiles.form.platform') }}</label>
            <input
              v-model="form.platform"
              type="text"
              class="input"
              :placeholder="t('admin.tlsFingerprintProfiles.form.platformPlaceholder')"
            />
          </div>
          <div>
            <label class="input-label">{{ t('admin.tlsFingerprintProfiles.form.name') }}</label>
            <input
              v-model="form.name"
              type="text"
              required
              class="input"
              :placeholder="t('admin.tlsFingerprintProfiles.form.namePlaceholder')"
            />
          </div>
          <div class="col-span-2">
            <label class="input-label">{{ t('admin.tlsFingerprintProfiles.form.description') }}</label>
            <input
              v-model="form.description"
              type="text"
              class="input"
              :placeholder="t('admin.tlsFingerprintProfiles.form.descriptionPlaceholder')"
            />
          </div>
        </div>

        <div>
          <label class="input-label">{{ t('admin.tlsFingerprintProfiles.form.userAgent') }}</label>
          <input
            v-model="form.user_agent"
            type="text"
            class="input font-mono text-sm"
            :placeholder="t('admin.tlsFingerprintProfiles.form.userAgentPlaceholder')"
          />
          <p class="input-hint text-xs">{{ t('admin.tlsFingerprintProfiles.form.userAgentHint') }}</p>
        </div>

        <div class="flex items-center gap-3">
          <button
            type="button"
            @click="form.enable_grease = !form.enable_grease"
            :class="[
              'relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2',
              form.enable_grease ? 'bg-primary-600' : 'bg-gray-200 dark:bg-dark-600'
            ]"
          >
            <span
              :class="[
                'pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out',
                form.enable_grease ? 'translate-x-4' : 'translate-x-0'
              ]"
            />
          </button>
          <div>
            <span class="text-sm font-medium text-gray-700 dark:text-gray-300">
              {{ t('admin.tlsFingerprintProfiles.form.enableGrease') }}
            </span>
            <p class="text-xs text-gray-500 dark:text-gray-400">
              {{ t('admin.tlsFingerprintProfiles.form.enableGreaseHint') }}
            </p>
          </div>
        </div>

        <div class="grid grid-cols-2 gap-4">
          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.cipherSuites') }}</label>
            <textarea v-model="fieldInputs.cipher_suites" rows="2" class="input font-mono text-xs" placeholder="0x1301, 0x1302, 0xc02c" />
            <p class="input-hint text-xs">{{ t('admin.tlsFingerprintProfiles.form.cipherSuitesHint') }}</p>
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.curves') }}</label>
            <textarea v-model="fieldInputs.curves" rows="2" class="input font-mono text-xs" placeholder="29, 23, 24" />
            <p class="input-hint text-xs">{{ t('admin.tlsFingerprintProfiles.form.curvesHint') }}</p>
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.signatureAlgorithms') }}</label>
            <textarea v-model="fieldInputs.signature_algorithms" rows="2" class="input font-mono text-xs" placeholder="0x0403, 0x0804, 0x0401" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.supportedVersions') }}</label>
            <textarea v-model="fieldInputs.supported_versions" rows="2" class="input font-mono text-xs" placeholder="0x0304, 0x0303" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.keyShareGroups') }}</label>
            <textarea v-model="fieldInputs.key_share_groups" rows="2" class="input font-mono text-xs" placeholder="29, 23" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.extensions') }}</label>
            <textarea v-model="fieldInputs.extensions" rows="2" class="input font-mono text-xs" placeholder="0x0000, 0x0005, 0x000a" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.pointFormats') }}</label>
            <textarea v-model="fieldInputs.point_formats" rows="2" class="input font-mono text-xs" placeholder="0" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.pskModes') }}</label>
            <textarea v-model="fieldInputs.psk_modes" rows="2" class="input font-mono text-xs" placeholder="1" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.compressCertAlgos') }}</label>
            <textarea v-model="fieldInputs.compress_cert_algos" rows="2" class="input font-mono text-xs" placeholder="2, 1" />
          </div>

          <div>
            <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.delegatedCredentialsAlgorithms') }}</label>
            <textarea v-model="fieldInputs.delegated_credentials_algorithms" rows="2" class="input font-mono text-xs" placeholder="0x0403, 0x0804" />
          </div>
        </div>

        <div>
          <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.alpnProtocols') }}</label>
          <textarea v-model="fieldInputs.alpn_protocols" rows="2" class="input font-mono text-xs" placeholder="h2, http/1.1" />
        </div>

        <div>
          <label class="input-label text-xs">{{ t('admin.tlsFingerprintProfiles.form.applicationSettingsProtocols') }}</label>
          <textarea v-model="fieldInputs.application_settings_protocols" rows="2" class="input font-mono text-xs" placeholder="h2" />
        </div>
      </form>

      <template #footer>
        <div class="flex justify-end gap-3">
          <button @click="closeFormModal" type="button" class="btn btn-secondary">
            {{ t('common.cancel') }}
          </button>
          <button @click="handleSubmit" :disabled="submitting" class="btn btn-primary">
            <Icon v-if="submitting" name="refresh" size="sm" class="mr-1 animate-spin" />
            {{ showEditModal ? t('common.update') : t('common.create') }}
          </button>
        </div>
      </template>
    </BaseDialog>

    <ConfirmDialog
      :show="showDeleteDialog"
      :title="t('admin.tlsFingerprintProfiles.deleteProfile')"
      :message="t('admin.tlsFingerprintProfiles.deleteConfirmMessage', { name: deletingProfile?.name })"
      :confirm-text="t('common.delete')"
      :cancel-text="t('common.cancel')"
      :danger="true"
      @confirm="confirmDelete"
      @cancel="showDeleteDialog = false"
    />
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAppStore } from '@/stores/app'
import { adminAPI } from '@/api/admin'
import type {
  TLSFingerprintCaptureSample,
  TLSFingerprintCaptureTask,
  TLSFingerprintProfile
} from '@/api/admin/tlsFingerprintProfile'
import { formatDateTime } from '@/utils/format'
import { parseTLSFingerprintYaml } from '@/utils/tlsFingerprintYaml'
import BaseDialog from '@/components/common/BaseDialog.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import Icon from '@/components/icons/Icon.vue'

const props = defineProps<{
  show: boolean
}>()

defineEmits<{
  close: []
}>()

const { t } = useI18n()
const appStore = useAppStore()

const profiles = ref<TLSFingerprintProfile[]>([])
const loading = ref(false)
const submitting = ref(false)
const showCreateModal = ref(false)
const showEditModal = ref(false)
const showDeleteDialog = ref(false)
const editingProfile = ref<TLSFingerprintProfile | null>(null)
const deletingProfile = ref<TLSFingerprintProfile | null>(null)
const yamlInput = ref('')
const profilePlatformFilter = ref('')

const captureTasks = ref<TLSFingerprintCaptureTask[]>([])
const selectedTaskID = ref<number | null>(null)
const captureSamples = ref<TLSFingerprintCaptureSample[]>([])
const selectedSampleIDs = ref<number[]>([])
const captureLoading = ref(false)
const captureSubmitting = ref(false)
const samplesLoading = ref(false)
const importingSamples = ref(false)
let capturePollTimer: ReturnType<typeof setInterval> | null = null

const captureForm = reactive({
  name: 'Codex TLS fingerprint capture',
  uaKeywords: 'codex, Codex Desktop, codex-tui, codex_exec'
})

const captureTargets = reactive([
  { platform: 'openai', label: 'OpenAI / Codex', count: 100 },
  { platform: 'anthropic', label: 'Anthropic / Claude', count: 0 },
  { platform: 'gemini', label: 'Gemini', count: 0 },
  { platform: 'kiro', label: 'Kiro', count: 0 },
  { platform: 'antigravity', label: 'Antigravity', count: 0 }
])

const customCaptureTarget = reactive({
  platform: '',
  count: 0
})

const fieldInputs = reactive({
  cipher_suites: '',
  curves: '',
  point_formats: '',
  signature_algorithms: '',
  alpn_protocols: '',
  supported_versions: '',
  key_share_groups: '',
  psk_modes: '',
  extensions: '',
  compress_cert_algos: '',
  delegated_credentials_algorithms: '',
  application_settings_protocols: ''
})

const form = reactive({
  platform: 'openai',
  name: '',
  description: null as string | null,
  user_agent: '',
  enable_grease: false
})

const selectedTask = computed(() => {
  return captureTasks.value.find(task => task.id === selectedTaskID.value) || null
})

const submitURL = computed(() => {
  const base = typeof window === 'undefined' ? '' : window.location.origin
  return `${base}/api/v1/tls-fingerprint-captures/submit`
})

const collectorURL = computed(() => {
  const base = typeof window === 'undefined' ? '' : window.location.origin
  const params = new URLSearchParams()
  params.set('endpoint', submitURL.value)

  if (selectedTask.value?.token) {
    params.set('token', selectedTask.value.token)
  }

  const firstTargetPlatform = Object.keys(selectedTask.value?.targets || {})[0]
  params.set('platform', firstTargetPlatform || form.platform || 'openai')

  const firstUAKeyword = parseStringArray(captureForm.uaKeywords)[0]
  if (firstUAKeyword) {
    params.set('user_agent', firstUAKeyword)
  }

  return `${base}/tls-fingerprint-collector?${params.toString()}`
})

const selectedSamples = computed(() => {
  const selected = new Set(selectedSampleIDs.value)
  return captureSamples.value.filter(sample => selected.has(sample.id))
})

const allSamplesSelected = computed(() => {
  return captureSamples.value.length > 0 && selectedSampleIDs.value.length === captureSamples.value.length
})

const profilePlatforms = computed(() => {
  return Array.from(new Set(profiles.value.map(profile => profile.platform || '').filter(Boolean))).sort()
})

const filteredProfiles = computed(() => {
  if (!profilePlatformFilter.value) {
    return profiles.value
  }
  return profiles.value.filter(profile => (profile.platform || '') === profilePlatformFilter.value)
})

watch(
  () => props.show,
  (newVal) => {
    if (newVal) {
      refreshAll()
      startCapturePolling()
    } else {
      stopCapturePolling()
    }
  },
  { immediate: true }
)

onBeforeUnmount(() => {
  stopCapturePolling()
})

const refreshAll = async () => {
  await Promise.all([loadProfiles(), loadCaptureTasks()])
}

const loadProfiles = async () => {
  loading.value = true
  try {
    profiles.value = await adminAPI.tlsFingerprintProfiles.list()
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.loadFailed'))
    console.error('Error loading TLS fingerprint profiles:', error)
  } finally {
    loading.value = false
  }
}

const loadCaptureTasks = async () => {
  captureLoading.value = true
  try {
    captureTasks.value = await adminAPI.tlsFingerprintProfiles.listCaptureTasks()
    if (!selectedTaskID.value && captureTasks.value.length > 0) {
      selectedTaskID.value = captureTasks.value[0].id
      await loadSelectedTaskSamples()
    }
    if (selectedTaskID.value && !captureTasks.value.some(task => task.id === selectedTaskID.value)) {
      selectedTaskID.value = captureTasks.value[0]?.id || null
      await loadSelectedTaskSamples()
    }
    updateCapturePollingState()
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.capture.loadFailed'))
    console.error('Error loading TLS fingerprint capture tasks:', error)
  } finally {
    captureLoading.value = false
  }
}

const loadSelectedTaskSamples = async () => {
  if (!selectedTaskID.value) {
    captureSamples.value = []
    selectedSampleIDs.value = []
    return
  }
  samplesLoading.value = true
  try {
    captureSamples.value = await adminAPI.tlsFingerprintProfiles.listCaptureSamples(selectedTaskID.value)
    const available = new Set(captureSamples.value.map(sample => sample.id))
    selectedSampleIDs.value = selectedSampleIDs.value.filter(id => available.has(id))
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.capture.samplesLoadFailed'))
    console.error('Error loading TLS fingerprint capture samples:', error)
  } finally {
    samplesLoading.value = false
  }
}

const selectTask = async (taskID: number) => {
  selectedTaskID.value = taskID
  selectedSampleIDs.value = []
  await loadSelectedTaskSamples()
}

const startCapturePolling = () => {
  if (capturePollTimer) {
    return
  }
  capturePollTimer = setInterval(async () => {
    if (!props.show) {
      return
    }
    await loadCaptureTasks()
    if (selectedTaskID.value) {
      await loadSelectedTaskSamples()
    }
  }, 3000)
}

const stopCapturePolling = () => {
  if (capturePollTimer) {
    clearInterval(capturePollTimer)
    capturePollTimer = null
  }
}

const updateCapturePollingState = () => {
  if (captureTasks.value.some(task => task.status === 'running')) {
    startCapturePolling()
    return
  }
  stopCapturePolling()
}

const buildCaptureTargets = (): Record<string, number> => {
  const targets: Record<string, number> = {}
  for (const target of captureTargets) {
    const count = Number(target.count || 0)
    if (count > 0) {
      targets[target.platform] = count
    }
  }
  const customPlatform = customCaptureTarget.platform.trim()
  const customCount = Number(customCaptureTarget.count || 0)
  if (customPlatform && customCount > 0) {
    targets[customPlatform] = customCount
  }
  return targets
}

const startCaptureTask = async () => {
  const targets = buildCaptureTargets()
  if (Object.keys(targets).length === 0) {
    appStore.showError(t('admin.tlsFingerprintProfiles.capture.targetRequired'))
    return
  }
  captureSubmitting.value = true
  try {
    const task = await adminAPI.tlsFingerprintProfiles.startCaptureTask({
      name: captureForm.name.trim() || undefined,
      targets,
      ua_keywords: parseStringArray(captureForm.uaKeywords)
    })
    captureTasks.value = [task, ...captureTasks.value.filter(item => item.id !== task.id)]
    selectedTaskID.value = task.id
    captureSamples.value = []
    selectedSampleIDs.value = []
    startCapturePolling()
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.capture.startSuccess'))
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.capture.startFailed'))
    console.error('Error starting TLS fingerprint capture task:', error)
  } finally {
    captureSubmitting.value = false
  }
}

const stopSelectedTask = async () => {
  if (!selectedTask.value) return
  captureSubmitting.value = true
  try {
    const task = await adminAPI.tlsFingerprintProfiles.stopCaptureTask(selectedTask.value.id)
    captureTasks.value = captureTasks.value.map(item => item.id === task.id ? task : item)
    updateCapturePollingState()
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.capture.stopSuccess'))
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.capture.stopFailed'))
    console.error('Error stopping TLS fingerprint capture task:', error)
  } finally {
    captureSubmitting.value = false
  }
}

const importSelectedSamples = async () => {
  if (!selectedTaskID.value || selectedSamples.value.length === 0) {
    return
  }
  await importSamples(selectedSampleIDs.value)
}

const importAllSamples = async () => {
  if (!selectedTaskID.value || captureSamples.value.length === 0) {
    return
  }
  await importSamples([])
}

const importSamples = async (sampleIDs: number[]) => {
  if (!selectedTaskID.value) return
  importingSamples.value = true
  try {
    const result = await adminAPI.tlsFingerprintProfiles.importCaptureTaskSamples(selectedTaskID.value, {
      sample_ids: sampleIDs
    })
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.capture.importSuccess', {
      imported: result.imported,
      duplicates: result.duplicates
    }))
    selectedSampleIDs.value = []
    await loadProfiles()
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.capture.importFailed'))
    console.error('Error importing TLS fingerprint capture samples:', error)
  } finally {
    importingSamples.value = false
  }
}

const toggleAllSamples = (checked: boolean) => {
  selectedSampleIDs.value = checked ? captureSamples.value.map(sample => sample.id) : []
}

const copyCaptureConfig = async () => {
  if (!selectedTask.value) return
  const config = JSON.stringify({
    collector_url: collectorURL.value,
    endpoint: submitURL.value,
    token: selectedTask.value.token,
    targets: selectedTask.value.targets,
    ua_keywords: selectedTask.value.ua_keywords
  }, null, 2)
  try {
    await navigator.clipboard.writeText(config)
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.capture.copySuccess'))
  } catch {
    appStore.showError(t('admin.tlsFingerprintProfiles.capture.copyFailed'))
  }
}

const captureStatusClass = (status: string): string => {
  if (status === 'running') return 'badge-primary'
  if (status === 'completed') return 'badge-success'
  if (status === 'stopped') return 'badge-gray'
  return 'badge-gray'
}

const formatSampleDetail = (sample: TLSFingerprintCaptureSample): string => {
  return JSON.stringify({
    platform: sample.platform,
    user_agent: sample.user_agent,
    fingerprint_hash: sample.fingerprint_hash,
    profile: sample.profile,
    raw_payload: sample.raw_payload
  }, null, 2)
}

const resetForm = () => {
  form.platform = 'openai'
  form.name = ''
  form.description = null
  form.user_agent = ''
  form.enable_grease = false
  fieldInputs.cipher_suites = ''
  fieldInputs.curves = ''
  fieldInputs.point_formats = ''
  fieldInputs.signature_algorithms = ''
  fieldInputs.alpn_protocols = ''
  fieldInputs.supported_versions = ''
  fieldInputs.key_share_groups = ''
  fieldInputs.psk_modes = ''
  fieldInputs.extensions = ''
  fieldInputs.compress_cert_algos = ''
  fieldInputs.delegated_credentials_algorithms = ''
  fieldInputs.application_settings_protocols = ''
  yamlInput.value = ''
}

const parseYamlInput = () => {
  const text = yamlInput.value.trim()
  if (!text) return

  const parsed = parseTLSFingerprintYaml(text)
  if (parsed.platform !== undefined) form.platform = parsed.platform
  if (parsed.name) form.name = parsed.name
  if (parsed.description !== undefined) form.description = parsed.description
  if (parsed.user_agent !== undefined) form.user_agent = parsed.user_agent
  if (parsed.enable_grease !== undefined) form.enable_grease = parsed.enable_grease
  if (parsed.cipher_suites !== undefined) fieldInputs.cipher_suites = parsed.cipher_suites
  if (parsed.curves !== undefined) fieldInputs.curves = parsed.curves
  if (parsed.point_formats !== undefined) fieldInputs.point_formats = parsed.point_formats
  if (parsed.signature_algorithms !== undefined) fieldInputs.signature_algorithms = parsed.signature_algorithms
  if (parsed.alpn_protocols !== undefined) fieldInputs.alpn_protocols = parsed.alpn_protocols
  if (parsed.supported_versions !== undefined) fieldInputs.supported_versions = parsed.supported_versions
  if (parsed.key_share_groups !== undefined) fieldInputs.key_share_groups = parsed.key_share_groups
  if (parsed.psk_modes !== undefined) fieldInputs.psk_modes = parsed.psk_modes
  if (parsed.extensions !== undefined) fieldInputs.extensions = parsed.extensions
  if (parsed.compress_cert_algos !== undefined) fieldInputs.compress_cert_algos = parsed.compress_cert_algos
  if (parsed.delegated_credentials_algorithms !== undefined) fieldInputs.delegated_credentials_algorithms = parsed.delegated_credentials_algorithms
  if (parsed.application_settings_protocols !== undefined) fieldInputs.application_settings_protocols = parsed.application_settings_protocols

  if (parsed.name) {
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.form.yamlParsed'))
  } else {
    appStore.showError(t('admin.tlsFingerprintProfiles.form.yamlParseFailed'))
  }
}

const handleYamlPaste = () => {
  setTimeout(() => parseYamlInput(), 50)
}

const closeFormModal = () => {
  showCreateModal.value = false
  showEditModal.value = false
  editingProfile.value = null
  resetForm()
}

const parseNumericArray = (input: string): number[] => {
  if (!input.trim()) return []
  return input
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0)
    .map(s => s.startsWith('0x') || s.startsWith('0X') ? parseInt(s, 16) : parseInt(s, 10))
    .filter(n => !isNaN(n))
}

const parseStringArray = (input: string): string[] => {
  if (!input.trim()) return []
  return input
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0)
}

const formatHex = (n: number): string => '0x' + n.toString(16).padStart(4, '0')
const formatNumericArray = (arr: number[] | null | undefined): string => (arr ?? []).map(formatHex).join(', ')
const formatPlainNumericArray = (arr: number[] | null | undefined): string => (arr ?? []).join(', ')

const handleEdit = (profile: TLSFingerprintProfile) => {
  editingProfile.value = profile
  form.platform = profile.platform || ''
  form.name = profile.name
  form.description = profile.description
  form.user_agent = profile.user_agent || ''
  form.enable_grease = profile.enable_grease
  fieldInputs.cipher_suites = formatNumericArray(profile.cipher_suites)
  fieldInputs.curves = formatPlainNumericArray(profile.curves)
  fieldInputs.point_formats = formatPlainNumericArray(profile.point_formats)
  fieldInputs.signature_algorithms = formatNumericArray(profile.signature_algorithms)
  fieldInputs.alpn_protocols = (profile.alpn_protocols ?? []).join(', ')
  fieldInputs.supported_versions = formatNumericArray(profile.supported_versions)
  fieldInputs.key_share_groups = formatPlainNumericArray(profile.key_share_groups)
  fieldInputs.psk_modes = formatPlainNumericArray(profile.psk_modes)
  fieldInputs.extensions = formatNumericArray(profile.extensions)
  fieldInputs.compress_cert_algos = formatPlainNumericArray(profile.compress_cert_algos)
  fieldInputs.delegated_credentials_algorithms = formatNumericArray(profile.delegated_credentials_algorithms)
  fieldInputs.application_settings_protocols = (profile.application_settings_protocols ?? []).join(', ')
  showEditModal.value = true
}

const handleDelete = (profile: TLSFingerprintProfile) => {
  deletingProfile.value = profile
  showDeleteDialog.value = true
}

const handleSubmit = async () => {
  if (!form.name.trim()) {
    appStore.showError(t('admin.tlsFingerprintProfiles.form.name') + ' ' + t('common.required'))
    return
  }

  submitting.value = true
  try {
    const data = {
      platform: form.platform.trim(),
      name: form.name.trim(),
      description: form.description?.trim() || null,
      user_agent: form.user_agent.trim(),
      enable_grease: form.enable_grease,
      cipher_suites: parseNumericArray(fieldInputs.cipher_suites),
      curves: parseNumericArray(fieldInputs.curves),
      point_formats: parseNumericArray(fieldInputs.point_formats),
      signature_algorithms: parseNumericArray(fieldInputs.signature_algorithms),
      alpn_protocols: parseStringArray(fieldInputs.alpn_protocols),
      supported_versions: parseNumericArray(fieldInputs.supported_versions),
      key_share_groups: parseNumericArray(fieldInputs.key_share_groups),
      psk_modes: parseNumericArray(fieldInputs.psk_modes),
      extensions: parseNumericArray(fieldInputs.extensions),
      compress_cert_algos: parseNumericArray(fieldInputs.compress_cert_algos),
      delegated_credentials_algorithms: parseNumericArray(fieldInputs.delegated_credentials_algorithms),
      application_settings_protocols: parseStringArray(fieldInputs.application_settings_protocols)
    }

    if (showEditModal.value && editingProfile.value) {
      await adminAPI.tlsFingerprintProfiles.update(editingProfile.value.id, data)
      appStore.showSuccess(t('admin.tlsFingerprintProfiles.updateSuccess'))
    } else {
      await adminAPI.tlsFingerprintProfiles.create(data)
      appStore.showSuccess(t('admin.tlsFingerprintProfiles.createSuccess'))
    }

    closeFormModal()
    await loadProfiles()
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.saveFailed'))
    console.error('Error saving TLS fingerprint profile:', error)
  } finally {
    submitting.value = false
  }
}

const confirmDelete = async () => {
  if (!deletingProfile.value) return

  try {
    await adminAPI.tlsFingerprintProfiles.delete(deletingProfile.value.id)
    appStore.showSuccess(t('admin.tlsFingerprintProfiles.deleteSuccess'))
    showDeleteDialog.value = false
    deletingProfile.value = null
    await loadProfiles()
  } catch (error: any) {
    appStore.showError(error?.message || t('admin.tlsFingerprintProfiles.deleteFailed'))
    console.error('Error deleting TLS fingerprint profile:', error)
  }
}
</script>
