/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation, useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { useFieldArray, useForm } from 'react-hook-form'
import { useTranslation } from 'react-i18next'
import { z } from 'zod'

import { ConfirmDialog } from '@/components/confirm-dialog'
import { DatePicker } from '@/components/date-picker'
import { EmptyState } from '@/components/empty-state'
import { Button } from '@/components/ui/button'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { api } from '@/lib/api'
import dayjs from '@/lib/dayjs'

import { SettingsForm } from '../components/settings-form-layout'
import { SettingsPageFormActions } from '../components/settings-page-context'
import { SettingsSection } from '../components/settings-section'
import { useResetForm } from '../hooks/use-reset-form'
import { useUpdateOption } from '../hooks/use-update-option'

const ruleSchema = z.object({
  label: z.string().trim().min(1),
  log_type: z.number().int().positive(),
  expire_days: z.number().int().positive().max(36500),
})
const rulesSchema = z
  .array(ruleSchema)
  .refine(
    (rules) => new Set(rules.map((rule) => rule.log_type)).size === rules.length
  )
const formSchema = z.object({ rules: rulesSchema })
type FormValues = z.infer<typeof formSchema>

type RebuildTask = {
  task_id: string
  status: 'running' | 'completed' | 'failed'
  error?: string
  stats?: { scanned_log_count: number; rebuilt_expiry_count: number }
}
type RebuildResponse = {
  success: boolean
  message?: string
  data?: RebuildTask
}

export function QuotaExpirySection(props: { defaultValue: string }) {
  const { t } = useTranslation()
  const updateOption = useUpdateOption()
  const initial = useMemo(() => {
    try {
      return rulesSchema.safeParse(JSON.parse(props.defaultValue || '[]'))
    } catch {
      return rulesSchema.safeParse(null)
    }
  }, [props.defaultValue])
  const defaults = { rules: initial.success ? initial.data : [] }
  const form = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: defaults,
    mode: 'onChange',
  })
  useResetForm(form, defaults)
  const rules = useFieldArray({ control: form.control, name: 'rules' })
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [startDate, setStartDate] = useState<Date>()
  const [taskId, setTaskId] = useState<string>()

  const rebuild = useMutation({
    mutationFn: async () => {
      if (!startDate) throw new Error(t('Select a start date'))
      const response = await api.post<RebuildResponse>(
        '/api/quota-expiry/rebuild',
        {
          start_date: dayjs(startDate).format('YYYY-MM-DD'),
        }
      )
      if (!response.data.success || !response.data.data) {
        throw new Error(
          response.data.message || t('Failed to start quota expiry rebuild')
        )
      }
      return response.data.data
    },
    onSuccess: (task) => {
      setTaskId(task.task_id)
      setConfirmOpen(false)
    },
  })
  const status = useQuery({
    queryKey: ['quota-expiry-rebuild', taskId],
    enabled: Boolean(taskId),
    queryFn: async () => {
      const response = await api.get<RebuildResponse>(
        '/api/quota-expiry/rebuild/status',
        {
          params: { task_id: taskId },
        }
      )
      if (!response.data.success || !response.data.data) {
        throw new Error(
          response.data.message || t('Failed to query rebuild status')
        )
      }
      return response.data.data
    },
    refetchInterval: (query) =>
      query.state.data?.status === 'running' ? 2000 : false,
  })
  const rebuilding =
    rebuild.isPending ||
    Boolean(
      taskId &&
      status.data?.status !== 'completed' &&
      status.data?.status !== 'failed'
    )
  const save = form.handleSubmit(async (values) => {
    const response = await updateOption.mutateAsync({
      key: 'quota_expiry_setting.rules',
      value: JSON.stringify(values.rules),
    })
    if (response.success) form.reset(values)
  })

  return (
    <SettingsSection title={t('Quota Expiry')}>
      <p className='text-muted-foreground text-sm'>
        {t(
          'Configure expiry by log type. Unused quota is voided when it expires.'
        )}
      </p>
      {!initial.success && (
        <p role='alert'>{t('Failed to load quota expiry rules')}</p>
      )}
      <SettingsForm onSubmit={save}>
        <SettingsPageFormActions
          onSave={save}
          isSaving={updateOption.isPending}
          isSaveDisabled={
            !initial.success || !form.formState.isValid || rebuilding
          }
        />
        <FieldGroup>
          {rules.fields.map((rule, index) => (
            <fieldset
              key={rule.id}
              className='grid gap-4 rounded-lg border p-4 sm:grid-cols-3'
              disabled={updateOption.isPending || rebuilding}
            >
              <legend className='px-1 text-sm'>
                {t('Rule {{number}}', { number: index + 1 })}
              </legend>
              <Field
                data-invalid={Boolean(
                  form.formState.errors.rules?.[index]?.label
                )}
              >
                <FieldLabel htmlFor={`${rule.id}-label`}>
                  {t('Log type name')}
                </FieldLabel>
                <Input
                  id={`${rule.id}-label`}
                  {...form.register(`rules.${index}.label`)}
                  aria-invalid={Boolean(
                    form.formState.errors.rules?.[index]?.label
                  )}
                />
              </Field>
              <Field
                data-invalid={Boolean(
                  form.formState.errors.rules?.[index]?.log_type
                )}
              >
                <FieldLabel htmlFor={`${rule.id}-type`}>
                  {t('Log type value')}
                </FieldLabel>
                <Input
                  id={`${rule.id}-type`}
                  type='number'
                  min={1}
                  step={1}
                  {...form.register(`rules.${index}.log_type`, {
                    valueAsNumber: true,
                  })}
                  aria-invalid={Boolean(
                    form.formState.errors.rules?.[index]?.log_type
                  )}
                />
              </Field>
              <Field
                data-invalid={Boolean(
                  form.formState.errors.rules?.[index]?.expire_days
                )}
              >
                <FieldLabel htmlFor={`${rule.id}-days`}>
                  {t('Expiry (days)')}
                </FieldLabel>
                <Input
                  id={`${rule.id}-days`}
                  max={36500}
                  type='number'
                  min={1}
                  step={1}
                  {...form.register(`rules.${index}.expire_days`, {
                    valueAsNumber: true,
                  })}
                  aria-invalid={Boolean(
                    form.formState.errors.rules?.[index]?.expire_days
                  )}
                />
              </Field>
              <Button
                type='button'
                variant='outline'
                onClick={() => rules.remove(index)}
                aria-label={t('Delete rule {{number}}', { number: index + 1 })}
              >
                {t('Delete')}
              </Button>
            </fieldset>
          ))}
        </FieldGroup>
        {rules.fields.length === 0 && (
          <EmptyState title={t('No expiry rules')} />
        )}
        {form.formState.errors.rules && (
          <p role='alert' className='text-destructive text-sm'>
            {t(
              'Each rule needs a name, a unique positive log type, and a positive number of days.'
            )}
          </p>
        )}
        <div className='flex flex-wrap gap-2'>
          <Button
            type='button'
            variant='outline'
            disabled={!initial.success || updateOption.isPending || rebuilding}
            onClick={() =>
              rules.append({ label: '', log_type: 0, expire_days: 30 })
            }
          >
            {t('Add rule')}
          </Button>
          <Button
            type='button'
            variant='outline'
            onClick={() => setConfirmOpen(true)}
            disabled={
              !initial.success ||
              rebuilding ||
              form.formState.isDirty ||
              updateOption.isPending
            }
          >
            {t('Rebuild quota expiry')}
          </Button>
        </div>
        {form.formState.isDirty && (
          <p className='text-muted-foreground text-sm'>
            {t('Save rules before rebuilding.')}
          </p>
        )}
      </SettingsForm>
      {rebuilding && <p role='status'>{t('Rebuilding quota expiry...')}</p>}
      {status.data?.status === 'completed' && (
        <p role='status'>{t('Quota expiry rebuild completed')}</p>
      )}
      {status.data?.stats && (
        <p className='text-muted-foreground text-sm'>
          {t('Scanned {{scanned}} logs; rebuilt {{rebuilt}} expiry records.', {
            scanned: status.data.stats.scanned_log_count,
            rebuilt: status.data.stats.rebuilt_expiry_count,
          })}
        </p>
      )}
      {status.data?.status === 'failed' && (
        <p role='alert'>
          {status.data.error || t('Quota expiry rebuild failed')}
        </p>
      )}
      {status.isError && (
        <div role='alert'>
          <p>{status.error.message}</p>
          <Button variant='outline' onClick={() => void status.refetch()}>
            {t('Retry')}
          </Button>
        </div>
      )}
      <ConfirmDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title={t('Rebuild quota expiry')}
        desc={t(
          'Rebuild records from the selected date. Expired unused quota will be deducted. Save rules before continuing.'
        )}
        confirmText={t('Start rebuild')}
        destructive
        disabled={!startDate || rebuilding}
        isLoading={rebuild.isPending}
        handleConfirm={() => rebuild.mutate()}
      >
        <DatePicker
          selected={startDate}
          onSelect={setStartDate}
          placeholder={t('Select a start date')}
        />
        {rebuild.isError && <p role='alert'>{rebuild.error.message}</p>}
      </ConfirmDialog>
    </SettingsSection>
  )
}
