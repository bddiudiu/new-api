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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { AxiosResponse } from 'axios'
import { useState } from 'react'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { api } from '@/lib/api'

import { SettingsPageProvider } from '../../components/settings-page-context'
import { QuotaExpirySection } from '../quota-expiry-section'

function QuotaExpiryTestPage(props: { value: string }) {
  const [actions, setActions] = useState<HTMLDivElement | null>(null)
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { retry: false },
          mutations: { retry: false },
        },
      })
  )
  return (
    <QueryClientProvider client={client}>
      <div ref={setActions} />
      <SettingsPageProvider actionsContainer={actions}>
        <QuotaExpirySection defaultValue={props.value} />
      </SettingsPageProvider>
    </QueryClientProvider>
  )
}

describe('quota expiry settings', () => {
  test('blocks invalid days and saves a valid rule with its existing log type', async () => {
    const put = vi
      .spyOn(api, 'put')
      .mockResolvedValue({ data: { success: true } } as AxiosResponse)
    render(
      <QuotaExpiryTestPage value='[{"label":"Activity","log_type":9,"expire_days":30}]' />
    )
    const days = screen.getByRole('spinbutton', { name: 'Expiry (days)' })
    const save = screen.getByRole('button', { name: 'Save Changes' })
    fireEvent.change(days, { target: { value: '0' } })
    await waitFor(() => expect(save).toBeDisabled())
    expect(put).not.toHaveBeenCalled()
    fireEvent.change(days, { target: { value: '45' } })
    await waitFor(() => expect(save).toBeEnabled())
    expect(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    ).toBeDisabled()
    fireEvent.click(save)
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/option/', {
        key: 'quota_expiry_setting.rules',
        value: '[{"label":"Activity","log_type":9,"expire_days":45}]',
      })
    )
  })

  test('deleting the last rule persists an empty configuration', async () => {
    const put = vi
      .spyOn(api, 'put')
      .mockResolvedValue({ data: { success: true } } as AxiosResponse)
    render(
      <QuotaExpiryTestPage value='[{"label":"Activity","log_type":9,"expire_days":30}]' />
    )
    fireEvent.click(screen.getByRole('button', { name: 'Delete rule 1' }))
    expect(screen.getByText('No expiry rules')).toBeInTheDocument()
    const save = screen.getByRole('button', { name: 'Save Changes' })
    await waitFor(() => expect(save).toBeEnabled())
    fireEvent.click(save)
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/option/', {
        key: 'quota_expiry_setting.rules',
        value: '[]',
      })
    )
  })

  test('an unreadable stored configuration cannot be overwritten or rebuilt', () => {
    render(<QuotaExpiryTestPage value='invalid-json' />)
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Failed to load quota expiry rules'
    )
    expect(screen.getByRole('button', { name: 'Save Changes' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Add rule' })).toBeDisabled()
    expect(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    ).toBeDisabled()
  })

  test('rebuilding requires choosing a start date before confirmation', async () => {
    render(<QuotaExpiryTestPage value='[]' />)
    fireEvent.click(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    )
    expect(await screen.findByRole('alertdialog')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Start rebuild' })).toBeDisabled()
  })
})

async function chooseQuotaRebuildDate() {
  fireEvent.click(screen.getByRole('button', { name: 'Rebuild quota expiry' }))
  fireEvent.click(
    await screen.findByRole('button', { name: 'Select a start date' })
  )
  fireEvent.click(
    await screen.findByRole('button', { name: /Tuesday, September 1st, 2026/ })
  )
  fireEvent.click(screen.getByRole('button', { name: '2026-09-01' }))
  const start = screen.getByRole('button', { name: 'Start rebuild' })
  await waitFor(() => expect(start).toBeEnabled())
  fireEvent.click(start)
}

function quotaTaskResponse(
  status: 'running' | 'completed' | 'failed',
  error?: string
) {
  return {
    data: {
      success: true,
      data: {
        task_id: 'selftest-task',
        status,
        error,
        stats: { scanned_log_count: 8, rebuilt_expiry_count: 3 },
      },
    },
  } as AxiosResponse
}

describe('quota expiry self-test workflows', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
    vi.setSystemTime(new Date('2026-09-07T12:00:00+08:00'))
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  test('chosen date is submitted and completion stops polling', async () => {
    const post = vi
      .spyOn(api, 'post')
      .mockResolvedValue(quotaTaskResponse('running'))
    const get = vi
      .spyOn(api, 'get')
      .mockResolvedValue(quotaTaskResponse('completed'))
    render(<QuotaExpiryTestPage value='[]' />)
    await chooseQuotaRebuildDate()
    await screen.findByText('Quota expiry rebuild completed')
    expect(post).toHaveBeenCalledWith('/api/quota-expiry/rebuild', {
      start_date: '2026-09-01',
    })
    expect(get).toHaveBeenCalledWith('/api/quota-expiry/rebuild/status', {
      params: { task_id: 'selftest-task' },
    })
    expect(
      screen.getByText('Scanned 8 logs; rebuilt 3 expiry records.')
    ).toBeInTheDocument()
    await act(() => vi.advanceTimersByTimeAsync(6000))
    expect(get).toHaveBeenCalledTimes(1)
    expect(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    ).toBeEnabled()
  })

  test('running task polls until failed and permits another rebuild', async () => {
    vi.spyOn(api, 'post').mockResolvedValue(quotaTaskResponse('running'))
    const get = vi
      .spyOn(api, 'get')
      .mockResolvedValueOnce(quotaTaskResponse('running'))
      .mockResolvedValue(quotaTaskResponse('failed', 'Rebuild query failed'))
    render(<QuotaExpiryTestPage value='[]' />)
    await chooseQuotaRebuildDate()
    await waitFor(() => expect(get).toHaveBeenCalledTimes(1))
    expect(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    ).toBeDisabled()
    await act(() => vi.advanceTimersByTimeAsync(2000))
    await screen.findByText('Rebuild query failed')
    expect(
      screen.getByRole('button', { name: 'Rebuild quota expiry' })
    ).toBeEnabled()
    await act(() => vi.advanceTimersByTimeAsync(6000))
    expect(get).toHaveBeenCalledTimes(2)
  })

  test('failed start displays its error and can retry the selected date', async () => {
    const post = vi
      .spyOn(api, 'post')
      .mockResolvedValueOnce({
        data: { success: false, message: 'Start rejected' },
      } as AxiosResponse)
      .mockResolvedValue(quotaTaskResponse('running'))
    vi.spyOn(api, 'get').mockResolvedValue(quotaTaskResponse('completed'))
    render(<QuotaExpiryTestPage value='[]' />)
    await chooseQuotaRebuildDate()
    await screen.findByText('Start rejected')
    expect(screen.getByRole('alertdialog')).toBeInTheDocument()
    const retry = screen.getByRole('button', { name: 'Start rebuild' })
    expect(retry).toBeEnabled()
    fireEvent.click(retry)
    await screen.findByText('Quota expiry rebuild completed')
    expect(post).toHaveBeenCalledTimes(2)
  })

  test('failed status query can retry and show the completed result', async () => {
    vi.spyOn(api, 'post').mockResolvedValue(quotaTaskResponse('running'))
    vi.spyOn(api, 'get')
      .mockRejectedValueOnce(new Error('Status unavailable'))
      .mockResolvedValue(quotaTaskResponse('completed'))
    render(<QuotaExpiryTestPage value='[]' />)
    await chooseQuotaRebuildDate()
    await screen.findByText('Status unavailable')
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await screen.findByText('Quota expiry rebuild completed')
  })

  test('rejects expiry above the backend maximum and saves the maximum', async () => {
    vi.spyOn(api, 'put').mockResolvedValue({ data: { success: true } })
    render(
      <QuotaExpiryTestPage value='[{"label":"Activity","log_type":9,"expire_days":30}]' />
    )
    const save = screen.getByRole('button', { name: 'Save Changes' })
    await waitFor(() => expect(save).toBeEnabled())
    await act(async () => {
      fireEvent.change(
        screen.getByRole('spinbutton', { name: 'Expiry (days)' }),
        { target: { value: '36501' } }
      )
    })
    await waitFor(() => expect(save).toBeDisabled())
    fireEvent.change(
      screen.getByRole('spinbutton', { name: 'Expiry (days)' }),
      { target: { value: '36500' } }
    )
    await waitFor(() => expect(save).toBeEnabled())
    fireEvent.click(save)
    await waitFor(() =>
      expect(api.put).toHaveBeenCalledWith('/api/option/', {
        key: 'quota_expiry_setting.rules',
        value: '[{"label":"Activity","log_type":9,"expire_days":36500}]',
      })
    )
  })
})
