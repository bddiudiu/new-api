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
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { AxiosResponse } from 'axios'
import { useState } from 'react'
import { describe, expect, test, vi } from 'vitest'

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
