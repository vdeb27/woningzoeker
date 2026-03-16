import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchWatchlist,
  removeFromWatchlist,
  updateWatchlistItem,
  WatchlistItem,
  formatPrijs,
} from '../services/api'

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    interested: 'bg-blue-100 text-blue-800',
    viewed: 'bg-purple-100 text-purple-800',
    bid: 'bg-yellow-100 text-yellow-800',
    rejected: 'bg-gray-100 text-gray-800',
  }
  const labels: Record<string, string> = {
    interested: 'Geinteresseerd',
    viewed: 'Bezichtigd',
    bid: 'Bod uitgebracht',
    rejected: 'Afgewezen',
  }

  return (
    <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors[status] || 'bg-gray-100'}`}>
      {labels[status] || status}
    </span>
  )
}

function PriorityStars({ priority, onChange }: { priority: number; onChange: (p: number) => void }) {
  return (
    <div className="flex space-x-1">
      {[1, 2, 3].map((star) => (
        <button
          key={star}
          onClick={() => onChange(star === priority ? 0 : star)}
          className={`text-lg ${star <= priority ? 'text-yellow-400' : 'text-gray-300'}`}
        >
          ★
        </button>
      ))}
    </div>
  )
}

function WatchlistCard({
  item,
  onUpdate,
  onRemove,
}: {
  item: WatchlistItem
  onUpdate: (id: number, data: Partial<WatchlistItem>) => void
  onRemove: (id: number) => void
}) {
  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex justify-between items-start mb-3">
        <div>
          <h3 className="font-semibold text-gray-900">{item.woning_adres}</h3>
          {item.woning_vraagprijs && (
            <div>
              <div className="text-primary-700 font-medium">
                {formatPrijs(item.woning_vraagprijs)}
              </div>
              {item.woning_woonoppervlakte && item.woning_woonoppervlakte > 0 && (
                <div className="text-sm font-medium text-primary-600">
                  {formatPrijs(Math.round(item.woning_vraagprijs / item.woning_woonoppervlakte))}/m²
                </div>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center space-x-2">
          <StatusBadge status={item.status} />
          <PriorityStars
            priority={item.prioriteit}
            onChange={(p) => onUpdate(item.id, { prioriteit: p })}
          />
        </div>
      </div>

      {item.notities && (
        <div className="bg-gray-50 rounded p-2 text-sm text-gray-600 mb-3">
          {item.notities}
        </div>
      )}

      <div className="flex justify-between items-center text-sm">
        <span className="text-gray-400">
          Toegevoegd: {new Date(item.added_at).toLocaleDateString('nl-NL')}
        </span>
        <div className="flex space-x-2">
          <select
            value={item.status}
            onChange={(e) => onUpdate(item.id, { status: e.target.value })}
            className="text-sm border border-gray-300 rounded px-2 py-1"
          >
            <option value="interested">Geinteresseerd</option>
            <option value="viewed">Bezichtigd</option>
            <option value="bid">Bod uitgebracht</option>
            <option value="rejected">Afgewezen</option>
          </select>
          <button
            onClick={() => onRemove(item.id)}
            className="text-red-600 hover:text-red-800"
          >
            Verwijderen
          </button>
        </div>
      </div>
    </div>
  )
}

type SortOption = 'prioriteit' | 'prijs' | 'prijs_m2'

export default function WatchlistPage() {
  const queryClient = useQueryClient()
  const [sortBy, setSortBy] = useState<SortOption>('prioriteit')

  const { data: items, isLoading, error } = useQuery({
    queryKey: ['watchlist'],
    queryFn: fetchWatchlist,
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: Partial<WatchlistItem> }) =>
      updateWatchlistItem(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist'] })
    },
  })

  const removeMutation = useMutation({
    mutationFn: removeFromWatchlist,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist'] })
    },
  })

  const handleUpdate = (id: number, data: Partial<WatchlistItem>) => {
    updateMutation.mutate({ id, data })
  }

  const handleRemove = (id: number) => {
    if (window.confirm('Weet je zeker dat je deze woning van je watchlist wilt verwijderen?')) {
      removeMutation.mutate(id)
    }
  }

  // Group by status
  const grouped = items?.reduce((acc, item) => {
    const status = item.status
    if (!acc[status]) acc[status] = []
    acc[status].push(item)
    return acc
  }, {} as Record<string, WatchlistItem[]>)

  const statusOrder = ['interested', 'viewed', 'bid', 'rejected']
  const statusLabels: Record<string, string> = {
    interested: 'Geinteresseerd',
    viewed: 'Bezichtigd',
    bid: 'Bod uitgebracht',
    rejected: 'Afgewezen',
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Watchlist</h1>
      <p className="text-gray-600 mb-4">Volg woningen die je interessant vindt</p>

      {items && items.length > 0 && (
        <div className="flex items-center gap-2 mb-6">
          <span className="text-sm text-gray-600">Sorteer op:</span>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as SortOption)}
            className="text-sm border border-gray-300 rounded px-2 py-1"
          >
            <option value="prioriteit">Prioriteit</option>
            <option value="prijs">Prijs</option>
            <option value="prijs_m2">Prijs/m² (laag → hoog)</option>
          </select>
        </div>
      )}

      {isLoading && (
        <div className="text-center py-12 text-gray-500">Watchlist laden...</div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
          Er is een fout opgetreden bij het laden van de watchlist.
        </div>
      )}

      {items && items.length === 0 && (
        <div className="text-center py-12">
          <div className="text-gray-500 mb-4">Je watchlist is nog leeg.</div>
          <p className="text-sm text-gray-400">
            Voeg woningen toe vanuit het overzicht om ze hier te volgen.
          </p>
        </div>
      )}

      {grouped && (
        <div className="space-y-8">
          {statusOrder.map((status) => {
            const statusItems = grouped[status]
            if (!statusItems || statusItems.length === 0) return null

            return (
              <div key={status}>
                <h2 className="text-lg font-semibold text-gray-700 mb-4">
                  {statusLabels[status]} ({statusItems.length})
                </h2>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {statusItems
                    .sort((a, b) => {
                      if (sortBy === 'prijs') {
                        return (b.woning_vraagprijs || 0) - (a.woning_vraagprijs || 0)
                      }
                      if (sortBy === 'prijs_m2') {
                        const aM2 = a.woning_vraagprijs && a.woning_woonoppervlakte
                          ? a.woning_vraagprijs / a.woning_woonoppervlakte : 0
                        const bM2 = b.woning_vraagprijs && b.woning_woonoppervlakte
                          ? b.woning_vraagprijs / b.woning_woonoppervlakte : 0
                        return aM2 - bM2
                      }
                      return b.prioriteit - a.prioriteit
                    })
                    .map((item) => (
                      <WatchlistCard
                        key={item.id}
                        item={item}
                        onUpdate={handleUpdate}
                        onRemove={handleRemove}
                      />
                    ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
