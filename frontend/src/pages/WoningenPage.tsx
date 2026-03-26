import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchWoningen, addToWatchlist, deleteWoning, Woning, formatPrijs } from '../services/api'

function WoningCard({ woning, onAddToWatchlist, onDelete }: {
  woning: Woning
  onAddToWatchlist: (id: number) => void
  onDelete: (id: number) => void
}) {
  const [added, setAdded] = useState(false)
  const navigate = useNavigate()

  const m2Prijs =
    woning.vraagprijs && woning.woonoppervlakte
      ? Math.round(woning.vraagprijs / woning.woonoppervlakte)
      : null

  const handleAdd = () => {
    onAddToWatchlist(woning.id)
    setAdded(true)
  }

  return (
    <div className="bg-white rounded-lg shadow hover:shadow-md transition-shadow p-4">
      <div className="flex justify-between items-start mb-2">
        <div>
          <h3 className="font-semibold text-gray-900">{woning.adres}</h3>
          <p className="text-sm text-gray-500">
            {woning.postcode} {woning.plaats}
          </p>
        </div>
        {woning.vraagprijs && (
          <div className="text-right">
            <div className="font-bold text-primary-700">{formatPrijs(woning.vraagprijs)}</div>
            {m2Prijs && <div className="text-sm font-medium text-primary-600">{formatPrijs(m2Prijs)}/m²</div>}
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-2 mt-3">
        {woning.woonoppervlakte && (
          <span className="px-2 py-1 bg-gray-100 rounded text-sm">{woning.woonoppervlakte} m²</span>
        )}
        {woning.kamers && (
          <span className="px-2 py-1 bg-gray-100 rounded text-sm">{woning.kamers} kamers</span>
        )}
        {woning.bouwjaar && (
          <span className="px-2 py-1 bg-gray-100 rounded text-sm">{woning.bouwjaar}</span>
        )}
        {woning.energielabel && (
          <span
            className={`px-2 py-1 rounded text-sm font-medium ${
              woning.energielabel.startsWith('A')
                ? 'bg-green-100 text-green-800'
                : woning.energielabel <= 'C'
                ? 'bg-yellow-100 text-yellow-800'
                : 'bg-red-100 text-red-800'
            }`}
          >
            {woning.energielabel}
          </span>
        )}
      </div>

      <div className="mt-4 flex justify-between items-center">
        <span className="text-xs text-gray-500">{woning.woningtype}</span>
        <div className="flex gap-2">
          {woning.postcode && woning.huisnummer && (
            <button
              onClick={() => navigate('/', { state: { autoWaardebepaling: {
                postcode: woning.postcode,
                huisnummer: woning.huisnummer,
                huisletter: woning.huisletter || undefined,
                toevoeging: woning.toevoeging || undefined,
                woonoppervlakte: woning.woonoppervlakte || undefined,
                vraagprijs: woning.vraagprijs || undefined,
                woningtype: woning.woningtype || undefined,
              }}})}
              className="text-sm px-3 py-1 rounded bg-blue-100 text-blue-700 hover:bg-blue-200 transition-colors"
            >
              Waardebepaling
            </button>
          )}
          <button
            onClick={handleAdd}
            disabled={added}
            className={`text-sm px-3 py-1 rounded transition-colors ${
              added
                ? 'bg-green-100 text-green-700 cursor-default'
                : 'bg-primary-100 text-primary-700 hover:bg-primary-200'
            }`}
          >
            {added ? 'Toegevoegd' : '+ Watchlist'}
          </button>
          {woning.url && (
            <a
              href={woning.url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary-600 text-sm hover:underline"
            >
              Funda
            </a>
          )}
          <button
            onClick={() => onDelete(woning.id)}
            className="text-sm px-3 py-1 rounded text-red-600 hover:bg-red-50 transition-colors"
          >
            Verwijder
          </button>
        </div>
      </div>
    </div>
  )
}

export default function WoningenPage() {
  const queryClient = useQueryClient()

  const [filters, setFilters] = useState({
    min_prijs: undefined as number | undefined,
    max_prijs: undefined as number | undefined,
    min_oppervlakte: undefined as number | undefined,
    energielabel: undefined as string | undefined,
  })

  const { data: woningen, isLoading, error } = useQuery({
    queryKey: ['woningen', filters],
    queryFn: () => fetchWoningen(filters),
  })

  const watchlistMutation = useMutation({
    mutationFn: (woningId: number) => addToWatchlist(woningId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlist'] })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (woningId: number) => deleteWoning(woningId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['woningen'] })
      queryClient.invalidateQueries({ queryKey: ['woningen-geojson'] })
      queryClient.invalidateQueries({ queryKey: ['watchlist'] })
    },
  })

  const handleAddToWatchlist = (woningId: number) => {
    watchlistMutation.mutate(woningId)
  }

  const handleDelete = (woningId: number) => {
    deleteMutation.mutate(woningId)
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Woningen</h1>
      <p className="text-gray-600 mb-8">Bekijk het woningaanbod in de regio Den Haag</p>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4 mb-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Min. prijs</label>
            <input
              type="number"
              step={10000}
              value={filters.min_prijs || ''}
              onChange={(e) =>
                setFilters({
                  ...filters,
                  min_prijs: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              placeholder="bijv. 300000"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Max. prijs</label>
            <input
              type="number"
              step={10000}
              value={filters.max_prijs || ''}
              onChange={(e) =>
                setFilters({
                  ...filters,
                  max_prijs: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              placeholder="bijv. 500000"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Min. oppervlakte</label>
            <input
              type="number"
              value={filters.min_oppervlakte || ''}
              onChange={(e) =>
                setFilters({
                  ...filters,
                  min_oppervlakte: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              placeholder="bijv. 80"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Energielabel</label>
            <select
              value={filters.energielabel || ''}
              onChange={(e) =>
                setFilters({
                  ...filters,
                  energielabel: e.target.value || undefined,
                })
              }
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm"
            >
              <option value="">Alle</option>
              <option value="A">A of beter</option>
              <option value="B">B of beter</option>
              <option value="C">C of beter</option>
            </select>
          </div>
        </div>
      </div>

      {/* Results */}
      {isLoading && (
        <div className="text-center py-12 text-gray-500">Woningen laden...</div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
          Er is een fout opgetreden bij het laden van woningen.
        </div>
      )}

      {woningen && woningen.length === 0 && (
        <div className="text-center py-12 text-gray-500">
          <p>Nog geen woningen opgeslagen.</p>
          <p className="mt-1 text-sm">Zoek een woning op via de <a href="/woningwaarde" className="text-primary-600 hover:underline">Woningwaarde</a> pagina om deze hier te bewaren.</p>
        </div>
      )}

      {woningen && woningen.length > 0 && (
        <>
          <div className="text-sm text-gray-500 mb-4">{woningen.length} woningen gevonden</div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {woningen.map((woning) => (
              <WoningCard
                key={woning.id}
                woning={woning}
                onAddToWatchlist={handleAddToWatchlist}
                onDelete={handleDelete}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}
