import { useState, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchBuurten, Buurt, formatPrijs } from '../services/api'

const BuurtMap = lazy(() => import('../components/BuurtMap'))

export function ScoreBar({ score, label }: { score?: number; label: string }) {
  if (score === undefined || score === null) {
    return (
      <div className="text-sm text-gray-400">
        {label}: Geen data
      </div>
    )
  }

  const percentage = Math.round(score * 100)
  const color =
    percentage >= 70
      ? 'bg-green-500'
      : percentage >= 50
      ? 'bg-yellow-500'
      : 'bg-red-500'

  return (
    <div className="text-sm">
      <div className="flex justify-between text-gray-600 mb-1">
        <span>{label}</span>
        <span className="font-medium">{percentage}</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-1.5">
        <div
          className={`${color} h-1.5 rounded-full`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}

function BuurtCard({ buurt }: { buurt: Buurt }) {
  return (
    <div className="bg-white rounded-lg shadow hover:shadow-md transition-shadow p-4">
      <div className="flex justify-between items-start mb-3">
        <div>
          <h3 className="font-semibold text-gray-900">{buurt.naam}</h3>
          <p className="text-sm text-gray-500">{buurt.gemeente_naam}</p>
        </div>
        {buurt.score_totaal !== undefined && (
          <div className="flex items-center">
            <div
              className={`w-10 h-10 rounded-full flex items-center justify-center text-white font-bold ${
                buurt.score_totaal >= 0.7
                  ? 'bg-green-500'
                  : buurt.score_totaal >= 0.5
                  ? 'bg-yellow-500'
                  : 'bg-red-500'
              }`}
            >
              {Math.round(buurt.score_totaal * 100)}
            </div>
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4 text-sm mb-4">
        <div>
          <span className="text-gray-500">Mediaan vraagprijs</span>
          <div className="font-medium">
            {buurt.median_vraagprijs ? formatPrijs(buurt.median_vraagprijs) : '-'}
          </div>
        </div>
        <div>
          <span className="text-gray-500">Te koop</span>
          <div className="font-medium">{buurt.aantal_te_koop ?? '-'} woningen</div>
        </div>
      </div>

      <div className="text-xs text-gray-400">{buurt.code}</div>
    </div>
  )
}

export default function BuurtenPage() {
  const [gemeente, setGemeente] = useState<string>('')
  const [minScore, setMinScore] = useState<number | undefined>()

  const { data: buurten, isLoading, error } = useQuery({
    queryKey: ['buurten', gemeente, minScore],
    queryFn: () =>
      fetchBuurten({
        gemeente: gemeente || undefined,
        min_score: minScore,
        limit: 100,
      }),
  })

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Buurten</h1>
      <p className="text-gray-600 mb-8">Vergelijk buurten op basis van CBS statistieken</p>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Gemeente</label>
            <select
              value={gemeente}
              onChange={(e) => setGemeente(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            >
              <option value="">Alle gemeenten</option>
              <option value="Den Haag">Den Haag</option>
              <option value="Leidschendam-Voorburg">Leidschendam-Voorburg</option>
              <option value="Rijswijk">Rijswijk</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Minimum score</label>
            <input
              type="number"
              min={0}
              max={100}
              step={10}
              value={minScore !== undefined ? minScore * 100 : ''}
              onChange={(e) =>
                setMinScore(e.target.value ? Number(e.target.value) / 100 : undefined)
              }
              placeholder="bijv. 60"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            />
          </div>
        </div>
      </div>

      {/* Kaart */}
      <Suspense fallback={<div className="h-[400px] bg-gray-100 rounded-lg animate-pulse mb-6" />}>
        <BuurtMap gemeente={gemeente || undefined} minScore={minScore} />
      </Suspense>

      {/* Results */}
      {isLoading && (
        <div className="text-center py-12 text-gray-500">Buurten laden...</div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
          Er is een fout opgetreden bij het laden van buurten.
        </div>
      )}

      {buurten && buurten.length === 0 && (
        <div className="text-center py-12 text-gray-500">
          Geen buurten gevonden met deze filters.
        </div>
      )}

      {buurten && buurten.length > 0 && (
        <>
          <div className="text-sm text-gray-500 mb-4">{buurten.length} buurten gevonden</div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {buurten.map((buurt) => (
              <BuurtCard key={buurt.code} buurt={buurt} />
            ))}
          </div>
        </>
      )}
    </div>
  )
}
