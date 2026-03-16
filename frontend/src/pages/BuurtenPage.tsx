import { useState, useMemo, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchBuurten,
  fetchIndicatorMeta,
  Buurt,
  formatPrijs,
} from '../services/api'

const BuurtMap = lazy(() => import('../components/BuurtMap'))
const BuurtVergelijker = lazy(() => import('../components/BuurtVergelijker'))

const MAX_SELECTED = 5

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

function BuurtCard({
  buurt,
  isSelected,
  onToggle,
}: {
  buurt: Buurt
  isSelected: boolean
  onToggle: () => void
}) {
  return (
    <div
      className={`bg-white rounded-lg shadow hover:shadow-md transition-shadow p-4 cursor-pointer border-2 ${
        isSelected ? 'border-primary-500 ring-2 ring-primary-200' : 'border-transparent'
      }`}
      onClick={onToggle}
    >
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
  const [colorIndicator, setColorIndicator] = useState<string>('score_totaal')
  const [selectedBuurten, setSelectedBuurten] = useState<string[]>([])
  const [searchQuery, setSearchQuery] = useState('')

  const { data: buurten, isLoading, error } = useQuery({
    queryKey: ['buurten', gemeente, minScore],
    queryFn: () =>
      fetchBuurten({
        gemeente: gemeente || undefined,
        min_score: minScore,
        limit: 200,
      }),
  })

  const { data: indicatorMeta } = useQuery({
    queryKey: ['indicator-meta'],
    queryFn: fetchIndicatorMeta,
  })

  // Build indicator options for dropdown
  const indicatorOptions = useMemo(() => {
    const options: { value: string; label: string; category?: string }[] = [
      { value: 'score_totaal', label: 'Score totaal' },
    ]

    if (indicatorMeta) {
      // Add category scores
      for (const [catId, cat] of Object.entries(indicatorMeta.categories)) {
        options.push({ value: `score_${catId}`, label: `Score: ${cat.label}` })
      }

      // Add individual indicators grouped by category
      for (const [indId, ind] of Object.entries(indicatorMeta.indicators)) {
        options.push({
          value: indId,
          label: ind.label,
          category: ind.category || undefined,
        })
      }
    }

    return options
  }, [indicatorMeta])

  // Filter buurten by search query
  const filteredBuurten = useMemo(() => {
    if (!buurten) return []
    if (!searchQuery.trim()) return buurten
    const q = searchQuery.toLowerCase()
    return buurten.filter(
      (b) => b.naam.toLowerCase().includes(q) || b.code.toLowerCase().includes(q)
    )
  }, [buurten, searchQuery])

  // Search suggestions
  const searchSuggestions = useMemo(() => {
    if (!buurten || searchQuery.length < 2) return []
    const q = searchQuery.toLowerCase()
    return buurten
      .filter((b) => b.naam.toLowerCase().includes(q) && !selectedBuurten.includes(b.code))
      .slice(0, 5)
  }, [buurten, searchQuery, selectedBuurten])

  const handleBuurtToggle = (code: string) => {
    setSelectedBuurten((prev) => {
      if (prev.includes(code)) {
        return prev.filter((c) => c !== code)
      }
      if (prev.length >= MAX_SELECTED) return prev
      return [...prev, code]
    })
  }

  const handleSearchSelect = (code: string) => {
    if (!selectedBuurten.includes(code) && selectedBuurten.length < MAX_SELECTED) {
      setSelectedBuurten((prev) => [...prev, code])
    }
    setSearchQuery('')
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Buurten</h1>
      <p className="text-gray-600 mb-8">
        Vergelijk buurten op basis van CBS statistieken, Leefbaarometer en meer
      </p>

      {/* Filters + Indicator Selector */}
      <div className="bg-white rounded-lg shadow p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
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
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Kleur op basis van</label>
            <select
              value={colorIndicator}
              onChange={(e) => setColorIndicator(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            >
              {indicatorOptions.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Zoek buurt</label>
            <div className="relative">
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Zoek op naam..."
                className="w-full px-3 py-2 border border-gray-300 rounded-lg"
              />
              {searchSuggestions.length > 0 && (
                <div className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto">
                  {searchSuggestions.map((buurt) => (
                    <button
                      key={buurt.code}
                      onClick={() => handleSearchSelect(buurt.code)}
                      className="w-full text-left px-3 py-2 hover:bg-primary-50 text-sm"
                    >
                      <span className="font-medium">{buurt.naam}</span>
                      <span className="text-gray-400 ml-2">{buurt.gemeente_naam}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Selected buurten chips */}
      {selectedBuurten.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-4">
          <span className="text-sm text-gray-500 self-center">Geselecteerd:</span>
          {selectedBuurten.map((code) => {
            const buurt = buurten?.find((b) => b.code === code)
            return (
              <span
                key={code}
                className="inline-flex items-center px-3 py-1 rounded-full text-sm bg-primary-100 text-primary-800"
              >
                {buurt?.naam || code}
                <button
                  onClick={() => handleBuurtToggle(code)}
                  className="ml-2 text-primary-600 hover:text-primary-800"
                >
                  &times;
                </button>
              </span>
            )
          })}
          {selectedBuurten.length < MAX_SELECTED && (
            <span className="text-xs text-gray-400 self-center">
              (max {MAX_SELECTED}, klik op kaart of zoek)
            </span>
          )}
        </div>
      )}

      {/* Kaart */}
      <Suspense fallback={<div className="h-[500px] bg-gray-100 rounded-lg animate-pulse mb-6" />}>
        <BuurtMap
          gemeente={gemeente || undefined}
          minScore={minScore}
          colorIndicator={colorIndicator}
          selectedBuurten={selectedBuurten}
          onBuurtClick={handleBuurtToggle}
        />
      </Suspense>

      {/* Vergelijker */}
      <Suspense fallback={null}>
        <BuurtVergelijker selectedCodes={selectedBuurten} />
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

      {filteredBuurten && filteredBuurten.length === 0 && !isLoading && (
        <div className="text-center py-12 text-gray-500">
          Geen buurten gevonden met deze filters.
        </div>
      )}

      {filteredBuurten && filteredBuurten.length > 0 && (
        <>
          <div className="text-sm text-gray-500 mb-4">{filteredBuurten.length} buurten gevonden</div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {filteredBuurten.map((buurt) => (
              <BuurtCard
                key={buurt.code}
                buurt={buurt}
                isSelected={selectedBuurten.includes(buurt.code)}
                onToggle={() => handleBuurtToggle(buurt.code)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}
