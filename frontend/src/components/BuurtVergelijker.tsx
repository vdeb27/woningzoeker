import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
  Legend,
  Tooltip,
} from 'recharts'
import {
  fetchBuurtenVergelijk,
  fetchIndicatorMeta,
  Buurt,
  CategoryMeta,
  IndicatorMeta,
  formatPrijs,
} from '../services/api'

const BUURT_COLORS = ['#3b82f6', '#ef4444', '#22c55e', '#f59e0b', '#8b5cf6']

interface BuurtVergelijkerProps {
  selectedCodes: string[]
}

export default function BuurtVergelijker({ selectedCodes }: BuurtVergelijkerProps) {
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set(['inkomen', 'veiligheid']))

  const { data: vergelijkData, isLoading } = useQuery({
    queryKey: ['buurten-vergelijk', selectedCodes],
    queryFn: () => fetchBuurtenVergelijk(selectedCodes),
    enabled: selectedCodes.length >= 2,
  })

  const { data: metaData } = useQuery({
    queryKey: ['indicator-meta'],
    queryFn: fetchIndicatorMeta,
  })

  const buurten = vergelijkData?.buurten ?? []
  const categories = vergelijkData?.categories ?? metaData?.categories ?? {}
  const indicators = metaData?.indicators ?? {}

  // Prepare radar chart data
  const radarData = useMemo(() => {
    if (!buurten.length) return []
    const categoryKeys = Object.keys(categories)
    return categoryKeys.map((catId) => {
      const entry: Record<string, unknown> = {
        category: categories[catId]?.label ?? catId,
      }
      buurten.forEach((buurt) => {
        const scoreKey = `score_${catId}` as keyof Buurt
        const val = buurt[scoreKey]
        entry[buurt.code] = typeof val === 'number' ? Math.round(val * 100) : 0
      })
      return entry
    })
  }, [buurten, categories])

  if (selectedCodes.length < 2) {
    return (
      <div className="bg-gray-50 rounded-lg p-8 text-center text-gray-500 mb-6">
        Selecteer minimaal 2 buurten om te vergelijken (klik op de kaart of gebruik de zoekbalk)
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-8 text-center text-gray-500 mb-6 animate-pulse">
        Vergelijking laden...
      </div>
    )
  }

  if (!buurten.length) return null

  const toggleCategory = (catId: string) => {
    setExpandedCategories((prev) => {
      const next = new Set(prev)
      if (next.has(catId)) next.delete(catId)
      else next.add(catId)
      return next
    })
  }

  return (
    <div className="space-y-6 mb-6">
      {/* Radar Chart */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Buurtprofiel vergelijking</h3>
        <div style={{ width: '100%', height: 400 }}>
          <ResponsiveContainer>
            <RadarChart data={radarData}>
              <PolarGrid />
              <PolarAngleAxis dataKey="category" tick={{ fontSize: 12 }} />
              <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fontSize: 10 }} />
              {buurten.map((buurt, idx) => (
                <Radar
                  key={buurt.code}
                  name={buurt.naam}
                  dataKey={buurt.code}
                  stroke={BUURT_COLORS[idx % BUURT_COLORS.length]}
                  fill={BUURT_COLORS[idx % BUURT_COLORS.length]}
                  fillOpacity={0.15}
                  strokeWidth={2}
                />
              ))}
              <Legend />
              <Tooltip />
            </RadarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Comparison Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="p-4 border-b">
          <h3 className="text-lg font-semibold text-gray-900">Gedetailleerde vergelijking</h3>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50">
                <th className="text-left px-4 py-3 font-medium text-gray-700 min-w-[200px]">Indicator</th>
                {buurten.map((buurt, idx) => (
                  <th key={buurt.code} className="text-right px-4 py-3 font-medium min-w-[140px]">
                    <span style={{ color: BUURT_COLORS[idx % BUURT_COLORS.length] }}>
                      {buurt.naam}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Total score row */}
              <tr className="bg-primary-50 font-semibold">
                <td className="px-4 py-2 text-gray-900">Score totaal</td>
                {buurten.map((buurt) => (
                  <td key={buurt.code} className="text-right px-4 py-2">
                    {buurt.score_totaal != null ? Math.round(buurt.score_totaal * 100) : '-'}
                  </td>
                ))}
              </tr>

              {/* Category sections */}
              {Object.entries(categories).map(([catId, cat]) => (
                <CategorySection
                  key={catId}
                  catId={catId}
                  cat={cat}
                  buurten={buurten}
                  indicators={indicators}
                  expanded={expandedCategories.has(catId)}
                  onToggle={() => toggleCategory(catId)}
                />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function CategorySection({
  catId,
  cat,
  buurten,
  indicators,
  expanded,
  onToggle,
}: {
  catId: string
  cat: CategoryMeta
  buurten: Buurt[]
  indicators: Record<string, IndicatorMeta>
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <>
      {/* Category header */}
      <tr
        className="bg-gray-50 cursor-pointer hover:bg-gray-100 transition-colors"
        onClick={onToggle}
      >
        <td className="px-4 py-2 font-medium text-gray-800">
          <span className="inline-block w-3 h-3 rounded-full mr-2" style={{ backgroundColor: cat.color }} />
          {cat.label}
          <span className="ml-2 text-xs text-gray-400">{expanded ? '▼' : '▶'}</span>
        </td>
        {buurten.map((buurt) => {
          const scoreKey = `score_${catId}` as keyof Buurt
          const val = buurt[scoreKey]
          return (
            <td key={buurt.code} className="text-right px-4 py-2 font-medium">
              {typeof val === 'number' ? Math.round(val * 100) : '-'}
            </td>
          )
        })}
      </tr>

      {/* Indicator rows */}
      {expanded &&
        cat.indicators.map((indId) => {
          const indMeta = indicators[indId]
          if (!indMeta) return null

          const values = buurten.map((buurt) => {
            return getIndicatorValue(buurt, indId)
          })

          const numericValues = values.filter((v): v is number => v !== null && v !== undefined)
          const best = numericValues.length > 0
            ? (indMeta.higher_is_better ? Math.max(...numericValues) : Math.min(...numericValues))
            : null
          const worst = numericValues.length > 0
            ? (indMeta.higher_is_better ? Math.min(...numericValues) : Math.max(...numericValues))
            : null

          return (
            <tr key={indId} className="hover:bg-gray-50">
              <td className="px-4 py-1.5 pl-9 text-gray-600">{indMeta.label}</td>
              {values.map((val, idx) => {
                const isBest = val !== null && val === best && numericValues.length > 1
                const isWorst = val !== null && val === worst && numericValues.length > 1
                return (
                  <td
                    key={buurten[idx].code}
                    className={`text-right px-4 py-1.5 ${
                      isBest ? 'text-green-700 font-medium' : isWorst ? 'text-red-600' : 'text-gray-700'
                    }`}
                  >
                    {formatIndicatorValue(val, indMeta.unit)}
                  </td>
                )
              })}
            </tr>
          )
        })}
    </>
  )
}

function getIndicatorValue(buurt: Buurt, indicatorId: string): number | null {
  // Check direct buurt fields first
  const directMap: Record<string, keyof Buurt> = {
    leefbaarometer_score: 'leefbaarometer_score',
    leefbaarometer_veiligheid: 'leefbaarometer_veiligheid',
    leefbaarometer_fysiek: 'leefbaarometer_fysiek',
  }

  if (directMap[indicatorId]) {
    const val = buurt[directMap[indicatorId]]
    return typeof val === 'number' ? val : null
  }

  // Check indicatoren JSON
  if (buurt.indicatoren && indicatorId in buurt.indicatoren) {
    return buurt.indicatoren[indicatorId] ?? null
  }

  // Map scoring yaml column names to buurt fields
  const fieldMap: Record<string, keyof Buurt> = {
    income_per_inhabitant: 'gemiddeld_inkomen',
    woz_value: 'woz_waarde',
  }

  if (fieldMap[indicatorId]) {
    const val = buurt[fieldMap[indicatorId]]
    return typeof val === 'number' ? val : null
  }

  return null
}

function formatIndicatorValue(val: number | null, unit: string): string {
  if (val === null || val === undefined) return '-'

  switch (unit) {
    case 'euro':
      return formatPrijs(val)
    case 'percentage':
      return `${val.toFixed(1)}%`
    case 'km':
      return `${val.toFixed(1)} km`
    case 'per_1000':
      return `${val.toFixed(1)}`
    case 'score':
      return val.toFixed(2)
    case 'ratio':
      return val.toFixed(2)
    case 'µg/m³':
      return `${val.toFixed(1)} µg/m³`
    default:
      return typeof val === 'number' && !Number.isInteger(val) ? val.toFixed(1) : String(val)
  }
}
