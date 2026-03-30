import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchBestemmingsplan,
  fetchOmgevingsAnalyse,
  BestemmingsplanResponse,
  OmgevingsAnalyseResponse,
  MaatvoeringItem,
  BurenBouwinfoItem,
} from '../services/api'

interface BestemmingsplanPanelProps {
  lat?: number | null
  lng?: number | null
  onOmgevingGeoJSON?: (data: OmgevingsAnalyseResponse | null) => void
}

const BESTEMMING_COLORS: Record<string, string> = {
  wonen: 'bg-yellow-100 text-yellow-800',
  gemengd: 'bg-purple-100 text-purple-800',
  centrum: 'bg-orange-100 text-orange-800',
  bedrijventerrein: 'bg-gray-100 text-gray-800',
  groen: 'bg-green-100 text-green-800',
  verkeer: 'bg-blue-100 text-blue-800',
  maatschappelijk: 'bg-pink-100 text-pink-800',
  recreatie: 'bg-teal-100 text-teal-800',
}

const CATEGORIE_KLEUREN: Record<string, string> = {
  wonen: '#fbbf24',
  groen: '#22c55e',
  verkeer: '#9ca3af',
  water: '#3b82f6',
  bedrijven: '#8b5cf6',
  maatschappelijk: '#ec4899',
  detailhandel: '#f97316',
  horeca: '#ef4444',
  recreatie: '#14b8a6',
  gemengd: '#a855f7',
  agrarisch: '#84cc16',
  tuin: '#86efac',
  overig: '#d1d5db',
}

const CATEGORIE_LABELS: Record<string, string> = {
  wonen: 'Wonen',
  groen: 'Groen',
  verkeer: 'Verkeer',
  water: 'Water',
  bedrijven: 'Bedrijven',
  maatschappelijk: 'Maatschappelijk',
  detailhandel: 'Detailhandel',
  horeca: 'Horeca',
  recreatie: 'Recreatie',
  gemengd: 'Gemengd',
  agrarisch: 'Agrarisch',
  tuin: 'Tuin',
  overig: 'Overig',
}

const INDICATOR_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  gunstig: { bg: 'bg-green-100', text: 'text-green-800', label: 'Gunstig' },
  beperkt: { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Beperkt' },
  ongunstig: { bg: 'bg-red-100', text: 'text-red-800', label: 'Ongunstig' },
}

function getBestemmingColor(bestemming: string): string {
  const lower = bestemming.toLowerCase()
  for (const [key, color] of Object.entries(BESTEMMING_COLORS)) {
    if (lower.includes(key)) return color
  }
  return 'bg-gray-100 text-gray-800'
}

function BestemmingBadge({ bestemming }: { bestemming: string }) {
  return (
    <span
      className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-medium ${getBestemmingColor(bestemming)}`}
    >
      {bestemming}
    </span>
  )
}

function BouwregelsSection({ data }: { data: BestemmingsplanResponse }) {
  const regels: { label: string; waarde: string }[] = []

  if (data.max_bouwhoogte != null) {
    regels.push({ label: 'Max. bouwhoogte', waarde: `${data.max_bouwhoogte} m` })
  }
  if (data.max_goothoogte != null) {
    regels.push({ label: 'Max. goothoogte', waarde: `${data.max_goothoogte} m` })
  }
  if (data.max_bebouwingspercentage != null) {
    regels.push({ label: 'Max. bebouwing', waarde: `${data.max_bebouwingspercentage}%` })
  }
  if (data.max_inhoud != null) {
    regels.push({ label: 'Max. inhoud', waarde: `${data.max_inhoud} m\u00B3` })
  }

  const standaard = ['bouwhoogte', 'goothoogte', 'bebouwingspercentage', 'inhoud']
  const overige = data.maatvoeringen.filter(
    (m: MaatvoeringItem) => !standaard.some((s) => m.naam.toLowerCase().includes(s))
  )
  for (const m of overige) {
    const eenheid = m.eenheid ? ` ${m.eenheid}` : ''
    regels.push({ label: m.naam, waarde: `${m.waarde}${eenheid}` })
  }

  if (regels.length === 0) return null

  return (
    <div>
      <h4 className="text-sm font-medium text-gray-700 mb-2">Bouwregels</h4>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1">
        {regels.map((r, i) => (
          <div key={i} className="contents">
            <span className="text-xs text-gray-500">{r.label}</span>
            <span className="text-xs font-medium text-gray-900 text-right">{r.waarde}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function UitbreidingsIndicator({
  indicator,
  toelichting,
}: {
  indicator: string
  toelichting?: string | null
}) {
  const style = INDICATOR_STYLES[indicator] || INDICATOR_STYLES.beperkt

  return (
    <div className={`${style.bg} rounded-lg p-3`}>
      <div className="flex items-center gap-2">
        <span className={`text-sm font-medium ${style.text}`}>
          Uitbreidingsmogelijkheden: {style.label}
        </span>
      </div>
      {toelichting && (
        <p className={`text-xs ${style.text} mt-1 opacity-80`}>{toelichting}</p>
      )}
    </div>
  )
}

function FunctiesSection({ items }: { items: string[] }) {
  if (items.length === 0) return null
  return (
    <div>
      <h4 className="text-sm font-medium text-gray-700 mb-1">Functieaanduidingen</h4>
      <div className="flex flex-wrap gap-1">
        {items.map((f, i) => (
          <span
            key={i}
            className="inline-block px-2 py-0.5 bg-blue-50 text-blue-700 text-xs rounded"
          >
            {f}
          </span>
        ))}
      </div>
    </div>
  )
}

function BouwaanduidingenSection({ items }: { items: string[] }) {
  if (items.length === 0) return null
  return (
    <div>
      <h4 className="text-sm font-medium text-gray-700 mb-1">Bouwaanduidingen</h4>
      <div className="flex flex-wrap gap-1">
        {items.map((b, i) => (
          <span
            key={i}
            className="inline-block px-2 py-0.5 bg-amber-50 text-amber-700 text-xs rounded"
          >
            {b}
          </span>
        ))}
      </div>
    </div>
  )
}

function RegelsSection({ samenvatting }: { samenvatting: string }) {
  const [open, setOpen] = useState(false)

  return (
    <div>
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1 text-sm font-medium text-gray-700 hover:text-gray-900"
      >
        <span className={`transform transition-transform ${open ? 'rotate-90' : ''}`}>
          &#9654;
        </span>
        Regelteksten samenvatting
      </button>
      {open && (
        <p className="mt-2 text-xs text-gray-600 leading-relaxed bg-gray-50 rounded p-2">
          {samenvatting}
        </p>
      )}
    </div>
  )
}

function OntwerpPlannenWarning({ plannen }: { plannen: BestemmingsplanResponse['ontwerp_plannen'] }) {
  if (plannen.length === 0) return null

  return (
    <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
      <div className="flex items-start gap-2">
        <span className="text-amber-500 text-sm mt-0.5">&#9888;</span>
        <div>
          <h4 className="text-sm font-medium text-amber-800">
            Ontwerp-bestemmingsplan{plannen.length > 1 ? 'nen' : ''}
          </h4>
          <p className="text-xs text-amber-700 mt-0.5">
            Er {plannen.length === 1 ? 'is' : 'zijn'} {plannen.length} ontwerp-plan
            {plannen.length > 1 ? 'nen' : ''} voor deze locatie. Dit kan wijzigingen in
            bouwmogelijkheden betekenen.
          </p>
          <ul className="mt-1 space-y-0.5">
            {plannen.map((p, i) => (
              <li key={i} className="text-xs text-amber-700">
                <span className="font-medium">{p.naam || 'Onbekend plan'}</span>
                {p.datum && <span className="text-amber-600"> ({p.datum})</span>}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  )
}

// --- Omgeving sub-components ---

function StatistiekenBar({ statistieken_pct }: { statistieken_pct: Record<string, number> }) {
  const entries = Object.entries(statistieken_pct).filter(([, pct]) => pct > 0)
  if (entries.length === 0) return null

  return (
    <div className="space-y-1.5">
      {entries.map(([cat, pct]) => (
        <div key={cat} className="flex items-center gap-2">
          <div
            className="w-3 h-3 rounded-sm flex-shrink-0"
            style={{ backgroundColor: CATEGORIE_KLEUREN[cat] || CATEGORIE_KLEUREN.overig }}
          />
          <span className="text-xs text-gray-600 w-24 flex-shrink-0">
            {CATEGORIE_LABELS[cat] || cat}
          </span>
          <div className="flex-1 bg-gray-100 rounded-full h-2">
            <div
              className="h-2 rounded-full"
              style={{
                width: `${Math.min(pct, 100)}%`,
                backgroundColor: CATEGORIE_KLEUREN[cat] || CATEGORIE_KLEUREN.overig,
              }}
            />
          </div>
          <span className="text-xs text-gray-500 w-10 text-right">{pct}%</span>
        </div>
      ))}
    </div>
  )
}

function BurenBouwinfoSection({ buren }: { buren: BurenBouwinfoItem[] }) {
  if (buren.length === 0) return null

  return (
    <div>
      <h4 className="text-sm font-medium text-gray-700 mb-1.5">Bouwmogelijkheden buren</h4>
      <div className="space-y-2">
        {buren.map((b, i) => (
          <div key={i} className="bg-gray-50 rounded p-2">
            <span className="text-xs font-medium text-gray-800">{b.bestemming}</span>
            <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-0.5">
              {b.max_bouwhoogte != null && (
                <span className="text-xs text-gray-500">hoogte: {b.max_bouwhoogte}m</span>
              )}
              {b.max_goothoogte != null && (
                <span className="text-xs text-gray-500">goothoogte: {b.max_goothoogte}m</span>
              )}
              {b.max_bebouwingspercentage != null && (
                <span className="text-xs text-gray-500">bebouwing: {b.max_bebouwingspercentage}%</span>
              )}
              {b.max_bouwhoogte == null && b.max_bebouwingspercentage == null && (
                <span className="text-xs text-gray-400 italic">geen bouwregels gevonden</span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function OmgevingSection({
  lat,
  lng,
  onGeoJSONLoaded,
}: {
  lat: number
  lng: number
  onGeoJSONLoaded?: (data: OmgevingsAnalyseResponse | null) => void
}) {
  const [open, setOpen] = useState(false)

  const { data: omgeving, isLoading } = useQuery({
    queryKey: ['omgevingsanalyse', lat, lng],
    queryFn: () => fetchOmgevingsAnalyse({ lat, lng }),
    enabled: !!lat && !!lng,
    staleTime: 30 * 60 * 1000,
  })

  // Propageer GeoJSON naar parent voor kaartweergave
  useEffect(() => {
    if (onGeoJSONLoaded) {
      onGeoJSONLoaded(omgeving && !omgeving.error ? omgeving : null)
    }
  }, [omgeving, onGeoJSONLoaded])

  return (
    <div className="border-t border-gray-100 pt-3">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1 text-sm font-medium text-gray-700 hover:text-gray-900 w-full"
      >
        <span className={`transform transition-transform ${open ? 'rotate-90' : ''}`}>
          &#9654;
        </span>
        Omgeving (500m)
        {isLoading && <span className="text-xs text-gray-400 ml-auto">laden...</span>}
        {omgeving && !omgeving.error && omgeving.features.length > 0 && (
          <span className="text-xs text-gray-400 ml-auto">
            {omgeving.features.length} vlakken
          </span>
        )}
      </button>

      {open && (
        <div className="mt-3 space-y-3">
          {isLoading && (
            <div className="animate-pulse space-y-2">
              <div className="h-3 bg-gray-200 rounded w-3/4"></div>
              <div className="h-3 bg-gray-200 rounded w-1/2"></div>
            </div>
          )}

          {omgeving && omgeving.error && (
            <p className="text-xs text-gray-500">{omgeving.error}</p>
          )}

          {omgeving && !omgeving.error && (
            <>
              {/* Statistieken */}
              {Object.keys(omgeving.statistieken_pct).length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-gray-700 mb-2">Bestemmingen</h4>
                  <StatistiekenBar statistieken_pct={omgeving.statistieken_pct} />
                </div>
              )}

              {/* Ontwerp-plannen in omgeving */}
              {omgeving.ontwerp_plannen.length > 0 && (
                <div className="bg-amber-50 border border-amber-200 rounded-lg p-2.5">
                  <div className="flex items-start gap-1.5">
                    <span className="text-amber-500 text-xs mt-0.5">&#9888;</span>
                    <div>
                      <span className="text-xs font-medium text-amber-800">
                        {omgeving.ontwerp_plannen.length} ontwerp-plan
                        {omgeving.ontwerp_plannen.length > 1 ? 'nen' : ''} in de omgeving
                      </span>
                      <ul className="mt-0.5 space-y-0.5">
                        {omgeving.ontwerp_plannen.map((p, i) => (
                          <li key={i} className="text-xs text-amber-700">
                            {p.naam || 'Onbekend'}
                            {p.datum && <span className="text-amber-600"> ({p.datum})</span>}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                </div>
              )}

              {/* Buren bouwmogelijkheden */}
              <BurenBouwinfoSection buren={omgeving.buren_bouwinfo} />
            </>
          )}
        </div>
      )}
    </div>
  )
}

// --- Exports ---

// Re-export kleuren voor BuurtMap
export { CATEGORIE_KLEUREN }

export default function BestemmingsplanPanel({ lat, lng, onOmgevingGeoJSON }: BestemmingsplanPanelProps) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['bestemmingsplan', lat, lng],
    queryFn: () => fetchBestemmingsplan({ lat: lat!, lng: lng! }),
    enabled: !!lat && !!lng,
    staleTime: 30 * 60 * 1000,
  })

  if (!lat || !lng) return null
  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-4">
        <h3 className="text-lg font-semibold text-gray-900 mb-2">Bestemmingsplan</h3>
        <div className="animate-pulse space-y-2">
          <div className="h-4 bg-gray-200 rounded w-3/4"></div>
          <div className="h-4 bg-gray-200 rounded w-1/2"></div>
          <div className="h-4 bg-gray-200 rounded w-2/3"></div>
        </div>
      </div>
    )
  }

  if (isError || !data) {
    return (
      <div className="bg-white rounded-lg shadow p-4">
        <h3 className="text-lg font-semibold text-gray-900 mb-2">Bestemmingsplan</h3>
        <p className="text-sm text-gray-500">Kon bestemmingsplan niet ophalen.</p>
      </div>
    )
  }

  if (data.error) {
    return (
      <div className="bg-white rounded-lg shadow p-4">
        <h3 className="text-lg font-semibold text-gray-900 mb-2">Bestemmingsplan</h3>
        <p className="text-sm text-gray-500">{data.error}</p>
        {data.link_plan && (
          <a
            href={data.link_plan}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-blue-600 hover:underline mt-1 inline-block"
          >
            Bekijk op Regels op de Kaart &#8599;
          </a>
        )}
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg shadow p-4 space-y-4">
      {/* Header */}
      <div>
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Bestemmingsplan</h3>
          <BestemmingBadge bestemming={data.bestemming} />
        </div>
        <p className="text-sm text-gray-600 mt-0.5">{data.plan_naam}</p>
        {data.bestemming_specifiek && (
          <p className="text-xs text-gray-500">{data.bestemming_specifiek}</p>
        )}
      </div>

      {/* Uitbreidingsindicator */}
      {data.uitbreidings_indicator && (
        <UitbreidingsIndicator
          indicator={data.uitbreidings_indicator}
          toelichting={data.uitbreidings_toelichting}
        />
      )}

      {/* Bouwregels */}
      <BouwregelsSection data={data} />

      {/* Functieaanduidingen */}
      <FunctiesSection items={data.functieaanduidingen} />

      {/* Bouwaanduidingen */}
      <BouwaanduidingenSection items={data.bouwaanduidingen} />

      {/* Regelteksten */}
      {data.regels_samenvatting && (
        <RegelsSection samenvatting={data.regels_samenvatting} />
      )}

      {/* Ontwerp-plannen waarschuwing */}
      <OntwerpPlannenWarning plannen={data.ontwerp_plannen} />

      {/* Omgeving sectie */}
      <OmgevingSection lat={lat} lng={lng} onGeoJSONLoaded={onOmgevingGeoJSON} />

      {/* Footer */}
      <div className="flex items-center justify-between pt-2 border-t border-gray-100">
        <div className="text-xs text-gray-400">
          {data.plan_type && <span>{data.plan_type}</span>}
          {data.datum_vaststelling && <span> &middot; {data.datum_vaststelling}</span>}
        </div>
        {data.link_plan && (
          <a
            href={data.link_plan}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-blue-600 hover:underline"
          >
            Regels op de Kaart &#8599;
          </a>
        )}
      </div>
    </div>
  )
}
