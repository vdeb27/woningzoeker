import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { MapContainer, TileLayer, Marker, Popup, CircleMarker, Polyline } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import {
  fetchVoorzieningen,
  VoorzieningenResponse,
  VoorzieningItem,
  CBSAfstand,
  FietsafstandItem,
  OVData,
} from '../services/api'

// Fix default marker icons for Vite bundler
// eslint-disable-next-line @typescript-eslint/no-require-imports
delete (L.Icon.Default.prototype as unknown as Record<string, unknown>)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: new URL('leaflet/dist/images/marker-icon-2x.png', import.meta.url).href,
  iconUrl: new URL('leaflet/dist/images/marker-icon.png', import.meta.url).href,
  shadowUrl: new URL('leaflet/dist/images/marker-shadow.png', import.meta.url).href,
})

interface VoorzieningenPanelProps {
  postcode: string
  huisnummer: number
}

const CATEGORIE_LABELS: Record<string, string> = {
  dagelijks: 'Dagelijks',
  winkels: 'Winkels',
  horeca: 'Horeca',
  zorg: 'Zorg',
  sport: 'Sport',
  cultuur: 'Cultuur',
  onderwijs: 'Onderwijs',
  winkels_horeca: 'Winkels & Horeca',
  vervoer: 'Vervoer',
  natuur: 'Natuur',
}

const CATEGORIE_ICONS: Record<string, string> = {
  dagelijks: '\u{1F6D2}',
  winkels: '\u{1F6CD}',
  horeca: '\u{1F374}',
  zorg: '\u{2695}',
  sport: '\u{1F3CB}',
  cultuur: '\u{1F3AD}',
  onderwijs: '\u{1F393}',
  winkels_horeca: '\u{1F6CD}',
  vervoer: '\u{1F689}',
  natuur: '\u{1F333}',
}

const CATEGORIE_COLORS: Record<string, string> = {
  dagelijks: '#3b82f6',
  winkels: '#8b5cf6',
  horeca: '#f59e0b',
  zorg: '#ef4444',
  sport: '#10b981',
  cultuur: '#ec4899',
  onderwijs: '#6366f1',
  winkels_horeca: '#8b5cf6',
  vervoer: '#6b7280',
  natuur: '#22c55e',
}

function afstandKleur(km: number): string {
  if (km <= 0.5) return 'text-green-700 bg-green-100'
  if (km <= 1.5) return 'text-yellow-700 bg-yellow-100'
  return 'text-red-700 bg-red-100'
}

function afstandMKleur(m: number): string {
  if (m <= 500) return 'text-green-700 bg-green-100'
  if (m <= 1500) return 'text-yellow-700 bg-yellow-100'
  return 'text-red-700 bg-red-100'
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score * 100)
  const color = pct >= 70 ? 'bg-green-500' : pct >= 40 ? 'bg-yellow-500' : 'bg-red-500'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-200 rounded-full h-2.5">
        <div className={`${color} h-2.5 rounded-full`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-sm font-medium text-gray-700">{pct}/100</span>
    </div>
  )
}

function CBSAfstandenSection({ afstanden }: { afstanden: Record<string, CBSAfstand[]> }) {
  const categories = Object.entries(afstanden)
  if (categories.length === 0) return null

  return (
    <div className="space-y-3">
      <h4 className="text-sm font-medium text-gray-700">CBS Afstanden (buurtgemiddelde, hemelsbreed)</h4>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {categories.map(([categorie, items]) => (
          <div key={categorie} className="bg-gray-50 rounded-lg p-3">
            <div className="text-xs font-medium text-gray-500 mb-2">
              {CATEGORIE_ICONS[categorie] || ''} {CATEGORIE_LABELS[categorie] || categorie}
            </div>
            <div className="space-y-1">
              {items.map((item) => (
                <div key={item.indicator} className="flex justify-between items-center text-xs">
                  <span className="text-gray-600">{item.label}</span>
                  <span className={`px-1.5 py-0.5 rounded ${afstandKleur(item.afstand_km)}`}>
                    {item.afstand_km} km ({item.looptijd_min} min)
                  </span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

const MODALITEIT_ICONS: Record<string, string> = {
  lopen: '\u{1F6B6}',
  fietsen: '\u{1F6B2}',
  auto: '\u{1F697}',
}

function reistijdLabel(item: VoorzieningItem): string {
  const min = item.reistijd_min || item.looptijd_min
  const icon = MODALITEIT_ICONS[item.modaliteit] || MODALITEIT_ICONS.lopen
  return `${icon} ${min} min`
}

function OSMLocatiesSection({ voorzieningen }: { voorzieningen: VoorzieningItem[] }) {
  const [openCategorie, setOpenCategorie] = useState<string | null>(null)

  if (voorzieningen.length === 0) return null

  // Group by category
  const grouped: Record<string, VoorzieningItem[]> = {}
  for (const v of voorzieningen) {
    if (!grouped[v.categorie]) grouped[v.categorie] = []
    grouped[v.categorie].push(v)
  }

  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium text-gray-700">Specifieke locaties (OSM)</h4>
      {Object.entries(grouped).map(([categorie, items]) => (
        <div key={categorie} className="border border-gray-200 rounded-lg overflow-hidden">
          <button
            onClick={() => setOpenCategorie(openCategorie === categorie ? null : categorie)}
            className="w-full flex items-center justify-between px-3 py-2 bg-white hover:bg-gray-50 transition-colors text-left"
          >
            <span className="text-sm font-medium text-gray-700">
              {CATEGORIE_ICONS[categorie] || ''} {CATEGORIE_LABELS[categorie] || categorie}
              <span className="text-gray-400 font-normal ml-1">({items.length})</span>
            </span>
            <span className="text-gray-400 text-xs">
              {openCategorie === categorie ? '\u25B2' : '\u25BC'}
            </span>
          </button>
          {openCategorie === categorie && (
            <div className="px-3 pb-2 space-y-1 bg-white">
              {items.map((item, idx) => (
                <div key={idx} className="flex justify-between items-center text-xs py-1 border-t border-gray-100 first:border-0">
                  <span className="text-gray-700 truncate mr-2" title={item.naam}>{item.naam}</span>
                  <span className={`px-1.5 py-0.5 rounded whitespace-nowrap ${afstandMKleur(item.afstand_m)}`}>
                    {item.afstand_m}m &middot; {reistijdLabel(item)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

function fietsKleur(min: number): string {
  if (min <= 15) return 'text-green-700 bg-green-100'
  if (min <= 30) return 'text-yellow-700 bg-yellow-100'
  return 'text-red-700 bg-red-100'
}

const ROUTE_COLORS = ['#2563eb', '#dc2626', '#059669', '#d97706']

function FietsafstandSection({ fietsafstanden }: { fietsafstanden: FietsafstandItem[] }) {
  if (fietsafstanden.length === 0) return null

  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium text-gray-700">{'\u{1F6B2}'} Fietsafstand werklocaties</h4>
      <div className="bg-blue-50 rounded-lg p-3 space-y-2">
        {fietsafstanden.map((item, idx) => (
          <div key={idx} className="flex justify-between items-center text-sm">
            <div className="flex items-center gap-2">
              <span
                className="w-3 h-3 rounded-full inline-block"
                style={{ backgroundColor: ROUTE_COLORS[idx % ROUTE_COLORS.length] }}
              />
              <span className="text-gray-700">{item.dest_naam}</span>
            </div>
            {item.error ? (
              <span className="text-xs text-gray-400">{item.error}</span>
            ) : (
              <span className={`px-2 py-0.5 rounded text-xs font-medium ${fietsKleur(item.reistijd_min)}`}>
                {item.afstand_km} km &middot; {item.reistijd_min} min
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

const OV_TYPE_ICONS: Record<string, string> = {
  trein: '\u{1F686}',
  metro: '\u{1F687}',
  tram: '\u{1F68A}',
  bus: '\u{1F68C}',
}

const OV_TYPE_COLORS: Record<string, string> = {
  trein: '#f97316',
  metro: '#ef4444',
  tram: '#3b82f6',
  bus: '#6b7280',
}

function ovAfstandKleur(m: number): string {
  if (m <= 300) return 'text-green-700 bg-green-100'
  if (m <= 500) return 'text-yellow-700 bg-yellow-100'
  if (m <= 800) return 'text-orange-700 bg-orange-100'
  return 'text-red-700 bg-red-100'
}

function ovReistijdKleur(min: number): string {
  if (min <= 20) return 'text-green-700 bg-green-100'
  if (min <= 40) return 'text-yellow-700 bg-yellow-100'
  return 'text-red-700 bg-red-100'
}

function OVScoreBar({ score, breakdown }: { score: number; breakdown: Record<string, number> }) {
  const pct = Math.round(score * 100)
  const color = pct >= 60 ? 'bg-green-500' : pct >= 35 ? 'bg-yellow-500' : 'bg-red-500'

  const breakdownLabels: Record<string, string> = {
    afstand_halte: 'Afstand halte',
    type_vervoer: 'Type vervoer',
    frequentie: 'Frequentie',
    verbinding_centrum: 'Verbinding centrum',
  }

  return (
    <div>
      <div className="flex items-center gap-2">
        <div className="flex-1 bg-gray-200 rounded-full h-2.5">
          <div className={`${color} h-2.5 rounded-full`} style={{ width: `${pct}%` }} />
        </div>
        <span className="text-sm font-medium text-gray-700">{pct}/100</span>
      </div>
      <div className="flex gap-3 mt-1.5 flex-wrap">
        {Object.entries(breakdown).map(([key, value]) => (
          <span key={key} className="text-[10px] text-gray-400">
            {breakdownLabels[key] || key}: {Math.round(value * 100)}%
          </span>
        ))}
      </div>
    </div>
  )
}

function OVSection({ ovData, fietsafstanden }: { ovData: OVData; fietsafstanden: FietsafstandItem[] }) {
  const [showAllHaltes, setShowAllHaltes] = useState(false)

  const haltesToShow = showAllHaltes ? ovData.haltes_nabij : ovData.haltes_nabij.slice(0, 3)
  const hasReistijden = ovData.reistijden_werklocaties.length > 0

  return (
    <div className="space-y-3">
      <h4 className="text-sm font-medium text-gray-700">{'\u{1F68A}'} OV-bereikbaarheid</h4>

      {/* OV Score */}
      <div className="bg-purple-50 border border-purple-200 rounded-lg p-3">
        <div className="text-xs font-medium text-purple-700 mb-1.5">OV-score</div>
        <OVScoreBar score={ovData.ov_score} breakdown={ovData.score_breakdown} />
      </div>

      {/* Nearest stops */}
      <div className="space-y-1.5">
        <div className="text-xs font-medium text-gray-500">Haltes in de buurt</div>
        {haltesToShow.map((halte, idx) => (
          <div key={idx} className="bg-gray-50 rounded-lg p-2.5">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-1.5">
                <span className="text-base">{OV_TYPE_ICONS[halte.type] || '\u{1F68F}'}</span>
                <div>
                  <div className="text-xs font-medium text-gray-800">{halte.naam}</div>
                  <div className="text-[10px] text-gray-400 mt-0.5">
                    {halte.lijnen.slice(0, 5).join(', ')}
                    {halte.lijnen.length > 5 && ` +${halte.lijnen.length - 5}`}
                  </div>
                </div>
              </div>
              <div className="text-right">
                <span className={`text-xs px-1.5 py-0.5 rounded ${ovAfstandKleur(halte.afstand_m)}`}>
                  {halte.afstand_m}m
                </span>
                {halte.frequentie_spits && (
                  <div className="text-[10px] text-gray-400 mt-0.5">{halte.frequentie_spits}x/uur spits</div>
                )}
              </div>
            </div>
          </div>
        ))}
        {ovData.haltes_nabij.length > 3 && (
          <button
            onClick={() => setShowAllHaltes(!showAllHaltes)}
            className="text-xs text-purple-600 hover:text-purple-800"
          >
            {showAllHaltes ? 'Minder tonen' : `Alle ${ovData.haltes_nabij.length} haltes tonen`}
          </button>
        )}
      </div>

      {/* Travel times comparison: OV vs Fiets */}
      {hasReistijden && (
        <div className="space-y-1.5">
          <div className="text-xs font-medium text-gray-500">Geschatte reistijd werklocaties</div>
          <div className="bg-gray-50 rounded-lg p-3">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 text-[10px]">
                  <th className="text-left font-normal pb-1.5">Bestemming</th>
                  <th className="text-right font-normal pb-1.5">{'\u{1F68A}'} OV</th>
                  <th className="text-right font-normal pb-1.5">{'\u{1F6B2}'} Fiets</th>
                </tr>
              </thead>
              <tbody>
                {ovData.reistijden_werklocaties.map((ov, idx) => {
                  const fiets = fietsafstanden[idx]
                  return (
                    <tr key={idx} className="border-t border-gray-100">
                      <td className="py-1.5 text-gray-700 pr-2">
                        <div>{ov.dest_naam}</div>
                        {!ov.error && (
                          <div className="text-[10px] text-gray-400">{ov.route_beschrijving}</div>
                        )}
                      </td>
                      <td className="py-1.5 text-right">
                        {ov.error ? (
                          <span className="text-gray-400">&mdash;</span>
                        ) : (
                          <span className={`px-1.5 py-0.5 rounded ${ovReistijdKleur(ov.reistijd_min)}`}>
                            {ov.reistijd_min} min
                          </span>
                        )}
                      </td>
                      <td className="py-1.5 text-right">
                        {fiets && !fiets.error ? (
                          <span className={`px-1.5 py-0.5 rounded ${fietsKleur(fiets.reistijd_min)}`}>
                            {fiets.reistijd_min} min
                          </span>
                        ) : (
                          <span className="text-gray-400">&mdash;</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

function VoorzieningenKaart({ data }: { data: VoorzieningenResponse }) {
  const hasVoorzieningen = data.voorzieningen.length > 0
  const hasRoutes = data.fietsafstanden?.some(f => f.geometry && !f.error)
  const hasOVHaltes = (data.ov_data?.haltes_nabij?.length ?? 0) > 0
  if (!hasVoorzieningen && !hasRoutes && !hasOVHaltes) return null

  return (
    <div className="rounded-lg overflow-hidden border border-gray-200" style={{ height: 300 }}>
      <MapContainer
        center={[data.lat, data.lng]}
        zoom={hasRoutes ? 13 : 15}
        style={{ height: '100%', width: '100%' }}
        scrollWheelZoom={false}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <Marker position={[data.lat, data.lng]}>
          <Popup>Zoekladres</Popup>
        </Marker>
        {data.voorzieningen.slice(0, 50).map((v, idx) => (
          <CircleMarker
            key={idx}
            center={[v.lat, v.lng]}
            radius={6}
            pathOptions={{
              color: CATEGORIE_COLORS[v.categorie] || '#6b7280',
              fillColor: CATEGORIE_COLORS[v.categorie] || '#6b7280',
              fillOpacity: 0.7,
            }}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-medium">{v.naam}</div>
                <div className="text-gray-500">{CATEGORIE_LABELS[v.categorie] || v.categorie}</div>
                <div>{v.afstand_m}m &middot; {reistijdLabel(v)}</div>
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {/* OV halte markers */}
        {data.ov_data?.haltes_nabij?.map((halte, idx) => (
          <CircleMarker
            key={`ov-${idx}`}
            center={[halte.lat, halte.lng]}
            radius={7}
            pathOptions={{
              color: OV_TYPE_COLORS[halte.type] || '#6b7280',
              fillColor: OV_TYPE_COLORS[halte.type] || '#6b7280',
              fillOpacity: 0.8,
              weight: 2,
            }}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-medium">{OV_TYPE_ICONS[halte.type] || ''} {halte.naam}</div>
                <div className="text-gray-500">{halte.type}</div>
                <div>{halte.afstand_m}m &middot; {halte.lijnen.slice(0, 4).join(', ')}</div>
                {halte.frequentie_spits && <div>{halte.frequentie_spits}x/uur (spits)</div>}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {/* Cycling route polylines */}
        {data.fietsafstanden?.map((route, idx) => {
          if (!route.geometry || route.error) return null
          // ORS geometry is [[lng, lat], ...], Leaflet needs [lat, lng]
          const positions = route.geometry.map(
            (coord: number[]) => [coord[1], coord[0]] as [number, number]
          )
          return (
            <Polyline
              key={`route-${idx}`}
              positions={positions}
              pathOptions={{
                color: ROUTE_COLORS[idx % ROUTE_COLORS.length],
                weight: 4,
                opacity: 0.8,
              }}
            />
          )
        })}
      </MapContainer>
    </div>
  )
}

export default function VoorzieningenPanel({ postcode, huisnummer }: VoorzieningenPanelProps) {
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['voorzieningen', postcode, huisnummer],
    queryFn: () => fetchVoorzieningen({ postcode, huisnummer }),
    enabled: !!postcode && huisnummer > 0,
    staleTime: 5 * 60 * 1000,
  })

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-4 animate-pulse">
        <div className="h-4 bg-gray-200 rounded w-1/2 mb-3"></div>
        <div className="h-3 bg-gray-200 rounded w-3/4 mb-2"></div>
        <div className="h-3 bg-gray-200 rounded w-2/3"></div>
      </div>
    )
  }

  if (isError) {
    return (
      <div className="bg-orange-50 border border-orange-200 rounded-lg p-4 text-sm text-orange-700">
        Voorzieningen konden niet worden opgehaald.
        {error instanceof Error && <span className="block text-xs mt-1">{error.message}</span>}
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold text-gray-900">Voorzieningen in de buurt</h2>

      {/* Header with buurt name + score */}
      <div className="bg-indigo-50 border border-indigo-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm text-indigo-700 font-medium">Nabijheid voorzieningen</div>
          {data.buurt_naam && (
            <div className="text-xs text-indigo-500 truncate max-w-[200px]" title={data.buurt_naam}>
              {data.buurt_naam}
            </div>
          )}
        </div>
        {data.score_voorzieningen !== undefined && data.score_voorzieningen !== null && (
          <ScoreBar score={data.score_voorzieningen} />
        )}
        <div className="flex gap-4 mt-2 text-xs text-indigo-500">
          <span>CBS afstanden: {Object.values(data.cbs_afstanden).flat().length}</span>
          <span>OSM locaties: {data.voorzieningen.length}</span>
        </div>
      </div>

      {/* OV bereikbaarheid */}
      {data.ov_data && (
        <OVSection ovData={data.ov_data} fietsafstanden={data.fietsafstanden || []} />
      )}

      {/* Fietsafstand werklocaties (only show if no OV section, to avoid duplicate) */}
      {!data.ov_data && <FietsafstandSection fietsafstanden={data.fietsafstanden || []} />}

      {/* CBS distances by category */}
      <CBSAfstandenSection afstanden={data.cbs_afstanden} />

      {/* OSM specific locations */}
      <OSMLocatiesSection voorzieningen={data.voorzieningen} />

      {/* Mini map */}
      <VoorzieningenKaart data={data} />

      <p className="text-xs text-gray-400">
        Bronnen: CBS Nabijheid voorzieningen, OpenStreetMap, OpenRouteService, OVapi.nl. CBS-afstanden zijn hemelsbreed. OSM-afstanden zijn routeafstanden via ORS ({'\u{1F6B6}'} lopen, {'\u{1F6B2}'} fietsen, {'\u{1F697}'} auto).
      </p>
    </div>
  )
}
