import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { MapContainer, TileLayer, Marker, Popup, CircleMarker } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import {
  fetchVoorzieningen,
  VoorzieningenResponse,
  VoorzieningItem,
  CBSAfstand,
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
                    {item.afstand_m}m ({item.looptijd_min} min)
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

function VoorzieningenKaart({ data }: { data: VoorzieningenResponse }) {
  if (data.voorzieningen.length === 0) return null

  return (
    <div className="rounded-lg overflow-hidden border border-gray-200" style={{ height: 300 }}>
      <MapContainer
        center={[data.lat, data.lng]}
        zoom={15}
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
                <div>{v.afstand_m}m ({v.looptijd_min} min lopen)</div>
              </div>
            </Popup>
          </CircleMarker>
        ))}
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

      {/* CBS distances by category */}
      <CBSAfstandenSection afstanden={data.cbs_afstanden} />

      {/* OSM specific locations */}
      <OSMLocatiesSection voorzieningen={data.voorzieningen} />

      {/* Mini map */}
      <VoorzieningenKaart data={data} />

      <p className="text-xs text-gray-400">
        Bronnen: CBS Nabijheid voorzieningen, OpenStreetMap. Afstanden zijn hemelsbreed.
      </p>
    </div>
  )
}
