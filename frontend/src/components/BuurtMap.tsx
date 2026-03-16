import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { MapContainer, TileLayer, GeoJSON, Marker, Popup } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

import { fetchBuurtenGeoJSON, fetchWoningenGeoJSON, formatPrijs } from '../services/api'

// Fix default marker icons for Vite bundler
// eslint-disable-next-line @typescript-eslint/no-require-imports
delete (L.Icon.Default.prototype as unknown as Record<string, unknown>)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: new URL('leaflet/dist/images/marker-icon-2x.png', import.meta.url).href,
  iconUrl: new URL('leaflet/dist/images/marker-icon.png', import.meta.url).href,
  shadowUrl: new URL('leaflet/dist/images/marker-shadow.png', import.meta.url).href,
})

interface BuurtMapProps {
  gemeente?: string
  minScore?: number
  colorIndicator?: string
  selectedBuurten?: string[]
  onBuurtClick?: (code: string) => void
}

// Color interpolation for dynamic indicator coloring
function interpolateColor(value: number, min: number, max: number): string {
  if (min === max) return '#9ca3af'
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)))
  // Red (low) -> Yellow (mid) -> Green (high)
  if (ratio < 0.5) {
    const t = ratio * 2
    const r = 239
    const g = Math.round(68 + t * (163 - 68))
    const b = Math.round(68 + t * (0 - 68))
    return `rgb(${r},${g},${b})`
  } else {
    const t = (ratio - 0.5) * 2
    const r = Math.round(234 - t * (234 - 34))
    const g = Math.round(179 + t * (197 - 179))
    const b = Math.round(8 + t * (94 - 8))
    return `rgb(${r},${g},${b})`
  }
}

function getScoreColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#9ca3af' // gray
  if (score >= 0.7) return '#22c55e' // green
  if (score >= 0.5) return '#eab308' // yellow
  return '#ef4444' // red
}

export default function BuurtMap({
  gemeente,
  minScore,
  colorIndicator = 'score_totaal',
  selectedBuurten = [],
  onBuurtClick,
}: BuurtMapProps) {
  const { data: buurtenGeoJSON } = useQuery({
    queryKey: ['buurten-geojson', gemeente, minScore, colorIndicator],
    queryFn: () =>
      fetchBuurtenGeoJSON({
        gemeente: gemeente || undefined,
        min_score: minScore,
        indicator: colorIndicator !== 'score_totaal' ? colorIndicator : undefined,
      }),
  })

  const { data: woningenGeoJSON } = useQuery({
    queryKey: ['woningen-geojson'],
    queryFn: fetchWoningenGeoJSON,
  })

  // Calculate min/max for dynamic coloring
  const { minVal, maxVal } = useMemo(() => {
    if (!buurtenGeoJSON?.features) return { minVal: 0, maxVal: 1 }

    const values = buurtenGeoJSON.features
      .map((f) => {
        if (colorIndicator === 'score_totaal') return f.properties.score_totaal as number
        return f.properties.indicator_value as number
      })
      .filter((v): v is number => v !== null && v !== undefined && !isNaN(v))

    if (values.length === 0) return { minVal: 0, maxVal: 1 }
    return { minVal: Math.min(...values), maxVal: Math.max(...values) }
  }, [buurtenGeoJSON, colorIndicator])

  // Key to force GeoJSON re-render on data change
  const geoJsonKey = useMemo(
    () =>
      JSON.stringify({
        gemeente,
        minScore,
        colorIndicator,
        selectedBuurten,
        count: buurtenGeoJSON?.features?.length,
      }),
    [gemeente, minScore, colorIndicator, selectedBuurten, buurtenGeoJSON]
  )

  const woningMarkers = useMemo(() => {
    if (!woningenGeoJSON?.features) return []
    return woningenGeoJSON.features
      .filter((f) => f.geometry?.type === 'Point' && f.geometry?.coordinates)
      .map((f) => ({
        position: [
          (f.geometry.coordinates as number[])[1],
          (f.geometry.coordinates as number[])[0],
        ] as [number, number],
        properties: f.properties,
      }))
  }, [woningenGeoJSON])

  return (
    <div className="rounded-lg overflow-hidden shadow mb-6 relative" style={{ height: 500 }}>
      <MapContainer
        center={[52.07, 4.3]}
        zoom={12}
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          url="https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0/standaard/EPSG:3857/{z}/{x}/{y}.png"
          attribution='Kaartgegevens &copy; <a href="https://www.kadaster.nl">Kadaster</a>'
          maxZoom={19}
        />

        {buurtenGeoJSON && (
          <GeoJSON
            key={geoJsonKey}
            data={buurtenGeoJSON as GeoJSON.FeatureCollection}
            style={(feature) => {
              const p = feature?.properties
              const isSelected = selectedBuurten.includes(p?.code as string)

              let fillColor: string
              if (colorIndicator === 'score_totaal') {
                fillColor = getScoreColor(p?.score_totaal as number)
              } else {
                const val = p?.indicator_value as number
                if (val === null || val === undefined) {
                  fillColor = '#9ca3af'
                } else {
                  fillColor = interpolateColor(val, minVal, maxVal)
                }
              }

              return {
                fillColor,
                fillOpacity: isSelected ? 0.6 : 0.3,
                color: isSelected ? '#1e40af' : fillColor,
                weight: isSelected ? 4 : 2,
              }
            }}
            onEachFeature={(feature, layer) => {
              const p = feature.properties
              const score = p.score_totaal != null ? Math.round((p.score_totaal as number) * 100) : '-'
              const prijs = p.median_vraagprijs ? formatPrijs(p.median_vraagprijs as number) : '-'

              let indicatorText = ''
              if (colorIndicator !== 'score_totaal' && p.indicator_value != null) {
                indicatorText = `<br/>${colorIndicator}: ${p.indicator_value}`
              }

              // Tooltip with key stats
              const inkomen = p.score_inkomen != null ? Math.round((p.score_inkomen as number) * 100) : '-'
              const veiligheid = p.score_veiligheid != null ? Math.round((p.score_veiligheid as number) * 100) : '-'

              layer.bindPopup(
                `<strong>${p.naam}</strong><br/>` +
                  `Score: ${score} | Inkomen: ${inkomen} | Veiligheid: ${veiligheid}<br/>` +
                  `Mediaan: ${prijs}` +
                  indicatorText
              )

              if (onBuurtClick) {
                layer.on('click', () => {
                  onBuurtClick(p.code as string)
                })
              }
            }}
          />
        )}

        {woningMarkers.map((marker, idx) => {
          const p = marker.properties
          return (
            <Marker key={idx} position={marker.position}>
              <Popup>
                <strong>{p.adres as string}</strong>
                <br />
                {p.vraagprijs ? formatPrijs(p.vraagprijs as number) : 'Prijs onbekend'}
                {p.woonoppervlakte ? ` | ${p.woonoppervlakte}m²` : ''}
                {p.woningtype ? <><br />{p.woningtype as string}</> : null}
              </Popup>
            </Marker>
          )
        })}
      </MapContainer>

      {/* Color Legend */}
      <div className="absolute bottom-4 right-4 bg-white bg-opacity-90 rounded-lg shadow px-3 py-2 z-[1000]">
        <div className="text-xs font-medium text-gray-700 mb-1">
          {colorIndicator === 'score_totaal' ? 'Score' : colorIndicator}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs text-gray-500">
            {colorIndicator === 'score_totaal' ? '0' : minVal.toFixed(1)}
          </span>
          <div
            className="h-3 rounded"
            style={{
              width: 80,
              background: 'linear-gradient(to right, #ef4444, #eab308, #22c55e)',
            }}
          />
          <span className="text-xs text-gray-500">
            {colorIndicator === 'score_totaal' ? '100' : maxVal.toFixed(1)}
          </span>
        </div>
      </div>
    </div>
  )
}
