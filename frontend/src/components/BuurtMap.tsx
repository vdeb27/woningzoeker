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
}

function getScoreColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#9ca3af' // gray
  if (score >= 0.7) return '#22c55e' // green
  if (score >= 0.5) return '#eab308' // yellow
  return '#ef4444' // red
}

export default function BuurtMap({ gemeente, minScore }: BuurtMapProps) {
  const { data: buurtenGeoJSON } = useQuery({
    queryKey: ['buurten-geojson', gemeente, minScore],
    queryFn: () =>
      fetchBuurtenGeoJSON({
        gemeente: gemeente || undefined,
        min_score: minScore,
      }),
  })

  const { data: woningenGeoJSON } = useQuery({
    queryKey: ['woningen-geojson'],
    queryFn: fetchWoningenGeoJSON,
  })

  // Key to force GeoJSON re-render on data change
  const geoJsonKey = useMemo(
    () => JSON.stringify({ gemeente, minScore, count: buurtenGeoJSON?.features?.length }),
    [gemeente, minScore, buurtenGeoJSON]
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
    <div className="rounded-lg overflow-hidden shadow mb-6" style={{ height: 400 }}>
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
              const score = feature?.properties?.score_totaal
              return {
                fillColor: getScoreColor(score),
                fillOpacity: 0.3,
                color: getScoreColor(score),
                weight: 2,
              }
            }}
            onEachFeature={(feature, layer) => {
              const p = feature.properties
              const score = p.score_totaal != null ? Math.round(p.score_totaal * 100) : '-'
              const prijs = p.median_vraagprijs ? formatPrijs(p.median_vraagprijs) : '-'
              layer.bindPopup(
                `<strong>${p.naam}</strong><br/>` +
                  `Score: ${score}<br/>` +
                  `Mediaan: ${prijs}`
              )
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
    </div>
  )
}
