import { useMemo, useEffect, useRef, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { MapContainer, TileLayer, Marker, Popup, CircleMarker, LayersControl, LayerGroup, FeatureGroup, useMap } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

import { fetchBuurtenGeoJSON, fetchWoningenGeoJSON, fetchScholenGeoJSON, fetchPostcode6GeoJSON, formatPrijs, OmgevingsAnalyseResponse } from '../services/api'
import { CATEGORIE_KLEUREN } from './BestemmingsplanPanel'

// Fix default marker icons for Vite bundler
// eslint-disable-next-line @typescript-eslint/no-require-imports
delete (L.Icon.Default.prototype as unknown as Record<string, unknown>)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: new URL('leaflet/dist/images/marker-icon-2x.png', import.meta.url).href,
  iconUrl: new URL('leaflet/dist/images/marker-icon.png', import.meta.url).href,
  shadowUrl: new URL('leaflet/dist/images/marker-shadow.png', import.meta.url).href,
})

// Imperative GeoJSON layer that adds to a FeatureGroup (not directly to the map).
// This lets LayersControl manage visibility via the parent FeatureGroup.
function BuurtGeoJSONLayer({
  data,
  colorIndicator,
  selectedBuurten,
  minVal,
  maxVal,
  onBuurtClick,
  parentGroup,
}: {
  data: GeoJSON.FeatureCollection | undefined
  colorIndicator: string
  selectedBuurten: string[]
  minVal: number
  maxVal: number
  onBuurtClick?: (code: string) => void
  parentGroup: React.RefObject<L.FeatureGroup | null>
}) {
  const map = useMap()
  const layerRef = useRef<L.GeoJSON | null>(null)
  const selectedRef = useRef<string[]>(selectedBuurten)
  selectedRef.current = selectedBuurten

  const getStyle = useCallback((feature: GeoJSON.Feature | undefined, selected: string[]) => {
    const p = feature?.properties
    const isSelected = selected.includes(p?.code)

    let fillColor: string
    if (colorIndicator === 'score_totaal') {
      fillColor = getScoreColor(p?.score_totaal)
    } else {
      const val = p?.indicator_value
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
  }, [colorIndicator, minVal, maxVal])

  // Create/recreate layer when data or color settings change
  useEffect(() => {
    const group = parentGroup.current
    if (!group) return

    if (layerRef.current) {
      group.removeLayer(layerRef.current)
      layerRef.current = null
    }

    if (!data || !data.features || data.features.length === 0) return

    const layer = L.geoJSON(data, {
      style: (feature) => getStyle(feature, selectedRef.current),
      onEachFeature: (feature, featureLayer) => {
        const p = feature.properties
        if (!p) return
        const score = p.score_totaal != null ? Math.round(p.score_totaal * 100) : '-'
        const prijs = p.median_vraagprijs ? `€${Math.round(p.median_vraagprijs / 1000)}k` : '-'
        const inkomen = p.score_inkomen != null ? Math.round(p.score_inkomen * 100) : '-'
        const veiligheid = p.score_veiligheid != null ? Math.round(p.score_veiligheid * 100) : '-'

        let indicatorText = ''
        if (colorIndicator !== 'score_totaal' && p.indicator_value != null) {
          indicatorText = `<br/>${colorIndicator}: ${p.indicator_value}`
        }

        const popupContent =
          `<strong>${p.naam}</strong><br/>` +
          `Score: ${score} | Inkomen: ${inkomen} | Veiligheid: ${veiligheid}<br/>` +
          `Mediaan: ${prijs}` +
          indicatorText

        featureLayer.bindPopup(popupContent, { autoPan: false })

        featureLayer.on('click', (e) => {
          if (onBuurtClick) {
            onBuurtClick(p.code)
          }
          setTimeout(() => {
            L.popup({ autoPan: false })
              .setLatLng(e.latlng)
              .setContent(popupContent)
              .openOn(map)
          }, 100)
        })
      },
    })

    group.addLayer(layer)
    layerRef.current = layer

    return () => {
      if (layerRef.current && group) {
        group.removeLayer(layerRef.current)
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, colorIndicator, minVal, maxVal, map, onBuurtClick, parentGroup])

  // Update styles only when selection changes (without recreating the layer)
  useEffect(() => {
    if (!layerRef.current) return
    layerRef.current.eachLayer((featureLayer) => {
      const feature = (featureLayer as L.GeoJSON & { feature?: GeoJSON.Feature }).feature
      if (feature) {
        (featureLayer as L.Path).setStyle(getStyle(feature, selectedBuurten))
      }
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedBuurten])

  return null
}

// Imperative PC6 layer that adds to a FeatureGroup
function Postcode6GeoJSONLayer({
  data,
  parentGroup,
}: {
  data: GeoJSON.FeatureCollection | undefined
  parentGroup: React.RefObject<L.FeatureGroup | null>
}) {
  const layerRef = useRef<L.GeoJSON | null>(null)

  useEffect(() => {
    const group = parentGroup.current
    if (!group) return

    if (layerRef.current) {
      group.removeLayer(layerRef.current)
      layerRef.current = null
    }

    if (!data || !data.features || data.features.length === 0) return

    const layer = L.geoJSON(data, {
      style: () => ({
        color: '#6b7280',
        weight: 1.5,
        opacity: 0.4,
        fillOpacity: 0,
      }),
      onEachFeature: (feature, featureLayer) => {
        const postcode = feature.properties?.postcode
        if (!postcode) return

        const aantalAdressen = feature.properties?.aantal_adressen
        const popupContent =
          `<strong>${postcode}</strong>` +
          (aantalAdressen != null ? `<br/>${aantalAdressen} adressen` : '')

        featureLayer.bindPopup(popupContent, { autoPan: false })
      },
    })

    group.addLayer(layer)
    layerRef.current = layer

    return () => {
      if (layerRef.current && group) {
        group.removeLayer(layerRef.current)
      }
    }
  }, [data, parentGroup])

  return null
}

// Imperative bestemmingsvlakken layer
function BestemmingsvlakkenGeoJSONLayer({
  data,
  parentGroup,
}: {
  data: GeoJSON.FeatureCollection
  parentGroup: React.RefObject<L.FeatureGroup | null>
}) {
  const layerRef = useRef<L.GeoJSON | null>(null)

  useEffect(() => {
    const group = parentGroup.current
    if (!group) return

    if (layerRef.current) {
      group.removeLayer(layerRef.current)
      layerRef.current = null
    }

    if (!data?.features?.length) return

    const layer = L.geoJSON(data, {
      style: (feature) => {
        const categorie = feature?.properties?.categorie || 'overig'
        return {
          fillColor: CATEGORIE_KLEUREN[categorie] || CATEGORIE_KLEUREN.overig || '#d1d5db',
          fillOpacity: 0.5,
          color: '#374151',
          weight: 1,
          opacity: 0.6,
        }
      },
      onEachFeature: (feature, featureLayer) => {
        const props = feature.properties || {}
        const popup = `<strong>${props.naam || 'Onbekend'}</strong>`
          + `<br/><em>${props.categorie || ''}</em>`
          + (props.plan_naam ? `<br/><span style="color:#6b7280;font-size:11px">${props.plan_naam}</span>` : '')
        featureLayer.bindPopup(popup, { autoPan: false })
      },
    })

    group.addLayer(layer)
    layerRef.current = layer

    return () => {
      if (layerRef.current && group) {
        group.removeLayer(layerRef.current)
      }
    }
  }, [data, parentGroup])

  return null
}

// Component to fly map to a specific center
function FlyToCenter({ center, zoom }: { center: [number, number]; zoom?: number }) {
  const map = useMap()

  useEffect(() => {
    if (center) {
      map.flyTo(center, zoom || 15, { duration: 1 })
    }
  }, [center, zoom, map])

  return null
}

interface BuurtMapProps {
  gemeente?: string
  minScore?: number
  colorIndicator?: string
  selectedBuurten?: string[]
  onBuurtClick?: (code: string) => void
  bestemmingsvlakkenGeoJSON?: OmgevingsAnalyseResponse | null
  bestemmingCenter?: [number, number]
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
  bestemmingsvlakkenGeoJSON,
  bestemmingCenter,
}: BuurtMapProps) {
  const buurtGroupRef = useRef<L.FeatureGroup>(null)
  const pc6GroupRef = useRef<L.FeatureGroup>(null)
  const bestemmingGroupRef = useRef<L.FeatureGroup>(null)

  const { data: buurtenGeoJSON, error: geoError } = useQuery({
    queryKey: ['buurten-geojson', gemeente, minScore, colorIndicator],
    queryFn: () =>
      fetchBuurtenGeoJSON({
        gemeente: gemeente || undefined,
        min_score: minScore,
        indicator: colorIndicator !== 'score_totaal' ? colorIndicator : undefined,
      }),
  })

  if (geoError) {
    console.error('GeoJSON fetch error:', geoError)
  }

  const { data: woningenGeoJSON } = useQuery({
    queryKey: ['woningen-geojson'],
    queryFn: fetchWoningenGeoJSON,
  })

  const { data: scholenGeoJSON } = useQuery({
    queryKey: ['scholen-geojson'],
    queryFn: () => fetchScholenGeoJSON(),
  })

  const { data: postcode6GeoJSON } = useQuery({
    queryKey: ['postcode6-geojson', gemeente],
    queryFn: () => fetchPostcode6GeoJSON({ gemeente: gemeente || undefined }),
    staleTime: Infinity,
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

  const schoolMarkers = useMemo(() => {
    if (!scholenGeoJSON?.features) return []
    return scholenGeoJSON.features
      .filter((f) => f.geometry?.type === 'Point' && f.geometry?.coordinates)
      .map((f) => ({
        position: [
          (f.geometry.coordinates as number[])[1],
          (f.geometry.coordinates as number[])[0],
        ] as [number, number],
        properties: f.properties,
      }))
  }, [scholenGeoJSON])

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

        {bestemmingCenter && <FlyToCenter center={bestemmingCenter} zoom={16} />}

        <LayersControl position="topright">
          <LayersControl.Overlay name="Buurten" checked>
            <FeatureGroup ref={buurtGroupRef}>
              <BuurtGeoJSONLayer
                data={buurtenGeoJSON as GeoJSON.FeatureCollection | undefined}
                colorIndicator={colorIndicator}
                selectedBuurten={selectedBuurten}
                minVal={minVal}
                maxVal={maxVal}
                onBuurtClick={onBuurtClick}
                parentGroup={buurtGroupRef}
              />
            </FeatureGroup>
          </LayersControl.Overlay>

          <LayersControl.Overlay name="Postcodes (PC6)">
            <FeatureGroup ref={pc6GroupRef}>
              <Postcode6GeoJSONLayer
                data={postcode6GeoJSON as GeoJSON.FeatureCollection | undefined}
                parentGroup={pc6GroupRef}
              />
            </FeatureGroup>
          </LayersControl.Overlay>

          {bestemmingsvlakkenGeoJSON && bestemmingsvlakkenGeoJSON.features.length > 0 && (
            <LayersControl.Overlay name="Bestemmingen" checked>
              <FeatureGroup ref={bestemmingGroupRef}>
                <BestemmingsvlakkenGeoJSONLayer
                  data={bestemmingsvlakkenGeoJSON as unknown as GeoJSON.FeatureCollection}
                  parentGroup={bestemmingGroupRef}
                />
              </FeatureGroup>
            </LayersControl.Overlay>
          )}

          <LayersControl.Overlay name="Woningen" checked>
            <LayerGroup>{woningMarkers.map((marker, idx) => {
              const p = marker.properties
              return (
                <Marker key={`w-${idx}`} position={marker.position}>
                  <Popup>
                    <strong>{p.adres as string}</strong>
                    <br />
                    {p.vraagprijs ? formatPrijs(p.vraagprijs as number) : 'Prijs onbekend'}
                    {p.woonoppervlakte ? ` | ${p.woonoppervlakte}m²` : ''}
                    {p.woningtype ? <><br />{p.woningtype as string}</> : null}
                  </Popup>
                </Marker>
              )
            })}</LayerGroup>
          </LayersControl.Overlay>

          <LayersControl.Overlay name="Basisscholen">
            <LayerGroup>{schoolMarkers
              .filter((m) => m.properties.type === 'basisonderwijs')
              .map((marker, idx) => {
                const p = marker.properties
                return (
                  <CircleMarker
                    key={`po-${idx}`}
                    center={marker.position}
                    radius={6}
                    pathOptions={{
                      color: '#1d4ed8',
                      fillColor: '#3b82f6',
                      fillOpacity: 0.8,
                      weight: 1.5,
                    }}
                  >
                    <Popup>
                      <strong>{p.naam as string}</strong>
                      <br />
                      <span className="text-xs text-gray-500">Basisonderwijs &middot; {p.denominatie as string}</span>
                      {p.leerlingen != null && <><br />{p.leerlingen as number} leerlingen</>}
                      {p.advies_havo_vwo_pct != null && (
                        <><br />HAVO/VWO advies: {p.advies_havo_vwo_pct as number}%</>
                      )}
                      {p.gem_eindtoets != null && (
                        <><br />Gem. eindtoets: {p.gem_eindtoets as number}</>
                      )}
                      {p.inspectie_oordeel ? (
                        <><br />Inspectie: {String(p.inspectie_oordeel)}</>
                      ) : null}
                    </Popup>
                  </CircleMarker>
                )
              })}</LayerGroup>
          </LayersControl.Overlay>

          <LayersControl.Overlay name="Middelbare scholen">
            <LayerGroup>{schoolMarkers
              .filter((m) => m.properties.type === 'voortgezet')
              .map((marker, idx) => {
                const p = marker.properties
                return (
                  <CircleMarker
                    key={`vo-${idx}`}
                    center={marker.position}
                    radius={7}
                    pathOptions={{
                      color: '#9333ea',
                      fillColor: '#a855f7',
                      fillOpacity: 0.8,
                      weight: 1.5,
                    }}
                  >
                    <Popup>
                      <strong>{p.naam as string}</strong>
                      <br />
                      <span className="text-xs text-gray-500">
                        {p.onderwijstype
                          ? (p.onderwijstype as string).toUpperCase()
                          : 'Voortgezet onderwijs'}
                        {p.denominatie ? ` \u00b7 ${p.denominatie as string}` : ''}
                      </span>
                      {p.slagingspercentage != null && (
                        <><br />Slagingspercentage: {p.slagingspercentage as number}%</>
                      )}
                      {p.gem_examencijfer != null && (
                        <><br />Gem. examencijfer: {p.gem_examencijfer as number}</>
                      )}
                      {p.inspectie_oordeel ? (
                        <><br />Inspectie: {String(p.inspectie_oordeel)}</>
                      ) : null}
                    </Popup>
                  </CircleMarker>
                )
              })}</LayerGroup>
          </LayersControl.Overlay>
        </LayersControl>
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
