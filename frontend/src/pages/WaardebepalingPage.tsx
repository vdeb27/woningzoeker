import { useState, useEffect, useRef } from 'react'
import { useLocation } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  berekenWaardeVoorAdres,
  getWaardebepaling3D,
  EnhancedWaardebepalingRequest,
  EnhancedWaardebepalingResponse,
  MonumentResponse,
  FundaListing,
  PlafondhoogteResponse,
  GlasvezelResponse,
  OrientatieResponse,
  formatPrijs,
  formatM2Prijs,
} from '../services/api'
import VoorzieningenPanel from '../components/VoorzieningenPanel'
import BestemmingsplanPanel from '../components/BestemmingsplanPanel'
import BuurtMap from '../components/BuurtMap'
import type { OmgevingsAnalyseResponse } from '../services/api'

function BiedAdviesBadge({ advies }: { advies: string }) {
  const colors: Record<string, string> = {
    onder_vraagprijs: 'bg-green-100 text-green-800',
    vraagprijs: 'bg-blue-100 text-blue-800',
    licht_boven: 'bg-yellow-100 text-yellow-800',
    boven_vraagprijs: 'bg-red-100 text-red-800',
  }
  const labels: Record<string, string> = {
    onder_vraagprijs: 'Onder vraagprijs',
    vraagprijs: 'Rond vraagprijs',
    licht_boven: 'Licht boven vraagprijs',
    boven_vraagprijs: 'Boven vraagprijs',
  }

  return (
    <span className={`px-3 py-1 rounded-full text-sm font-medium ${colors[advies] || 'bg-gray-100'}`}>
      {labels[advies] || advies}
    </span>
  )
}

function ConfidenceBar({ confidence }: { confidence: number }) {
  const percentage = Math.round(confidence * 100)
  const color = percentage >= 70 ? 'bg-green-500' : percentage >= 40 ? 'bg-yellow-500' : 'bg-red-500'

  return (
    <div className="mt-2">
      <div className="flex justify-between text-sm text-gray-600 mb-1">
        <span>Betrouwbaarheid</span>
        <span>{percentage}%</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-2">
        <div
          className={`${color} h-2 rounded-full transition-all`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}

function EnergyLabelBadge({ label }: { label: string }) {
  const colors: Record<string, string> = {
    'A++++': 'bg-green-700 text-white',
    'A+++': 'bg-green-600 text-white',
    'A++': 'bg-green-500 text-white',
    'A+': 'bg-green-400 text-white',
    A: 'bg-green-300 text-gray-800',
    B: 'bg-lime-300 text-gray-800',
    C: 'bg-yellow-300 text-gray-800',
    D: 'bg-orange-300 text-gray-800',
    E: 'bg-orange-400 text-white',
    F: 'bg-red-400 text-white',
    G: 'bg-red-600 text-white',
  }

  return (
    <span className={`px-2 py-1 rounded text-sm font-bold ${colors[label] || 'bg-gray-200'}`}>
      {label}
    </span>
  )
}

function DataBronnenFooter({ bronnen }: { bronnen: string[] }) {
  if (!bronnen || bronnen.length === 0) return null

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 mt-4">
      <div className="text-xs text-gray-500 font-medium mb-2">Gebruikte databronnen</div>
      <div className="flex flex-wrap gap-2">
        {bronnen.map((bron, index) => (
          <span
            key={index}
            className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-gray-200 text-gray-700"
          >
            {bron}
          </span>
        ))}
      </div>
    </div>
  )
}

function MonumentPanel({ monument }: { monument: MonumentResponse }) {
  if (!monument.heeft_monumentstatus) return null

  const items: { label: string; detail: string; url?: string }[] = []

  if (monument.rijksmonument?.is_monument) {
    items.push({
      label: 'Rijksmonument',
      detail: monument.rijksmonument.omschrijving
        ? `Nr. ${monument.rijksmonument.monumentnummer} — ${monument.rijksmonument.omschrijving}`
        : `Nr. ${monument.rijksmonument.monumentnummer}`,
      url: monument.rijksmonument.url,
    })
  }

  if (monument.gemeentelijk_monument?.is_monument) {
    items.push({
      label: 'Gemeentelijk monument',
      detail: monument.gemeentelijk_monument.omschrijving
        || `Gemeente ${monument.gemeentelijk_monument.gemeente}`,
    })
  }

  if (monument.beschermd_gezicht?.in_beschermd_gezicht) {
    const niveau = monument.beschermd_gezicht.niveau === 'gemeentelijk' ? 'Gemeentelijk' : 'Rijks'
    const type = monument.beschermd_gezicht.type === 'dorpsgezicht'
      ? 'beschermd dorpsgezicht'
      : 'beschermd stadsgezicht'
    items.push({
      label: `${niveau} ${type}`,
      detail: monument.beschermd_gezicht.naam || '',
    })
  }

  if (monument.unesco?.in_unesco) {
    items.push({
      label: 'UNESCO Werelderfgoed',
      detail: monument.unesco.naam || '',
    })
  }

  return (
    <div className="bg-amber-50 border border-amber-300 rounded-lg p-4">
      <div className="flex items-center gap-2 mb-2">
        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold bg-amber-200 text-amber-900">
          Monument
        </span>
        <span className="text-sm text-amber-700 font-medium">
          Deze woning heeft een monumentstatus
        </span>
      </div>
      <div className="space-y-2">
        {items.map((item, idx) => (
          <div key={idx} className="text-sm">
            <div className="font-medium text-amber-900">{item.label}</div>
            <div className="text-amber-700">
              {item.detail}
              {item.url && (
                <>
                  {' — '}
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-amber-900"
                  >
                    Bekijk in register
                  </a>
                </>
              )}
            </div>
          </div>
        ))}
      </div>
      <p className="text-xs text-amber-600 mt-2">
        Let op: monumentenstatus kan beperkingen opleggen bij verbouwing.
      </p>
    </div>
  )
}

function WoningGegevensColumn({ result }: { result: EnhancedWaardebepalingResponse }) {
  const WONINGTYPE_LABELS: Record<string, string> = {
    appartement: 'Appartement',
    tussenwoning: 'Tussenwoning',
    hoekwoning: 'Hoekwoning',
    'twee-onder-een-kap': 'Twee-onder-een-kap',
    vrijstaand: 'Vrijstaand',
  }

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold text-gray-900">Woninggegevens</h2>

      {/* Adres */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="text-sm text-gray-500">Adres</div>
        <div className="text-lg font-medium">{result.adres || `${result.postcode} ${result.huisnummer}`}</div>
      </div>

      {/* Kenmerken */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="text-sm text-gray-500 font-medium mb-3">Kenmerken</div>
        <dl className="space-y-2 text-sm">
          {result.woonoppervlakte && (
            <div className="flex justify-between">
              <dt className="text-gray-600">Woonoppervlakte</dt>
              <dd className="font-medium">{result.woonoppervlakte} m²</dd>
            </div>
          )}
          {result.grondoppervlakte && (
            <div className="flex justify-between">
              <dt className="text-gray-600">Grondoppervlakte</dt>
              <dd className="font-medium">{result.grondoppervlakte} m²</dd>
            </div>
          )}
          {result.bouwjaar && (
            <div className="flex justify-between">
              <dt className="text-gray-600">Bouwjaar</dt>
              <dd className="font-medium">{result.bouwjaar}</dd>
            </div>
          )}
          {result.woningtype && (
            <div className="flex justify-between">
              <dt className="text-gray-600">Woningtype</dt>
              <dd className="font-medium">{WONINGTYPE_LABELS[result.woningtype] || result.woningtype}</dd>
            </div>
          )}
          <div className="flex justify-between items-center">
            <dt className="text-gray-600">Energielabel</dt>
            <dd>
              {result.energielabel ? (
                <div className="flex items-center gap-2">
                  <EnergyLabelBadge label={result.energielabel} />
                  <span className="text-xs text-gray-400">({result.energielabel_bron === 'auto' ? 'EP-Online' : result.energielabel_bron})</span>
                </div>
              ) : (
                <span className="text-gray-400">Niet gevonden</span>
              )}
            </dd>
          </div>
        </dl>
      </div>

      {/* Monumentstatus */}
      {result.monument?.heeft_monumentstatus && (
        <MonumentPanel monument={result.monument} />
      )}

      {/* WOZ-waarde */}
      {(result.woz_waarde || result.grondoppervlakte) && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              {result.woz_waarde && (
                <>
                  <div className="text-sm text-blue-600 font-medium">WOZ-waarde</div>
                  <div className="text-lg font-semibold text-blue-800">{formatPrijs(result.woz_waarde)}</div>
                </>
              )}
            </div>
            {result.woz_peiljaar && (
              <div className="text-sm text-blue-500 text-right">
                Peildatum: 1 jan {result.woz_peiljaar}
              </div>
            )}
          </div>
          <p className="text-xs text-blue-600 mt-2">
            De WOZ-waarde is de waarde voor belastingdoeleinden, bepaald door de gemeente.
          </p>
        </div>
      )}

      {/* Buurtgegevens */}
      {(result.buurt_naam || result.buurt_gem_woz) && (
        <div className="bg-teal-50 border border-teal-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm text-teal-700 font-medium">Buurtindicatoren</div>
            {result.buurt_naam && (
              <div className="text-xs text-teal-600 truncate max-w-[180px]" title={result.buurt_naam}>
                {result.buurt_naam}
              </div>
            )}
          </div>
          <div className="grid grid-cols-3 gap-3">
            {result.buurt_gem_woz && (
              <div>
                <div className="text-xs text-teal-600">Gem. WOZ buurt</div>
                <div className="text-sm font-semibold text-teal-800">{formatPrijs(result.buurt_gem_woz)}</div>
              </div>
            )}
            {result.buurt_koopwoningen_pct !== undefined && result.buurt_koopwoningen_pct !== null && (
              <div>
                <div className="text-xs text-teal-600">Koopwoningen</div>
                <div className="text-sm font-semibold text-teal-800">{result.buurt_koopwoningen_pct.toFixed(0)}%</div>
              </div>
            )}
            {result.buurt_gem_inkomen && (
              <div>
                <div className="text-xs text-teal-600">Gem. inkomen</div>
                <div className="text-sm font-semibold text-teal-800">{formatPrijs(result.buurt_gem_inkomen)}</div>
              </div>
            )}
          </div>
          <p className="text-xs text-teal-600 mt-2">
            Bron: CBS Kerncijfers wijken en buurten
          </p>
        </div>
      )}
    </div>
  )
}

function GlasvezelPanel({ glasvezel }: { glasvezel: GlasvezelResponse }) {
  const beschikbaar = glasvezel.glasvezel_beschikbaar
  const formatSnelheid = (mbit: number) => mbit >= 1000 ? `${(mbit / 1000).toFixed(0)} Gbit/s` : `${mbit} Mbit/s`

  return (
    <div className="bg-indigo-50 border border-indigo-200 rounded-lg p-4">
      <div className="text-sm text-indigo-700 font-medium mb-2">Internet beschikbaarheid</div>
      <div className="space-y-1">
        <div className="flex justify-between text-sm">
          <span className="text-indigo-600">Glasvezel</span>
          <span className={`font-medium ${beschikbaar ? 'text-green-700' : 'text-gray-500'}`}>
            {beschikbaar ? 'Beschikbaar' : 'Niet beschikbaar'}
          </span>
        </div>
        {beschikbaar && glasvezel.glasvezel_snelheid && (
          <div className="flex justify-between text-sm">
            <span className="text-indigo-600">Max. snelheid glasvezel</span>
            <span className="text-indigo-800 font-medium">{formatSnelheid(glasvezel.glasvezel_snelheid)}</span>
          </div>
        )}
        {beschikbaar && glasvezel.glasvezel_provider && (
          <div className="flex justify-between text-sm">
            <span className="text-indigo-600">Glasvezel netwerk</span>
            <span className="text-indigo-800 font-medium capitalize">{glasvezel.glasvezel_provider}</span>
          </div>
        )}
        {glasvezel.kabel_beschikbaar && glasvezel.kabel_snelheid && (
          <div className="flex justify-between text-sm">
            <span className="text-indigo-600">Kabel</span>
            <span className="text-indigo-800 font-medium">
              {formatSnelheid(glasvezel.kabel_snelheid)}
              {glasvezel.kabel_provider && <span className="text-indigo-500 font-normal capitalize"> ({glasvezel.kabel_provider})</span>}
            </span>
          </div>
        )}
        {glasvezel.dsl_snelheid && !glasvezel.glasvezel_beschikbaar && !glasvezel.kabel_beschikbaar && (
          <div className="flex justify-between text-sm">
            <span className="text-indigo-600">DSL</span>
            <span className="text-indigo-800 font-medium">{formatSnelheid(glasvezel.dsl_snelheid)}</span>
          </div>
        )}
      </div>
    </div>
  )
}

function OrientatiePanel({ orientatie }: { orientatie: OrientatieResponse }) {
  const scoreKleur = (score: number) => {
    if (score >= 8) return 'text-green-700 bg-green-100'
    if (score >= 6) return 'text-yellow-700 bg-yellow-100'
    if (score >= 4) return 'text-orange-700 bg-orange-100'
    return 'text-red-700 bg-red-100'
  }

  const zonLabelKleur = (label: string) => {
    if (label === 'Veel zon') return 'text-yellow-700'
    if (label === 'Gemiddeld') return 'text-orange-600'
    return 'text-gray-600'
  }

  return (
    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
      <div className="text-sm text-yellow-700 font-medium mb-3">Zon & Oriëntatie</div>

      {/* Tuinoriëntatie */}
      {orientatie.tuin_orientatie && (
        <div className="mb-3">
          <div className="flex justify-between text-sm">
            <span className="text-yellow-600">Tuin</span>
            <span className="text-yellow-800 font-medium">
              {orientatie.tuin_orientatie}
              {orientatie.funda_tuin_orientatie && (
                <span className="ml-2 text-xs font-normal">
                  {orientatie.tuin_orientatie === orientatie.funda_tuin_orientatie ? (
                    <span className="text-green-600">· Funda: {orientatie.funda_tuin_orientatie} ✓</span>
                  ) : (
                    <span className="text-orange-500">· Funda: {orientatie.funda_tuin_orientatie}</span>
                  )}
                </span>
              )}
            </span>
          </div>
          {orientatie.tuin_oppervlakte_berekend != null && orientatie.tuin_oppervlakte_berekend > 0 && (
            <div className="flex justify-between text-sm">
              <span className="text-yellow-600">Tuinoppervlakte</span>
              <span className="text-yellow-800">
                {Math.round(orientatie.tuin_oppervlakte_berekend)} m²
                {orientatie.tuin_oppervlakte_bron === 'schatting' && (
                  <span className="ml-1 text-xs text-yellow-400 italic">(inschatting)</span>
                )}
                {orientatie.tuin_oppervlakte_bron === 'funda' && orientatie.funda_tuin_orientatie && (
                  <span className="ml-1 text-xs text-green-500">(Funda)</span>
                )}
              </span>
            </div>
          )}
          {orientatie.betrouwbaarheid && (
            <div className="text-xs mt-0.5">
              <span className={
                orientatie.betrouwbaarheid === 'hoog' ? 'text-green-600' :
                orientatie.betrouwbaarheid === 'middel' ? 'text-yellow-600' :
                'text-orange-500'
              }>
                Betrouwbaarheid: {orientatie.betrouwbaarheid}
              </span>
            </div>
          )}
        </div>
      )}

      {/* Zonuren */}
      {orientatie.zon_uren_zomer != null && (
        <div className="mb-3">
          <div className="flex items-center justify-between text-sm mb-1">
            <span className="text-yellow-600">Zonuren tuin</span>
            {orientatie.zon_label && (
              <span className={`text-xs font-medium ${zonLabelKleur(orientatie.zon_label)}`}>
                {orientatie.zon_label}
              </span>
            )}
          </div>
          <div className="grid grid-cols-3 gap-2">
            <div className="text-center">
              <div className="text-xs text-yellow-500">Zomer</div>
              <div className="text-sm font-semibold text-yellow-800">{orientatie.zon_uren_zomer}u</div>
            </div>
            {orientatie.zon_uren_lente != null && (
              <div className="text-center">
                <div className="text-xs text-yellow-500">Lente/Herfst</div>
                <div className="text-sm font-semibold text-yellow-800">{orientatie.zon_uren_lente}u</div>
              </div>
            )}
            {orientatie.zon_uren_winter != null && (
              <div className="text-center">
                <div className="text-xs text-yellow-500">Winter</div>
                <div className="text-sm font-semibold text-yellow-800">{orientatie.zon_uren_winter}u</div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Schaduw info */}
      {(orientatie.schaduw_eigen_gebouw || orientatie.schaduw_buren || orientatie.schaduw_bomen) && (
        <div className="mb-3 space-y-1">
          {orientatie.schaduw_eigen_gebouw && (
            <div className="text-xs text-yellow-600">{orientatie.schaduw_eigen_gebouw}</div>
          )}
          {orientatie.schaduw_buren && (
            <div className="text-xs text-yellow-600">{orientatie.schaduw_buren}</div>
          )}
          {orientatie.schaduw_bomen && (
            <div className="text-xs text-yellow-600">{orientatie.schaduw_bomen}</div>
          )}
        </div>
      )}

      {/* Zonnepanelen */}
      {orientatie.zonnepanelen_score != null && (
        <div className="border-t border-yellow-200 pt-2 mt-2">
          <div className="flex items-center justify-between text-sm mb-1">
            <span className="text-yellow-600">Zonnepanelen</span>
            <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${scoreKleur(orientatie.zonnepanelen_score)}`}>
              {orientatie.zonnepanelen_score}/10 {orientatie.zonnepanelen_label}
            </span>
          </div>
          <div className="space-y-0.5">
            {orientatie.dak_orientatie && (
              <div className="flex justify-between text-xs">
                <span className="text-yellow-500">Dak</span>
                <span className="text-yellow-700">{orientatie.dak_orientatie}{orientatie.dak_hellingshoek != null && `, ${orientatie.dak_hellingshoek}°`}</span>
              </div>
            )}
            {orientatie.geschikt_dakoppervlak != null && orientatie.geschikt_dakoppervlak > 0 && (
              <div className="flex justify-between text-xs">
                <span className="text-yellow-500">Geschikt oppervlak</span>
                <span className="text-yellow-700">{Math.round(orientatie.geschikt_dakoppervlak)} m²</span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Disclaimer */}
      {orientatie.methode && (
        <div className="text-xs text-yellow-400 mt-2">
          Indicatief ({orientatie.methode})
        </div>
      )}
    </div>
  )
}

function FundaListingPanel({ listing, bagWoonoppervlakte, plafondhoogte }: { listing: FundaListing, bagWoonoppervlakte?: number, plafondhoogte?: PlafondhoogteResponse }) {
  const detailRow = (label: string, value: string | number | boolean | undefined | null) => {
    if (value === undefined || value === null) return null
    const display = typeof value === 'boolean' ? (value ? 'Ja' : 'Nee') : String(value)
    return (
      <div className="flex justify-between text-sm">
        <span className="text-orange-600">{label}</span>
        <span className="text-orange-800 font-medium text-right max-w-[60%]">{display}</span>
      </div>
    )
  }

  return (
    <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="text-sm text-orange-700 font-medium">Funda listing</div>
          {listing.status === 'verkocht' && (
            <span className="px-2 py-0.5 bg-red-100 text-red-700 text-xs font-semibold rounded-full border border-red-200 uppercase tracking-wide">
              Verkocht
            </span>
          )}
          {listing.status === 'onder bod' && (
            <span className="px-2 py-0.5 bg-yellow-100 text-yellow-700 text-xs font-semibold rounded-full border border-yellow-200 uppercase tracking-wide">
              Onder bod
            </span>
          )}
        </div>
        <a
          href={listing.url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-orange-500 hover:text-orange-700 underline"
        >
          Bekijk op Funda &rarr;
        </a>
      </div>

      {/* Basis kenmerken */}
      <div className="space-y-1 mb-3">
        {listing.vraagprijs && (
          <div className="flex justify-between">
            <span className="text-sm text-orange-600">Vraagprijs</span>
            <span className="text-lg font-semibold text-orange-800">
              {formatPrijs(listing.vraagprijs)}
              {listing.vraagprijs_suffix && (
                <span className="text-xs font-normal ml-1">{listing.vraagprijs_suffix}</span>
              )}
            </span>
          </div>
        )}
        {listing.prijs_per_m2 && detailRow('Prijs/m²', formatM2Prijs(listing.prijs_per_m2))}
        {detailRow('Woonoppervlakte', listing.woonoppervlakte ? `${listing.woonoppervlakte} m²` : undefined)}
        {listing.woonoppervlakte && bagWoonoppervlakte && Math.abs(listing.woonoppervlakte - bagWoonoppervlakte) / bagWoonoppervlakte > 0.05 && (
          <div className="text-xs text-orange-500 italic pl-1">
            BAG: {bagWoonoppervlakte} m² (verschil {Math.round(Math.abs(listing.woonoppervlakte - bagWoonoppervlakte) / bagWoonoppervlakte * 100)}%)
          </div>
        )}
        {detailRow('Buitenruimte', listing.buitenruimte ? `${listing.buitenruimte} m²` : undefined)}
        {detailRow('Perceeloppervlakte', listing.perceeloppervlakte ? `${listing.perceeloppervlakte} m²` : undefined)}
        {detailRow('Inhoud', listing.inhoud ? `${listing.inhoud} m³` : undefined)}
        {detailRow('Kamers', listing.kamers ? `${listing.kamers}${listing.slaapkamers ? ` (${listing.slaapkamers} slaapkamers)` : ''}` : undefined)}
        {detailRow('Badkamers', listing.badkamers)}
        {detailRow('Bouwjaar', listing.bouwjaar)}
        {detailRow('Woningtype', listing.woningtype)}
        {detailRow('Energielabel', listing.energielabel)}
        {listing.aangeboden_sinds && detailRow('Aangeboden sinds', listing.aangeboden_sinds)}
        {listing.verkoopdatum && detailRow('Verkoopdatum', listing.verkoopdatum)}
        {listing.looptijd_dagen != null && detailRow('Looptijd', `${listing.looptijd_dagen} dagen`)}
      </div>

      {/* Eigendom */}
      {(listing.eigendom_type || listing.vve_bijdrage || listing.erfpacht_bedrag) && (
        <div className="border-t border-orange-200 pt-2 mt-2 space-y-1">
          <div className="text-xs text-orange-500 font-medium uppercase tracking-wide">Eigendom</div>
          {detailRow('Eigendomssituatie', listing.eigendom_type)}
          {listing.vve_bijdrage && detailRow('VvE-bijdrage', `€ ${listing.vve_bijdrage}/mnd`)}
          {listing.erfpacht_bedrag && detailRow('Erfpacht', `€ ${listing.erfpacht_bedrag}/jaar`)}
        </div>
      )}

      {/* Tuin & buitenruimte */}
      {(listing.tuin_type || listing.balkon || listing.dakterras) && (
        <div className="border-t border-orange-200 pt-2 mt-2 space-y-1">
          <div className="text-xs text-orange-500 font-medium uppercase tracking-wide">Buitenruimte</div>
          {detailRow('Tuin', listing.tuin_type)}
          {listing.tuin_oppervlakte && detailRow('Tuinoppervlakte', `${listing.tuin_oppervlakte} m²`)}
          {detailRow('Ligging tuin', listing.tuin_orientatie)}
          {listing.balkon && detailRow('Balkon', true)}
          {listing.dakterras && detailRow('Dakterras', true)}
        </div>
      )}

      {/* Indeling & parkeren */}
      {(listing.verdiepingen || listing.garage_type || listing.kelder || listing.zolder) && (
        <div className="border-t border-orange-200 pt-2 mt-2 space-y-1">
          <div className="text-xs text-orange-500 font-medium uppercase tracking-wide">Indeling & parkeren</div>
          {detailRow('Woonlagen', listing.verdiepingen)}
          {detailRow('Garage', listing.garage_type)}
          {listing.parkeerplaatsen && detailRow('Parkeerplaatsen', listing.parkeerplaatsen)}
          {detailRow('Parkeren', listing.parkeer_type)}
          {listing.kelder && detailRow('Kelder', true)}
          {detailRow('Zolder', listing.zolder)}
          {detailRow('Berging', listing.berging)}
        </div>
      )}

      {/* Extra */}
      {(listing.isolatie || listing.verwarming || listing.dak_type || plafondhoogte?.geschatte_verdiepingshoogte) && (
        <div className="border-t border-orange-200 pt-2 mt-2 space-y-1">
          <div className="text-xs text-orange-500 font-medium uppercase tracking-wide">Technisch</div>
          {detailRow('Isolatie', listing.isolatie)}
          {detailRow('Verwarming', listing.verwarming)}
          {detailRow('Dak', listing.dak_type)}
          {plafondhoogte?.geschatte_verdiepingshoogte && detailRow(
            'Geschatte plafondhoogte',
            `~${plafondhoogte.geschatte_verdiepingshoogte.toFixed(1)}m (${plafondhoogte.label}${plafondhoogte.betrouwbaarheid ? `, ${plafondhoogte.betrouwbaarheid}` : ''})`
          )}
        </div>
      )}
    </div>
  )
}

function AnalyseColumn({ result, onCopy, copied, orientatieData, isOrientatieLoading }: {
  result: EnhancedWaardebepalingResponse
  onCopy: (waarde: number) => void
  copied: boolean
  orientatieData?: OrientatieResponse
  isOrientatieLoading?: boolean
}) {
  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold text-gray-900">Analyse & Advies</h2>

      {/* Geschatte waarde */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-center">
          <div className="text-sm text-gray-500 mb-1">Geschatte marktwaarde</div>
          <div className="text-4xl font-bold text-primary-700">
            {formatPrijs(result.waarde_midden)}
          </div>
          <div className="text-gray-500 mt-1">
            {formatPrijs(result.waarde_laag)} - {formatPrijs(result.waarde_hoog)}
          </div>
          {result.woonoppervlakte && (
            <div className="text-sm text-gray-500 mt-1">
              {formatM2Prijs(result.waarde_midden / result.woonoppervlakte)}
            </div>
          )}
        </div>

        <ConfidenceBar confidence={result.confidence} />

        {result.vraagprijs && (
          <div className="mt-4 pt-4 border-t">
            <div className="flex justify-between items-center">
              <span className="text-gray-600">Vraagprijs</span>
              <span className="font-medium">{formatPrijs(result.vraagprijs)}</span>
            </div>
            {result.woonoppervlakte && (
              <div className="flex justify-between items-center mt-1">
                <span className="text-gray-600">Vraagprijs/m²</span>
                <span className="font-medium">{formatM2Prijs(result.vraagprijs! / result.woonoppervlakte)}</span>
              </div>
            )}
            {result.verschil_percentage !== null && result.verschil_percentage !== undefined && (
              <div className="flex justify-between items-center mt-1">
                <span className="text-gray-600">Verschil</span>
                <span
                  className={
                    result.verschil_percentage > 0
                      ? 'text-red-600 font-medium'
                      : 'text-green-600 font-medium'
                  }
                >
                  {result.verschil_percentage > 0 ? '+' : ''}
                  {result.verschil_percentage.toFixed(1)}%
                </span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Biedadvies */}
      {result.vraagprijs && (
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-lg font-semibold">Biedadvies</h3>
            <BiedAdviesBadge advies={result.bied_advies} />
          </div>
          <div className="bg-gray-50 rounded-lg p-4">
            <div className="text-sm text-gray-600">Aanbevolen biedingsbereik</div>
            <div className="flex items-center justify-between">
              <div className="text-xl font-semibold">
                {formatPrijs(result.bied_range_laag)} - {formatPrijs(result.bied_range_hoog)}
              </div>
              <button
                onClick={() => onCopy(result.waarde_midden)}
                className="px-3 py-1 text-sm font-medium rounded-lg bg-primary-100 text-primary-700 hover:bg-primary-200 transition-colors"
              >
                {copied ? 'Gekopieerd!' : 'Kopieer bedrag'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Vergelijkbare verkopen */}
      <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
        <div className="text-sm text-purple-600 font-medium">Vergelijkbare verkopen</div>
        {result.comparables_count > 0 ? (
          <div className="mt-1">
            <span className="text-lg font-semibold text-purple-800">{result.comparables_count}</span>
            <span className="text-purple-600 text-sm ml-1">recent verkochte woningen in de buurt</span>
            {result.comparables_avg_m2 && (
              <div className="text-sm text-purple-700 mt-1">
                Gem. {formatM2Prijs(result.comparables_avg_m2)}
              </div>
            )}
            {result.comparables_avg_m2 && result.woonoppervlakte && (() => {
              const geschatM2 = result.waarde_midden / result.woonoppervlakte
              const verschilPct = ((geschatM2 - result.comparables_avg_m2!) / result.comparables_avg_m2!) * 100
              const color = verschilPct < -10 ? 'text-green-700 bg-green-100' : verschilPct > 10 ? 'text-red-700 bg-red-100' : 'text-yellow-700 bg-yellow-100'
              const label = verschilPct < -10 ? 'Onder buurtgemiddelde' : verschilPct > 10 ? 'Boven buurtgemiddelde' : 'Rond buurtgemiddelde'
              return (
                <div className={`text-xs font-medium mt-2 px-2 py-1 rounded-full inline-block ${color}`}>
                  {label} ({verschilPct > 0 ? '+' : ''}{verschilPct.toFixed(0)}%)
                </div>
              )
            })()}
          </div>
        ) : (
          <div className="text-sm text-purple-600 mt-1">
            Geen recente transacties gevonden
          </div>
        )}
      </div>

      {/* Funda listing */}
      {result.funda_listing && (
        <FundaListingPanel listing={result.funda_listing} bagWoonoppervlakte={result.woonoppervlakte} plafondhoogte={result.plafondhoogte} />
      )}

      {/* Plafondhoogte (standalone als geen Funda listing) */}
      {!result.funda_listing && result.plafondhoogte?.geschatte_verdiepingshoogte && (
        <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-orange-800 mb-2">Geschatte plafondhoogte</h3>
          <div className="flex justify-between text-sm">
            <span className="text-orange-600">Verdiepingshoogte</span>
            <span className="text-orange-800 font-medium text-right max-w-[60%]">
              ~{result.plafondhoogte.geschatte_verdiepingshoogte.toFixed(1)}m ({result.plafondhoogte.label}{result.plafondhoogte.betrouwbaarheid ? `, ${result.plafondhoogte.betrouwbaarheid}` : ''})
            </span>
          </div>
          {result.plafondhoogte.details && (
            <div className="text-xs text-orange-400 mt-1">{result.plafondhoogte.details}</div>
          )}
        </div>
      )}

      {/* Glasvezel beschikbaarheid */}
      {result.glasvezel && (
        <GlasvezelPanel glasvezel={result.glasvezel} />
      )}

      {/* Zon en oriëntatie — asynchroon geladen via /3d endpoint */}
      {isOrientatieLoading ? (
        <div className="bg-white rounded-lg shadow p-4 animate-pulse">
          <div className="h-5 bg-gray-200 rounded w-1/3 mb-3"></div>
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
          <div className="h-4 bg-gray-200 rounded w-2/3 mb-2"></div>
          <div className="h-4 bg-gray-200 rounded w-1/2"></div>
        </div>
      ) : orientatieData ? (
        <OrientatiePanel orientatie={orientatieData} />
      ) : null}

      {/* Marktindicatoren */}
      {(result.markt_gem_prijs || result.markt_overbiedpct || result.markt_verkooptijd) && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm text-amber-700 font-medium">Marktindicatoren regio</div>
            {result.markt_peildatum && (
              <div className="text-xs text-amber-500">{result.markt_peildatum}</div>
            )}
          </div>
          <div className="grid grid-cols-3 gap-3">
            {result.markt_gem_prijs && (
              <div>
                <div className="text-xs text-amber-600">Gem. verkoopprijs</div>
                <div className="text-sm font-semibold text-amber-800">{formatPrijs(result.markt_gem_prijs)}</div>
              </div>
            )}
            {result.markt_overbiedpct !== undefined && result.markt_overbiedpct !== null && (
              <div>
                <div className="text-xs text-amber-600">Overbieden</div>
                <div className={`text-sm font-semibold ${result.markt_overbiedpct >= 0 ? 'text-red-600' : 'text-green-600'}`}>
                  {result.markt_overbiedpct >= 0 ? '+' : ''}{result.markt_overbiedpct.toFixed(1)}%
                </div>
              </div>
            )}
            {result.markt_verkooptijd && (
              <div>
                <div className="text-xs text-amber-600">Gem. verkooptijd</div>
                <div className="text-sm font-semibold text-amber-800">{result.markt_verkooptijd} dagen</div>
              </div>
            )}
          </div>
          <p className="text-xs text-amber-600 mt-2">
            Bron: CBS StatLine
          </p>
        </div>
      )}

      {/* Opbouw schatting */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">Opbouw schatting</h3>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600">Basiswaarde (m² x buurtprijs)</span>
            <span>{formatPrijs(result.basis_waarde)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Energielabel correctie</span>
            <span className={result.energielabel_correctie >= 0 ? 'text-green-600' : 'text-red-600'}>
              {result.energielabel_correctie >= 0 ? '+' : ''}{formatPrijs(result.energielabel_correctie)}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Bouwjaar correctie</span>
            <span className={result.bouwjaar_correctie >= 0 ? 'text-green-600' : 'text-red-600'}>
              {result.bouwjaar_correctie >= 0 ? '+' : ''}{formatPrijs(result.bouwjaar_correctie)}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Woningtype correctie</span>
            <span className={result.woningtype_correctie >= 0 ? 'text-green-600' : 'text-red-600'}>
              {result.woningtype_correctie >= 0 ? '+' : ''}{formatPrijs(result.woningtype_correctie)}
            </span>
          </div>
          {result.perceel_correctie !== 0 && (
            <div className="flex justify-between">
              <span className="text-gray-600">Perceelgrootte correctie</span>
              <span className={result.perceel_correctie >= 0 ? 'text-green-600' : 'text-red-600'}>
                {result.perceel_correctie >= 0 ? '+' : ''}{formatPrijs(result.perceel_correctie)}
              </span>
            </div>
          )}
          {result.buurt_kwaliteit_correctie !== 0 && (
            <div className="flex justify-between">
              <span className="text-gray-600">Buurtcorrectie (kwaliteit)</span>
              <span className={result.buurt_kwaliteit_correctie >= 0 ? 'text-green-600' : 'text-red-600'}>
                {result.buurt_kwaliteit_correctie >= 0 ? '+' : ''}{formatPrijs(result.buurt_kwaliteit_correctie)}
              </span>
            </div>
          )}
          <div className="flex justify-between">
            <span className="text-gray-600">Marktcorrectie (overbieden)</span>
            <span className="text-green-600">
              +{formatPrijs(result.markt_correctie)}
            </span>
          </div>
          <div className="flex justify-between pt-2 border-t font-semibold">
            <span>Totaal</span>
            <span>{formatPrijs(result.waarde_midden)}</span>
          </div>
        </div>
      </div>

      {/* Databronnen */}
      <DataBronnenFooter bronnen={result.data_bronnen} />
    </div>
  )
}

export default function WaardebepalingPage() {
  const [formData, setFormData] = useState<EnhancedWaardebepalingRequest>({
    postcode: '',
    huisnummer: 0,
    huisletter: undefined,
    toevoeging: undefined,
  })

  const huisnummerRef = useRef<HTMLInputElement>(null)

  const [copied, setCopied] = useState(false)
  const [omgevingGeoJSON, setOmgevingGeoJSON] = useState<OmgevingsAnalyseResponse | null>(null)

  const handleCopy = (waardeMidden: number) => {
    navigator.clipboard.writeText(String(Math.round(waardeMidden)))
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const location = useLocation()
  const queryClient = useQueryClient()

  const mutation = useMutation({
    mutationFn: berekenWaardeVoorAdres,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['woningen'] })
      queryClient.invalidateQueries({ queryKey: ['woningen-geojson'] })
    },
  })

  // 3D oriëntatie-data — asynchroon opgehaald zodra waardebepaling klaar is
  const { data: orientatieData, isLoading: isOrientatieLoading } = useQuery({
    queryKey: ['waardebepaling-3d', mutation.data?.postcode, mutation.data?.huisnummer],
    queryFn: () => getWaardebepaling3D(
      mutation.data!.postcode,
      mutation.data!.huisnummer,
      mutation.data?.pand_identificatie ?? undefined,
    ),
    enabled: !!mutation.data,
    staleTime: Infinity,
    retry: 1,
  })

  // Auto-fill en auto-submit vanuit WoningCard (via navigate state)
  useEffect(() => {
    const state = location.state as { autoWaardebepaling?: EnhancedWaardebepalingRequest } | null
    if (state?.autoWaardebepaling) {
      const data = state.autoWaardebepaling
      setFormData(data)
      mutation.mutate(data)
      // State opschonen zodat refresh niet opnieuw triggert
      window.history.replaceState({}, '', '/')
    }
  }, [location.state]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    mutation.mutate(formData)
  }

  const result = mutation.data

  return (
    <div className="max-w-6xl mx-auto">
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Waardebepaling</h1>
      <p className="text-gray-600 mb-6">
        Bereken de geschatte marktwaarde en krijg biedadvies
      </p>

      {/* Zoekformulier */}
      <div className="bg-white rounded-lg shadow p-6 mb-8">
        <form onSubmit={handleSubmit}>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Postcode *
              </label>
              <input
                type="text"
                required
                placeholder="1234 AB"
                maxLength={7}
                value={formData.postcode}
                onChange={(e) => {
                  const val = e.target.value.toUpperCase()
                  setFormData({ ...formData, postcode: val })
                  if (/^\d{4}\s?[A-Z]{2}$/.test(val)) {
                    huisnummerRef.current?.focus()
                  }
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Huisnummer *
              </label>
              <input
                ref={huisnummerRef}
                type="number"
                required
                min={1}
                value={formData.huisnummer || ''}
                onChange={(e) =>
                  setFormData({ ...formData, huisnummer: Number(e.target.value) })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Huisletter
              </label>
              <input
                type="text"
                maxLength={2}
                placeholder="A"
                value={formData.huisletter || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    huisletter: e.target.value.toUpperCase() || undefined,
                  })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Toevoeging
              </label>
              <input
                type="text"
                maxLength={10}
                placeholder="bis, 2"
                value={formData.toevoeging || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    toevoeging: e.target.value || undefined,
                  })
                }
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
            <div className="flex items-end">
              <button
                type="submit"
                disabled={mutation.isPending}
                className="w-full bg-primary-600 text-white py-2 px-4 rounded-lg hover:bg-primary-700 transition-colors disabled:opacity-50"
              >
                {mutation.isPending ? 'Ophalen...' : 'Zoek waarde'}
              </button>
            </div>
          </div>
        </form>
      </div>

      {/* Error */}
      {mutation.isError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 mb-8">
          {mutation.error instanceof Error ? mutation.error.message : 'Er is een fout opgetreden bij het berekenen van de waarde.'}
        </div>
      )}

      {/* Resultaten in twee kolommen */}
      {result && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <div className="lg:col-span-1 space-y-6">
            <WoningGegevensColumn result={result} />
            <VoorzieningenPanel postcode={result.postcode} huisnummer={result.huisnummer} />
            <BestemmingsplanPanel
              lat={result.latitude}
              lng={result.longitude}
              onOmgevingGeoJSON={setOmgevingGeoJSON}
            />
          </div>
          <div className="lg:col-span-2">
            <AnalyseColumn
              result={result}
              onCopy={handleCopy}
              copied={copied}
              orientatieData={orientatieData}
              isOrientatieLoading={isOrientatieLoading}
            />
          </div>
        </div>
      )}

      {/* Bestemmingen kaart */}
      {result && omgevingGeoJSON && omgevingGeoJSON.features.length > 0 && result.latitude && result.longitude && (
        <div className="mt-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-2">Bestemmingen omgeving</h3>
          <BuurtMap
            bestemmingsvlakkenGeoJSON={omgevingGeoJSON}
            bestemmingCenter={[result.latitude, result.longitude]}
          />
        </div>
      )}

      {/* Lege staat */}
      {!result && !mutation.isError && !mutation.isPending && (
        <div className="bg-gray-50 rounded-lg p-8 text-center text-gray-500">
          Vul het adres in om WOZ-waarde, energielabel en waardebepaling op te halen
        </div>
      )}
    </div>
  )
}
