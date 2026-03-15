import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  berekenWaarde,
  berekenWaardeVoorAdres,
  WaardebepalingRequest,
  EnhancedWaardebepalingRequest,
  formatPrijs,
  formatM2Prijs,
} from '../services/api'

const ENERGY_LABELS = ['A++++', 'A+++', 'A++', 'A+', 'A', 'B', 'C', 'D', 'E', 'F', 'G']
const PROPERTY_TYPES = [
  { value: 'appartement', label: 'Appartement' },
  { value: 'tussenwoning', label: 'Tussenwoning' },
  { value: 'hoekwoning', label: 'Hoekwoning' },
  { value: 'twee-onder-een-kap', label: 'Twee-onder-een-kap' },
  { value: 'vrijstaand', label: 'Vrijstaand' },
]

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

function WOZCard({
  wozWaarde,
  peiljaar,
  grondoppervlakte,
}: {
  wozWaarde?: number
  peiljaar?: number
  grondoppervlakte?: number
}) {
  if (!wozWaarde && !grondoppervlakte) return null

  return (
    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
      <div className="flex items-center justify-between">
        <div>
          {wozWaarde && (
            <>
              <div className="text-sm text-blue-600 font-medium">WOZ-waarde</div>
              <div className="text-lg font-semibold text-blue-800">{formatPrijs(wozWaarde)}</div>
            </>
          )}
        </div>
        <div className="text-right">
          {peiljaar && (
            <div className="text-sm text-blue-500">
              Peildatum: 1 jan {peiljaar}
            </div>
          )}
          {grondoppervlakte && (
            <div className="text-sm text-blue-600 mt-1">
              Perceel: <span className="font-medium">{grondoppervlakte} m²</span>
            </div>
          )}
        </div>
      </div>
      <p className="text-xs text-blue-600 mt-2">
        De WOZ-waarde is de waarde voor belastingdoeleinden, bepaald door de gemeente.
      </p>
    </div>
  )
}

function ComparablesCard({
  count,
  avgM2,
}: {
  count: number
  avgM2?: number
}) {
  return (
    <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
      <div className="text-sm text-purple-600 font-medium">Vergelijkbare verkopen</div>
      {count > 0 ? (
        <div className="mt-1">
          <span className="text-lg font-semibold text-purple-800">{count}</span>
          <span className="text-purple-600 text-sm ml-1">recent verkochte woningen in de buurt</span>
          {avgM2 && (
            <div className="text-sm text-purple-700 mt-1">
              Gem. {formatM2Prijs(avgM2)}
            </div>
          )}
        </div>
      ) : (
        <div className="text-sm text-purple-600 mt-1">
          Geen recente transacties gevonden
        </div>
      )}
    </div>
  )
}

function MarktIndicatorenCard({
  gemPrijs,
  overbiedPct,
  verkooptijd,
  peildatum,
}: {
  gemPrijs?: number
  overbiedPct?: number
  verkooptijd?: number
  peildatum?: string
}) {
  if (!gemPrijs && !overbiedPct && !verkooptijd) return null

  return (
    <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <div className="text-sm text-amber-700 font-medium">Marktindicatoren regio</div>
        {peildatum && (
          <div className="text-xs text-amber-500">{peildatum}</div>
        )}
      </div>
      <div className="grid grid-cols-3 gap-3">
        {gemPrijs && (
          <div>
            <div className="text-xs text-amber-600">Gem. verkoopprijs</div>
            <div className="text-sm font-semibold text-amber-800">{formatPrijs(gemPrijs)}</div>
          </div>
        )}
        {overbiedPct !== undefined && overbiedPct !== null && (
          <div>
            <div className="text-xs text-amber-600">Overbieden</div>
            <div className={`text-sm font-semibold ${overbiedPct >= 0 ? 'text-red-600' : 'text-green-600'}`}>
              {overbiedPct >= 0 ? '+' : ''}{overbiedPct.toFixed(1)}%
            </div>
          </div>
        )}
        {verkooptijd && (
          <div>
            <div className="text-xs text-amber-600">Gem. verkooptijd</div>
            <div className="text-sm font-semibold text-amber-800">{verkooptijd} dagen</div>
          </div>
        )}
      </div>
      <p className="text-xs text-amber-600 mt-2">
        Bron: CBS StatLine
      </p>
    </div>
  )
}

function BuurtCard({
  buurtNaam,
  gemWoz,
  koopwoningenPct,
  gemInkomen,
}: {
  buurtNaam?: string
  gemWoz?: number
  koopwoningenPct?: number
  gemInkomen?: number
}) {
  if (!buurtNaam && !gemWoz) return null

  return (
    <div className="bg-teal-50 border border-teal-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <div className="text-sm text-teal-700 font-medium">Buurtindicatoren</div>
        {buurtNaam && (
          <div className="text-xs text-teal-600 truncate max-w-[180px]" title={buurtNaam}>
            {buurtNaam}
          </div>
        )}
      </div>
      <div className="grid grid-cols-3 gap-3">
        {gemWoz && (
          <div>
            <div className="text-xs text-teal-600">Gem. WOZ buurt</div>
            <div className="text-sm font-semibold text-teal-800">{formatPrijs(gemWoz)}</div>
          </div>
        )}
        {koopwoningenPct !== undefined && koopwoningenPct !== null && (
          <div>
            <div className="text-xs text-teal-600">Koopwoningen</div>
            <div className="text-sm font-semibold text-teal-800">{koopwoningenPct.toFixed(0)}%</div>
          </div>
        )}
        {gemInkomen && (
          <div>
            <div className="text-xs text-teal-600">Gem. inkomen</div>
            <div className="text-sm font-semibold text-teal-800">{formatPrijs(gemInkomen)}</div>
          </div>
        )}
      </div>
      <p className="text-xs text-teal-600 mt-2">
        Bron: CBS Kerncijfers wijken en buurten
      </p>
    </div>
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

type Mode = 'simple' | 'address'

export default function WaardebepalingPage() {
  const [mode, setMode] = useState<Mode>('address')

  // Simple mode form data
  const [simpleFormData, setSimpleFormData] = useState<WaardebepalingRequest>({
    woonoppervlakte: 100,
    energielabel: 'C',
    bouwjaar: 1980,
    woningtype: 'tussenwoning',
    vraagprijs: undefined,
  })

  // Address mode form data
  const [addressFormData, setAddressFormData] = useState<EnhancedWaardebepalingRequest>({
    postcode: '',
    huisnummer: 0,
    huisletter: undefined,
    toevoeging: undefined,
    woonoppervlakte: undefined,
    vraagprijs: undefined,
    woningtype: undefined,
  })

  // Simple mode mutation
  const simpleMutation = useMutation({
    mutationFn: berekenWaarde,
  })

  // Address mode mutation
  const addressMutation = useMutation({
    mutationFn: berekenWaardeVoorAdres,
  })

  const handleSimpleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    simpleMutation.mutate(simpleFormData)
  }

  const handleAddressSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    addressMutation.mutate(addressFormData)
  }

  const simpleResult = simpleMutation.data
  const addressResult = addressMutation.data
  const isPending = simpleMutation.isPending || addressMutation.isPending
  const isError = simpleMutation.isError || addressMutation.isError
  const error = simpleMutation.error || addressMutation.error

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-3xl font-bold text-gray-900 mb-2">Waardebepaling</h1>
      <p className="text-gray-600 mb-6">
        Bereken de geschatte marktwaarde en krijg biedadvies
      </p>

      {/* Mode selector */}
      <div className="flex space-x-2 mb-6">
        <button
          onClick={() => setMode('address')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            mode === 'address'
              ? 'bg-primary-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
        >
          Op basis van adres
        </button>
        <button
          onClick={() => setMode('simple')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            mode === 'simple'
              ? 'bg-primary-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
        >
          Handmatig invoeren
        </button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Form */}
        <div className="bg-white rounded-lg shadow p-6">
          {mode === 'address' ? (
            <>
              <h2 className="text-lg font-semibold mb-4">Adresgegevens</h2>
              <p className="text-sm text-gray-500 mb-4">
                Voer het adres in en we halen automatisch WOZ-waarde, energielabel en vergelijkbare verkopen op.
              </p>
              <form onSubmit={handleAddressSubmit} className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Postcode *
                    </label>
                    <input
                      type="text"
                      required
                      placeholder="1234 AB"
                      maxLength={7}
                      value={addressFormData.postcode}
                      onChange={(e) =>
                        setAddressFormData({ ...addressFormData, postcode: e.target.value.toUpperCase() })
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Huisnummer *
                    </label>
                    <input
                      type="number"
                      required
                      min={1}
                      value={addressFormData.huisnummer || ''}
                      onChange={(e) =>
                        setAddressFormData({ ...addressFormData, huisnummer: Number(e.target.value) })
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Huisletter
                    </label>
                    <input
                      type="text"
                      maxLength={2}
                      placeholder="A"
                      value={addressFormData.huisletter || ''}
                      onChange={(e) =>
                        setAddressFormData({
                          ...addressFormData,
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
                      value={addressFormData.toevoeging || ''}
                      onChange={(e) =>
                        setAddressFormData({
                          ...addressFormData,
                          toevoeging: e.target.value || undefined,
                        })
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Woonoppervlakte (m²)
                  </label>
                  <input
                    type="number"
                    min={1}
                    max={1000}
                    placeholder="Wordt automatisch opgehaald"
                    value={addressFormData.woonoppervlakte || ''}
                    onChange={(e) =>
                      setAddressFormData({
                        ...addressFormData,
                        woonoppervlakte: e.target.value ? Number(e.target.value) : undefined,
                      })
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Wordt opgehaald uit BAG. Vul in als backup.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Vraagprijs (optioneel)
                  </label>
                  <input
                    type="number"
                    min={0}
                    step={1000}
                    value={addressFormData.vraagprijs || ''}
                    onChange={(e) =>
                      setAddressFormData({
                        ...addressFormData,
                        vraagprijs: e.target.value ? Number(e.target.value) : undefined,
                      })
                    }
                    placeholder="bijv. 450000"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Woningtype (optioneel)
                  </label>
                  <select
                    value={addressFormData.woningtype || ''}
                    onChange={(e) =>
                      setAddressFormData({ ...addressFormData, woningtype: e.target.value || undefined })
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  >
                    <option value="">Onbekend</option>
                    {PROPERTY_TYPES.map((type) => (
                      <option key={type.value} value={type.value}>
                        {type.label}
                      </option>
                    ))}
                  </select>
                </div>

                <button
                  type="submit"
                  disabled={isPending}
                  className="w-full bg-primary-600 text-white py-2 px-4 rounded-lg hover:bg-primary-700 transition-colors disabled:opacity-50"
                >
                  {isPending ? 'Gegevens ophalen...' : 'Bereken waarde'}
                </button>
              </form>
            </>
          ) : (
            <>
              <h2 className="text-lg font-semibold mb-4">Woninggegevens</h2>
              <form onSubmit={handleSimpleSubmit} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Woonoppervlakte (m²) *
                  </label>
                  <input
                    type="number"
                    required
                    min={1}
                    max={1000}
                    value={simpleFormData.woonoppervlakte}
                    onChange={(e) =>
                      setSimpleFormData({ ...simpleFormData, woonoppervlakte: Number(e.target.value) })
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Vraagprijs (optioneel)
                  </label>
                  <input
                    type="number"
                    min={0}
                    step={1000}
                    value={simpleFormData.vraagprijs || ''}
                    onChange={(e) =>
                      setSimpleFormData({
                        ...simpleFormData,
                        vraagprijs: e.target.value ? Number(e.target.value) : undefined,
                      })
                    }
                    placeholder="bijv. 450000"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Energielabel
                  </label>
                  <select
                    value={simpleFormData.energielabel || ''}
                    onChange={(e) =>
                      setSimpleFormData({ ...simpleFormData, energielabel: e.target.value || undefined })
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  >
                    <option value="">Onbekend</option>
                    {ENERGY_LABELS.map((label) => (
                      <option key={label} value={label}>
                        {label}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Bouwjaar
                  </label>
                  <input
                    type="number"
                    min={1500}
                    max={2030}
                    value={simpleFormData.bouwjaar || ''}
                    onChange={(e) =>
                      setSimpleFormData({
                        ...simpleFormData,
                        bouwjaar: e.target.value ? Number(e.target.value) : undefined,
                      })
                    }
                    placeholder="bijv. 1985"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Woningtype
                  </label>
                  <select
                    value={simpleFormData.woningtype || ''}
                    onChange={(e) =>
                      setSimpleFormData({ ...simpleFormData, woningtype: e.target.value || undefined })
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  >
                    <option value="">Onbekend</option>
                    {PROPERTY_TYPES.map((type) => (
                      <option key={type.value} value={type.value}>
                        {type.label}
                      </option>
                    ))}
                  </select>
                </div>

                <button
                  type="submit"
                  disabled={isPending}
                  className="w-full bg-primary-600 text-white py-2 px-4 rounded-lg hover:bg-primary-700 transition-colors disabled:opacity-50"
                >
                  {isPending ? 'Berekenen...' : 'Bereken waarde'}
                </button>
              </form>
            </>
          )}
        </div>

        {/* Results */}
        <div>
          {isError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 mb-4">
              {error instanceof Error ? error.message : 'Er is een fout opgetreden bij het berekenen van de waarde.'}
            </div>
          )}

          {/* Address mode results */}
          {addressResult && mode === 'address' && (
            <div className="space-y-4">
              {/* Address header */}
              <div className="bg-gray-50 rounded-lg p-4">
                <div className="flex justify-between items-start">
                  <div>
                    <div className="text-sm text-gray-500">Adres</div>
                    <div className="text-lg font-medium">{addressResult.adres}</div>
                  </div>
                  <div className="text-xs text-gray-400">via BAG</div>
                </div>
              </div>

              {/* WOZ value */}
              <WOZCard
                wozWaarde={addressResult.woz_waarde}
                peiljaar={addressResult.woz_peiljaar}
                grondoppervlakte={addressResult.grondoppervlakte}
              />

              {/* Comparables summary */}
              <ComparablesCard
                count={addressResult.comparables_count}
                avgM2={addressResult.comparables_avg_m2}
              />

              {/* Market indicators */}
              <MarktIndicatorenCard
                gemPrijs={addressResult.markt_gem_prijs}
                overbiedPct={addressResult.markt_overbiedpct}
                verkooptijd={addressResult.markt_verkooptijd}
                peildatum={addressResult.markt_peildatum}
              />

              {/* Buurt indicators */}
              <BuurtCard
                buurtNaam={addressResult.buurt_naam}
                gemWoz={addressResult.buurt_gem_woz}
                koopwoningenPct={addressResult.buurt_koopwoningen_pct}
                gemInkomen={addressResult.buurt_gem_inkomen}
              />

              {/* Main result */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Geschatte waarde</h2>
                <div className="text-center">
                  <div className="text-4xl font-bold text-primary-700">
                    {formatPrijs(addressResult.waarde_midden)}
                  </div>
                  <div className="text-gray-500 mt-1">
                    {formatPrijs(addressResult.waarde_laag)} - {formatPrijs(addressResult.waarde_hoog)}
                  </div>
                </div>

                {/* Energielabel */}
                <div className="flex items-center justify-center mt-4 space-x-2">
                  <span className="text-sm text-gray-600">Energielabel:</span>
                  {addressResult.energielabel ? (
                    <>
                      <EnergyLabelBadge label={addressResult.energielabel} />
                      <span className="text-xs text-green-600">(EP-Online)</span>
                    </>
                  ) : (
                    <span className="text-sm text-gray-400">Niet gevonden in EP-Online</span>
                  )}
                </div>

                <ConfidenceBar confidence={addressResult.confidence} />

                {addressResult.vraagprijs && (
                  <div className="mt-4 pt-4 border-t">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-600">Vraagprijs</span>
                      <span className="font-medium">{formatPrijs(addressResult.vraagprijs)}</span>
                    </div>
                    {addressResult.verschil_percentage !== null && addressResult.verschil_percentage !== undefined && (
                      <div className="flex justify-between items-center mt-1">
                        <span className="text-gray-600">Verschil</span>
                        <span
                          className={
                            addressResult.verschil_percentage > 0
                              ? 'text-red-600 font-medium'
                              : 'text-green-600 font-medium'
                          }
                        >
                          {addressResult.verschil_percentage > 0 ? '+' : ''}
                          {addressResult.verschil_percentage.toFixed(1)}%
                        </span>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Bid advice */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Biedadvies</h2>
                <div className="flex items-center justify-between">
                  <BiedAdviesBadge advies={addressResult.bied_advies} />
                </div>
                <div className="mt-4 bg-gray-50 rounded-lg p-4">
                  <div className="text-sm text-gray-600">Aanbevolen biedingsbereik</div>
                  <div className="text-xl font-semibold">
                    {formatPrijs(addressResult.bied_range_laag)} - {formatPrijs(addressResult.bied_range_hoog)}
                  </div>
                </div>
              </div>

              {/* Breakdown */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Opbouw schatting</h2>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Basiswaarde (m² x buurtprijs)</span>
                    <span>{formatPrijs(addressResult.basis_waarde)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Energielabel correctie</span>
                    <span
                      className={
                        addressResult.energielabel_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {addressResult.energielabel_correctie >= 0 ? '+' : ''}
                      {formatPrijs(addressResult.energielabel_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Bouwjaar correctie</span>
                    <span
                      className={
                        addressResult.bouwjaar_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {addressResult.bouwjaar_correctie >= 0 ? '+' : ''}
                      {formatPrijs(addressResult.bouwjaar_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Woningtype correctie</span>
                    <span
                      className={
                        addressResult.woningtype_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {addressResult.woningtype_correctie >= 0 ? '+' : ''}
                      {formatPrijs(addressResult.woningtype_correctie)}
                    </span>
                  </div>
                  {addressResult.perceel_correctie !== 0 && (
                    <div className="flex justify-between">
                      <span className="text-gray-600">Perceelgrootte correctie</span>
                      <span
                        className={
                          addressResult.perceel_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                        }
                      >
                        {addressResult.perceel_correctie >= 0 ? '+' : ''}
                        {formatPrijs(addressResult.perceel_correctie)}
                      </span>
                    </div>
                  )}
                  <div className="flex justify-between">
                    <span className="text-gray-600">Marktcorrectie (overbieden)</span>
                    <span className="text-green-600">
                      +{formatPrijs(addressResult.markt_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between pt-2 border-t font-semibold">
                    <span>Totaal</span>
                    <span>{formatPrijs(addressResult.waarde_midden)}</span>
                  </div>
                </div>
              </div>

              {/* Data sources footer */}
              <DataBronnenFooter bronnen={addressResult.data_bronnen} />
            </div>
          )}

          {/* Simple mode results */}
          {simpleResult && mode === 'simple' && (
            <div className="space-y-4">
              {/* Main result */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Geschatte waarde</h2>
                <div className="text-center">
                  <div className="text-4xl font-bold text-primary-700">
                    {formatPrijs(simpleResult.waarde_midden)}
                  </div>
                  <div className="text-gray-500 mt-1">
                    {formatPrijs(simpleResult.waarde_laag)} - {formatPrijs(simpleResult.waarde_hoog)}
                  </div>
                </div>

                <ConfidenceBar confidence={simpleResult.confidence} />

                {simpleResult.vraagprijs && (
                  <div className="mt-4 pt-4 border-t">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-600">Vraagprijs</span>
                      <span className="font-medium">{formatPrijs(simpleResult.vraagprijs)}</span>
                    </div>
                    {simpleResult.verschil_percentage !== null && simpleResult.verschil_percentage !== undefined && (
                      <div className="flex justify-between items-center mt-1">
                        <span className="text-gray-600">Verschil</span>
                        <span
                          className={
                            simpleResult.verschil_percentage > 0
                              ? 'text-red-600 font-medium'
                              : 'text-green-600 font-medium'
                          }
                        >
                          {simpleResult.verschil_percentage > 0 ? '+' : ''}
                          {simpleResult.verschil_percentage.toFixed(1)}%
                        </span>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Bid advice */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Biedadvies</h2>
                <div className="flex items-center justify-between">
                  <BiedAdviesBadge advies={simpleResult.bied_advies} />
                </div>
                <div className="mt-4 bg-gray-50 rounded-lg p-4">
                  <div className="text-sm text-gray-600">Aanbevolen biedingsbereik</div>
                  <div className="text-xl font-semibold">
                    {formatPrijs(simpleResult.bied_range_laag)} - {formatPrijs(simpleResult.bied_range_hoog)}
                  </div>
                </div>
              </div>

              {/* Breakdown */}
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Opbouw schatting</h2>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Basiswaarde (m² x buurtprijs)</span>
                    <span>{formatPrijs(simpleResult.basis_waarde)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Energielabel correctie</span>
                    <span
                      className={
                        simpleResult.energielabel_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {simpleResult.energielabel_correctie >= 0 ? '+' : ''}
                      {formatPrijs(simpleResult.energielabel_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Bouwjaar correctie</span>
                    <span
                      className={
                        simpleResult.bouwjaar_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {simpleResult.bouwjaar_correctie >= 0 ? '+' : ''}
                      {formatPrijs(simpleResult.bouwjaar_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Woningtype correctie</span>
                    <span
                      className={
                        simpleResult.woningtype_correctie >= 0 ? 'text-green-600' : 'text-red-600'
                      }
                    >
                      {simpleResult.woningtype_correctie >= 0 ? '+' : ''}
                      {formatPrijs(simpleResult.woningtype_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Marktcorrectie (overbieden)</span>
                    <span className="text-green-600">
                      +{formatPrijs(simpleResult.markt_correctie)}
                    </span>
                  </div>
                  <div className="flex justify-between pt-2 border-t font-semibold">
                    <span>Totaal</span>
                    <span>{formatPrijs(simpleResult.waarde_midden)}</span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {!simpleResult && !addressResult && !isError && (
            <div className="bg-gray-50 rounded-lg p-8 text-center text-gray-500">
              {mode === 'address'
                ? 'Vul het adres in om WOZ-waarde, energielabel en waardebepaling op te halen'
                : 'Vul de woninggegevens in om een waardebepaling te krijgen'}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
