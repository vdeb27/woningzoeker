import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  berekenWaardeVoorAdres,
  EnhancedWaardebepalingRequest,
  EnhancedWaardebepalingResponse,
  formatPrijs,
  formatM2Prijs,
} from '../services/api'
import VoorzieningenPanel from '../components/VoorzieningenPanel'

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

function AnalyseColumn({ result, onCopy, copied }: {
  result: EnhancedWaardebepalingResponse
  onCopy: (waarde: number) => void
  copied: boolean
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
          <h3 className="text-lg font-semibold mb-4">Biedadvies</h3>
          <div className="flex items-center justify-between">
            <BiedAdviesBadge advies={result.bied_advies} />
          </div>
          <div className="mt-4 bg-gray-50 rounded-lg p-4">
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
    vraagprijs: undefined,
  })

  const [copied, setCopied] = useState(false)

  const handleCopy = (waardeMidden: number) => {
    navigator.clipboard.writeText(String(Math.round(waardeMidden)))
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const mutation = useMutation({
    mutationFn: berekenWaardeVoorAdres,
  })

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
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
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
                onChange={(e) =>
                  setFormData({ ...formData, postcode: e.target.value.toUpperCase() })
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
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Vraagprijs
              </label>
              <input
                type="number"
                min={0}
                step={1000}
                value={formData.vraagprijs || ''}
                onChange={(e) =>
                  setFormData({
                    ...formData,
                    vraagprijs: e.target.value ? Number(e.target.value) : undefined,
                  })
                }
                placeholder="450000"
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
          </div>
          <div className="lg:col-span-2">
            <AnalyseColumn result={result} onCopy={handleCopy} copied={copied} />
          </div>
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
