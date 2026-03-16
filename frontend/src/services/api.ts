const API_BASE = '/api'

export interface WaardebepalingRequest {
  woonoppervlakte: number
  buurt_code?: string
  energielabel?: string
  bouwjaar?: number
  woningtype?: string
  vraagprijs?: number
}

export interface WaardebepalingResponse {
  waarde_laag: number
  waarde_hoog: number
  waarde_midden: number
  vraagprijs?: number
  verschil_percentage?: number
  bied_advies: string
  bied_range_laag: number
  bied_range_hoog: number
  basis_waarde: number
  energielabel_correctie: number
  bouwjaar_correctie: number
  woningtype_correctie: number
  perceel_correctie: number
  markt_correctie: number
  confidence: number
  confidence_factors: Record<string, unknown>
}

// WOZ value lookup
export interface WOZResponse {
  postcode: string
  huisnummer: number
  huisletter?: string
  toevoeging?: string
  woz_waarde?: number
  peildatum?: string
  peiljaar?: number
  adres?: string
  woonplaats?: string
  error?: string
}

// Energy label lookup
export interface EnergielabelResponse {
  postcode: string
  huisnummer: number
  huisletter?: string
  toevoeging?: string
  energielabel?: string
  energieindex?: number
  registratiedatum?: string
  geldig_tot?: string
  gebouwtype?: string
  bouwjaar?: number
  gebruiksoppervlakte?: number
  error?: string
}

// Comparable sales
export interface TransactionResponse {
  postcode?: string
  huisnummer?: number
  straat?: string
  woonplaats?: string
  transactie_datum?: string
  transactie_prijs?: number
  oppervlakte?: number
  prijs_per_m2?: number
  bouwjaar?: number
  woningtype?: string
}

export interface ComparablesResponse {
  target_postcode: string
  target_huisnummer: number
  target_address?: string
  transactions: TransactionResponse[]
  avg_prijs_per_m2?: number
  count: number
  search_radius_pc4: boolean
  error?: string
}

// Enhanced valuation with auto-fetch
export interface EnhancedWaardebepalingRequest {
  postcode: string
  huisnummer: number
  huisletter?: string
  toevoeging?: string
  woonoppervlakte?: number
  vraagprijs?: number
  woningtype?: string
}

export interface EnhancedWaardebepalingResponse {
  postcode: string
  huisnummer: number
  adres?: string
  // WOZ
  woz_waarde?: number
  woz_peiljaar?: number
  grondoppervlakte?: number
  // Energielabel (auto-fetched)
  energielabel?: string
  energielabel_bron: string
  // Valuation
  waarde_laag: number
  waarde_hoog: number
  waarde_midden: number
  vraagprijs?: number
  verschil_percentage?: number
  // Bidding
  bied_advies: string
  bied_range_laag: number
  bied_range_hoog: number
  // Breakdown
  basis_waarde: number
  energielabel_correctie: number
  bouwjaar_correctie: number
  woningtype_correctie: number
  perceel_correctie: number
  markt_correctie: number
  confidence: number
  confidence_factors: Record<string, unknown>
  // Comparables
  comparables_count: number
  comparables_avg_m2?: number
  // Market data (CBS StatLine)
  markt_gem_prijs?: number
  markt_overbiedpct?: number
  markt_verkooptijd?: number
  markt_peildatum?: string
  // Buurt data (CBS Kerncijfers)
  buurt_code?: string
  buurt_naam?: string
  buurt_gem_woz?: number
  buurt_koopwoningen_pct?: number
  buurt_gem_inkomen?: number
  // Data sources
  data_bronnen: string[]
}

export interface Woning {
  id: number
  adres: string
  postcode?: string
  plaats?: string
  vraagprijs?: number
  woonoppervlakte?: number
  kamers?: number
  energielabel?: string
  bouwjaar?: number
  woningtype?: string
  url?: string
  status: string
}

export interface Buurt {
  code: string
  naam: string
  gemeente_naam?: string
  score_totaal?: number
  median_vraagprijs?: number
  aantal_te_koop?: number
}

export interface WatchlistItem {
  id: number
  woning_id: number
  notities?: string
  prioriteit: number
  status: string
  woning_adres?: string
  woning_vraagprijs?: number
  woning_woonoppervlakte?: number
  added_at: string
}

// Waardebepaling
export async function berekenWaarde(
  data: WaardebepalingRequest
): Promise<WaardebepalingResponse> {
  const response = await fetch(`${API_BASE}/woningen/waardebepaling`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) {
    throw new Error('Waardebepaling mislukt')
  }
  return response.json()
}

// Enhanced waardebepaling (auto-fetches WOZ, energielabel, comparables)
export async function berekenWaardeVoorAdres(
  data: EnhancedWaardebepalingRequest
): Promise<EnhancedWaardebepalingResponse> {
  const response = await fetch(`${API_BASE}/woningen/waardebepaling/adres`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}))
    throw new Error(errorData.detail || 'Waardebepaling mislukt')
  }
  return response.json()
}

// WOZ value lookup
export async function fetchWOZ(params: {
  postcode: string
  huisnummer: number
  huisletter?: string
  toevoeging?: string
}): Promise<WOZResponse> {
  const searchParams = new URLSearchParams({
    postcode: params.postcode,
    huisnummer: String(params.huisnummer),
  })
  if (params.huisletter) searchParams.append('huisletter', params.huisletter)
  if (params.toevoeging) searchParams.append('toevoeging', params.toevoeging)

  const response = await fetch(`${API_BASE}/woningen/woz?${searchParams}`)
  if (!response.ok) {
    throw new Error('WOZ ophalen mislukt')
  }
  return response.json()
}

// Energielabel lookup
export async function fetchEnergielabel(params: {
  postcode: string
  huisnummer: number
  huisletter?: string
  toevoeging?: string
}): Promise<EnergielabelResponse> {
  const searchParams = new URLSearchParams({
    postcode: params.postcode,
    huisnummer: String(params.huisnummer),
  })
  if (params.huisletter) searchParams.append('huisletter', params.huisletter)
  if (params.toevoeging) searchParams.append('toevoeging', params.toevoeging)

  const response = await fetch(`${API_BASE}/woningen/energielabel?${searchParams}`)
  if (!response.ok) {
    throw new Error('Energielabel ophalen mislukt')
  }
  return response.json()
}

// Comparable sales lookup
export async function fetchComparables(params: {
  postcode: string
  huisnummer: number
  oppervlakte?: number
  max_years?: number
  max_results?: number
}): Promise<ComparablesResponse> {
  const searchParams = new URLSearchParams({
    postcode: params.postcode,
    huisnummer: String(params.huisnummer),
  })
  if (params.oppervlakte) searchParams.append('oppervlakte', String(params.oppervlakte))
  if (params.max_years) searchParams.append('max_years', String(params.max_years))
  if (params.max_results) searchParams.append('max_results', String(params.max_results))

  const response = await fetch(`${API_BASE}/woningen/comparables?${searchParams}`)
  if (!response.ok) {
    throw new Error('Vergelijkbare verkopen ophalen mislukt')
  }
  return response.json()
}

// Woningen
export async function fetchWoningen(params?: {
  min_prijs?: number
  max_prijs?: number
  min_oppervlakte?: number
  buurt?: string
  energielabel?: string
  limit?: number
}): Promise<Woning[]> {
  const searchParams = new URLSearchParams()
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        searchParams.append(key, String(value))
      }
    })
  }
  const response = await fetch(`${API_BASE}/woningen?${searchParams}`)
  if (!response.ok) {
    throw new Error('Woningen ophalen mislukt')
  }
  return response.json()
}

export async function fetchWoning(id: number): Promise<Woning> {
  const response = await fetch(`${API_BASE}/woningen/${id}`)
  if (!response.ok) {
    throw new Error('Woning niet gevonden')
  }
  return response.json()
}

export async function fetchWoningWaarde(
  id: number
): Promise<WaardebepalingResponse> {
  const response = await fetch(`${API_BASE}/woningen/${id}/waarde`)
  if (!response.ok) {
    throw new Error('Waardebepaling mislukt')
  }
  return response.json()
}

// Buurten
export async function fetchBuurten(params?: {
  gemeente?: string
  min_score?: number
  limit?: number
}): Promise<Buurt[]> {
  const searchParams = new URLSearchParams()
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        searchParams.append(key, String(value))
      }
    })
  }
  const response = await fetch(`${API_BASE}/buurten?${searchParams}`)
  if (!response.ok) {
    throw new Error('Buurten ophalen mislukt')
  }
  return response.json()
}

export async function fetchBuurt(code: string): Promise<Buurt> {
  const response = await fetch(`${API_BASE}/buurten/${code}`)
  if (!response.ok) {
    throw new Error('Buurt niet gevonden')
  }
  return response.json()
}

// Watchlist
export async function fetchWatchlist(): Promise<WatchlistItem[]> {
  const response = await fetch(`${API_BASE}/watchlist`)
  if (!response.ok) {
    throw new Error('Watchlist ophalen mislukt')
  }
  return response.json()
}

export async function addToWatchlist(
  woningId: number,
  notities?: string
): Promise<WatchlistItem> {
  const response = await fetch(`${API_BASE}/watchlist`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ woning_id: woningId, notities }),
  })
  if (!response.ok) {
    throw new Error('Toevoegen aan watchlist mislukt')
  }
  return response.json()
}

export async function removeFromWatchlist(id: number): Promise<void> {
  const response = await fetch(`${API_BASE}/watchlist/${id}`, {
    method: 'DELETE',
  })
  if (!response.ok) {
    throw new Error('Verwijderen van watchlist mislukt')
  }
}

export async function updateWatchlistItem(
  id: number,
  data: Partial<WatchlistItem>
): Promise<WatchlistItem> {
  const response = await fetch(`${API_BASE}/watchlist/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) {
    throw new Error('Bijwerken watchlist mislukt')
  }
  return response.json()
}

// Markt
export async function fetchMarktOverzicht(gemeente?: string) {
  const params = gemeente ? `?gemeente=${gemeente}` : ''
  const response = await fetch(`${API_BASE}/markt/overzicht${params}`)
  if (!response.ok) {
    throw new Error('Marktoverzicht ophalen mislukt')
  }
  return response.json()
}

// Utility
export function formatPrijs(prijs: number): string {
  return new Intl.NumberFormat('nl-NL', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  }).format(prijs)
}

export function formatM2Prijs(prijs: number): string {
  return `${formatPrijs(prijs)}/m²`
}
