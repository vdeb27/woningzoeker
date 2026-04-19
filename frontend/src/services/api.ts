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
  buurt_kwaliteit_correctie: number
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

export interface FundaListing {
  url: string
  adres: string
  postcode?: string
  plaats?: string
  vraagprijs?: number
  vraagprijs_suffix?: string
  woonoppervlakte?: number
  perceeloppervlakte?: number
  inhoud?: number
  prijs_per_m2?: number
  kamers?: number
  slaapkamers?: number
  badkamers?: number
  bouwjaar?: number
  woningtype?: string
  bouwtype?: string
  energielabel?: string
  // Eigendomsituatie
  eigendom_type?: string
  vve_bijdrage?: number
  erfpacht_bedrag?: number
  // Tuin & buitenruimte
  tuin_type?: string
  tuin_oppervlakte?: number
  tuin_orientatie?: string
  buitenruimte?: number
  balkon?: boolean
  dakterras?: boolean
  // Indeling & parkeren
  verdiepingen?: number
  garage_type?: string
  parkeerplaatsen?: number
  parkeer_type?: string
  kelder?: boolean
  zolder?: string
  berging?: string
  // Extra
  isolatie?: string
  verwarming?: string
  dak_type?: string
  aangeboden_sinds?: string
  status: string
  // Verkocht-specifiek
  verkoopdatum?: string
  looptijd_dagen?: number
}

export interface EnhancedWaardebepalingResponse {
  postcode: string
  huisnummer: number
  adres?: string
  // WOZ
  woz_waarde?: number
  woz_peiljaar?: number
  grondoppervlakte?: number
  // Woninggegevens (auto-fetched)
  woonoppervlakte?: number
  bouwjaar?: number
  woningtype?: string
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
  buurt_kwaliteit_correctie: number
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
  // Monument status
  monument?: MonumentResponse
  // Funda listing
  funda_listing?: FundaListing
  // Coordinaten
  latitude?: number | null
  longitude?: number | null
  // Plafondhoogte inschatting
  plafondhoogte?: PlafondhoogteResponse
  // Glasvezel beschikbaarheid
  glasvezel?: GlasvezelResponse
  // Zon en oriëntatie
  orientatie?: OrientatieResponse
  // Data sources
  data_bronnen: string[]
}

// Monument status
export interface RijksmonumentInfo {
  is_monument: boolean
  monumentnummer?: number
  omschrijving?: string
  categorie?: string
  url?: string
}

export interface GemeentelijkMonumentInfo {
  is_monument: boolean
  gemeente?: string
  omschrijving?: string
}

export interface BeschermdGezichtInfo {
  in_beschermd_gezicht: boolean
  naam?: string
  type?: string
  niveau?: string  // "rijks" or "gemeentelijk"
}

export interface UnescoInfo {
  in_unesco: boolean
  naam?: string
}

export interface MonumentResponse {
  rijksmonument?: RijksmonumentInfo
  gemeentelijk_monument?: GemeentelijkMonumentInfo
  beschermd_gezicht?: BeschermdGezichtInfo
  unesco?: UnescoInfo
  heeft_monumentstatus: boolean
}

// Plafondhoogte inschatting
export interface PlafondhoogteResponse {
  geschatte_verdiepingshoogte?: number
  label?: string
  methode?: string
  betrouwbaarheid?: string
  details?: string
}

// Glasvezel beschikbaarheid
export interface GlasvezelResponse {
  glasvezel_beschikbaar?: boolean
  glasvezel_snelheid?: number  // Mbit/s
  glasvezel_provider?: string
  kabel_beschikbaar?: boolean
  kabel_snelheid?: number  // Mbit/s
  kabel_provider?: string
  dsl_snelheid?: number  // Mbit/s
  max_snelheid?: number  // Mbit/s
  adres_gevonden: boolean
}

// Zon en oriëntatie
export interface OrientatieResponse {
  tuin_orientatie?: string
  tuin_azimut?: number
  tuin_oppervlakte_berekend?: number
  zon_uren_zomer?: number
  zon_uren_lente?: number
  zon_uren_winter?: number
  zon_label?: string
  schaduw_eigen_gebouw?: string
  schaduw_buren?: string
  schaduw_bomen?: string
  effectieve_tuin_diepte?: number
  zonnepanelen_score?: number
  zonnepanelen_label?: string
  dak_orientatie?: string
  dak_hellingshoek?: number
  geschikt_dakoppervlak?: number
  funda_tuin_orientatie?: string
  funda_tuin_oppervlakte?: number
  tuin_oppervlakte_bron?: string
  methode?: string
  betrouwbaarheid?: string
  details?: string
}

// GeoJSON types
export interface GeoJSONFeature {
  type: 'Feature'
  geometry: {
    type: string
    coordinates: unknown
  }
  properties: Record<string, unknown>
}

export interface GeoJSONFeatureCollection {
  type: 'FeatureCollection'
  features: GeoJSONFeature[]
}

export interface Woning {
  id: number
  adres: string
  postcode?: string
  plaats?: string
  huisnummer?: number
  huisletter?: string
  toevoeging?: string
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
  score_inkomen?: number
  score_veiligheid?: number
  score_voorzieningen?: number
  score_woningen?: number
  score_bereikbaarheid?: number
  score_leefbaarheid?: number
  score_coverage?: number
  median_vraagprijs?: number
  median_m2_prijs?: number
  aantal_te_koop?: number
  inwoners?: number
  huishoudens?: number
  gemiddeld_inkomen?: number
  woz_waarde?: number
  leefbaarometer_score?: number
  leefbaarometer_fysiek?: number
  leefbaarometer_voorzieningen?: number
  leefbaarometer_veiligheid?: number
  leefbaarometer_bevolking?: number
  leefbaarometer_woningen?: number
  indicatoren?: Record<string, number>
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

// Indicator metadata types
export interface IndicatorMeta {
  label: string
  category?: string
  unit: string
  higher_is_better: boolean
  weight: number
  description: string
}

export interface CategoryMeta {
  label: string
  color: string
  weight: number
  indicators: string[]
}

export interface IndicatorMetaResponse {
  indicators: Record<string, IndicatorMeta>
  categories: Record<string, CategoryMeta>
}

export interface BuurtVergelijkResponse {
  buurten: Buurt[]
  categories: Record<string, CategoryMeta>
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

export async function deleteWoning(id: number): Promise<void> {
  const response = await fetch(`${API_BASE}/woningen/${id}`, {
    method: 'DELETE',
  })
  if (!response.ok) {
    throw new Error('Verwijderen mislukt')
  }
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

export async function fetchIndicatorMeta(): Promise<IndicatorMetaResponse> {
  const response = await fetch(`${API_BASE}/buurten/indicatoren/meta`)
  if (!response.ok) {
    throw new Error('Indicator metadata ophalen mislukt')
  }
  return response.json()
}

export async function fetchBuurtenVergelijk(
  codes: string[]
): Promise<BuurtVergelijkResponse> {
  const searchParams = new URLSearchParams()
  codes.forEach((c) => searchParams.append('codes', c))
  const response = await fetch(`${API_BASE}/buurten/vergelijk/?${searchParams}`)
  if (!response.ok) {
    throw new Error('Buurtenvergelijking ophalen mislukt')
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

// GeoJSON endpoints
export async function fetchBuurtenGeoJSON(params?: {
  gemeente?: string
  min_score?: number
  indicator?: string
}): Promise<GeoJSONFeatureCollection> {
  const searchParams = new URLSearchParams()
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        searchParams.append(key, String(value))
      }
    })
  }
  const response = await fetch(`${API_BASE}/buurten/geojson?${searchParams}`)
  if (!response.ok) {
    throw new Error('Buurtgrenzen ophalen mislukt')
  }
  return response.json()
}

export async function fetchPostcode6GeoJSON(params?: {
  gemeente?: string
}): Promise<GeoJSONFeatureCollection> {
  const searchParams = new URLSearchParams()
  if (params?.gemeente) searchParams.append('gemeente', params.gemeente)
  const response = await fetch(`${API_BASE}/postcode6/geojson?${searchParams}`)
  if (!response.ok) {
    throw new Error('Postcode6 grenzen ophalen mislukt')
  }
  return response.json()
}

export async function fetchWoningenGeoJSON(): Promise<GeoJSONFeatureCollection> {
  const response = await fetch(`${API_BASE}/woningen/geojson`)
  if (!response.ok) {
    throw new Error('Woningen GeoJSON ophalen mislukt')
  }
  return response.json()
}

// Scholen
export interface SchoolSummary {
  brin: string
  vestigingsnummer: string
  naam: string
  type: string  // "basisonderwijs" | "voortgezet"
  gemeente: string
  onderwijstype?: string
  postcode?: string
  plaats?: string
  leerlingen?: number
  lat?: number
  lng?: number
  advies_havo_vwo_pct?: number
  gem_eindtoets?: number
  slagingspercentage?: number
  gem_examencijfer?: number
  inspectie_oordeel?: string
}

export async function fetchScholenGeoJSON(params?: {
  type?: string
}): Promise<GeoJSONFeatureCollection> {
  const searchParams = new URLSearchParams()
  if (params?.type) searchParams.append('type', params.type)
  const response = await fetch(`${API_BASE}/scholen/geojson?${searchParams}`)
  if (!response.ok) {
    throw new Error('Scholen GeoJSON ophalen mislukt')
  }
  return response.json()
}

export async function fetchScholenNabij(params: {
  lat: number
  lng: number
  radius?: number
  type?: string
}): Promise<(SchoolSummary & { afstand_m: number })[]> {
  const searchParams = new URLSearchParams({
    lat: String(params.lat),
    lng: String(params.lng),
  })
  if (params.radius) searchParams.append('radius', String(params.radius))
  if (params.type) searchParams.append('type', params.type)
  const response = await fetch(`${API_BASE}/scholen/nabij?${searchParams}`)
  if (!response.ok) {
    throw new Error('Nabije scholen ophalen mislukt')
  }
  return response.json()
}

// Voorzieningen
export interface VoorzieningItem {
  naam: string
  type: string
  categorie: string
  afstand_m: number
  looptijd_min: number
  lat: number
  lng: number
}

export interface CBSAfstand {
  indicator: string
  label: string
  afstand_km: number
  looptijd_min: number
}

export interface FietsafstandItem {
  dest_naam: string
  afstand_km: number
  reistijd_min: number
  geometry?: number[][] | null
  error?: string | null
}

export interface OVHalteItem {
  naam: string
  type: string  // "trein" | "tram" | "bus" | "metro"
  lat: number
  lng: number
  afstand_m: number
  lijnen: string[]
  frequentie_spits?: number | null
}

export interface OVReistijdItem {
  dest_naam: string
  reistijd_min: number
  overstappen: number
  route_beschrijving: string
  halte_naam: string
  error?: string | null
}

export interface OVData {
  ov_score: number
  dichtstbijzijnde_halte?: OVHalteItem | null
  haltes_nabij: OVHalteItem[]
  reistijden_werklocaties: OVReistijdItem[]
  score_breakdown: Record<string, number>
}

export interface VoorzieningenResponse {
  cbs_afstanden: Record<string, CBSAfstand[]>
  voorzieningen: VoorzieningItem[]
  fietsafstanden: FietsafstandItem[]
  ov_data?: OVData | null
  score_voorzieningen?: number
  buurt_code?: string
  buurt_naam?: string
  lat: number
  lng: number
}

export async function fetchVoorzieningen(params: {
  postcode: string
  huisnummer: number
  radius_m?: number
}): Promise<VoorzieningenResponse> {
  const searchParams = new URLSearchParams({
    postcode: params.postcode,
    huisnummer: String(params.huisnummer),
  })
  if (params.radius_m) searchParams.append('radius_m', String(params.radius_m))

  const response = await fetch(`${API_BASE}/voorzieningen/adres?${searchParams}`)
  if (!response.ok) {
    throw new Error('Voorzieningen ophalen mislukt')
  }
  return response.json()
}

// Monument status
export async function fetchMonumentStatus(params: {
  postcode: string
  huisnummer: number
}): Promise<MonumentResponse> {
  const pc = params.postcode.replace(/\s/g, '').toUpperCase()
  const response = await fetch(
    `${API_BASE}/woningen/adres/${pc}/${params.huisnummer}/monument`
  )
  if (!response.ok) {
    throw new Error('Monumentstatus ophalen mislukt')
  }
  return response.json()
}

export async function fetchWoningMonumentStatus(
  woningId: number
): Promise<MonumentResponse> {
  const response = await fetch(`${API_BASE}/woningen/${woningId}/monument`)
  if (!response.ok) {
    throw new Error('Monumentstatus ophalen mislukt')
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

// Bestemmingsplan
export interface MaatvoeringItem {
  naam: string
  waarde: string
  eenheid?: string | null
  waarde_type?: string | null
}

export interface BouwvlakItem {
  geometrie?: Record<string, unknown> | null
  maatvoeringen: MaatvoeringItem[]
}

export interface OntwerpPlanItem {
  naam: string
  type: string
  status: string
  datum: string
  id: string
}

export interface BestemmingsplanResponse {
  plan_naam: string
  plan_type: string
  plan_status: string
  datum_vaststelling?: string | null
  bestemming: string
  bestemming_specifiek?: string | null
  max_bouwhoogte?: number | null
  max_goothoogte?: number | null
  max_bebouwingspercentage?: number | null
  max_inhoud?: number | null
  bouwvlak?: BouwvlakItem | null
  functieaanduidingen: string[]
  bouwaanduidingen: string[]
  maatvoeringen: MaatvoeringItem[]
  regels_samenvatting?: string | null
  regels_url?: string | null
  ontwerp_plannen: OntwerpPlanItem[]
  link_plan: string
  uitbreidings_indicator?: string | null
  uitbreidings_toelichting?: string | null
  error?: string | null
}

export async function fetchBestemmingsplan(params: {
  lat: number
  lng: number
}): Promise<BestemmingsplanResponse> {
  const response = await fetch(
    `${API_BASE}/bestemmingsplan?lat=${params.lat}&lng=${params.lng}`
  )
  if (!response.ok) {
    throw new Error('Bestemmingsplan ophalen mislukt')
  }
  return response.json()
}

// Omgevingsanalyse
export interface BurenBouwinfoItem {
  bestemming: string
  max_bouwhoogte?: number | null
  max_goothoogte?: number | null
  max_bebouwingspercentage?: number | null
}

export interface OmgevingsAnalyseResponse {
  type: 'FeatureCollection'
  features: Array<{
    type: 'Feature'
    geometry: Record<string, unknown> | null
    properties: {
      naam: string
      categorie: string
      plan_naam?: string
    }
  }>
  statistieken: Record<string, number>
  statistieken_pct: Record<string, number>
  ontwerp_plannen: OntwerpPlanItem[]
  buren_bouwinfo: BurenBouwinfoItem[]
  center: [number, number]
  radius_m: number
  error?: string | null
}

export async function fetchOmgevingsAnalyse(params: {
  lat: number
  lng: number
  radius_m?: number
}): Promise<OmgevingsAnalyseResponse> {
  const searchParams = new URLSearchParams({
    lat: String(params.lat),
    lng: String(params.lng),
  })
  if (params.radius_m) searchParams.append('radius_m', String(params.radius_m))

  const response = await fetch(`${API_BASE}/bestemmingsplan/omgeving?${searchParams}`)
  if (!response.ok) {
    throw new Error('Omgevingsanalyse ophalen mislukt')
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
