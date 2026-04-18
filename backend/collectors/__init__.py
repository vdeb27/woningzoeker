"""Data collectors for Woningzoeker."""

from collectors.bag_collector import BagClient, BagRateLimitError
from collectors.cbs_collector import download_cbs_dataset, filter_for_region
from collectors.cbs_market_collector import (
    CBSMarketCollector,
    MarketDataResult,
    create_cbs_market_collector,
)
from collectors.cbs_buurt_collector import (
    CBSBuurtCollector,
    BuurtData,
    create_cbs_buurt_collector,
    lookup_buurt_code_pdok,
    geocode_address_pdok,
)
from collectors.cbs_nabijheid_collector import (
    CBSNabijheidCollector,
    NabijheidResult,
    create_cbs_nabijheid_collector,
)
from collectors.leefbaarometer_collector import (
    LeefbaarometerCollector,
    LeefbaarometerResult,
    create_leefbaarometer_collector,
)
from collectors.cbs_extra_collector import (
    CBSExtraCollector,
    create_cbs_extra_collector,
)
from collectors.rivm_collector import (
    RIVMCollector,
    RIVMResult,
    create_rivm_collector,
)
from collectors.woz_collector import WOZCollector, WOZResult, create_woz_collector
from collectors.energielabel_collector import (
    EnergielabelCollector,
    EnergielabelResult,
    create_energielabel_collector,
)
from collectors.kadaster_collector import (
    KadasterCollector,
    TransactionRecord,
    ComparablesResult,
    create_kadaster_collector,
)
from collectors.miljoenhuizen_collector import (
    MiljoenhuizenCollector,
    MiljoenhuizenWoning,
    PrijsHistorieEntry,
    create_miljoenhuizen_collector,
)
from collectors.osm_overpass_collector import (
    OSMOverpassCollector,
    OverpassResult,
    Voorziening,
    create_osm_overpass_collector,
)
from collectors.duo_school_collector import (
    DUOSchoolCollector,
    SchoolInfo,
    create_duo_school_collector,
)
from collectors.cycling_collector import (
    CyclingCollector,
    CyclingRoute,
    create_cycling_collector,
)
from collectors.ov_collector import (
    OVCollector,
    OVHalte,
    OVReistijd,
    OVBereikbaarheid,
    create_ov_collector,
)
from collectors.rce_collector import (
    RCECollector,
    RijksmonumentResult,
    create_rce_collector,
)
from collectors.pdok_beschermde_gebieden_collector import (
    PDOKBeschermdeGebiedenCollector,
    BeschermdGebiedResult,
    create_pdok_beschermde_gebieden_collector,
)
from collectors.luchtmeetnet_collector import (
    LuchtmeetnetCollector,
    LuchtmeetnetResult,
    create_luchtmeetnet_collector,
)
from collectors.rivm_pfas_collector import (
    RIVMPFASCollector,
    PFASResult,
    PFASSample,
    create_rivm_pfas_collector,
)
from collectors.pfas_bodemkaart_collector import (
    PFASBodemkaartCollector,
    BodemkaartResult,
    create_pfas_bodemkaart_collector,
)
from collectors.bestemmingsplan_collector import (
    BestemmingsplanCollector,
    BestemmingsplanInfo,
    Maatvoering,
    OmgevingsAnalyse,
    OmgevingsBestemming,
    BurenBouwinfo,
    create_bestemmingsplan_collector,
)
from collectors.driedbag_collector import (
    DrieDBagCollector,
    DrieDBagResult,
    create_driedbag_collector,
)
from collectors.glasvezel_collector import (
    GlasvezelCollector,
    GlasvezelResult,
    create_glasvezel_collector,
)
from collectors.funda_collector import (
    FundaCollector,
    PropertyListing,
    parse_funda_url,
    create_funda_collector,
)

__all__ = [
    # BAG
    "BagClient",
    "BagRateLimitError",
    # CBS
    "download_cbs_dataset",
    "filter_for_region",
    # CBS Market Data
    "CBSMarketCollector",
    "MarketDataResult",
    "create_cbs_market_collector",
    # CBS Buurt Data
    "CBSBuurtCollector",
    "BuurtData",
    "create_cbs_buurt_collector",
    "lookup_buurt_code_pdok",
    "geocode_address_pdok",
    # CBS Nabijheid
    "CBSNabijheidCollector",
    "NabijheidResult",
    "create_cbs_nabijheid_collector",
    # Leefbaarometer
    "LeefbaarometerCollector",
    "LeefbaarometerResult",
    "create_leefbaarometer_collector",
    # CBS Extra (misdrijven, arbeid, SES, opleiding, bodemgebruik)
    "CBSExtraCollector",
    "create_cbs_extra_collector",
    # RIVM
    "RIVMCollector",
    "RIVMResult",
    "create_rivm_collector",
    # WOZ
    "WOZCollector",
    "WOZResult",
    "create_woz_collector",
    # Energielabel
    "EnergielabelCollector",
    "EnergielabelResult",
    "create_energielabel_collector",
    # Kadaster
    "KadasterCollector",
    "TransactionRecord",
    "ComparablesResult",
    "create_kadaster_collector",
    # Miljoenhuizen
    "MiljoenhuizenCollector",
    "MiljoenhuizenWoning",
    "PrijsHistorieEntry",
    "create_miljoenhuizen_collector",
    # OSM Overpass
    "OSMOverpassCollector",
    "OverpassResult",
    "Voorziening",
    "create_osm_overpass_collector",
    # DUO Scholen
    "DUOSchoolCollector",
    "SchoolInfo",
    "create_duo_school_collector",
    # Cycling (OpenRouteService)
    "CyclingCollector",
    "CyclingRoute",
    "create_cycling_collector",
    # OV (OVapi.nl)
    "OVCollector",
    "OVHalte",
    "OVReistijd",
    "OVBereikbaarheid",
    "create_ov_collector",
    # RCE Rijksmonumenten
    "RCECollector",
    "RijksmonumentResult",
    "create_rce_collector",
    # PDOK Beschermde Gebieden
    "PDOKBeschermdeGebiedenCollector",
    "BeschermdGebiedResult",
    "create_pdok_beschermde_gebieden_collector",
    # Luchtmeetnet
    "LuchtmeetnetCollector",
    "LuchtmeetnetResult",
    "create_luchtmeetnet_collector",
    # RIVM PFAS
    "RIVMPFASCollector",
    "PFASResult",
    "PFASSample",
    "create_rivm_pfas_collector",
    # PFAS Bodemkaart Den Haag
    "PFASBodemkaartCollector",
    "BodemkaartResult",
    "create_pfas_bodemkaart_collector",
    # Bestemmingsplan (DSO)
    "BestemmingsplanCollector",
    "BestemmingsplanInfo",
    "Maatvoering",
    "OmgevingsAnalyse",
    "OmgevingsBestemming",
    "BurenBouwinfo",
    "create_bestemmingsplan_collector",
    # 3DBAG
    "DrieDBagCollector",
    "DrieDBagResult",
    "create_driedbag_collector",
    # Glasvezel
    "GlasvezelCollector",
    "GlasvezelResult",
    "create_glasvezel_collector",
    # Funda
    "FundaCollector",
    "PropertyListing",
    "parse_funda_url",
    "create_funda_collector",
]
