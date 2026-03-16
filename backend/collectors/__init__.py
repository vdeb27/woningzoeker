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
]
