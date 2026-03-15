"""Data collectors for Woningzoeker."""

from collectors.bag_collector import BagClient, BagRateLimitError
from collectors.cbs_collector import download_cbs_dataset, filter_for_region
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

__all__ = [
    # BAG
    "BagClient",
    "BagRateLimitError",
    # CBS
    "download_cbs_dataset",
    "filter_for_region",
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
]
