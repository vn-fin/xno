from .. import settings
from .provider import DataProvider

_DataProvider = None


def DataProviderInstance(force: bool = False) -> DataProvider:
    global _DataProvider

    if _DataProvider is None or force:
        _DataProvider = DataProvider(
            external_db="xno_data",
            consumer_config=dict(
                topic=settings.kafka_market_data_topic,
                **{
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "enable.auto.commit": False,
                    "group.id": "xno-data-consumer-group",
                    "auto.offset.reset": "latest",
                },
            ),
        )
    return _DataProvider


__all__ = ["DataProviderInstance"]
