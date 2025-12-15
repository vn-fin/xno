from .. import settings
from .provider import DataProvider as DataProviderClass

DataProvider = DataProviderClass(
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

__all__ = ["DataProvider"]
