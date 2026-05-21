"""Registry of available ingestion pipelines."""

from ingestion.base import BasePipeline
from ingestion.cms_ingest import CMSIngestPipeline


class ClaimsPipeline(BasePipeline):
    name = "claims"

    def extract(self) -> None:
        raise NotImplementedError

    def load(self) -> None:
        raise NotImplementedError


class EhrPipeline(BasePipeline):
    name = "ehr"

    def extract(self) -> None:
        raise NotImplementedError

    def load(self) -> None:
        raise NotImplementedError


REGISTRY: dict[str, type[BasePipeline]] = {
    "claims": ClaimsPipeline,
    "ehr": EhrPipeline,
    "cms": CMSIngestPipeline,
}
