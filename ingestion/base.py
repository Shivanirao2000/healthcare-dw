"""Abstract base class for all ingestion pipelines."""

from abc import ABC, abstractmethod
from loguru import logger


class BasePipeline(ABC):
    name: str = ""

    def run(self) -> None:
        logger.info(f"[{self.name}] starting")
        self.extract()
        self.load()
        logger.info(f"[{self.name}] complete")

    @abstractmethod
    def extract(self) -> None: ...

    @abstractmethod
    def load(self) -> None: ...
