"""Crawler auto-discovery and registration."""

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import Dict, Type

from crawlers.base import BaseCrawler


class CrawlerRegistry:
    """
    Singleton registry that discovers and manages crawler classes.

    Responsibility: Auto-discovery and instantiation of crawlers
    from the crawlers package.
    """

    _instance: "CrawlerRegistry | None" = None
    _crawlers: Dict[str, BaseCrawler]

    def __new__(cls) -> "CrawlerRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._crawlers = {}
        return cls._instance

    def discover_crawlers(self, package_path: str = "crawlers") -> Dict[str, BaseCrawler]:
        """
        Scan the crawlers package and instantiate all BaseCrawler subclasses.

        Args:
            package_path: The package path to scan for crawlers.

        Returns:
            Dictionary mapping crawler names to crawler instances.
        """
        self._crawlers = {}

        try:
            package = importlib.import_module(package_path)
        except ImportError as e:
            print(f"Failed to import package {package_path}: {e}")
            return self._crawlers

        package_dir = Path(package.__file__).parent

        for module_info in pkgutil.iter_modules([str(package_dir)]):
            # Skip private modules and base module
            if module_info.name.startswith("_") or module_info.name in ("base", "registry"):
                continue

            try:
                module = importlib.import_module(f"{package_path}.{module_info.name}")
            except ImportError as e:
                print(f"Failed to import module {module_info.name}: {e}")
                continue

            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a concrete subclass of BaseCrawler
                if (
                    issubclass(obj, BaseCrawler)
                    and obj is not BaseCrawler
                    and not inspect.isabstract(obj)
                ):
                    try:
                        crawler_instance = obj()
                        self._crawlers[crawler_instance.name] = crawler_instance
                        print(f"Discovered crawler: {crawler_instance.name}")
                    except Exception as e:
                        print(f"Failed to instantiate crawler {name}: {e}")

        return self._crawlers

    def get_crawler(self, name: str) -> BaseCrawler | None:
        """
        Get a crawler instance by name.

        Args:
            name: The crawler's unique name.

        Returns:
            The crawler instance or None if not found.
        """
        return self._crawlers.get(name)

    def get_all_crawlers(self) -> Dict[str, BaseCrawler]:
        """
        Get all registered crawler instances.

        Returns:
            Dictionary mapping crawler names to crawler instances.
        """
        return self._crawlers.copy()

    def clear(self) -> None:
        """Clear all registered crawlers. Useful for testing."""
        self._crawlers = {}

    def get_article_crawler_by_source(self, source: str) -> BaseCrawler | None:
        """
        Get an article crawler instance by source name.

        Args:
            source: The news source name (e.g., "ETtoday", "UDN").

        Returns:
            The article crawler instance or None if not found.
        """
        from crawlers.base import CrawlerType

        for crawler in self._crawlers.values():
            if crawler.source == source and crawler.crawler_type == CrawlerType.ARTICLE:
                return crawler
        return None


# Global singleton instance
crawler_registry = CrawlerRegistry()


def get_article_crawler_by_source(source: str) -> BaseCrawler | None:
    """
    Convenience function to get an article crawler by source name.

    Args:
        source: The news source name (e.g., "ETtoday", "UDN").

    Returns:
        The article crawler instance or None if not found.
    """
    return crawler_registry.get_article_crawler_by_source(source)
