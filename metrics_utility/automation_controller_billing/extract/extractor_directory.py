import os
import tempfile

from datetime import datetime
from typing import List, Tuple

from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.logger import logger


class ExtractorDirectory(Base):
    LOG_PREFIX = '[ExtractorDirectory]'

    def iter_batches(self, date, columns=None, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size()

        # Read tarball in memory in batches
        logger.debug(f'{self.LOG_PREFIX} Processing {date}')
        paths = self.fetch_partition_paths(date)

        if batch_size is None:
            batch_size = self.batch_size()

        for path in paths:
            if not path.endswith('.tar.gz'):
                continue

            with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                try:
                    yield self.process_tarballs(path, temp_dir)

                except Exception as e:
                    logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {path} failed with {e}')

    def fetch_partition_paths(self, date):
        prefix = self.get_path_prefix(date)

        try:
            paths = [os.path.join(prefix, f) for f in os.listdir(prefix) if os.path.isfile(os.path.join(prefix, f))]
        except FileNotFoundError:
            paths = []

        return paths

    @staticmethod
    def batch_size():
        return 100000

    def scan_tarballs_for_date(self, target_date) -> List[Tuple[str, datetime]]:
        """
        Scan for tarballs available for a specific date without extracting them

        Args:
            target_date: Date to scan for

        Returns:
            List of tuples (tarball_path, modification_time)
        """
        prefix = self.get_path_prefix(target_date)
        tarballs = []

        try:
            if not os.path.exists(prefix):
                logger.debug(f'{self.LOG_PREFIX} Directory {prefix} does not exist for {target_date}')
                return tarballs

            for filename in os.listdir(prefix):
                if filename.endswith('.tar.gz'):
                    tarball_path = os.path.join(prefix, filename)
                    if os.path.isfile(tarball_path):
                        # Get modification time
                        stat_result = os.stat(tarball_path)
                        modification_time = datetime.fromtimestamp(stat_result.st_mtime)
                        tarballs.append((tarball_path, modification_time))

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.warning(f'{self.LOG_PREFIX} Failed to scan directory {prefix} for {target_date}: {e}')

        logger.debug(f'{self.LOG_PREFIX} Found {len(tarballs)} tarballs for {target_date}')
        return tarballs
