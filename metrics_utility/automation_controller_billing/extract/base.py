import json
import os
import tarfile
import time

from datetime import datetime
from typing import List, Tuple

import polars as pd

from opentelemetry import trace

from metrics_utility.logger import logger
from metrics_utility.tracing import add_span_attributes


# csv name => [ sheet_names ]
CSV_SHEETS = {
    'job_host_summary': [
        'ccsp_summary',
        'indirectly_managed_nodes',
        'inventory_scope',
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_organizations',
    ],
    'main_host': [
        'inventory_scope',
        'jobs',
        'managed_nodes',
        'managed_nodes_by_organizations',
        'usage_by_collections',
        'usage_by_modules',
        'usage_by_roles',
    ],
    'main_indirectmanagednodeaudit': [
        'indirectly_managed_nodes',
        'managed_nodes',
        'usage_by_organizations',
    ],
    'main_jobevent': [
        'usage_by_collections',
        'usage_by_modules',
        'usage_by_organizations',
        'usage_by_roles',
    ],
    'data_collection_status': [
        'data_collection_status',
    ],
}


class Base:
    LOG_PREFIX = '[ExtractorBase]'

    def __init__(self, extra_params):
        self.extra_params = extra_params

    def load_config(self, file_path):
        try:
            with open(file_path) as f:
                return json.loads(f.read())
        except FileNotFoundError:
            logger.warning(f'{self.LOG_PREFIX} missing required file under path: {file_path} and date: {self.date}')

    def process_tarballs(self, path, temp_dir):
        """Process tarball extraction and CSV data loading with comprehensive tracing"""

        with trace.get_tracer(__name__).start_as_current_span('tarball.extraction') as extraction_span:
            # Add tarball context to span
            tarball_size = os.path.getsize(path) if os.path.exists(path) else 0
            add_span_attributes(extraction_span, **{'tarball.path': path, 'tarball.size_bytes': tarball_size, 'tarball.temp_dir': temp_dir})

            start_time = time.time()
            _safe_extract(path, temp_dir)
            extraction_time = time.time() - start_time

            add_span_attributes(
                extraction_span,
                **{
                    'tarball.extraction_time_seconds': extraction_time,
                    'tarball.extraction_rate_mbps': (tarball_size / (1024 * 1024)) / extraction_time if extraction_time > 0 else 0,
                },
            )

        with trace.get_tracer(__name__).start_as_current_span('tarball.config.loading') as config_span:
            config = self.load_config(os.path.join(temp_dir, 'config.json'))
            add_span_attributes(config_span, **{'config.loaded': True, 'config.path': os.path.join(temp_dir, 'config.json')})

        empty_dataframe = pd.DataFrame([{}])
        needed_data = {
            'config': config,
            'data_collection_status': empty_dataframe,
            'indirect_nodes': empty_dataframe,
            'job_host_summary': empty_dataframe,
            'main_host': empty_dataframe,
            'main_jobevent': empty_dataframe,
        }

        # Track which CSVs are processed
        processed_csvs = []
        total_records = 0

        if self.csv_enabled('data_collection_status'):
            with trace.get_tracer(__name__).start_as_current_span('csv.processing.data_collection_status') as csv_span:
                df = self.build_data_batch(temp_dir, 'data_collection_status')
                needed_data['data_collection_status'] = df
                records = len(df) if df is not None and len(df) > 0 else 0
                total_records += records
                processed_csvs.append('data_collection_status')
                add_span_attributes(csv_span, **{'csv.name': 'data_collection_status', 'csv.records': records})

        if self.csv_enabled('job_host_summary'):
            with trace.get_tracer(__name__).start_as_current_span('csv.processing.job_host_summary') as csv_span:
                df = self.build_data_batch(temp_dir, 'job_host_summary')
                needed_data['job_host_summary'] = df
                records = len(df) if df is not None and len(df) > 0 else 0
                total_records += records
                processed_csvs.append('job_host_summary')
                add_span_attributes(csv_span, **{'csv.name': 'job_host_summary', 'csv.records': records})

        if self.csv_enabled('main_indirectmanagednodeaudit'):
            with trace.get_tracer(__name__).start_as_current_span('csv.processing.main_indirectmanagednodeaudit') as csv_span:
                df = self.build_data_batch(temp_dir, 'main_indirectmanagednodeaudit')
                needed_data['indirect_nodes'] = df
                records = len(df) if df is not None and len(df) > 0 else 0
                total_records += records
                processed_csvs.append('main_indirectmanagednodeaudit')
                add_span_attributes(csv_span, **{'csv.name': 'main_indirectmanagednodeaudit', 'csv.records': records})

        if self.csv_enabled('main_jobevent'):
            with trace.get_tracer(__name__).start_as_current_span('csv.processing.main_jobevent') as csv_span:
                df = self.build_data_batch(temp_dir, 'main_jobevent')
                needed_data['main_jobevent'] = df
                records = len(df) if df is not None and len(df) > 0 else 0
                total_records += records
                processed_csvs.append('main_jobevent')
                add_span_attributes(csv_span, **{'csv.name': 'main_jobevent', 'csv.records': records})

        if self.csv_enabled('main_host'):
            with trace.get_tracer(__name__).start_as_current_span('csv.processing.main_host') as csv_span:
                df = self.build_data_batch(temp_dir, 'main_host')
                needed_data['main_host'] = df
                records = len(df) if df is not None and len(df) > 0 else 0
                total_records += records
                processed_csvs.append('main_host')
                add_span_attributes(csv_span, **{'csv.name': 'main_host', 'csv.records': records})

        # Add summary metrics to the current span
        current_span = trace.get_current_span()
        add_span_attributes(
            current_span,
            **{
                'tarball.processing.total_records': total_records,
                'tarball.processing.csv_count': len(processed_csvs),
                'tarball.processing.csvs_processed': ','.join(processed_csvs),
            },
        )

        return needed_data

    def build_data_batch(self, temp_dir, file_name):
        """
        Builds the report with only the necessary sheets.
        """

        if os.path.exists(os.path.join(temp_dir, f'{file_name}.csv')):
            return pd.read_csv(os.path.join(temp_dir, f'{file_name}.csv'))
        else:
            return pd.DataFrame([{}])

    def csv_enabled(self, name):
        """Enable CSV extraction based on list of rendered sheets"""
        return self.sheet_enabled(CSV_SHEETS[name])

    def get_path_prefix(self, date):
        """Return the data/Y/m/d path"""
        ship_path = self.extra_params['ship_path']

        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        return f'{ship_path}/data/{year}/{month}/{day}'

    def sheet_enabled(self, sheets_required):
        """
        Checks if any sheets_required item is in METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS
        Returns a boolean so we know which sheets to provide in the report.
        """
        sheet_options = self.extra_params.get('optional_sheets')
        if sheet_options is None:
            return False
        return bool(set(sheet_options) & set(sheets_required))

    def scan_tarballs_for_date(self, target_date) -> List[Tuple[str, datetime]]:
        """
        Scan for tarballs available for a specific date without extracting them

        Args:
            target_date: Date to scan for

        Returns:
            List of tuples (tarball_path, modification_time)

        Note:
            This is a base implementation that should be overridden by specific extractors
        """
        logger.debug(f'{self.LOG_PREFIX} Base scan_tarballs_for_date called for {target_date} - no tarballs found')
        return []


def _write_member(member_path, file_obj, max_size, total_extracted_size):
    with open(member_path, 'wb') as out_f:
        chunk_size = 1024 * 1024  # 1 MB buffer
        while True:
            data = file_obj.read(chunk_size)
            if not data:
                break

            total_extracted_size += len(data)
            if total_extracted_size > max_size:
                # Stop if we exceed total extraction size
                raise ValueError('Extraction aborted: Maximum total size exceeded.')

            out_f.write(data)

    return total_extracted_size


def _safe_extract(tar_path, extract_path, max_files=100, max_size=1024 * 1024 * 1024):
    """
    Safely extract a tar archive from 'tar_path' into 'extract_path' with constraints:
      - Only extract *.json or *.csv files
      - Skip directories, symbolic links, and hard links
      - Limit number of extracted files to 'max_files'
      - Limit total uncompressed size to 'max_size' bytes
    """
    extracted_files = 0
    total_extracted_size = 0

    # Ensure the extraction directory exists
    os.makedirs(extract_path, exist_ok=True)

    with tarfile.open(tar_path, 'r:*') as tar:
        for member in tar.getmembers():
            # 1) Skip directories and links
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                logger.warning(f'Skipping link: {member.name}')
                continue

            # 2) Only allow .json or .csv
            if not (member.name.endswith('.json') or member.name.endswith('.csv')):
                continue

            # 3) Build a fully qualified path for this member
            #    and ensure it stays within extract_path.
            member_path = os.path.abspath(os.path.join(extract_path, member.name))
            extract_path_abs = os.path.abspath(extract_path)
            if not member_path.startswith(extract_path_abs + os.sep):
                logger.warning(f'Skipping potentially unsafe file (path traversal): {member.name}')
                continue

            # 4) Limit total files
            if extracted_files >= max_files:
                logger.warning(f'Reached max file limit of {max_files}.')
                break

            # 5) Extract file contents manually, in chunks,
            #    to avoid trusting the tar's metadata size.
            file_obj = tar.extractfile(member)
            if file_obj is None:
                # Could not read the file content for some reason
                continue

            # Make sure the subdirectory structure exists
            os.makedirs(os.path.dirname(member_path), exist_ok=True)

            # Write out the file, limiting max size
            total_extracted_size = _write_member(member_path, file_obj, max_size, total_extracted_size)

            extracted_files += 1

    logger.debug(f'Extraction complete. Files extracted: {extracted_files}, Total size: {total_extracted_size} bytes.')
