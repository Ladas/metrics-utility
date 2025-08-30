import polars as pd


class BaseDedupRenewal:
    """Base class for renewal guidance deduplication containing common functionality."""

    def __init__(self, dataframes, extra_params):
        self.dataframe = dataframes['host_metric'].build_dataframe()
        self.extra_params = extra_params

    def _cleanup_null_values(self):
        """Clean up null-like values in key fields."""
        # Cleanup ansible_host_variable - Use Polars with_columns
        self.dataframe = self.dataframe.with_columns(self.dataframe['ansible_host_variable'].str.replace('', None).alias('ansible_host_variable'))

        # Cleanup ansible_product_serial - Use Polars with_columns
        self.dataframe = self.dataframe.with_columns(
            self.dataframe['ansible_product_serial'].str.replace('NA', None).str.replace('', None).alias('ansible_product_serial')
        )

        # Cleanup ansible_machine_id - Use Polars with_columns
        self.dataframe = self.dataframe.with_columns(
            self.dataframe['ansible_machine_id'].str.replace('NA', None).str.replace('', None).alias('ansible_machine_id')
        )

    def _get_latest_hostname(self, dupes):
        """Get the latest non-deleted hostname to represent the duplicate group."""
        return dupes.sort_values(by=['deleted', 'last_automation'], ascending=[True, False])['hostname'].iloc[0]

    def _build_deduped_record(self, dupes, latest_hostname, dupes_clean=None):
        """Build a standardized deduped record from duplicate group."""
        if dupes_clean is None:
            dupes_clean = dupes

        return {
            'hostname': latest_hostname,
            'hostmetric_record_count': dupes['hostname'].nunique(),
            'hostmetric_record_count_active': (dupes[~dupes['deleted']]['hostname'].nunique()),
            'hostmetric_record_count_deleted': (dupes[dupes['deleted']]['hostname'].nunique()),
            'hostnames': self.stringify(set(dupes['hostname'])),
            'ansible_host_variables': self.stringify(set(dupes['ansible_host_variable'])),
            'ansible_product_serials': self.stringify(set(dupes_clean['ansible_product_serial'])),
            'ansible_machine_ids': self.stringify(set(dupes_clean['ansible_machine_id'])),
            'deleted': min(dupes['deleted']),
            'first_automation': min(dupes['first_automation']),
            'last_automation': max(dupes['last_automation']),
            'automated_counter': sum(dupes['automated_counter']),
            'deleted_counter': sum(dupes['deleted_counter']),
            'last_deleted': max(dupes['last_deleted']),
        }

    def stringify(self, value):
        """Convert a set of values to a comma-separated string, filtering out None."""
        return ', '.join([v for v in list(value) if v is not None])

    def run(self):
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError('Subclasses must implement the run method')


class DedupRenewal(BaseDedupRenewal):
    """Original deduplication logic with iterative relationship discovery."""

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        self._cleanup_null_values()
        deduped_list = []
        processed_dupes_index = set()

        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(self.dataframe, 'iter_rows'):  # Polars DataFrame
            iterator = enumerate(self.dataframe.iter_rows(named=True))
        else:  # pandas DataFrame fallback
            iterator = self.dataframe.iterrows()

        for index, row in iterator:
            # Skip if index is in existing dupes_index
            if index in processed_dupes_index:
                continue

            # Start with hostname matches using Polars-compatible syntax
            hostname_filter = self.dataframe['hostname'] == row['hostname']
            dupes = self.dataframe.filter(hostname_filter) if hasattr(self.dataframe, 'filter') else self.dataframe[hostname_filter]

            # Iterative search to cover indirect relationships
            iterations = int(self.extra_params['report_renewal_guidance_dedup_iterations'])
            for i in range(iterations):
                # Hostname dupe lookup
                dupes = self.find_dupes(dupes, 'hostname', dupes['hostname'])

                # Host variable dupe lookup
                dupes = self.find_dupes(dupes, 'ansible_host_variable', dupes['ansible_host_variable'])

                # Serial key dupes lookup
                dupes = self.find_dupes(dupes, 'ansible_product_serial', dupes['ansible_product_serial'])

                # ansible_machine_id key dupes lookup
                dupes = self.find_dupes(dupes, 'ansible_machine_id', dupes['ansible_machine_id'])

            processed_dupes_index.update(dupes['index'])
            latest_hostname = self._get_latest_hostname(dupes)
            deduped_list.append(self._build_deduped_record(dupes, latest_hostname))

        return {'host_metric': pd.DataFrame(deduped_list)}

    def find_dupes(self, dupes, key, values):
        """Find additional duplicates based on a specific key and values."""
        next_iteration_dupes = self.dataframe[self.dataframe[key].notnull() & self.dataframe[key].isin(values)]
        dupes = pd.concat([dupes, next_iteration_dupes]).drop_duplicates().reset_index(drop=True)
        return dupes


class DedupRenewalHostname(BaseDedupRenewal):
    """
    Hostname-based deduplication for renewal guidance that mirrors CCSP logic.
    Uses ansible_host_variable || hostname for deduplication, similar to CCSP.
    """

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        # Ensure required columns exist
        if 'ansible_host_variable' not in self.dataframe.columns:
            self.dataframe = self.dataframe.with_columns(pd.lit(None).alias('ansible_host_variable'))

        self._cleanup_null_values()

        # Apply hostname normalization logic similar to CCSP:
        # ansible_host_variable || hostname
        self.dataframe = self.dataframe.with_columns(
            self.dataframe['ansible_host_variable'].fill_null(self.dataframe['hostname']).alias('normalized_hostname')
        )

        deduped_list = []
        processed_dupes_index = set()

        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(self.dataframe, 'iter_rows'):  # Polars DataFrame
            iterator = enumerate(self.dataframe.iter_rows(named=True))
        else:  # pandas DataFrame fallback
            iterator = self.dataframe.iterrows()

        for index, row in iterator:
            # Skip if index is in existing dupes_index
            if index in processed_dupes_index:
                continue

            # Find duplicates based on normalized hostname only using Polars-compatible syntax
            normalized_hostname_filter = self.dataframe['normalized_hostname'] == row['normalized_hostname']
            dupes = (
                self.dataframe.filter(normalized_hostname_filter) if hasattr(self.dataframe, 'filter') else self.dataframe[normalized_hostname_filter]
            )
            processed_dupes_index.update(dupes['index'])

            latest_hostname = self._get_latest_hostname(dupes)

            # Clean up product serial and machine ID for consistent output
            # Handle both pandas and polars DataFrame copy/clone methods
            if hasattr(dupes, 'clone'):  # Polars DataFrame
                dupes_clean = dupes.clone()
            else:  # pandas DataFrame fallback
                dupes_clean = dupes.copy()
            dupes_clean = dupes_clean.with_columns(
                [
                    dupes_clean['ansible_product_serial'].str.replace('NA', None).str.replace('', None).alias('ansible_product_serial'),
                    dupes_clean['ansible_machine_id'].str.replace('NA', None).str.replace('', None).alias('ansible_machine_id'),
                ]
            )

            deduped_list.append(self._build_deduped_record(dupes, latest_hostname, dupes_clean))

        return {'host_metric': pd.DataFrame(deduped_list)}


class DedupRenewalExperimental(BaseDedupRenewal):
    """
    Experimental deduplication for renewal guidance that combines hostname-based
    deduplication with serial-based deduplication (product_serial + machine_id).
    Mimics the CCSP experimental approach with proper multiple serial handling.
    """

    def run(self):
        # Check if dataframe is empty or missing required columns
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        # Step 1: Apply hostname-based deduplication first
        # Create a mock dataframe object that returns our dataframe
        class MockDataframe:
            def __init__(self, dataframe):
                self.dataframe = dataframe

            def build_dataframe(self):
                return self.dataframe

        mock_dataframe = MockDataframe(self.dataframe)

        hostname_dedup = DedupRenewalHostname({'host_metric': mock_dataframe}, self.extra_params)
        hostname_result = hostname_dedup.run()
        hostname_df = hostname_result['host_metric']

        # Step 2: Apply serial-based deduplication on top of hostname results
        return self._apply_serial_deduplication(hostname_df)

    def _apply_serial_deduplication(self, hostname_df):
        """Apply serial-based deduplication similar to CCSP experimental mode."""
        # Handle both pandas and polars DataFrame copy/clone methods
        if hasattr(hostname_df, 'clone'):  # Polars DataFrame
            hostname_df = hostname_df.clone()
        else:  # pandas DataFrame fallback
            hostname_df = hostname_df.copy()
        expanded_df = self._expand_serial_records(hostname_df)
        if expanded_df is None or expanded_df.empty:
            return {'host_metric': hostname_df}

        serial_groups, processed_hostname_groups = self._create_serial_groups(expanded_df)
        final_deduped_list = self._build_final_deduped_list(hostname_df, serial_groups, processed_hostname_groups)
        return {'host_metric': pd.DataFrame(final_deduped_list)}

    def _expand_serial_records(self, hostname_df):
        """Expand aggregated serial data back to individual records for processing."""
        expanded_records = []
        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(hostname_df, 'iter_rows'):  # Polars DataFrame
            hostname_iterator = hostname_df.iter_rows(named=True)
        else:  # pandas DataFrame fallback
            hostname_iterator = (row for _, row in hostname_df.iterrows())

        for group_row in hostname_iterator:
            hostnames_in_group = [h.strip() for h in group_row['hostnames'].split(',') if h.strip()]
            original_records = self.dataframe[self.dataframe['hostname'].isin(hostnames_in_group)]

            # Handle both pandas and polars DataFrame iteration methods for original_records
            if hasattr(original_records, 'iter_rows'):  # Polars DataFrame
                orig_iterator = original_records.iter_rows(named=True)
            else:  # pandas DataFrame fallback
                orig_iterator = (row for _, row in original_records.iterrows())

            for orig_row in orig_iterator:
                # Parse multiple serial numbers from comma-separated values
                product_serials = self._parse_multiple_serials(orig_row.get('ansible_product_serial'))
                machine_ids = self._parse_multiple_serials(orig_row.get('ansible_machine_id'))

                # Create records for all combinations of serials
                serial_combinations = self._create_serial_combinations(product_serials, machine_ids)

                for serial_info in serial_combinations:
                    expanded_records.append(
                        {
                            'hostname': orig_row['hostname'],
                            'hostname_group': group_row['hostname'],
                            'product_serial': serial_info['product_serial'],
                            'machine_id': serial_info['machine_id'],
                            'compound_serial': serial_info['compound_serial'],
                            'individual_serials': serial_info['individual_serials'],
                            'original_data': group_row.to_dict(),
                        }
                    )
        if not expanded_records:
            return None
        return pd.DataFrame(expanded_records)

    def _parse_multiple_serials(self, value):
        """Parse multiple serial numbers from comma-separated values."""
        if pd.isna(value) or value in ['', 'NA']:
            return []

        # Handle single value or comma-separated values
        if isinstance(value, str):
            serials = [s.strip() for s in value.split(',') if s.strip() and s.strip() != 'NA']
            return [s for s in serials if s]  # Remove empty strings

        return [value] if value else []

    def _create_serial_combinations(self, product_serials, machine_ids):
        """Create all valid serial combinations for deduplication."""
        combinations = []

        # If we have both product serials and machine IDs, create compound keys
        if product_serials and machine_ids:
            for ps in product_serials:
                for mid in machine_ids:
                    combinations.append(
                        {
                            'product_serial': ps,
                            'machine_id': mid,
                            'compound_serial': f'{ps}/{mid}',
                            'individual_serials': [ps, mid],
                        }
                    )

        # Also add individual serials for partial matching
        for ps in product_serials:
            combinations.append(
                {
                    'product_serial': ps,
                    'machine_id': None,
                    'compound_serial': f'ps:{ps}',
                    'individual_serials': [ps],
                }
            )

        for mid in machine_ids:
            combinations.append(
                {
                    'product_serial': None,
                    'machine_id': mid,
                    'compound_serial': f'mid:{mid}',
                    'individual_serials': [mid],
                }
            )

        # If no serials, return empty list
        if not combinations:
            combinations.append(
                {
                    'product_serial': None,
                    'machine_id': None,
                    'compound_serial': None,
                    'individual_serials': [],
                }
            )

        return combinations

    def _create_serial_groups(self, expanded_df):
        """Create serial-based groupings with support for multiple serials."""
        serial_groups = {}
        processed_hostname_groups = set()

        self._group_by_compound_serial(expanded_df, serial_groups, processed_hostname_groups)
        self._group_by_individual_serial(expanded_df, serial_groups, processed_hostname_groups)

        return serial_groups, processed_hostname_groups

    def _group_by_compound_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        """Helper to group by compound serials."""
        compound_serials = expanded_df['compound_serial'].dropna()
        for compound_serial in compound_serials.unique():
            # Use Polars-compatible filtering syntax
            serial_filter = expanded_df['compound_serial'] == compound_serial
            serial_matches = expanded_df.filter(serial_filter) if hasattr(expanded_df, 'filter') else expanded_df[serial_filter]
            hostname_groups_in_serial = serial_matches['hostname_group'].unique()
            if len(hostname_groups_in_serial) > 1:
                canonical_group = hostname_groups_in_serial[0]
                serial_groups[compound_serial] = {
                    'canonical_group': canonical_group,
                    'groups_to_merge': hostname_groups_in_serial,
                    'serial_type': 'compound',
                }
                processed_hostname_groups.update(hostname_groups_in_serial)

    def _group_by_individual_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        """Helper to group by individual serials."""
        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(expanded_df, 'iter_rows'):  # Polars DataFrame
            iterator = expanded_df.iter_rows(named=True)
        else:  # pandas DataFrame fallback
            iterator = (row for _, row in expanded_df.iterrows())

        for row in iterator:
            if row['hostname_group'] not in processed_hostname_groups:
                for serial in row['individual_serials']:
                    if serial:
                        serial_matches = expanded_df[expanded_df['individual_serials'].apply(lambda x: serial in x if x else False)]
                        hostname_groups_in_serial = serial_matches['hostname_group'].unique()
                        if len(hostname_groups_in_serial) > 1:
                            canonical_group = hostname_groups_in_serial[0]
                            serial_key = f'individual:{serial}'
                            if serial_key not in serial_groups:
                                serial_groups[serial_key] = {
                                    'canonical_group': canonical_group,
                                    'groups_to_merge': hostname_groups_in_serial,
                                    'serial_type': 'individual',
                                }
                                processed_hostname_groups.update(hostname_groups_in_serial)

    def _build_final_deduped_list(self, hostname_df, serial_groups, processed_hostname_groups):
        """Build the final deduped list based on serial and hostname groups."""
        final_deduped_list = []
        canonical_groups = {info['canonical_group'] for info in serial_groups.values()}

        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(hostname_df, 'iter_rows'):  # Polars DataFrame
            iterator = hostname_df.iter_rows(named=True)
        else:  # pandas DataFrame fallback
            iterator = (row for _, row in hostname_df.iterrows())

        for row in iterator:
            hostname_group = row['hostname']
            if hostname_group in processed_hostname_groups:
                self._handle_processed_group(
                    hostname_group,
                    hostname_df,
                    serial_groups,
                    canonical_groups,
                    final_deduped_list,
                )
            else:
                final_deduped_list.append(row.to_dict())
        return final_deduped_list

    def _handle_processed_group(
        self,
        hostname_group,
        hostname_df,
        serial_groups,
        canonical_groups,
        final_deduped_list,
    ):
        if hostname_group in canonical_groups:
            self._append_canonical_group(hostname_group, hostname_df, serial_groups, final_deduped_list)
        # else: skip non-canonical processed groups

    def _append_canonical_group(self, hostname_group, hostname_df, serial_groups, final_deduped_list):
        for serial_info in serial_groups.values():
            if hostname_group == serial_info['canonical_group']:
                merged_data = self._merge_hostname_groups(hostname_df, serial_info['groups_to_merge'])
                final_deduped_list.append(merged_data)
                break

    def _merge_hostname_groups(self, hostname_df, groups_to_merge):
        """Merge multiple hostname groups into a single group."""
        groups_data = hostname_df[hostname_df['hostname'].isin(groups_to_merge)]

        if groups_data.empty:
            return {}

        # Take the latest hostname as representative
        latest_group = groups_data.sort_values(by=['last_automation'], ascending=[False]).iloc[0]

        # Merge the data with improved serial handling
        merged = {
            'hostname': latest_group['hostname'],
            'hostmetric_record_count': (groups_data['hostmetric_record_count'].sum()),
            'hostmetric_record_count_active': (groups_data['hostmetric_record_count_active'].sum()),
            'hostmetric_record_count_deleted': (groups_data['hostmetric_record_count_deleted'].sum()),
            'hostnames': ', '.join([h for group in groups_data['hostnames'] for h in group.split(', ') if h]),
            'ansible_host_variables': ', '.join([h for group in groups_data['ansible_host_variables'] for h in group.split(', ') if h]),
            'ansible_product_serials': self._merge_serial_fields(
                [
                    group['ansible_product_serials']
                    for group in (
                        groups_data.iter_rows(named=True) if hasattr(groups_data, 'iter_rows') else (row for _, row in groups_data.iterrows())
                    )
                ]
            ),
            'ansible_machine_ids': self._merge_serial_fields(
                [
                    group['ansible_machine_ids']
                    for group in (
                        groups_data.iter_rows(named=True) if hasattr(groups_data, 'iter_rows') else (row for _, row in groups_data.iterrows())
                    )
                ]
            ),
            'deleted': groups_data['deleted'].min(),
            'first_automation': groups_data['first_automation'].min(),
            'last_automation': groups_data['last_automation'].max(),
            'automated_counter': groups_data['automated_counter'].sum(),
            'deleted_counter': groups_data['deleted_counter'].sum(),
            'last_deleted': groups_data['last_deleted'].max(),
        }

        return merged

    def _merge_serial_fields(self, serial_fields):
        """Merge serial fields while preserving individual serial numbers."""
        all_serials = set()
        for field in serial_fields:
            if field:
                # Split comma-separated values and add to set
                serials = [s.strip() for s in str(field).split(', ') if s.strip()]
                all_serials.update(serials)

        # Remove None and empty values, sort for consistency
        clean_serials = sorted([s for s in all_serials if s and s != 'None'])
        return ', '.join(clean_serials) if clean_serials else ''
