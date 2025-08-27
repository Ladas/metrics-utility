from collections import defaultdict


class DedupCCSP:
    def __init__(self, dataframes, extra_params, experimental=False):
        self.dataframes = dataframes
        self.extra_params = extra_params
        self.experimental = experimental

    def run(self):
        new = {}
        # The dataframes dict now contains actual dataframe data keyed by class name
        for name, dataframe in self.dataframes.items():
            new[name] = dataframe

        # Get dedup_info from DataframeInventoryScope (main_host equivalent)
        dedup_info = new.get('DataframeInventoryScope')
        if dedup_info is None or len(dedup_info) == 0:
            return new

        if not self.experimental:
            # For non-experimental mode, apply basic deduplication using canonical hostname
            # Create mapping and apply deduplication
            # Handle both pandas and polars DataFrame copy/clone methods
            if hasattr(dedup_info, 'clone'):  # Polars DataFrame
                mapping = self.df_to_mapping(dedup_info.clone())
            else:  # pandas DataFrame fallback
                mapping = self.df_to_mapping(dedup_info.copy())

            # Apply dedup to the class names we have
            # We need the dataframe instances to call dedup methods, but we return the actual data
            for class_name in ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']:
                if class_name in new and new[class_name] is not None and len(new[class_name]) > 0:
                    # Get the dataframe instance to call the dedup method
                    if hasattr(self, 'dataframe_instances') and class_name in self.dataframe_instances:
                        # Call dedup on the instance but make sure we get back the actual DataFrame
                        deduped_df = self.dataframe_instances[class_name].dedup(new[class_name], mapping)
                        new[class_name] = deduped_df

            return new

        # each host_name in dedup_info has a list of combined serials
        # convert to a mapping from any hostname with that serial to a canonical hostname
        # Make a copy to avoid modifying the original dataframe
        # Handle both pandas and polars DataFrame copy/clone methods
        if hasattr(dedup_info, 'clone'):  # Polars DataFrame
            dedup_info_copy = dedup_info.clone()
        else:  # pandas DataFrame fallback
            dedup_info_copy = dedup_info.copy()
        mapping = self.df_to_mapping(dedup_info_copy)
        if 'serials' in dedup_info_copy.columns:
            # Use Polars drop() method instead of del statement
            dedup_info_copy = dedup_info_copy.drop('serials')

        for class_name in ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']:
            if class_name in new and new[class_name] is not None and len(new[class_name]) > 0:
                # Get the dataframe instance to call the dedup method
                if hasattr(self, 'dataframe_instances') and class_name in self.dataframe_instances:
                    # Pass main_host dataframe to job_host_summary for canonical facts enrichment
                    if class_name == 'DataframeJobhostSummaryUsage':
                        deduped_df = self.dataframe_instances[class_name].dedup(new[class_name], mapping, scope_dataframe=dedup_info_copy)
                    else:
                        deduped_df = self.dataframe_instances[class_name].dedup(new[class_name], mapping)

                    new[class_name] = deduped_df

                    # Regrouping is now handled automatically in the base class dedup method
                    # Only regroups when actual duplicates are detected after hostname mapping
        # no dedup on data_collection_status

        return new

    def df_to_mapping(self, df):
        serial_to_hosts = defaultdict(set)
        serial_to_first = {}

        print(f"DEBUG DEDUP MAPPING: Input dataframe has {len(df)} records")
        if 'host_name' in df.columns:
            print(f"DEBUG DEDUP MAPPING: Input hosts: {sorted(df['host_name'].unique().to_list())}")

        # Handle both pandas and polars DataFrame iteration methods
        if hasattr(df, 'iter_rows'):  # Polars DataFrame
            iterator = df.iter_rows(named=True)
        else:  # pandas DataFrame fallback
            iterator = (row for _, row in df.iterrows())

        missing_hosts = ['manually_created_host_1', 'test_host_42']
        
        for row in iterator:
            host = row['host_name']
            serials = row['serials']

            if host in missing_hosts:
                print(f"DEBUG DEDUP MAPPING: Processing {host}: serials={serials}, type={type(serials)}")

            if serials is not None and len(serials) > 0:
                # Handle string serials (convert to list if needed)
                if isinstance(serials, str):
                    serial_list = [serials] if serials else []
                else:
                    serial_list = serials if hasattr(serials, '__iter__') else [serials]
                    
                for serial in serial_list:
                    if serial:
                        serial_to_hosts[serial].add(host)
                        if serial not in serial_to_first:
                            serial_to_first[serial] = host
                            
                        if host in missing_hosts:
                            print(f"DEBUG DEDUP MAPPING: {host} mapped to serial {serial}")

        host_to_canonical = {}
        for serial, hosts in serial_to_hosts.items():
            canonical = serial_to_first[serial]
            for host in hosts:
                host_to_canonical[host] = canonical
                if host in missing_hosts:
                    print(f"DEBUG DEDUP MAPPING: {host} -> canonical {canonical}")

        print(f"DEBUG DEDUP MAPPING: Created mapping for {len(host_to_canonical)} hosts")
        for missing_host in missing_hosts:
            if missing_host in host_to_canonical:
                print(f"DEBUG DEDUP MAPPING: ✓ {missing_host} mapped to {host_to_canonical[missing_host]}")
            else:
                print(f"DEBUG DEDUP MAPPING: ✗ {missing_host} NOT in mapping (will be filtered out)")

        return host_to_canonical
