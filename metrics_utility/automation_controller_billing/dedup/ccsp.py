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
        if dedup_info is None or dedup_info.empty:
            return new

        if not self.experimental:
            # For non-experimental mode, apply basic deduplication using canonical hostname
            # Create mapping and apply deduplication
            mapping = self.df_to_mapping(dedup_info.copy())

            # Apply dedup to the class names we have
            # We need the dataframe instances to call dedup methods, but we return the actual data
            for class_name in ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']:
                if class_name in new and new[class_name] is not None and not new[class_name].empty:
                    # Get the dataframe instance to call the dedup method
                    if hasattr(self, 'dataframe_instances') and class_name in self.dataframe_instances:
                        # Call dedup on the instance but make sure we get back the actual DataFrame
                        deduped_df = self.dataframe_instances[class_name].dedup(new[class_name], mapping)
                        new[class_name] = deduped_df

            return new

        # each host_name in dedup_info has a list of combined serials
        # convert to a mapping from any hostname with that serial to a canonical hostname
        # Make a copy to avoid modifying the original dataframe
        dedup_info_copy = dedup_info.copy()
        mapping = self.df_to_mapping(dedup_info_copy)
        if 'serials' in dedup_info_copy.columns:
            del dedup_info_copy['serials']

        for class_name in ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope']:
            if class_name in new and new[class_name] is not None and not new[class_name].empty:
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

        for _, row in df.iterrows():
            host = row['host_name']
            serials = row['serials']

            if serials is not None and len(serials) > 0:
                for serial in serials:
                    if serial:
                        serial_to_hosts[serial].add(host)
                        if serial not in serial_to_first:
                            serial_to_first[serial] = host

        host_to_canonical = {}
        for serial, hosts in serial_to_hosts.items():
            canonical = serial_to_first[serial]
            for host in hosts:
                host_to_canonical[host] = canonical

        return host_to_canonical
