import time

import polars as pd

from opentelemetry import trace

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.tracing import add_span_attributes, traced_method


# dataframe for data_collection_status
class DataframeCollectionStatus(Base):
    @traced_method('collection_status.build_dataframe')
    def build_dataframe(self, batch_data_iterator):
        """Build Collection Status dataframe by processing batch data iterator.

        Args:
            batch_data_iterator: Iterator yielding batch_data from CSV scanning
                               Each batch_data: {'data_collection_status': DataFrame, ...}

        Returns:
            Concatenated dataframe containing all processed batches
        """
        current_span = trace.get_current_span()
        build_start_time = time.time()
        total_records = 0
        groups_processed = 0

        add_span_attributes(current_span, **{'dataframe.build.dataframe_type': 'CollectionStatus', 'dataframe.build.mode': 'batch_iterator'})

        # Initialize accumulated dataframe (concat-based, no aggregation)
        accumulated_dataframe = None

        # Process each batch from the iterator
        for batch_data in batch_data_iterator:
            # Get data_collection_status data from this batch
            batch = batch_data.get('data_collection_status')
            if batch is None or len(batch) == 0:
                continue

            # Process this batch into a group (consistent with other dataframes)
            date = batch_data.get('_date_context')  # Get date from context
            group_dataframe = self._process_batch_data(batch, batch_data, date)
            if group_dataframe is None or len(group_dataframe) == 0:
                continue

            # Merge with accumulated dataframe (consistent with other dataframes)
            if accumulated_dataframe is None:
                accumulated_dataframe = group_dataframe
            else:
                accumulated_dataframe = self.merge(accumulated_dataframe, group_dataframe)

            total_records += len(group_dataframe)
            groups_processed += 1

        build_duration = time.time() - build_start_time
        final_count = len(accumulated_dataframe) if accumulated_dataframe is not None else 0

        add_span_attributes(
            current_span,
            **{
                'dataframe.build.duration_seconds': build_duration,
                'dataframe.build.groups_processed': groups_processed,
                'dataframe.build.total_input_records': total_records,
                'dataframe.build.final_record_count': final_count,
            },
        )

        return accumulated_dataframe if accumulated_dataframe is not None else self.empty()

    def _process_batch_data(self, batch, batch_data, date):
        """Process individual batch data with comprehensive schema validation and data quality filtering."""
        from metrics_utility.tracing import add_span_attributes
        from opentelemetry import trace
        current_span = trace.get_current_span()
        
        # Handle empty DataFrame case
        if batch is None or len(batch) == 0:
            return None
        
        input_row_count = len(batch)
        
        # COMPREHENSIVE SCHEMA DEFINITION: Define complete schema with validation rules
        required_schema = {
            'collection_start_timestamp': {'type': str, 'required': True, 'allow_null': False},
            'since': {'type': str, 'required': True, 'allow_null': False}, 
            'until': {'type': str, 'required': True, 'allow_null': False},
            'file_name': {'type': str, 'required': True, 'allow_null': False},
            'status': {'type': str, 'required': True, 'allow_null': False},
            'elapsed': {'type': float, 'required': True, 'allow_null': True, 'min_value': 0.0}
        }
        
        # Schema validation and data quality metrics
        validation_metrics = {
            'missing_columns': [],
            'invalid_rows_count': 0,
            'rows_with_wrong_types': 0,
            'rows_with_null_required_fields': 0,
            'rows_with_invalid_values': 0,
            'total_input_rows': input_row_count
        }
        
        # Add missing columns with proper defaults
        for col, schema_def in required_schema.items():
            if col not in batch.columns:
                validation_metrics['missing_columns'].append(col)
                col_type = schema_def['type']
                if col_type == str:
                    batch = batch.with_columns(pd.lit("").alias(col))
                elif col_type == int:
                    batch = batch.with_columns(pd.lit(0).cast(pd.Int64).alias(col))
                elif col_type == float:
                    batch = batch.with_columns(pd.lit(0.0).alias(col))
        
        # Data quality validation and filtering
        valid_rows_mask = pd.lit(True)  # Start with all rows as valid
        
        for col, schema_def in required_schema.items():
            col_type = schema_def['type']
            required = schema_def['required']
            allow_null = schema_def.get('allow_null', True)
            
            # Type casting with error tracking
            try:
                if col_type == float:
                    if col in batch.columns:
                        try:
                            if not allow_null:
                                batch = batch.with_columns(
                                    batch[col].fill_null(value=0.0).cast(pd.Float64, strict=False).alias(col)
                                )
                            else:
                                batch = batch.with_columns(
                                    batch[col].cast(pd.Float64, strict=False).alias(col)
                                )
                        except Exception as cast_error:
                            # If casting fails, try string-based approach
                            try:
                                if not allow_null:
                                    batch = batch.with_columns(
                                        batch[col].fill_null(value="0.0").cast(str).str.extract(r'([+-]?\d*\.?\d*)', 1).cast(pd.Float64, strict=False).alias(col)
                                    )
                                else:
                                    batch = batch.with_columns(
                                        batch[col].cast(str).str.extract(r'([+-]?\d*\.?\d*)', 1).cast(pd.Float64, strict=False).alias(col)
                                    )
                            except Exception:
                                # Last resort: set default values
                                if not allow_null:
                                    batch = batch.with_columns(pd.lit(0.0).alias(col))
                        
                        # Only filter out rows with truly invalid values (required fields that are null when not allowed)
                        if not allow_null and required:
                            valid_rows_mask = valid_rows_mask & batch[col].is_not_null()
                        
                        # Validate min_value if specified - be more permissive
                        if 'min_value' in schema_def:
                            min_val = schema_def['min_value']
                            # Only filter out clearly invalid values (null or negative where positive required)
                            valid_rows_mask = valid_rows_mask & (batch[col].is_null() | (batch[col] >= min_val))
                
                elif col_type == str:
                    if col in batch.columns:
                        # Cast to string and handle nulls - be more permissive
                        try:
                            batch = batch.with_columns(
                                batch[col].cast(str).alias(col)
                            )
                        except Exception:
                            # Fallback for problematic string casting
                            batch = batch.with_columns(
                                batch[col].fill_null("").cast(str).alias(col)
                            )
                        
                        # Only filter out rows where required string fields are truly empty/null
                        if not allow_null and required:
                            # Be more permissive - only filter out if completely empty or "null" string
                            valid_rows_mask = valid_rows_mask & batch[col].is_not_null() & (batch[col] != "") & (batch[col] != "null")
                
            except Exception as e:
                # Log type casting errors but continue processing
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f'Type casting error for column {col}: {e}')
        
        # Apply the validation filter
        initial_count = len(batch)
        batch = batch.filter(valid_rows_mask)
        final_count = len(batch)
        
        validation_metrics['invalid_rows_count'] = initial_count - final_count
        validation_metrics['valid_rows_count'] = final_count
        validation_metrics['data_quality_ratio'] = final_count / initial_count if initial_count > 0 else 1.0
        
        # Add comprehensive validation metrics to tracing
        add_span_attributes(current_span, **{
            f'data_quality.{date.isoformat()}.input_rows': input_row_count,
            f'data_quality.{date.isoformat()}.valid_rows': final_count,
            f'data_quality.{date.isoformat()}.invalid_rows': validation_metrics['invalid_rows_count'],
            f'data_quality.{date.isoformat()}.quality_ratio': validation_metrics['data_quality_ratio'],
            f'schema.{date.isoformat()}.missing_columns': ','.join(validation_metrics['missing_columns']),
            f'schema.{date.isoformat()}.missing_count': len(validation_metrics['missing_columns'])
        })
        
        # Log data quality issues
        if validation_metrics['invalid_rows_count'] > 0:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                f'Data quality filtering for {date}: {validation_metrics["invalid_rows_count"]} invalid rows removed '
                f'out of {input_row_count} total rows. Quality ratio: {validation_metrics["data_quality_ratio"]:.2%}'
            )

        # Do the aggregation (consistent with other dataframes)
        batch_group = self.group(batch)
        return batch_group

    def group(self, dataframe):
        """Group collection status dataframe by unique index columns."""
        if dataframe is None or len(dataframe) == 0:
            return self.empty()
        
        # For collection status, we group by unique_index_columns and sum elapsed time
        group = dataframe.group_by(self.unique_index_columns(), maintain_order=True).agg([
            pd.col('elapsed').sum().alias('elapsed')  # Sum elapsed time for identical entries
        ])
        
        # Cast types to match the table
        result = self.cast_dataframe(group, self.cast_types())
        return result

    @staticmethod
    def unique_index_columns():
        return ['collection_start_timestamp', 'since', 'until', 'file_name', 'status']

    @staticmethod
    def data_columns():
        return ['elapsed']

    @staticmethod
    def cast_types():
        return {
            'elapsed': float,
        }

    @staticmethod
    def index_cast_types():
        """Return casting types for index columns (unique_index_columns)."""
        return {
            'collection_start_timestamp': 'datetime64[ns]',
            'since': 'datetime64[ns]',
            'until': 'datetime64[ns]',
            'file_name': str,
            'status': str,
        }

    @staticmethod  
    def operations():
        return {
            'elapsed': 'sum',  # Sum elapsed time when merging rollups
        }

    @staticmethod
    def raw_data_cast_types():
        """Return casting types for raw data columns (before grouping)."""
        return {
            'elapsed': float,
            # Index columns - ensure datetime columns are consistently cast
            'collection_start_timestamp': 'datetime64[ns]',
            'since': 'datetime64[ns]',
            'until': 'datetime64[ns]',
            'file_name': str,
            'status': str,
        }

    @staticmethod
    def operations():
        return {
            'elapsed': 'max',  # Use max for elapsed times
        }

    @traced_method('collection_status.build_group')
    def build_group(self, batch_data):
        """Build Collection Status dataframe group from a single tarball's CSV data.

        Args:
            batch_data: Data from a single tarball extraction
                       Format: {'data_collection_status': DataFrame, 'main_host': DataFrame, ...}
        """
        # Get data_collection_status data from this batch
        batch = batch_data.get('data_collection_status')
        if batch is None or len(batch) == 0:
            return self.empty()

        # Process the batch and return it (no aggregation)
        result = self._process_batch_data(batch, batch_data)
        return result.reset_index(drop=True) if result is not None else self.empty()
