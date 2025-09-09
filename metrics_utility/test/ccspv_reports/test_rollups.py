import json
import os
import shutil
import tempfile

from datetime import date

import pytest

from conftest import transform_sheet
from pandas import Timestamp

from metrics_utility.automation_controller_billing.rollups.manager import RollupManager
from metrics_utility.automation_controller_billing.rollups.reader import RollupReader
from metrics_utility.test.util import run_command_int


def transform_sheet_with_json_normalization(sheet_dict):
    """Transform sheet and normalize JSON fields for consistent comparison.

    This function:
    1. Transforms the sheet data using transform_sheet
    2. Sorts all dictionary keys alphabetically
    3. Parses JSON strings into actual dict/list structures
    4. Recursively sorts all nested structures
    """
    transformed = transform_sheet(sheet_dict)

    # Create new dict with sorted keys for each row
    sorted_transformed = {}
    for row_idx, row_data in transformed.items():
        # Sort the keys in each row
        sorted_row = {}
        for key in sorted(row_data.keys()):
            value = row_data[key]
            # Parse JSON fields into actual dict/list structures
            if isinstance(value, str) and value.strip().startswith(('[', '{')):
                try:
                    # Parse JSON string
                    parsed = json.loads(value)
                    # Sort the parsed structure
                    sorted_row[key] = sort_json_fields(parsed)
                except (json.JSONDecodeError, TypeError, ValueError):
                    # Keep original value if not valid JSON
                    sorted_row[key] = value
            elif isinstance(value, list):
                sorted_row[key] = sort_json_fields(value)
            else:
                sorted_row[key] = value
        sorted_transformed[row_idx] = sorted_row

    return sorted_transformed


def sort_json_fields(obj):
    """Recursively sort JSON fields for consistent testing."""
    if isinstance(obj, dict):
        sorted_dict = {}
        for key in sorted(obj.keys()):
            value = obj[key]
            if isinstance(value, list):
                # Sort list values
                sorted_values = []
                for v in value:
                    if v is not None:
                        sorted_values.append(v)
                sorted_values.sort(key=lambda x: str(x))
                sorted_dict[key] = sorted_values
            else:
                sorted_dict[key] = sort_json_fields(value)
        return sorted_dict
    elif isinstance(obj, list):
        # Sort list elements
        sorted_list = []
        for item in obj:
            if item is not None:
                sorted_list.append(sort_json_fields(item))
        sorted_list.sort(key=lambda x: str(x))
        return sorted_list
    else:
        return obj


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': (
        'ccsp_summary,managed_nodes,inventory_scope,usage_by_collections,usage_by_roles,usage_by_modules,data_collection_status'
    ),
}


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_rollups_comprehensive():
    """Test rollup data content across full date range for comprehensive coverage."""

    # Create temporary directory for rollups output
    temp_dir = tempfile.mkdtemp()
    test_env_vars = env_vars.copy()
    test_env_vars['METRICS_UTILITY_SHIP_PATH'] = temp_dir

    try:
        # Copy test data to temp directory
        os.makedirs(f'{temp_dir}/data', exist_ok=True)
        shutil.copytree('./metrics_utility/test/test_data/data', f'{temp_dir}/data', dirs_exist_ok=True)

        # Run compute_rollups command for extended date range to get all collections
        run_command_int(
            'compute_rollups',
            test_env_vars,
            {
                'since': '2025-07-08',
                'until': '2025-07-11',
                'force': True,
            },
        )

        # Load rollup data for all types
        rollup_reader = RollupReader(temp_dir)
        dataframes = rollup_reader.load_rollup_dataframes(
            date(2025, 7, 8), date(2025, 7, 11), ['job_host_summary', 'main_jobevent', 'main_host', 'data_collection_status']
        )

        # Validate each rollup type
        validate_jobhost_summary_rollup(dataframes['job_host_summary'])
        validate_content_usage_rollup(dataframes['main_jobevent'])
        validate_inventory_scope_rollup(dataframes['main_host'])
        validate_collection_status_rollup(dataframes['data_collection_status'])

    finally:
        shutil.rmtree(temp_dir)


def validate_jobhost_summary_rollup(dataframe):
    """Validate DataframeJobhostSummaryUsage rollup data content."""

    actual = transform_sheet_with_json_normalization(dataframe.to_dict())

    # Expected first few records to validate structure and content
    expected_jobhost_summary = {
        0: {
            'canonical_facts': {
                'ansible_kubernetes_node_id': {
                    'node-12345',
                },
                'ansible_port': {
                    22,
                },
            },
            'events': set(),
            'facts': {
                'platform': {
                    'kubernetes',
                },
            },
            'first_automation': Timestamp('2025-07-08 10:00:10'),
            'host_name': 'k8s-worker-01.internal',
            'host_names_before_dedup': {
                'k8s-worker-01.internal',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 10:00:00'),
            'job_remote_id': 20,
            'job_template_name': 'Kubernetes Template',
            'last_automation': Timestamp('2025-07-08 10:00:10'),
            'managed_node_type': 1,
            'managed_node_types_set': {
                'INDIRECT',
            },
            'organization_name': 'Default',
            'original_host_name': 'k8s-worker-01.internal',
            'task_runs': 1,
        },
        1: {
            'canonical_facts': {
                'ansible_port': {
                    22,
                },
                'ansible_vmware_bios_uuid': {
                    '420b1367-1e11-c9d7-4d0f-c3b3cba9ae16',
                },
                'ansible_vmware_instance_uuid': {
                    '500b3d2e-9abe-8ee1-98ea-bf67b591c104',
                },
                'ansible_vmware_moid': {
                    'vm-87212',
                },
            },
            'events': set(),
            'facts': {
                'device_type': {
                    'VM',
                },
            },
            'first_automation': Timestamp('2025-07-08 09:33:11.556896'),
            'host_name': 'vcenter-vm-01.internal',
            'host_names_before_dedup': {
                'vcenter-vm-01.internal',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 09:22:20.674373'),
            'job_remote_id': 17,
            'job_template_name': 'VMware Template',
            'last_automation': Timestamp('2025-07-08 09:33:11.556896'),
            'managed_node_type': 1,
            'managed_node_types_set': {
                'INDIRECT',
            },
            'organization_name': 'Default',
            'original_host_name': 'vcenter-vm-01.internal',
            'task_runs': 1,
        },
        2: {
            'canonical_facts': {
                'ansible_port': {
                    443,
                },
                'ansible_vmware_bios_uuid': {
                    '420ba1d2-3793-215c-30f0-5957a405d4e6',
                },
                'ansible_vmware_instance_uuid': {
                    '500b1a63-d55d-bf21-c104-1617888dd7d2',
                },
                'ansible_vmware_moid': {
                    'vm-87213',
                },
            },
            'events': set(),
            'facts': {
                'device_type': {
                    'VM',
                },
            },
            'first_automation': Timestamp('2025-07-08 09:44:27.146879'),
            'host_name': 'vcenter-vm-02.internal',
            'host_names_before_dedup': {
                'vcenter-vm-02.internal',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 09:42:03.435561'),
            'job_remote_id': 19,
            'job_template_name': 'VMware_Template2',
            'last_automation': Timestamp('2025-07-08 09:44:27.146879'),
            'managed_node_type': 1,
            'managed_node_types_set': {
                'INDIRECT',
            },
            'organization_name': 'Default',
            'original_host_name': 'vcenter-vm-02.internal',
            'task_runs': 1,
        },
        3: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 14:25:15'),
            'host_name': 'cache01.internal',
            'host_names_before_dedup': {
                'cache01.internal',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 14:25:15'),
            'job_remote_id': 225,
            'job_template_name': 'Dev Cache Management',
            'last_automation': Timestamp('2025-07-09 14:25:15'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Development',
            'original_host_name': 'cache01.internal',
            'task_runs': 10,
        },
        4: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 13:40:04'),
            'host_name': 'db02.dev',
            'host_names_before_dedup': {
                'db02.dev',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 13:40:04'),
            'job_remote_id': 230,
            'job_template_name': 'Dev Database Setup',
            'last_automation': Timestamp('2025-07-09 13:40:04'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Development',
            'original_host_name': 'db02.dev',
            'task_runs': 12,
        },
        5: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 17:05:00'),
            'host_name': 'app01.cluster',
            'host_names_before_dedup': {
                'app01.cluster',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 17:05:00'),
            'job_remote_id': 210,
            'job_template_name': 'Dev Multi-Node App',
            'last_automation': Timestamp('2025-07-10 17:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Development',
            'original_host_name': 'app01.cluster',
            'task_runs': 12,
        },
        6: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 15:05:00'),
            'host_name': 'k8s-node-01.internal',
            'host_names_before_dedup': {
                'k8s-node-01.internal',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 15:05:00'),
            'job_remote_id': 291,
            'job_template_name': 'K8S Deployment Dev',
            'last_automation': Timestamp('2025-07-08 15:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Development',
            'original_host_name': 'k8s-node-01.internal',
            'task_runs': 5,
        },
        7: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 19:00:00'),
            'host_name': 'web04.dev',
            'host_names_before_dedup': {
                'web04.dev',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 19:00:00'),
            'job_remote_id': 255,
            'job_template_name': 'Web04 Dev Deploy',
            'last_automation': Timestamp('2025-07-09 19:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Development',
            'original_host_name': 'web04.dev',
            'task_runs': 14,
        },
        8: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 13:00:00'),
            'host_name': 'api-server',
            'host_names_before_dedup': {
                'api-server',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 13:00:00'),
            'job_remote_id': 284,
            'job_template_name': 'API Server Deploy',
            'last_automation': Timestamp('2025-07-08 13:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'api-server',
            'task_runs': 6,
        },
        9: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 13:05:00'),
            'host_name': 'api-server.company.com',
            'host_names_before_dedup': {
                'api-server.company.com',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 13:05:00'),
            'job_remote_id': 285,
            'job_template_name': 'API Server Deploy',
            'last_automation': Timestamp('2025-07-08 13:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'api-server.company.com',
            'task_runs': 6,
        },
        10: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 13:10:00'),
            'host_name': 'api-server.company.com.east',
            'host_names_before_dedup': {
                'api-server.company.com.east',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 13:10:00'),
            'job_remote_id': 286,
            'job_template_name': 'API Server Deploy',
            'last_automation': Timestamp('2025-07-08 13:10:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'api-server.company.com.east',
            'task_runs': 6,
        },
        11: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 21:00:00'),
            'host_name': 'aws-vm-01.us-east',
            'host_names_before_dedup': {
                'aws-vm-01.us-east',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 21:00:00'),
            'job_remote_id': 223,
            'job_template_name': 'AWS Instance Configuration',
            'last_automation': Timestamp('2025-07-10 21:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'aws-vm-01.us-east',
            'task_runs': 12,
        },
        12: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 21:05:00'),
            'host_name': 'aws-vm-02.us-west',
            'host_names_before_dedup': {
                'aws-vm-02.us-west',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 21:05:00'),
            'job_remote_id': 224,
            'job_template_name': 'AWS Instance Configuration',
            'last_automation': Timestamp('2025-07-10 21:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'aws-vm-02.us-west',
            'task_runs': 12,
        },
        13: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 14:20:15'),
            'host_name': 'cache01.internal',
            'host_names_before_dedup': {
                'cache01.internal',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 14:20:15'),
            'job_remote_id': 200,
            'job_template_name': 'Cache Management',
            'last_automation': Timestamp('2025-07-09 14:20:15'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'cache01.internal',
            'task_runs': 21,
        },
        14: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 17:20:00'),
            'host_name': 'app01.cluster',
            'host_names_before_dedup': {
                'app01.cluster',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 17:20:00'),
            'job_remote_id': 220,
            'job_template_name': 'Cross-Org App Deploy',
            'last_automation': Timestamp('2025-07-10 17:20:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'app01.cluster',
            'task_runs': 14,
        },
        15: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 10:55:58'),
            'host_name': 'web01.internal',
            'host_names_before_dedup': {
                'web01.internal',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 10:55:58'),
            'job_remote_id': 240,
            'job_template_name': 'Cross-Org Web Deploy',
            'last_automation': Timestamp('2025-07-09 10:55:58'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web01.internal',
            'task_runs': 8,
        },
        16: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 13:36:04.823484'),
            'host_name': 'db01.company.com',
            'host_names_before_dedup': {
                'db01.company.com',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 13:36:04.823484'),
            'job_remote_id': 199,
            'job_template_name': 'Database Backup',
            'last_automation': Timestamp('2025-07-09 13:36:04.823484'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'db01.company.com',
            'task_runs': 12,
        },
        17: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 14:00:00'),
            'host_name': 'db-primary',
            'host_names_before_dedup': {
                'db-primary',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 14:00:00'),
            'job_remote_id': 287,
            'job_template_name': 'Database Primary Deploy',
            'last_automation': Timestamp('2025-07-08 14:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'db-primary',
            'task_runs': 7,
        },
        18: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 14:05:00'),
            'host_name': 'db-primary.company.com',
            'host_names_before_dedup': {
                'db-primary.company.com',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 14:05:00'),
            'job_remote_id': 288,
            'job_template_name': 'Database Primary Deploy',
            'last_automation': Timestamp('2025-07-08 14:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'db-primary.company.com',
            'task_runs': 7,
        },
        19: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 14:10:00'),
            'host_name': 'db-primary.company.com.west',
            'host_names_before_dedup': {
                'db-primary.company.com.west',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 14:10:00'),
            'job_remote_id': 289,
            'job_template_name': 'Database Primary Deploy',
            'last_automation': Timestamp('2025-07-08 14:10:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'db-primary.company.com.west',
            'task_runs': 7,
        },
        20: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 10:50:58.950423'),
            'host_name': 'web01.internal',
            'host_names_before_dedup': {
                'web01.internal',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 10:50:58.950423'),
            'job_remote_id': 198,
            'job_template_name': 'Deploy Web Application',
            'last_automation': Timestamp('2025-07-09 10:50:58.950423'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web01.internal',
            'task_runs': 18,
        },
        21: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 11:15:20.123456'),
            'host_name': 'web01.prod.company.com',
            'host_names_before_dedup': {
                'web01.prod.company.com',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 11:15:20.123456'),
            'job_remote_id': 198,
            'job_template_name': 'Deploy Web Application',
            'last_automation': Timestamp('2025-07-09 11:15:20.123456'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web01.prod.company.com',
            'task_runs': 9,
        },
        22: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 16:05:05'),
            'host_name': 'secure-host-01-readonly.internal',
            'host_names_before_dedup': {
                'secure-host-01-readonly.internal',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 16:05:00'),
            'job_remote_id': 502,
            'job_template_name': 'Health Check',
            'last_automation': Timestamp('2025-07-08 16:05:05'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'secure-host-01-readonly.internal',
            'task_runs': 5,
        },
        23: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 15:00:00'),
            'host_name': 'k8s-node-01.cluster',
            'host_names_before_dedup': {
                'k8s-node-01.cluster',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 15:00:00'),
            'job_remote_id': 290,
            'job_template_name': 'K8S Deployment',
            'last_automation': Timestamp('2025-07-08 15:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'k8s-node-01.cluster',
            'task_runs': 5,
        },
        24: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 14:10:30.123456'),
            'host_name': 'log01.company.com',
            'host_names_before_dedup': {
                'log01.company.com',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 14:10:30.123456'),
            'job_remote_id': 201,
            'job_template_name': 'Log Management',
            'last_automation': Timestamp('2025-07-09 14:10:30.123456'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'log01.company.com',
            'task_runs': 6,
        },
        25: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 17:00:00'),
            'host_name': 'app01.cluster',
            'host_names_before_dedup': {
                'app01.cluster',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 17:00:00'),
            'job_remote_id': 205,
            'job_template_name': 'Multi-Node App',
            'last_automation': Timestamp('2025-07-10 17:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'app01.cluster',
            'task_runs': 16,
        },
        26: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 17:30:00'),
            'host_name': 'app01.failover',
            'host_names_before_dedup': {
                'app01.failover',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 17:30:00'),
            'job_remote_id': 206,
            'job_template_name': 'Multi-Node App',
            'last_automation': Timestamp('2025-07-10 17:30:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'app01.failover',
            'task_runs': 8,
        },
        27: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 22:00:00'),
            'host_name': '203.0.113.10',
            'host_names_before_dedup': {
                '203.0.113.10',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 22:00:00'),
            'job_remote_id': 225,
            'job_template_name': 'Remote Site Management',
            'last_automation': Timestamp('2025-07-10 22:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'nat-host-01.external',
            'task_runs': 10,
        },
        28: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 22:05:00'),
            'host_name': '203.0.113.10',
            'host_names_before_dedup': {
                '203.0.113.10',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 22:05:00'),
            'job_remote_id': 226,
            'job_template_name': 'Remote Site Management',
            'last_automation': Timestamp('2025-07-10 22:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'nat-host-02.external',
            'task_runs': 10,
        },
        29: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 16:00:00'),
            'host_name': 'secure-host-01.company.com',
            'host_names_before_dedup': {
                'secure-host-01.company.com',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 16:00:00'),
            'job_remote_id': 292,
            'job_template_name': 'Secure Host Admin',
            'last_automation': Timestamp('2025-07-08 16:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'secure-host-01.company.com',
            'task_runs': 10,
        },
        30: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 16:05:00'),
            'host_name': 'secure-host-01.company.com',
            'host_names_before_dedup': {
                'secure-host-01.company.com',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 16:05:00'),
            'job_remote_id': 293,
            'job_template_name': 'Secure Host Readonly',
            'last_automation': Timestamp('2025-07-08 16:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'secure-host-01.company.com',
            'task_runs': 3,
        },
        31: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-08 16:00:05'),
            'host_name': 'secure-host-01.company.com',
            'host_names_before_dedup': {
                'secure-host-01.company.com',
            },
            'host_runs': 1,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-08 16:00:00'),
            'job_remote_id': 501,
            'job_template_name': 'System Update',
            'last_automation': Timestamp('2025-07-08 16:00:05'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'secure-host-01.company.com',
            'task_runs': 8,
        },
        32: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 16:30:00'),
            'host_name': 'web02.external',
            'host_names_before_dedup': {
                'web02.external',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 16:30:00'),
            'job_remote_id': 204,
            'job_template_name': 'Web Deployment',
            'last_automation': Timestamp('2025-07-09 16:30:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web02.external',
            'task_runs': 9,
        },
        33: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 16:00:00'),
            'host_name': 'web02.internal',
            'host_names_before_dedup': {
                'web02.internal',
            },
            'host_runs': 3,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 16:00:00'),
            'job_remote_id': 203,
            'job_template_name': 'Web Deployment',
            'last_automation': Timestamp('2025-07-09 16:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web02.internal',
            'task_runs': 15,
        },
        34: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 18:00:00'),
            'host_name': 'web03.internal',
            'host_names_before_dedup': {
                'web03.internal',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 18:00:00'),
            'job_remote_id': 245,
            'job_template_name': 'Web03 Deploy',
            'last_automation': Timestamp('2025-07-09 18:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web03.internal',
            'task_runs': 16,
        },
        35: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 18:05:00'),
            'host_name': 'web03.prod.internal',
            'host_names_before_dedup': {
                'web03.prod.internal',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 18:05:00'),
            'job_remote_id': 250,
            'job_template_name': 'Web03 Prod Deploy',
            'last_automation': Timestamp('2025-07-09 18:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'web03.prod.internal',
            'task_runs': 12,
        },
        36: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 20:00:00'),
            'host_name': 'win-srv01.company.com',
            'host_names_before_dedup': {
                'win-srv01.company.com',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 20:00:00'),
            'job_remote_id': 221,
            'job_template_name': 'Windows Patching',
            'last_automation': Timestamp('2025-07-10 20:00:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'win-srv01.company.com',
            'task_runs': 16,
        },
        37: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 20:05:00'),
            'host_name': 'win-srv02.company.com',
            'host_names_before_dedup': {
                'win-srv02.company.com',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 20:05:00'),
            'job_remote_id': 222,
            'job_template_name': 'Windows Patching',
            'last_automation': Timestamp('2025-07-10 20:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Production',
            'original_host_name': 'win-srv02.company.com',
            'task_runs': 16,
        },
        38: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 13:45:04'),
            'host_name': 'db02.staging',
            'host_names_before_dedup': {
                'db02.staging',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 13:45:04'),
            'job_remote_id': 235,
            'job_template_name': 'Staging Database Setup',
            'last_automation': Timestamp('2025-07-09 13:45:04'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Staging',
            'original_host_name': 'db02.staging',
            'task_runs': 10,
        },
        39: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-10 17:10:00'),
            'host_name': 'app01.cluster',
            'host_names_before_dedup': {
                'app01.cluster',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-10 17:10:00'),
            'job_remote_id': 215,
            'job_template_name': 'Staging Multi-Node App',
            'last_automation': Timestamp('2025-07-10 17:10:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Staging',
            'original_host_name': 'app01.cluster',
            'task_runs': 10,
        },
        40: {
            'canonical_facts': {},
            'events': set(),
            'facts': {},
            'first_automation': Timestamp('2025-07-09 19:05:00'),
            'host_name': 'web04.staging',
            'host_names_before_dedup': {
                'web04.staging',
            },
            'host_runs': 2,
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_created': Timestamp('2025-07-09 19:05:00'),
            'job_remote_id': 260,
            'job_template_name': 'Web04 Staging Deploy',
            'last_automation': Timestamp('2025-07-09 19:05:00'),
            'managed_node_type': 0,
            'managed_node_types_set': {
                'DIRECT',
            },
            'organization_name': 'Staging',
            'original_host_name': 'web04.staging',
            'task_runs': 12,
        },
    }

    # Validate all records match expected structure exactly
    assert actual == expected_jobhost_summary


def validate_content_usage_rollup(dataframe):
    """Validate DataframeContentUsage rollup data content."""

    actual = transform_sheet_with_json_normalization(dataframe.to_dict())

    # Expected content usage records based on actual rollup data
    expected_content_usage = {
        0: {
            'host_name': 'db01.company.com',
            'module_name': 'ansible.builtin.debug',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 199,
            'task_runs': 1,
            'duration': 0.6,
        },
        1: {
            'host_name': 'db01.company.com',
            'module_name': 'ansible.builtin.setup',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 199,
            'task_runs': 1,
            'duration': 2.5,
        },
        2: {
            'host_name': 'web01.internal',
            'module_name': 'ansible.builtin.debug',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 198,
            'task_runs': 1,
            'duration': 0.5,
        },
        3: {
            'host_name': 'web01.internal',
            'module_name': 'ansible.builtin.setup',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 198,
            'task_runs': 1,
            'duration': 1.5,
        },
        4: {
            'host_name': 'web01.prod.company.com',
            'module_name': 'ansible.builtin.debug',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 198,
            'task_runs': 1,
            'duration': 1.0,
        },
        5: {
            'host_name': 'web01.prod.company.com',
            'module_name': 'ansible.builtin.setup',
            'collection_name': 'ansible.builtin',
            'role_name': 'No role used',
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'job_remote_id': 198,
            'task_runs': 1,
            'duration': 2.0,
        },
    }

    # Validate all records match expected structure exactly
    assert actual == expected_content_usage


def validate_inventory_scope_rollup(dataframe):
    """Validate DataframeInventoryScope rollup data content."""

    actual = transform_sheet_with_json_normalization(dataframe.to_dict())

    # Expected first few inventory scope records based on actual rollup data
    expected_inventory_scope = {
        0: {
            'canonical_facts': {
                'ansible_host': {'203.0.113.10'},
                'ansible_machine_id': {'639d3a53a94028d35a3f5f244793dad2'},
                'ansible_port': {2201, 2202},
                'ansible_product_serial': {'CN7792194B0NAT'},
                'host_name': {'nat-host-01.external', 'nat-host-02.external'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'1.2.3'},
                'ansible_board_serial': {'NAT-GW-002', 'NAT-GW-001'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Desktop'},
                'ansible_processor': {'Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz'},
                'ansible_product_name': {'OptiPlex 7090'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'kvm'},
            },
            'host_name': '203.0.113.10',
            'host_names_before_dedup': ['203.0.113.10'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 22:05:00'),
            'organizations': ['Production'],
            'serials': ['CN7792194B0NAT/639d3a53a94028d35a3f5f244793dad2'],
        },
        1: {
            'canonical_facts': {
                'ansible_host': {'api-server'},
                'ansible_machine_id': {'a644029003e46b31d1a09ecec6c77b02'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE1845G8K1'},
                'host_name': {'api-server'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}},
            'host_name': 'api-server',
            'host_names_before_dedup': ['api-server'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 13:00:00'),
            'organizations': ['Production'],
            'serials': ['USE1845G8K1/a644029003e46b31d1a09ecec6c77b02'],
        },
        2: {
            'canonical_facts': {
                'ansible_host': {'api-server.company.com'},
                'ansible_machine_id': {'a644029003e46b31d1a09ecec6c77b02'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE1845G8K1'},
                'host_name': {'api-server.company.com'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}},
            'host_name': 'api-server.company.com',
            'host_names_before_dedup': ['api-server.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 13:05:00'),
            'organizations': ['Production'],
            'serials': ['USE1845G8K1/a644029003e46b31d1a09ecec6c77b02'],
        },
        3: {
            'canonical_facts': {
                'ansible_host': {'api-server.company.com.east'},
                'ansible_machine_id': {'a644029003e46b31d1a09ecec6c77b02'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE1845G8K1'},
                'host_name': {'api-server.company.com.east'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}},
            'host_name': 'api-server.company.com.east',
            'host_names_before_dedup': ['api-server.company.com.east'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 13:10:00'),
            'organizations': ['Production'],
            'serials': ['USE1845G8K1/a644029003e46b31d1a09ecec6c77b02'],
        },
        4: {
            'canonical_facts': {
                'ansible_host': {'app01.cluster'},
                'ansible_machine_id': {'e56eb592febecd4e03860514ce5a9f55'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE1234567'},
                'host_name': {'app01.cluster'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'HP'},
                'ansible_bios_version': {'U30'},
                'ansible_board_serial': {'USE1234567'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'ProLiant DL380 Gen10'},
                'ansible_system_vendor': {'HP'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'kvm'},
            },
            'host_name': 'app01.cluster',
            'host_names_before_dedup': ['app01.cluster'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Cross-Org Inventory', 'Development Inventory', 'Production Inventory', 'Staging Inventory'],
            'last_automation': Timestamp('2025-07-09 17:20:15'),
            'organizations': ['Development', 'Production', 'Staging'],
            'serials': ['USE1234567/e56eb592febecd4e03860514ce5a9f55'],
        },
        5: {
            'canonical_facts': {
                'ansible_host': {'app01.failover'},
                'ansible_machine_id': {'1a17f31cc8a19e2e1d3aa4901cb47939'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE1234567'},
                'host_name': {'app01.failover'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'HP'},
                'ansible_bios_version': {'U30'},
                'ansible_board_serial': {'USE7654321'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'ProLiant DL380 Gen10'},
                'ansible_system_vendor': {'HP'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'kvm'},
            },
            'host_name': 'app01.failover',
            'host_names_before_dedup': ['app01.failover'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 17:30:12'),
            'organizations': ['Production'],
            'serials': ['USE1234567/1a17f31cc8a19e2e1d3aa4901cb47939'],
        },
        6: {
            'canonical_facts': {
                'ansible_host': {'aws-vm-01.us-east'},
                'ansible_machine_id': {'81b0f5bd1078b9636e2a5a8f9a9e14df'},
                'ansible_port': {22},
                'ansible_product_serial': {'ec2-instance'},
                'host_name': {'aws-vm-01.us-east'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Amazon EC2'},
                'ansible_bios_version': {'1.0'},
                'ansible_board_serial': {'ec2-instance'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz'},
                'ansible_product_name': {'m5.large'},
                'ansible_system_vendor': {'Amazon EC2'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'xen'},
                'aws_instance_id': {'i-0a1b2c3d4e5f6g7h8'},
            },
            'host_name': 'aws-vm-01.us-east',
            'host_names_before_dedup': ['aws-vm-01.us-east'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 21:00:00'),
            'organizations': ['Production'],
            'serials': ['ec2-instance/81b0f5bd1078b9636e2a5a8f9a9e14df'],
        },
        7: {
            'canonical_facts': {
                'ansible_host': {'aws-vm-02.us-west'},
                'ansible_machine_id': {'81b0f5bd1078b9636e2a5a8f9a9e14df'},
                'ansible_port': {22},
                'ansible_product_serial': {'ec2-instance'},
                'host_name': {'aws-vm-02.us-west'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Amazon EC2'},
                'ansible_bios_version': {'1.0'},
                'ansible_board_serial': {'ec2-instance'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz'},
                'ansible_product_name': {'m5.large'},
                'ansible_system_vendor': {'Amazon EC2'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'xen'},
                'aws_instance_id': {'i-9z8y7x6w5v4u3t2s'},
            },
            'host_name': 'aws-vm-02.us-west',
            'host_names_before_dedup': ['aws-vm-02.us-west'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 21:05:00'),
            'organizations': ['Production'],
            'serials': ['ec2-instance/81b0f5bd1078b9636e2a5a8f9a9e14df'],
        },
        8: {
            'canonical_facts': {
                'ansible_host': {'cache01.internal'},
                'ansible_machine_id': {'0267fc0887de14e8c994d1025a445221'},
                'ansible_port': {6379},
                'host_name': {'cache01.internal'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_connection_variable': {'ssh'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_virtualization_type': {'docker'},
            },
            'host_name': 'cache01.internal',
            'host_names_before_dedup': ['cache01.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory', 'Production Inventory'],
            'last_automation': Timestamp('2025-07-09 14:25:30'),
            'organizations': ['Development', 'Production'],
            'serials': [],
        },
        9: {
            'canonical_facts': {
                'ansible_host': {'db-cluster-node1.internal'},
                'ansible_machine_id': {'986e14d2a7634f9bf27fa6e3e5158966'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7016194B0001'},
                'host_name': {'db-cluster-node1.internal'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'db_role': {'primary'}},
            'host_name': 'db-cluster-node1.internal',
            'host_names_before_dedup': ['db-cluster-node1.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 11:00:00'),
            'organizations': ['Production'],
            'serials': ['CN7016194B0001/986e14d2a7634f9bf27fa6e3e5158966'],
        },
        10: {
            'canonical_facts': {
                'ansible_host': {'db-cluster-node2.internal'},
                'ansible_machine_id': {'a3f70fd70db4b3daf1a0ffaec2c5d1f5'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7016194B0002'},
                'host_name': {'db-cluster-node2.internal'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'db_role': {'secondary'}},
            'host_name': 'db-cluster-node2.internal',
            'host_names_before_dedup': ['db-cluster-node2.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 11:05:00'),
            'organizations': ['Production'],
            'serials': ['CN7016194B0002/a3f70fd70db4b3daf1a0ffaec2c5d1f5'],
        },
        11: {
            'canonical_facts': {
                'ansible_host': {'db-primary'},
                'ansible_machine_id': {'bc2fa6de408414cef69227ebf4cf0f7e'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7016194B0DB1'},
                'host_name': {'db-primary'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'db_role': {'primary'}},
            'host_name': 'db-primary',
            'host_names_before_dedup': ['db-primary'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 14:00:00'),
            'organizations': ['Production'],
            'serials': ['CN7016194B0DB1/bc2fa6de408414cef69227ebf4cf0f7e'],
        },
        12: {
            'canonical_facts': {'ansible_host': {'db-primary.company.com'}, 'ansible_port': {22}, 'host_name': {'db-primary.company.com'}},
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'db_role': {'primary'}},
            'host_name': 'db-primary.company.com',
            'host_names_before_dedup': ['db-primary.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 14:05:00'),
            'organizations': ['Production'],
            'serials': [],
        },
        13: {
            'canonical_facts': {'ansible_host': {'db-primary.company.com.west'}, 'ansible_port': {22}, 'host_name': {'db-primary.company.com.west'}},
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'db_role': {'primary'}},
            'host_name': 'db-primary.company.com.west',
            'host_names_before_dedup': ['db-primary.company.com.west'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 14:10:00'),
            'organizations': ['Production'],
            'serials': [],
        },
        14: {
            'canonical_facts': {
                'ansible_host': {'db01.company.com'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7792194B0740'},
                'host_name': {'db01.company.com'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.13.0'},
                'ansible_board_serial': {'CN7792194B0A86'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R740'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'xen'},
            },
            'host_name': 'db01.company.com',
            'host_names_before_dedup': ['db01.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 13:36:08.627277'),
            'organizations': ['Production'],
            'serials': [],
        },
        15: {
            'canonical_facts': {
                'ansible_host': {'db02.company.com'},
                'ansible_machine_id': {'eddfa033379afb7784abb2e4c7dc2cf1'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7016194B0750'},
                'host_name': {'db02.dev'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.13.0'},
                'ansible_board_serial': {'CN7792194B0A87'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R750'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'xen'},
            },
            'host_name': 'db02.dev',
            'host_names_before_dedup': ['db02.dev'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory'],
            'last_automation': Timestamp('2025-07-09 13:40:08'),
            'organizations': ['Development'],
            'serials': ['CN7016194B0750/eddfa033379afb7784abb2e4c7dc2cf1'],
        },
        16: {
            'canonical_facts': {
                'ansible_host': {'db02.company.com'},
                'ansible_machine_id': {'eddfa033379afb7784abb2e4c7dc2cf1'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7016194B0750'},
                'host_name': {'db02.staging'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.13.0'},
                'ansible_board_serial': {'CN7792194B0A87'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R750'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'xen'},
            },
            'host_name': 'db02.staging',
            'host_names_before_dedup': ['db02.staging'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Staging Inventory'],
            'last_automation': Timestamp('2025-07-09 13:45:08'),
            'organizations': ['Staging'],
            'serials': ['CN7016194B0750/eddfa033379afb7784abb2e4c7dc2cf1'],
        },
        17: {
            'canonical_facts': {'ansible_host': {'k8s-node-01.cluster'}, 'ansible_port': {22}, 'host_name': {'k8s-node-01.cluster'}},
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'docker'}, 'container_runtime': {'containerd'}},
            'host_name': 'k8s-node-01.cluster',
            'host_names_before_dedup': ['k8s-node-01.cluster'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 15:00:00'),
            'organizations': ['Production'],
            'serials': [],
        },
        18: {
            'canonical_facts': {'ansible_host': {'k8s-node-01.internal'}, 'ansible_port': {22}, 'host_name': {'k8s-node-01.internal'}},
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'docker'}, 'container_runtime': {'containerd'}},
            'host_name': 'k8s-node-01.internal',
            'host_names_before_dedup': ['k8s-node-01.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory'],
            'last_automation': Timestamp('2025-07-08 15:05:00'),
            'organizations': ['Development'],
            'serials': [],
        },
        19: {
            'canonical_facts': {
                'ansible_host': {'legacy-server.company.com'},
                'ansible_machine_id': {'7d4afb3f5aaf1350bc54dd686568bc2d'},
                'ansible_port': {22},
                'ansible_product_serial': {'USE0123456'},
                'host_name': {'legacy-server.company.com'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'physical'}, 'server_type': {'legacy'}},
            'host_name': 'legacy-server.company.com',
            'host_names_before_dedup': ['legacy-server.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory', 'Production Inventory'],
            'last_automation': Timestamp('2025-07-08 12:05:00'),
            'organizations': ['Development', 'Production'],
            'serials': ['USE0123456/7d4afb3f5aaf1350bc54dd686568bc2d'],
        },
        20: {
            'canonical_facts': {'ansible_host': {'log01.company.com'}, 'ansible_port': {514}, 'host_name': {'log01.company.com'}},
            'facts': {'ansible_connection_variable': {'tcp'}, 'ansible_virtualization_type': {'lxc'}},
            'host_name': 'log01.company.com',
            'host_names_before_dedup': ['log01.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 14:10:35.987654'),
            'organizations': ['Production'],
            'serials': [],
        },
        21: {
            'canonical_facts': {
                'ansible_host': {'mobile-dev-laptop.office.company.com'},
                'ansible_machine_id': {'797690615d609504271f6d3467fb7c7d'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN0123456789'},
                'host_name': {'mobile-dev-laptop.office.company.com'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'physical'}, 'network_context': {'office'}},
            'host_name': 'mobile-dev-laptop.office.company.com',
            'host_names_before_dedup': ['mobile-dev-laptop.office.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory'],
            'last_automation': Timestamp('2025-07-08 09:00:00'),
            'organizations': ['Development'],
            'serials': ['CN0123456789/797690615d609504271f6d3467fb7c7d'],
        },
        22: {
            'canonical_facts': {
                'ansible_host': {'secure-host-01-readonly.internal'},
                'ansible_machine_id': {'f8e7d6c5b4a3928170605040302010'},
                'ansible_port': {22},
                'host_name': {'secure-host-01-readonly.internal'},
            },
            'facts': {'ansible_architecture': {'x86_64'}, 'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'physical'}},
            'host_name': 'secure-host-01-readonly.internal',
            'host_names_before_dedup': ['secure-host-01-readonly.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Restricted Inventory'],
            'last_automation': Timestamp('2025-07-08 16:05:00'),
            'organizations': ['Production'],
            'serials': [],
        },
        23: {
            'canonical_facts': {
                'ansible_host': {'secure-host-01.company.com'},
                'ansible_machine_id': {'f8e7d6c5b4a3928170605040302010'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7792194B0SEC'},
                'host_name': {'secure-host-01.company.com'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.5.4'},
                'ansible_board_serial': {'CN7792194B0001'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R840'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'physical'},
            },
            'host_name': 'secure-host-01.company.com',
            'host_names_before_dedup': ['secure-host-01.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 16:00:00'),
            'organizations': ['Production'],
            'serials': ['CN7792194B0SEC/f8e7d6c5b4a3928170605040302010'],
        },
        24: {
            'canonical_facts': {
                'ansible_host': {'web01.internal'},
                'ansible_machine_id': {'3a2f8c9b123456789012345678901234'},
                'ansible_port': {22},
                'ansible_product_serial': {'VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34'},
                'host_name': {'web01.internal'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web01.internal',
            'host_names_before_dedup': ['web01.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Cross-Org Inventory', 'Production Inventory'],
            'last_automation': Timestamp('2025-07-09 10:55:58'),
            'organizations': ['Production'],
            'serials': ['VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34/3a2f8c9b123456789012345678901234'],
        },
        25: {
            'canonical_facts': {
                'ansible_host': {'web01.prod.company.com'},
                'ansible_machine_id': {'3a2f8c9b123456789012345678901234'},
                'ansible_port': {2222},
                'ansible_product_serial': {'VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34'},
                'host_name': {'web01.prod.company.com'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web01.prod.company.com',
            'host_names_before_dedup': ['web01.prod.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 11:15:25.987654'),
            'organizations': ['Production'],
            'serials': ['VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34/3a2f8c9b123456789012345678901234'],
        },
        26: {
            'canonical_facts': {
                'ansible_host': {'web02.external'},
                'ansible_machine_id': {'f3e2da65c5d34e59151db7ec18b868d9'},
                'ansible_port': {443},
                'ansible_product_serial': {'VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef'},
                'host_name': {'web02.external'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web02.external',
            'host_names_before_dedup': ['web02.external'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 16:30:08'),
            'organizations': ['Production'],
            'serials': ['VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef/f3e2da65c5d34e59151db7ec18b868d9'],
        },
        27: {
            'canonical_facts': {
                'ansible_host': {'web02.internal'},
                'ansible_machine_id': {'f3e2da65c5d34e59151db7ec18b868d9'},
                'ansible_product_serial': {'VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef'},
                'host_name': {'web02.internal'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web02.internal',
            'host_names_before_dedup': ['web02.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 16:00:00'),
            'organizations': ['Production'],
            'serials': ['VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef/f3e2da65c5d34e59151db7ec18b868d9'],
        },
        28: {
            'canonical_facts': {
                'ansible_host': {'web03.company.com'},
                'ansible_machine_id': {'01b6b28643a6a867e339e957c8ed9d37'},
                'ansible_port': {22},
                'ansible_product_serial': {'VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90'},
                'host_name': {'web03.internal'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web03.internal',
            'host_names_before_dedup': ['web03.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 18:00:00'),
            'organizations': ['Production'],
            'serials': ['VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90/01b6b28643a6a867e339e957c8ed9d37'],
        },
        29: {
            'canonical_facts': {
                'ansible_host': {'web03.company.com'},
                'ansible_machine_id': {'01b6b28643a6a867e339e957c8ed9d37'},
                'ansible_port': {2223},
                'ansible_product_serial': {'VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90'},
                'host_name': {'web03.prod.internal'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web03.prod.internal',
            'host_names_before_dedup': ['web03.prod.internal'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-09 18:05:00'),
            'organizations': ['Production'],
            'serials': ['VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90/01b6b28643a6a867e339e957c8ed9d37'],
        },
        30: {
            'canonical_facts': {
                'ansible_host': {'web04.company.com'},
                'ansible_machine_id': {'ae920ed940e880003e264a357de969c1'},
                'ansible_port': {22},
                'ansible_product_serial': {'VMware-dev-01-02-03-04-05-06-07-08-09-10-11-12'},
                'host_name': {'web04.dev'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web04.dev',
            'host_names_before_dedup': ['web04.dev'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Development Inventory'],
            'last_automation': Timestamp('2025-07-09 19:00:00'),
            'organizations': ['Development'],
            'serials': ['VMware-dev-01-02-03-04-05-06-07-08-09-10-11-12/ae920ed940e880003e264a357de969c1'],
        },
        31: {
            'canonical_facts': {
                'ansible_host': {'web04.company.com'},
                'ansible_machine_id': {'d1134fec21d571a9b596f7dbf7dc5673'},
                'ansible_port': {22},
                'ansible_product_serial': {'VMware-stg-01-02-03-04-05-06-07-08-09-10-11-12'},
                'host_name': {'web04.staging'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Phoenix Technologies LTD'},
                'ansible_bios_version': {'6.00'},
                'ansible_board_serial': {'None'},
                'ansible_connection_variable': {'ssh'},
                'ansible_form_factor': {'Virtual'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'VMware Virtual Platform'},
                'ansible_system_vendor': {'VMware, Inc.'},
                'ansible_virtualization_role': {'guest'},
                'ansible_virtualization_type': {'VMware'},
            },
            'host_name': 'web04.staging',
            'host_names_before_dedup': ['web04.staging'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Staging Inventory'],
            'last_automation': Timestamp('2025-07-09 19:05:00'),
            'organizations': ['Staging'],
            'serials': ['VMware-stg-01-02-03-04-05-06-07-08-09-10-11-12/d1134fec21d571a9b596f7dbf7dc5673'],
        },
        32: {
            'canonical_facts': {
                'ansible_host': {'webserver.company.com'},
                'ansible_machine_id': {'1dcd7ec391a45938c8ab4ec198a24dc5', '78a5084255b084eebb58b41f5eb85c06'},
                'ansible_port': {22},
                'ansible_product_serial': {'CN7792194B0W01', 'CN7792194B0W02'},
                'host_name': {'webserver.company.com'},
            },
            'facts': {'ansible_connection_variable': {'ssh'}, 'ansible_virtualization_type': {'kvm'}, 'server_role': {'primary', 'backup'}},
            'host_name': 'webserver.company.com',
            'host_names_before_dedup': ['webserver.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 10:05:00'),
            'organizations': ['Production'],
            'serials': ['CN7792194B0W01/78a5084255b084eebb58b41f5eb85c06', 'CN7792194B0W02/1dcd7ec391a45938c8ab4ec198a24dc5'],
        },
        33: {
            'canonical_facts': {
                'ansible_host': {'win-srv01.company.com'},
                'ansible_port': {5985},
                'ansible_product_serial': {'USE9876543'},
                'host_name': {'win-srv01.company.com'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.13.0'},
                'ansible_board_serial': {'CN7792194B0A88'},
                'ansible_connection_variable': {'winrm'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R740'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'VirtualPC'},
            },
            'host_name': 'win-srv01.company.com',
            'host_names_before_dedup': ['win-srv01.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 20:00:00'),
            'organizations': ['Production'],
            'serials': [],
        },
        34: {
            'canonical_facts': {
                'ansible_host': {'win-srv02.company.com'},
                'ansible_port': {5985},
                'ansible_product_serial': {'USE9876543'},
                'host_name': {'win-srv02.company.com'},
            },
            'facts': {
                'ansible_architecture': {'x86_64'},
                'ansible_bios_vendor': {'Dell Inc.'},
                'ansible_bios_version': {'2.13.0'},
                'ansible_board_serial': {'CN7792194B0A89'},
                'ansible_connection_variable': {'winrm'},
                'ansible_form_factor': {'Rack Mount Chassis'},
                'ansible_processor': {'Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'},
                'ansible_product_name': {'PowerEdge R740'},
                'ansible_system_vendor': {'Dell Inc.'},
                'ansible_virtualization_role': {'host'},
                'ansible_virtualization_type': {'VirtualPC'},
            },
            'host_name': 'win-srv02.company.com',
            'host_names_before_dedup': ['win-srv02.company.com'],
            'install_uuid': '4566f69f-4c03-4369-a97c-d0865e8fe9d2',
            'inventories': ['Production Inventory'],
            'last_automation': Timestamp('2025-07-08 20:05:00'),
            'organizations': ['Production'],
            'serials': [],
        },
    }

    # Validate all records match expected structure exactly
    assert actual == expected_inventory_scope


def validate_collection_status_rollup(dataframe):
    """Validate DataframeCollectionStatus rollup data content."""

    actual = transform_sheet_with_json_normalization(dataframe.to_dict())

    # Expected first few collection status records based on actual rollup data
    expected_collection_status = {
        0: {
            'collection_start_timestamp': Timestamp('2025-07-08 00:00:00'),
            'elapsed': 0.0,
            'file_name': 'job_host_summary.csv',
            'since': Timestamp('2025-07-08 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-08 23:59:59'),
        },
        1: {
            'collection_start_timestamp': Timestamp('2025-07-08 00:00:00'),
            'elapsed': 0.0,
            'file_name': 'main_host.csv',
            'since': Timestamp('2025-07-08 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-08 23:59:59'),
        },
        2: {
            'collection_start_timestamp': Timestamp('2025-07-08 00:00:00'),
            'elapsed': 0.0,
            'file_name': 'main_indirectmanagednodeaudit.csv',
            'since': Timestamp('2025-07-08 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-08 23:59:59'),
        },
        3: {
            'collection_start_timestamp': Timestamp('2025-07-08 00:00:01'),
            'elapsed': 0.0,
            'file_name': 'main_host.csv',
            'since': Timestamp('2025-07-08 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-08 23:59:59'),
        },
        4: {
            'collection_start_timestamp': Timestamp('2025-07-08 00:00:02'),
            'elapsed': 0.0,
            'file_name': 'main_indirectmanagednodeaudit.csv',
            'since': Timestamp('2025-07-08 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-08 23:59:59'),
        },
        5: {
            'collection_start_timestamp': Timestamp('2025-07-09 00:00:00'),
            'elapsed': 0.0,
            'file_name': 'job_host_summary.csv',
            'since': Timestamp('2025-07-09 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-09 23:59:59'),
        },
        6: {
            'collection_start_timestamp': Timestamp('2025-07-09 00:00:01'),
            'elapsed': 0.0,
            'file_name': 'main_host.csv',
            'since': Timestamp('2025-07-09 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-09 23:59:59'),
        },
        7: {
            'collection_start_timestamp': Timestamp('2025-07-09 00:00:02'),
            'elapsed': 0.0,
            'file_name': 'main_indirectmanagednodeaudit.csv',
            'since': Timestamp('2025-07-09 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-09 23:59:59'),
        },
        8: {
            'collection_start_timestamp': Timestamp('2025-07-10 00:00:00'),
            'elapsed': 0.0,
            'file_name': 'job_host_summary.csv',
            'since': Timestamp('2025-07-10 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-10 23:59:59'),
        },
        9: {
            'collection_start_timestamp': Timestamp('2025-07-10 00:00:01'),
            'elapsed': 0.0,
            'file_name': 'main_host.csv',
            'since': Timestamp('2025-07-10 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-10 23:59:59'),
        },
        10: {
            'collection_start_timestamp': Timestamp('2025-07-10 00:00:02'),
            'elapsed': 0.0,
            'file_name': 'main_indirectmanagednodeaudit.csv',
            'since': Timestamp('2025-07-10 00:00:00'),
            'status': 'ok',
            'until': Timestamp('2025-07-10 23:59:59'),
        },
        11: {
            'collection_start_timestamp': Timestamp('2025-07-10 01:00:42'),
            'elapsed': 0.0,
            'file_name': 'job_host_summary.csv',
            'since': Timestamp('2025-07-10 01:00:42'),
            'status': 'ok',
            'until': Timestamp('2025-07-10 23:59:59'),
        },
    }

    # Validate all records match expected structure exactly
    assert actual == expected_collection_status


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_rollup_no_data_status():
    """Test rollup handling when there is no data for a date."""

    # Create temporary directory for rollups output
    temp_dir = tempfile.mkdtemp()
    test_env_vars = env_vars.copy()
    test_env_vars['METRICS_UTILITY_SHIP_PATH'] = temp_dir

    try:
        # Copy test data to temp directory
        os.makedirs(f'{temp_dir}/data', exist_ok=True)
        shutil.copytree('./metrics_utility/test/test_data/data', f'{temp_dir}/data', dirs_exist_ok=True)

        # Run compute_rollups command for a date with no data
        run_command_int(
            'compute_rollups',
            test_env_vars,
            {
                'since': '2025-07-01',
                'until': '2025-07-01',
                'force': True,
            },
        )

        # Validate that status rollups were created
        rollup_manager = RollupManager(temp_dir)

        # Check for status versions
        for dataframe_name in ['DataframeJobhostSummaryUsage', 'DataframeContentUsage', 'DataframeInventoryScope', 'DataframeCollectionStatus']:
            versions = rollup_manager.get_available_versions(date(2025, 7, 1), dataframe_name)
            status_versions = [v for v in versions if '__status__no_source_data' in v]
            assert len(status_versions) > 0, f'Should have __status__no_source_data version for {dataframe_name}'

    finally:
        shutil.rmtree(temp_dir)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_rollup_build_report_integration():
    """Test that build_report can use rollups to generate reports."""

    temp_dir = tempfile.mkdtemp()
    test_env_vars = env_vars.copy()
    test_env_vars['METRICS_UTILITY_SHIP_PATH'] = temp_dir

    try:
        # Copy test data to temp directory
        os.makedirs(f'{temp_dir}/data', exist_ok=True)
        shutil.copytree('./metrics_utility/test/test_data/data', f'{temp_dir}/data', dirs_exist_ok=True)

        # First, compute rollups
        run_command_int(
            'compute_rollups',
            test_env_vars,
            {
                'since': '2025-07-08',
                'until': '2025-07-11',
                'force': True,
            },
        )

        # Then, build report using rollups
        report_path = f'{temp_dir}/reports/2025/07/CCSPv2-2025-07-08--2025-07-11.xlsx'

        run_command_int(
            'build_report',
            test_env_vars,
            {
                'since': '2025-07-08',
                'until': '2025-07-11',
                'force': True,
            },
        )

        # Verify report was generated
        assert os.path.exists(report_path), 'Report should be generated from rollups'

        # Basic validation that report can be opened
        import openpyxl

        workbook = openpyxl.load_workbook(filename=report_path)
        assert 'Managed nodes' in workbook.sheetnames, 'Report should contain Managed nodes sheet'
        workbook.close()

    finally:
        shutil.rmtree(temp_dir)
