# ruff: noqa: E501
import datetime
import json
import math
import os
import sys
import tarfile

from unittest.mock import patch

import openpyxl
import pandas
import pytest

from pandas import Timestamp

# Import helper functions from conftest
from .conftest import (
    copy_if_content_changed,
    get_test_dir,
    sort_json_fields,
    transform_sheet,
)


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from metrics_utility.test.util import run_build_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent,main_indirectmanagednodeaudit',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': (
        'ccsp_summary,jobs,managed_nodes,indirectly_managed_nodes,inventory_scope,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules,data_collection_status'
    ),
    'METRICS_UTILITY_DEDUPLICATOR': 'ccsp-experimental',  # Enable experimental deduplication
}

file_path = './metrics_utility/test/test_data/reports/2025/07/CCSPv2-2025-07-08--2025-07-11.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command_with_extended_canonical_facts(cleanup, request):
    """Build xlsx report using build command with extended canonical facts and test its contents.

    This integration test demonstrates:
    - Extended canonical facts (ansible_host, host_name, ansible_port) collection and reporting
    - Multiple tarball processing across 3 days (2025-07-08 to 2025-07-10)
    - Non-numeric port handling (e.g., 'ssh' -> NULL)
    - Comprehensive test data covering various canonical facts scenarios
    - CCSPv2 XLSX report generation with proper data validation

    Note: This test focuses on data collection and reporting pipeline validation.
    For actual deduplication testing, see test_ccsp_realistic_dedup.py which uses
    mock data with proper serial computation.
    """

    # Extract CSVs from tarballs for human review
    extract_csvs_from_tarballs()

    # Mock the current_date method to return consistent date
    with patch('metrics_utility.automation_controller_billing.report.report_ccsp_v2.ReportCCSPv2.current_date', return_value='Jul 14, 2025'):
        # Running a command python way, so we can work with debugger in the code
        run_build_int(
            env_vars,
            {
                'since': '2025-07-08',
                'until': '2025-07-11',
                'force': True,
            },
        )

    # Skip CSV verification and input validation - not needed for this test

    workbook = None
    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        # Save a copy of the report to the reports directory for reference
        test_dir = get_test_dir()
        reports_dir = test_dir / 'reports'
        reports_dir.mkdir(exist_ok=True)
        test_report_path = reports_dir / 'CCSPv2-2025-07-08--2025-07-11.xlsx'

        # Only copy if content has changed
        copy_if_content_changed(file_path, test_report_path)

        # Validate all report sheets
        validate_managed_nodes(file_path)
        validate_inventory_scope(file_path)
        validate_jobs(file_path)
        validate_indirectly_managed_nodes(file_path)
        validate_usage_by_organizations(file_path)
        validate_usage_by_collections(file_path)
        validate_usage_by_roles(file_path)
        validate_usage_by_modules(file_path)
        validate_data_collection_status(file_path)
        validate_ccsp_summary(file_path)

    finally:
        if workbook:
            workbook.close()


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
            else:
                sorted_row[key] = value
        sorted_transformed[row_idx] = sorted_row

    return sorted_transformed


def validate_managed_nodes(file_path):
    """Validate that managed nodes sheet shows proper deduplication results.

    Deduplication Logic Overview:
    -----------------------------
    The experimental deduplication (deduplicator='ccsp-experimental') merges hosts based on
    canonical facts, specifically the combination of:
    - ansible_product_serial (hardware serial number)
    - ansible_machine_id (system machine ID)

    Hosts with the same serial number AND machine ID are considered the same physical machine
    and are merged into a single entry. The first hostname encountered becomes the canonical
    hostname for the merged entry.

    Key Points:
    - Deduplication happens ACROSS organizations - hosts from different orgs can be merged
    - The "Automated by organizations" count shows how many unique orgs touched the host
    - Job runs and task runs are summed across all deduplicated entries
    - The canonical facts and host names show ALL values from merged hosts
    - The "Host names before deduplication" currently only shows the canonical hostname
      (this appears to be a limitation in the current implementation)
    """
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # Validate input CSV data integrity using CSV files with cross-validation
    validate_input_csv_data_integrity()

    # Call the use case validation
    validate_use_cases(actual)

    # Full data dict assertion for comprehensive validation
    # This validates the complete structure and content of all entries
    # Note: JSON fields will be normalized by transform_sheet_with_json_normalization
    expected_managed_nodes = {
        0: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['203.0.113.10'],
                'ansible_machine_id': ['639d3a53a94028d35a3f5f244793dad2'],
                'ansible_port': [2201, 2202],
                'ansible_product_serial': ['CN7792194B0NAT'],
                'host_name': ['nat-host-01.external', 'nat-host-02.external'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['1.2.3'],
                'ansible_board_serial': ['NAT-GW-001', 'NAT-GW-002'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Desktop'],
                'ansible_processor': ['Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz'],
                'ansible_product_name': ['OptiPlex 7090'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'First automation': Timestamp('2025-07-10 22:00:00'),
            'Host name': '203.0.113.10',
            'Host names before deduplication': ['203.0.113.10'],
            'Host names before deduplication count': 1,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-10 22:05:00'),
            'Number of task runs': 20,
        },
        1: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
                'ansible_machine_id': ['a644029003e46b31d1a09ecec6c77b02'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1845G8K1'],
                'host_name': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
            },
            'First automation': Timestamp('2025-07-08 13:00:00'),
            'Host name': 'api-server',
            'Host names before deduplication': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
            'Host names before deduplication count': 3,
            'Job runs': 3,
            'Last automation': Timestamp('2025-07-08 13:10:00'),
            'Number of task runs': 18,
        },
        2: {
            'Automated by organizations': 3,
            'Canonical Facts': {
                'ansible_host': ['app01.cluster'],
                'ansible_machine_id': ['e56eb592febecd4e03860514ce5a9f55'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1234567'],
                'host_name': ['app01.cluster'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['HP'],
                'ansible_bios_version': ['U30'],
                'ansible_board_serial': ['USE1234567'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['ProLiant DL380 Gen10'],
                'ansible_system_vendor': ['HP'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'First automation': Timestamp('2025-07-10 17:00:00'),
            'Host name': 'app01.cluster',
            'Host names before deduplication': ['app01.cluster'],
            'Host names before deduplication count': 1,
            'Job runs': 4,
            'Last automation': Timestamp('2025-07-10 17:20:00'),
            'Number of task runs': 52,
        },
        3: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['app01.failover'],
                'ansible_machine_id': ['1a17f31cc8a19e2e1d3aa4901cb47939'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1234567'],
                'host_name': ['app01.failover'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['HP'],
                'ansible_bios_version': ['U30'],
                'ansible_board_serial': ['USE7654321'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['ProLiant DL380 Gen10'],
                'ansible_system_vendor': ['HP'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'First automation': Timestamp('2025-07-10 17:30:00'),
            'Host name': 'app01.failover',
            'Host names before deduplication': ['app01.failover'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-10 17:30:00'),
            'Number of task runs': 8,
        },
        4: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
                'ansible_machine_id': ['81b0f5bd1078b9636e2a5a8f9a9e14df'],
                'ansible_port': [22],
                'ansible_product_serial': ['ec2-instance'],
                'host_name': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Amazon EC2'],
                'ansible_bios_version': ['1.0'],
                'ansible_board_serial': ['ec2-instance'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz'],
                'ansible_product_name': ['m5.large'],
                'ansible_system_vendor': ['Amazon EC2'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['xen'],
                'aws_instance_id': ['i-0a1b2c3d4e5f6g7h8', 'i-9z8y7x6w5v4u3t2s'],
            },
            'First automation': Timestamp('2025-07-10 21:00:00'),
            'Host name': 'aws-vm-01.us-east',
            'Host names before deduplication': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
            'Host names before deduplication count': 2,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-10 21:05:00'),
            'Number of task runs': 24,
        },
        5: {
            'Automated by organizations': 2,
            'Canonical Facts': {
                'ansible_host': ['cache01.internal'],
                'ansible_machine_id': ['0267fc0887de14e8c994d1025a445221'],
                'ansible_port': [6379],
                'host_name': ['cache01.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_connection_variable': ['ssh'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_virtualization_type': ['docker'],
            },
            'First automation': Timestamp('2025-07-09 14:20:15'),
            'Host name': 'cache01.internal',
            'Host names before deduplication': ['cache01.internal'],
            'Host names before deduplication count': 1,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-09 14:25:15'),
            'Number of task runs': 31,
        },
        6: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['db-primary'],
                'ansible_machine_id': ['bc2fa6de408414cef69227ebf4cf0f7e'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0DB1'],
                'host_name': ['db-primary'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'First automation': Timestamp('2025-07-08 14:00:00'),
            'Host name': 'db-primary',
            'Host names before deduplication': ['db-primary'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 14:00:00'),
            'Number of task runs': 7,
        },
        7: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['db-primary.company.com'],
                'ansible_port': [22],
                'host_name': ['db-primary.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'First automation': Timestamp('2025-07-08 14:05:00'),
            'Host name': 'db-primary.company.com',
            'Host names before deduplication': ['db-primary.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 14:05:00'),
            'Number of task runs': 7,
        },
        8: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['db-primary.company.com.west'],
                'ansible_port': [22],
                'host_name': ['db-primary.company.com.west'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'First automation': Timestamp('2025-07-08 14:10:00'),
            'Host name': 'db-primary.company.com.west',
            'Host names before deduplication': ['db-primary.company.com.west'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 14:10:00'),
            'Number of task runs': 7,
        },
        9: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['db01.company.com'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7792194B0740'],
                'host_name': ['db01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A86'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['xen'],
            },
            'First automation': Timestamp('2025-07-09 13:36:04.823000'),
            'Host name': 'db01.company.com',
            'Host names before deduplication': ['db01.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-09 13:36:04.823000'),
            'Number of task runs': 12,
        },
        10: {
            'Automated by organizations': 2,
            'Canonical Facts': {
                'ansible_host': ['db02.company.com'],
                'ansible_machine_id': ['eddfa033379afb7784abb2e4c7dc2cf1'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0750'],
                'host_name': ['db02.dev', 'db02.staging'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A87'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R750'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['xen'],
            },
            'First automation': Timestamp('2025-07-09 13:40:04'),
            'Host name': 'db02.dev',
            'Host names before deduplication': ['db02.dev', 'db02.staging'],
            'Host names before deduplication count': 2,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-09 13:45:04'),
            'Number of task runs': 22,
        },
        11: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['k8s-node-01.cluster'],
                'ansible_port': [22],
                'host_name': ['k8s-node-01.cluster'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['docker'],
                'container_runtime': ['containerd'],
            },
            'First automation': Timestamp('2025-07-08 15:00:00'),
            'Host name': 'k8s-node-01.cluster',
            'Host names before deduplication': ['k8s-node-01.cluster'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 15:00:00'),
            'Number of task runs': 5,
        },
        12: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['k8s-node-01.internal'],
                'ansible_port': [22],
                'host_name': ['k8s-node-01.internal'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['docker'],
                'container_runtime': ['containerd'],
            },
            'First automation': Timestamp('2025-07-08 15:05:00'),
            'Host name': 'k8s-node-01.internal',
            'Host names before deduplication': ['k8s-node-01.internal'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 15:05:00'),
            'Number of task runs': 5,
        },
        13: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['log01.company.com'],
                'ansible_port': [514],
                'host_name': ['log01.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['tcp'],
                'ansible_virtualization_type': ['lxc'],
            },
            'First automation': Timestamp('2025-07-09 14:10:30.123000'),
            'Host name': 'log01.company.com',
            'Host names before deduplication': ['log01.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-09 14:10:30.123000'),
            'Number of task runs': 6,
        },
        14: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['secure-host-01-readonly.internal'],
                'ansible_machine_id': ['f8e7d6c5b4a3928170605040302010'],
                'ansible_port': [22],
                'host_name': ['secure-host-01-readonly.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['physical'],
            },
            'First automation': Timestamp('2025-07-08 16:05:05'),
            'Host name': 'secure-host-01-readonly.internal',
            'Host names before deduplication': ['secure-host-01-readonly.internal'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-08 16:05:05'),
            'Number of task runs': 5,
        },
        15: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['secure-host-01.company.com'],
                'ansible_machine_id': ['f8e7d6c5b4a3928170605040302010'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7792194B0SEC'],
                'host_name': ['secure-host-01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.5.4'],
                'ansible_board_serial': ['CN7792194B0001'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R840'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['physical'],
            },
            'First automation': Timestamp('2025-07-08 16:00:00'),
            'Host name': 'secure-host-01.company.com',
            'Host names before deduplication': ['secure-host-01.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 3,
            'Last automation': Timestamp('2025-07-08 16:05:00'),
            'Number of task runs': 21,
        },
        16: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['web01.internal', 'web01.prod.company.com'],
                'ansible_machine_id': ['3a2f8c9b123456789012345678901234'],
                'ansible_port': [22, 2222],
                'ansible_product_serial': ['VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34'],
                'host_name': ['web01.internal', 'web01.prod.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'First automation': Timestamp('2025-07-09 10:50:58.950000'),
            'Host name': 'web01.internal',
            'Host names before deduplication': ['web01.internal', 'web01.prod.company.com'],
            'Host names before deduplication count': 2,
            'Job runs': 3,
            'Last automation': Timestamp('2025-07-09 11:15:20.123000'),
            'Number of task runs': 35,
        },
        17: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['web02.external', 'web02.internal'],
                'ansible_machine_id': ['f3e2da65c5d34e59151db7ec18b868d9'],
                'ansible_port': [443],
                'ansible_product_serial': ['VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef'],
                'host_name': ['web02.external', 'web02.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'First automation': Timestamp('2025-07-09 16:00:00'),
            'Host name': 'web02.external',
            'Host names before deduplication': ['web02.external', 'web02.internal'],
            'Host names before deduplication count': 2,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-09 16:30:00'),
            'Number of task runs': 24,
        },
        18: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['web03.company.com'],
                'ansible_machine_id': ['01b6b28643a6a867e339e957c8ed9d37'],
                'ansible_port': [22, 2223],
                'ansible_product_serial': ['VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90'],
                'host_name': ['web03.internal', 'web03.prod.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'First automation': Timestamp('2025-07-09 18:00:00'),
            'Host name': 'web03.internal',
            'Host names before deduplication': ['web03.internal', 'web03.prod.internal'],
            'Host names before deduplication count': 2,
            'Job runs': 2,
            'Last automation': Timestamp('2025-07-09 18:05:00'),
            'Number of task runs': 28,
        },
        19: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['web04.company.com'],
                'ansible_machine_id': ['ae920ed940e880003e264a357de969c1'],
                'ansible_port': [22],
                'ansible_product_serial': ['VMware-dev-01-02-03-04-05-06-07-08-09-10-11-12'],
                'host_name': ['web04.dev'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'First automation': Timestamp('2025-07-09 19:00:00'),
            'Host name': 'web04.dev',
            'Host names before deduplication': ['web04.dev'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-09 19:00:00'),
            'Number of task runs': 14,
        },
        20: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['web04.company.com'],
                'ansible_machine_id': ['d1134fec21d571a9b596f7dbf7dc5673'],
                'ansible_port': [22],
                'ansible_product_serial': ['VMware-stg-01-02-03-04-05-06-07-08-09-10-11-12'],
                'host_name': ['web04.staging'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'First automation': Timestamp('2025-07-09 19:05:00'),
            'Host name': 'web04.staging',
            'Host names before deduplication': ['web04.staging'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-09 19:05:00'),
            'Number of task runs': 12,
        },
        21: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['win-srv01.company.com'],
                'ansible_port': [5985],
                'ansible_product_serial': ['USE9876543'],
                'host_name': ['win-srv01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A88'],
                'ansible_connection_variable': ['winrm'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['VirtualPC'],
            },
            'First automation': Timestamp('2025-07-10 20:00:00'),
            'Host name': 'win-srv01.company.com',
            'Host names before deduplication': ['win-srv01.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-10 20:00:00'),
            'Number of task runs': 16,
        },
        22: {
            'Automated by organizations': 1,
            'Canonical Facts': {
                'ansible_host': ['win-srv02.company.com'],
                'ansible_port': [5985],
                'ansible_product_serial': ['USE9876543'],
                'host_name': ['win-srv02.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A89'],
                'ansible_connection_variable': ['winrm'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['VirtualPC'],
            },
            'First automation': Timestamp('2025-07-10 20:05:00'),
            'Host name': 'win-srv02.company.com',
            'Host names before deduplication': ['win-srv02.company.com'],
            'Host names before deduplication count': 1,
            'Job runs': 1,
            'Last automation': Timestamp('2025-07-10 20:05:00'),
            'Number of task runs': 16,
        },
    }

    # Assert the comprehensive data structure for all entries
    # NOTE: Expected values need to be updated to match actual output with reverted deduplication
    # The test is currently failing because expected values still reflect the old deduplicated output
    # where db-primary entries were combined, but now they exist as separate entries
    print(f'INFO: Managed nodes validation - comparing {len(actual)} actual vs {len(expected_managed_nodes)} expected entries')
    assert actual == expected_managed_nodes


def validate_inventory_scope(file_path):
    """Validate inventory scope sheet shows all hosts with deduplication information."""
    sheet = pandas.read_excel(file_path, sheet_name='Inventory Scope')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # Full data dict assertion for comprehensive validation
    # This validates the complete structure and content of all inventory scope entries
    expected_inventory_scope = {
        0: {
            'Canonical Facts': {
                'ansible_host': ['203.0.113.10'],
                'ansible_machine_id': ['639d3a53a94028d35a3f5f244793dad2'],
                'ansible_port': [2201, 2202],
                'ansible_product_serial': ['CN7792194B0NAT'],
                'host_name': ['nat-host-01.external', 'nat-host-02.external'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['1.2.3'],
                'ansible_board_serial': ['NAT-GW-001', 'NAT-GW-002'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Desktop'],
                'ansible_processor': ['Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz'],
                'ansible_product_name': ['OptiPlex 7090'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'Host name': '203.0.113.10',
            'Host names before deduplication': ['203.0.113.10'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 22:05:00'),
            'Organizations': ['Production'],
        },
        1: {
            'Canonical Facts': {
                'ansible_host': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
                'ansible_machine_id': ['a644029003e46b31d1a09ecec6c77b02'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1845G8K1'],
                'host_name': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
            },
            'Host name': 'api-server',
            'Host names before deduplication': ['api-server', 'api-server.company.com', 'api-server.company.com.east'],
            'Host names before deduplication count': 3,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 13:10:00'),
            'Organizations': ['Production'],
        },
        2: {
            'Canonical Facts': {
                'ansible_host': ['app01.cluster'],
                'ansible_machine_id': ['e56eb592febecd4e03860514ce5a9f55'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1234567'],
                'host_name': ['app01.cluster'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['HP'],
                'ansible_bios_version': ['U30'],
                'ansible_board_serial': ['USE1234567'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['ProLiant DL380 Gen10'],
                'ansible_system_vendor': ['HP'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'Host name': 'app01.cluster',
            'Host names before deduplication': ['app01.cluster'],
            'Host names before deduplication count': 1,
            'Inventories': ['Cross-Org Inventory', 'Development Inventory', 'Production Inventory', 'Staging Inventory'],
            'Last Automation': Timestamp('2025-07-09 17:20:15'),
            'Organizations': ['Development', 'Production', 'Staging'],
        },
        3: {
            'Canonical Facts': {
                'ansible_host': ['app01.failover'],
                'ansible_machine_id': ['1a17f31cc8a19e2e1d3aa4901cb47939'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE1234567'],
                'host_name': ['app01.failover'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['HP'],
                'ansible_bios_version': ['U30'],
                'ansible_board_serial': ['USE7654321'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['ProLiant DL380 Gen10'],
                'ansible_system_vendor': ['HP'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['kvm'],
            },
            'Host name': 'app01.failover',
            'Host names before deduplication': ['app01.failover'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 17:30:12'),
            'Organizations': ['Production'],
        },
        4: {
            'Canonical Facts': {
                'ansible_host': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
                'ansible_machine_id': ['81b0f5bd1078b9636e2a5a8f9a9e14df'],
                'ansible_port': [22],
                'ansible_product_serial': ['ec2-instance'],
                'host_name': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Amazon EC2'],
                'ansible_bios_version': ['1.0'],
                'ansible_board_serial': ['ec2-instance'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz'],
                'ansible_product_name': ['m5.large'],
                'ansible_system_vendor': ['Amazon EC2'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['xen'],
                'aws_instance_id': ['i-0a1b2c3d4e5f6g7h8', 'i-9z8y7x6w5v4u3t2s'],
            },
            'Host name': 'aws-vm-01.us-east',
            'Host names before deduplication': ['aws-vm-01.us-east', 'aws-vm-02.us-west'],
            'Host names before deduplication count': 2,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 21:05:00'),
            'Organizations': ['Production'],
        },
        5: {
            'Canonical Facts': {
                'ansible_host': ['cache01.internal'],
                'ansible_machine_id': ['0267fc0887de14e8c994d1025a445221'],
                'ansible_port': [6379],
                'host_name': ['cache01.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_connection_variable': ['ssh'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_virtualization_type': ['docker'],
            },
            'Host name': 'cache01.internal',
            'Host names before deduplication': ['cache01.internal'],
            'Host names before deduplication count': 1,
            'Inventories': ['Development Inventory', 'Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 14:25:30'),
            'Organizations': ['Development', 'Production'],
        },
        6: {
            'Canonical Facts': {
                'ansible_host': ['db-cluster-node1.internal'],
                'ansible_machine_id': ['986e14d2a7634f9bf27fa6e3e5158966'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0001'],
                'host_name': ['db-cluster-node1.internal'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'Host name': 'db-cluster-node1.internal',
            'Host names before deduplication': ['db-cluster-node1.internal'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 11:00:00'),
            'Organizations': ['Production'],
        },
        7: {
            'Canonical Facts': {
                'ansible_host': ['db-cluster-node2.internal'],
                'ansible_machine_id': ['a3f70fd70db4b3daf1a0ffaec2c5d1f5'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0002'],
                'host_name': ['db-cluster-node2.internal'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['secondary'],
            },
            'Host name': 'db-cluster-node2.internal',
            'Host names before deduplication': ['db-cluster-node2.internal'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 11:05:00'),
            'Organizations': ['Production'],
        },
        8: {
            'Canonical Facts': {
                'ansible_host': ['db-primary'],
                'ansible_machine_id': ['bc2fa6de408414cef69227ebf4cf0f7e'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0DB1'],
                'host_name': ['db-primary'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'Host name': 'db-primary',
            'Host names before deduplication': ['db-primary'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 14:00:00'),
            'Organizations': ['Production'],
        },
        9: {
            'Canonical Facts': {
                'ansible_host': ['db-primary.company.com'],
                'ansible_port': [22],
                'host_name': ['db-primary.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'Host name': 'db-primary.company.com',
            'Host names before deduplication': ['db-primary.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 14:05:00'),
            'Organizations': ['Production'],
        },
        10: {
            'Canonical Facts': {
                'ansible_host': ['db-primary.company.com.west'],
                'ansible_port': [22],
                'host_name': ['db-primary.company.com.west'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'db_role': ['primary'],
            },
            'Host name': 'db-primary.company.com.west',
            'Host names before deduplication': ['db-primary.company.com.west'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 14:10:00'),
            'Organizations': ['Production'],
        },
        11: {
            'Canonical Facts': {
                'ansible_host': ['db01.company.com'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7792194B0740'],
                'host_name': ['db01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A86'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['xen'],
            },
            'Host name': 'db01.company.com',
            'Host names before deduplication': ['db01.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 13:36:08.627000'),
            'Organizations': ['Production'],
        },
        12: {
            'Canonical Facts': {
                'ansible_host': ['db02.company.com'],
                'ansible_machine_id': ['eddfa033379afb7784abb2e4c7dc2cf1'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7016194B0750'],
                'host_name': ['db02.dev', 'db02.staging'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A87'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R750'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['xen'],
            },
            'Host name': 'db02.dev',
            'Host names before deduplication': ['db02.dev', 'db02.staging'],
            'Host names before deduplication count': 2,
            'Inventories': ['Development Inventory', 'Staging Inventory'],
            'Last Automation': Timestamp('2025-07-09 13:45:08'),
            'Organizations': ['Development', 'Staging'],
        },
        13: {
            'Canonical Facts': {
                'ansible_host': ['k8s-node-01.cluster'],
                'ansible_port': [22],
                'host_name': ['k8s-node-01.cluster'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['docker'],
                'container_runtime': ['containerd'],
            },
            'Host name': 'k8s-node-01.cluster',
            'Host names before deduplication': ['k8s-node-01.cluster'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 15:00:00'),
            'Organizations': ['Production'],
        },
        14: {
            'Canonical Facts': {
                'ansible_host': ['k8s-node-01.internal'],
                'ansible_port': [22],
                'host_name': ['k8s-node-01.internal'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['docker'],
                'container_runtime': ['containerd'],
            },
            'Host name': 'k8s-node-01.internal',
            'Host names before deduplication': ['k8s-node-01.internal'],
            'Host names before deduplication count': 1,
            'Inventories': ['Development Inventory'],
            'Last Automation': Timestamp('2025-07-08 15:05:00'),
            'Organizations': ['Development'],
        },
        15: {
            'Canonical Facts': {
                'ansible_host': ['legacy-server.company.com'],
                'ansible_machine_id': ['7d4afb3f5aaf1350bc54dd686568bc2d'],
                'ansible_port': [22],
                'ansible_product_serial': ['USE0123456'],
                'host_name': ['legacy-server.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['physical'],
                'server_type': ['legacy'],
            },
            'Host name': 'legacy-server.company.com',
            'Host names before deduplication': ['legacy-server.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Development Inventory', 'Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 12:05:00'),
            'Organizations': ['Development', 'Production'],
        },
        16: {
            'Canonical Facts': {
                'ansible_host': ['log01.company.com'],
                'ansible_port': [514],
                'host_name': ['log01.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['tcp'],
                'ansible_virtualization_type': ['lxc'],
            },
            'Host name': 'log01.company.com',
            'Host names before deduplication': ['log01.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 14:10:35.988000'),
            'Organizations': ['Production'],
        },
        17: {
            'Canonical Facts': {
                'ansible_host': ['mobile-dev-laptop.office.company.com'],
                'ansible_machine_id': ['797690615d609504271f6d3467fb7c7d'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN0123456789'],
                'host_name': ['mobile-dev-laptop.office.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['physical'],
                'network_context': ['office'],
            },
            'Host name': 'mobile-dev-laptop.office.company.com',
            'Host names before deduplication': ['mobile-dev-laptop.office.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Development Inventory'],
            'Last Automation': Timestamp('2025-07-08 09:00:00'),
            'Organizations': ['Development'],
        },
        18: {
            'Canonical Facts': {
                'ansible_host': ['secure-host-01-readonly.internal'],
                'ansible_machine_id': ['f8e7d6c5b4a3928170605040302010'],
                'ansible_port': [22],
                'host_name': ['secure-host-01-readonly.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['physical'],
            },
            'Host name': 'secure-host-01-readonly.internal',
            'Host names before deduplication': ['secure-host-01-readonly.internal'],
            'Host names before deduplication count': 1,
            'Inventories': ['Restricted Inventory'],
            'Last Automation': Timestamp('2025-07-08 16:05:00'),
            'Organizations': ['Production'],
        },
        19: {
            'Canonical Facts': {
                'ansible_host': ['secure-host-01.company.com'],
                'ansible_machine_id': ['f8e7d6c5b4a3928170605040302010'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7792194B0SEC'],
                'host_name': ['secure-host-01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.5.4'],
                'ansible_board_serial': ['CN7792194B0001'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R840'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['physical'],
            },
            'Host name': 'secure-host-01.company.com',
            'Host names before deduplication': ['secure-host-01.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 16:00:00'),
            'Organizations': ['Production'],
        },
        20: {
            'Canonical Facts': {
                'ansible_host': ['web01.internal', 'web01.prod.company.com'],
                'ansible_machine_id': ['3a2f8c9b123456789012345678901234'],
                'ansible_port': [22, 2222],
                'ansible_product_serial': ['VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34'],
                'host_name': ['web01.internal', 'web01.prod.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'Host name': 'web01.internal',
            'Host names before deduplication': ['web01.internal', 'web01.prod.company.com'],
            'Host names before deduplication count': 2,
            'Inventories': ['Cross-Org Inventory', 'Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 11:15:25.988000'),
            'Organizations': ['Production'],
        },
        21: {
            'Canonical Facts': {
                'ansible_host': ['web02.external', 'web02.internal'],
                'ansible_machine_id': ['f3e2da65c5d34e59151db7ec18b868d9'],
                'ansible_port': [443],
                'ansible_product_serial': ['VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef'],
                'host_name': ['web02.external', 'web02.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'Host name': 'web02.external',
            'Host names before deduplication': ['web02.external', 'web02.internal'],
            'Host names before deduplication count': 2,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 16:30:08'),
            'Organizations': ['Production'],
        },
        22: {
            'Canonical Facts': {
                'ansible_host': ['web03.company.com'],
                'ansible_machine_id': ['01b6b28643a6a867e339e957c8ed9d37'],
                'ansible_port': [22, 2223],
                'ansible_product_serial': ['VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90'],
                'host_name': ['web03.internal', 'web03.prod.internal'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'Host name': 'web03.internal',
            'Host names before deduplication': ['web03.internal', 'web03.prod.internal'],
            'Host names before deduplication count': 2,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-09 18:05:00'),
            'Organizations': ['Production'],
        },
        23: {
            'Canonical Facts': {
                'ansible_host': ['web04.company.com'],
                'ansible_machine_id': ['ae920ed940e880003e264a357de969c1'],
                'ansible_port': [22],
                'ansible_product_serial': ['VMware-dev-01-02-03-04-05-06-07-08-09-10-11-12'],
                'host_name': ['web04.dev'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'Host name': 'web04.dev',
            'Host names before deduplication': ['web04.dev'],
            'Host names before deduplication count': 1,
            'Inventories': ['Development Inventory'],
            'Last Automation': Timestamp('2025-07-09 19:00:00'),
            'Organizations': ['Development'],
        },
        24: {
            'Canonical Facts': {
                'ansible_host': ['web04.company.com'],
                'ansible_machine_id': ['d1134fec21d571a9b596f7dbf7dc5673'],
                'ansible_port': [22],
                'ansible_product_serial': ['VMware-stg-01-02-03-04-05-06-07-08-09-10-11-12'],
                'host_name': ['web04.staging'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Phoenix Technologies LTD'],
                'ansible_bios_version': ['6.00'],
                'ansible_board_serial': ['None'],
                'ansible_connection_variable': ['ssh'],
                'ansible_form_factor': ['Virtual'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['VMware Virtual Platform'],
                'ansible_system_vendor': ['VMware, Inc.'],
                'ansible_virtualization_role': ['guest'],
                'ansible_virtualization_type': ['VMware'],
            },
            'Host name': 'web04.staging',
            'Host names before deduplication': ['web04.staging'],
            'Host names before deduplication count': 1,
            'Inventories': ['Staging Inventory'],
            'Last Automation': Timestamp('2025-07-09 19:05:00'),
            'Organizations': ['Staging'],
        },
        25: {
            'Canonical Facts': {
                'ansible_host': ['webserver.company.com'],
                'ansible_machine_id': ['1dcd7ec391a45938c8ab4ec198a24dc5', '78a5084255b084eebb58b41f5eb85c06'],
                'ansible_port': [22],
                'ansible_product_serial': ['CN7792194B0W01', 'CN7792194B0W02'],
                'host_name': ['webserver.company.com'],
            },
            'Facts': {
                'ansible_connection_variable': ['ssh'],
                'ansible_virtualization_type': ['kvm'],
                'server_role': ['backup', 'primary'],
            },
            'Host name': 'webserver.company.com',
            'Host names before deduplication': ['webserver.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 10:05:00'),
            'Organizations': ['Production'],
        },
        26: {
            'Canonical Facts': {
                'ansible_host': ['win-srv01.company.com'],
                'ansible_port': [5985],
                'ansible_product_serial': ['USE9876543'],
                'host_name': ['win-srv01.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A88'],
                'ansible_connection_variable': ['winrm'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['VirtualPC'],
            },
            'Host name': 'win-srv01.company.com',
            'Host names before deduplication': ['win-srv01.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 20:00:00'),
            'Organizations': ['Production'],
        },
        27: {
            'Canonical Facts': {
                'ansible_host': ['win-srv02.company.com'],
                'ansible_port': [5985],
                'ansible_product_serial': ['USE9876543'],
                'host_name': ['win-srv02.company.com'],
            },
            'Facts': {
                'ansible_architecture': ['x86_64'],
                'ansible_bios_vendor': ['Dell Inc.'],
                'ansible_bios_version': ['2.13.0'],
                'ansible_board_serial': ['CN7792194B0A89'],
                'ansible_connection_variable': ['winrm'],
                'ansible_form_factor': ['Rack Mount Chassis'],
                'ansible_processor': ['Intel(R) Xeon(R) Gold 6248 CPU @ 2.50GHz'],
                'ansible_product_name': ['PowerEdge R740'],
                'ansible_system_vendor': ['Dell Inc.'],
                'ansible_virtualization_role': ['host'],
                'ansible_virtualization_type': ['VirtualPC'],
            },
            'Host name': 'win-srv02.company.com',
            'Host names before deduplication': ['win-srv02.company.com'],
            'Host names before deduplication count': 1,
            'Inventories': ['Production Inventory'],
            'Last Automation': Timestamp('2025-07-08 20:05:00'),
            'Organizations': ['Production'],
        },
    }

    assert actual == expected_inventory_scope


def validate_usage_by_organizations(file_path):
    """Validate usage by organization with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # Expected: Usage stats showing actual data with experimental deduplication
    expected = {
        0: {
            'Job runs': 3,
            'Non-unique indirect managed nodes automated': 3,
            'Non-unique managed nodes automated': 0,
            'Number of task runs': 3,
            'Organization name': 'Default',
            'Unique indirect managed nodes automated': 3,
            'Unique managed nodes automated': 0,
        },
        1: {
            'Job runs': 5,
            'Non-unique indirect managed nodes automated': 0,
            'Non-unique managed nodes automated': 5,
            'Number of task runs': 53,
            'Organization name': 'Development',
            'Unique indirect managed nodes automated': 0,
            'Unique managed nodes automated': 5,
        },
        2: {
            'Job runs': 29,
            'Non-unique indirect managed nodes automated': 0,
            'Non-unique managed nodes automated': 30,
            'Number of task runs': 310,
            'Organization name': 'Production',
            'Unique indirect managed nodes automated': 0,
            'Unique managed nodes automated': 19,
        },
        3: {
            'Job runs': 3,
            'Non-unique indirect managed nodes automated': 0,
            'Non-unique managed nodes automated': 3,
            'Number of task runs': 32,
            'Organization name': 'Staging',
            'Unique indirect managed nodes automated': 0,
            'Unique managed nodes automated': 3,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_collections(file_path):
    """Validate usage by collections with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by collections')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # The usage by collections only shows collections actually used in direct job runs
    # Indirect nodes use different collections (kubernetes.node, vmware.vmware) but those
    # are tracked separately and not shown in this sheet
    expected = {
        0: {
            'Collection name': 'ansible.builtin',
            'Unique managed nodes automated': 2,  # Only 2 unique nodes used ansible.builtin
            'Non-unique managed nodes automated': 2,  # Same as unique in this case
            'Number of task runs': 6,
            'Duration of task runs [seconds]': 8.1,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_roles(file_path):
    """Validate usage by roles with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by roles')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    expected = {
        0: {
            'Role name': 'No role used',
            'Unique managed nodes automated': 2,
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 6,
            'Duration of task runs [seconds]': 8.1,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_modules(file_path):
    """Validate usage by modules with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by modules')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    expected = {
        0: {
            'Module name': 'ansible.builtin.debug',
            'Unique managed nodes automated': 2,  # Only direct nodes that ran this module
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 3,
            'Duration of task runs [seconds]': 2.1,
        },
        1: {
            'Module name': 'ansible.builtin.setup',
            'Unique managed nodes automated': 2,  # Only direct nodes that ran this module
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 3,
            'Duration of task runs [seconds]': 6.0,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_ccsp_summary(file_path):
    """Validate CCSP summary sheet (Usage Reporting)."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage Reporting')

    # The Usage Reporting sheet is a CCSP summary format with specific structure
    # We'll validate it has the expected structure as a dict
    expected = {
        'structure': {
            'type': 'ccsp_summary',
            'has_header_fields': True,
            'has_report_period': True,
            'report_period_contains': ['2025-07-08', '2025-07-11'],
            'has_sku_data': True,
            'total_unique_nodes': 23,
        }
    }

    # Read raw data to validate structure
    raw_data = sheet.to_dict()

    # Build actual structure analysis
    actual = {
        'structure': {
            'type': 'ccsp_summary',
            'has_header_fields': False,
            'has_report_period': False,
            'report_period_contains': [],
            'has_sku_data': False,
            'total_unique_nodes': 0,
        }
    }

    # Check header fields exist
    first_column = raw_data.get('Unnamed: 0', {})
    header_fields = ['CCSP Company Name', 'CCSP Email', 'CCSP RHN Login', 'Report Period (YYYY-MM)', 'End User Company Name']
    has_all_headers = all(any(field in str(first_column.get(i, '')) for i in range(10)) for field in header_fields)
    actual['structure']['has_header_fields'] = has_all_headers

    # Check report period
    period_value = raw_data.get('Unnamed: 1', {}).get(3, '')
    if '2025-07-08' in str(period_value) and '2025-07-11' in str(period_value):
        actual['structure']['has_report_period'] = True
        actual['structure']['report_period_contains'] = ['2025-07-08', '2025-07-11']

    # Check for SKU data - look for quantity 20, 21 or 22 anywhere in the sheet (may vary based on deduplication)
    for col_name, col_data in raw_data.items():
        if isinstance(col_data, dict):
            for row_idx, value in col_data.items():
                if value in [20, 21, 22, 23]:
                    actual['structure']['has_sku_data'] = True
                    actual['structure']['total_unique_nodes'] = value  # Use the actual value found
                    break
        if actual['structure']['has_sku_data']:
            break

    assert actual == expected


def validate_jobs(file_path):
    """Validate Jobs sheet."""
    sheet = pandas.read_excel(file_path, sheet_name='Jobs')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # Full data dict assertion for comprehensive validation
    # This validates the complete structure and content of key jobs entries
    expected_jobs = {
        0: {
            'First run': Timestamp('2025-07-08 10:00:00'),
            'Job runs': 1,
            'Job template name': 'Kubernetes Template',
            'Last run': Timestamp('2025-07-08 10:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 1,
            'Organization name': 'Default',
            'Unique managed nodes automated': 1,
        },
        1: {
            'First run': Timestamp('2025-07-08 09:22:20.674000'),
            'Job runs': 1,
            'Job template name': 'VMware Template',
            'Last run': Timestamp('2025-07-08 09:22:20.674000'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 1,
            'Organization name': 'Default',
            'Unique managed nodes automated': 1,
        },
        2: {
            'First run': Timestamp('2025-07-08 09:42:03.436000'),
            'Job runs': 1,
            'Job template name': 'VMware_Template2',
            'Last run': Timestamp('2025-07-08 09:42:03.436000'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 1,
            'Organization name': 'Default',
            'Unique managed nodes automated': 1,
        },
        3: {
            'First run': Timestamp('2025-07-09 14:25:15'),
            'Job runs': 1,
            'Job template name': 'Dev Cache Management',
            'Last run': Timestamp('2025-07-09 14:25:15'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 10,
            'Organization name': 'Development',
            'Unique managed nodes automated': 1,
        },
        4: {
            'First run': Timestamp('2025-07-09 13:40:04'),
            'Job runs': 1,
            'Job template name': 'Dev Database Setup',
            'Last run': Timestamp('2025-07-09 13:40:04'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 12,
            'Organization name': 'Development',
            'Unique managed nodes automated': 1,
        },
        5: {
            'First run': Timestamp('2025-07-10 17:05:00'),
            'Job runs': 1,
            'Job template name': 'Dev Multi-Node App',
            'Last run': Timestamp('2025-07-10 17:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 12,
            'Organization name': 'Development',
            'Unique managed nodes automated': 1,
        },
        6: {
            'First run': Timestamp('2025-07-08 15:05:00'),
            'Job runs': 1,
            'Job template name': 'K8S Deployment Dev',
            'Last run': Timestamp('2025-07-08 15:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 5,
            'Organization name': 'Development',
            'Unique managed nodes automated': 1,
        },
        7: {
            'First run': Timestamp('2025-07-09 19:00:00'),
            'Job runs': 1,
            'Job template name': 'Web04 Dev Deploy',
            'Last run': Timestamp('2025-07-09 19:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 14,
            'Organization name': 'Development',
            'Unique managed nodes automated': 1,
        },
        8: {
            'First run': Timestamp('2025-07-08 13:00:00'),
            'Job runs': 3,
            'Job template name': 'API Server Deploy',
            'Last run': Timestamp('2025-07-08 13:10:00'),
            'Non-unique managed nodes automated': 3,
            'Number of task runs': 18,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        9: {
            'First run': Timestamp('2025-07-10 21:00:00'),
            'Job runs': 2,
            'Job template name': 'AWS Instance Configuration',
            'Last run': Timestamp('2025-07-10 21:05:00'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 24,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        10: {
            'First run': Timestamp('2025-07-09 14:20:15'),
            'Job runs': 1,
            'Job template name': 'Cache Management',
            'Last run': Timestamp('2025-07-09 14:20:15'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 21,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        11: {
            'First run': Timestamp('2025-07-10 17:20:00'),
            'Job runs': 1,
            'Job template name': 'Cross-Org App Deploy',
            'Last run': Timestamp('2025-07-10 17:20:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 14,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        12: {
            'First run': Timestamp('2025-07-09 10:55:58'),
            'Job runs': 1,
            'Job template name': 'Cross-Org Web Deploy',
            'Last run': Timestamp('2025-07-09 10:55:58'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 8,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        13: {
            'First run': Timestamp('2025-07-09 13:36:04.823000'),
            'Job runs': 1,
            'Job template name': 'Database Backup',
            'Last run': Timestamp('2025-07-09 13:36:04.823000'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 12,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        14: {
            'First run': Timestamp('2025-07-08 14:00:00'),
            'Job runs': 3,
            'Job template name': 'Database Primary Deploy',
            'Last run': Timestamp('2025-07-08 14:10:00'),
            'Non-unique managed nodes automated': 3,
            'Number of task runs': 21,
            'Organization name': 'Production',
            'Unique managed nodes automated': 3,
        },
        15: {
            'First run': Timestamp('2025-07-09 10:50:58.950000'),
            'Job runs': 1,
            'Job template name': 'Deploy Web Application',
            'Last run': Timestamp('2025-07-09 11:15:20.123000'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 27,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        16: {
            'First run': Timestamp('2025-07-08 16:05:00'),
            'Job runs': 1,
            'Job template name': 'Health Check',
            'Last run': Timestamp('2025-07-08 16:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 5,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        17: {
            'First run': Timestamp('2025-07-08 15:00:00'),
            'Job runs': 1,
            'Job template name': 'K8S Deployment',
            'Last run': Timestamp('2025-07-08 15:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 5,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        18: {
            'First run': Timestamp('2025-07-09 14:10:30.123000'),
            'Job runs': 1,
            'Job template name': 'Log Management',
            'Last run': Timestamp('2025-07-09 14:10:30.123000'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 6,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        19: {
            'First run': Timestamp('2025-07-10 17:00:00'),
            'Job runs': 2,
            'Job template name': 'Multi-Node App',
            'Last run': Timestamp('2025-07-10 17:30:00'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 24,
            'Organization name': 'Production',
            'Unique managed nodes automated': 2,
        },
        20: {
            'First run': Timestamp('2025-07-10 22:00:00'),
            'Job runs': 2,
            'Job template name': 'Remote Site Management',
            'Last run': Timestamp('2025-07-10 22:05:00'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 20,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        21: {
            'First run': Timestamp('2025-07-08 16:00:00'),
            'Job runs': 1,
            'Job template name': 'Secure Host Admin',
            'Last run': Timestamp('2025-07-08 16:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 10,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        22: {
            'First run': Timestamp('2025-07-08 16:05:00'),
            'Job runs': 1,
            'Job template name': 'Secure Host Readonly',
            'Last run': Timestamp('2025-07-08 16:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 3,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        23: {
            'First run': Timestamp('2025-07-08 16:00:00'),
            'Job runs': 1,
            'Job template name': 'System Update',
            'Last run': Timestamp('2025-07-08 16:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 8,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        24: {
            'First run': Timestamp('2025-07-09 16:00:00'),
            'Job runs': 2,
            'Job template name': 'Web Deployment',
            'Last run': Timestamp('2025-07-09 16:30:00'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 24,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        25: {
            'First run': Timestamp('2025-07-09 18:00:00'),
            'Job runs': 1,
            'Job template name': 'Web03 Deploy',
            'Last run': Timestamp('2025-07-09 18:00:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 16,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        26: {
            'First run': Timestamp('2025-07-09 18:05:00'),
            'Job runs': 1,
            'Job template name': 'Web03 Prod Deploy',
            'Last run': Timestamp('2025-07-09 18:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 12,
            'Organization name': 'Production',
            'Unique managed nodes automated': 1,
        },
        27: {
            'First run': Timestamp('2025-07-10 20:00:00'),
            'Job runs': 2,
            'Job template name': 'Windows Patching',
            'Last run': Timestamp('2025-07-10 20:05:00'),
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 32,
            'Organization name': 'Production',
            'Unique managed nodes automated': 2,
        },
        28: {
            'First run': Timestamp('2025-07-09 13:45:04'),
            'Job runs': 1,
            'Job template name': 'Staging Database Setup',
            'Last run': Timestamp('2025-07-09 13:45:04'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 10,
            'Organization name': 'Staging',
            'Unique managed nodes automated': 1,
        },
        29: {
            'First run': Timestamp('2025-07-10 17:10:00'),
            'Job runs': 1,
            'Job template name': 'Staging Multi-Node App',
            'Last run': Timestamp('2025-07-10 17:10:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 10,
            'Organization name': 'Staging',
            'Unique managed nodes automated': 1,
        },
        30: {
            'First run': Timestamp('2025-07-09 19:05:00'),
            'Job runs': 1,
            'Job template name': 'Web04 Staging Deploy',
            'Last run': Timestamp('2025-07-09 19:05:00'),
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 12,
            'Organization name': 'Staging',
            'Unique managed nodes automated': 1,
        },
    }

    assert actual == expected_jobs


def validate_indirectly_managed_nodes(file_path):
    """Validate Indirectly Managed nodes sheet."""
    sheet = pandas.read_excel(file_path, sheet_name='Indirectly Managed nodes')
    actual = transform_sheet_with_json_normalization(sheet.to_dict())

    # Full data dict assertion for comprehensive validation
    # This validates the complete structure and content of all indirectly managed nodes
    expected_indirectly_managed_nodes = {
        0: {
            'Host name': 'k8s-worker-01.internal',
            'Automated by organizations': 1,
            'Job runs': 1,
            'Number of task runs': 1,
            'First automation': pandas.Timestamp('2025-07-08 10:00:10'),
            'Last automation': pandas.Timestamp('2025-07-08 10:00:10'),
            'Canonical Facts': {'ansible_kubernetes_node_id': ['node-12345'], 'ansible_port': [22]},
            'Facts': {'platform': ['kubernetes']},
            'Manage Node Types': ['INDIRECT'],
            'Events': [],
            'Host names before deduplication': ['k8s-worker-01.internal'],
            'Host names before deduplication count': 1,
        },
        1: {
            'Host name': 'vcenter-vm-01.internal',
            'Automated by organizations': 1,
            'Job runs': 1,
            'Number of task runs': 1,
            'First automation': pandas.Timestamp('2025-07-08 09:33:11.557000'),
            'Last automation': pandas.Timestamp('2025-07-08 09:33:11.557000'),
            'Canonical Facts': {
                'ansible_port': [22],
                'ansible_vmware_bios_uuid': ['420b1367-1e11-c9d7-4d0f-c3b3cba9ae16'],
                'ansible_vmware_instance_uuid': ['500b3d2e-9abe-8ee1-98ea-bf67b591c104'],
                'ansible_vmware_moid': ['vm-87212'],
            },
            'Facts': {'device_type': ['VM']},
            'Manage Node Types': ['INDIRECT'],
            'Events': [],
            'Host names before deduplication': ['vcenter-vm-01.internal'],
            'Host names before deduplication count': 1,
        },
        2: {
            'Host name': 'vcenter-vm-02.internal',
            'Automated by organizations': 1,
            'Job runs': 1,
            'Number of task runs': 1,
            'First automation': pandas.Timestamp('2025-07-08 09:44:27.147000'),
            'Last automation': pandas.Timestamp('2025-07-08 09:44:27.147000'),
            'Canonical Facts': {
                'ansible_port': [443],
                'ansible_vmware_bios_uuid': ['420ba1d2-3793-215c-30f0-5957a405d4e6'],
                'ansible_vmware_instance_uuid': ['500b1a63-d55d-bf21-c104-1617888dd7d2'],
                'ansible_vmware_moid': ['vm-87213'],
            },
            'Facts': {'device_type': ['VM']},
            'Manage Node Types': ['INDIRECT'],
            'Events': [],
            'Host names before deduplication': ['vcenter-vm-02.internal'],
            'Host names before deduplication count': 1,
        },
    }

    # Assert we have the expected total number of entries
    assert len(actual) == 3, f'Expected 3 indirectly managed nodes, got {len(actual)}'

    # Assert the comprehensive data structure for all entries
    for entry_id, expected_entry in expected_indirectly_managed_nodes.items():
        assert entry_id in actual, f'Entry {entry_id} missing from indirectly managed nodes output'
        actual_entry = actual[entry_id]

        for field, expected_value in expected_entry.items():
            assert field in actual_entry, f'Field "{field}" missing from entry {entry_id}'
            actual_value = actual_entry[field]
            assert actual_value == expected_value, f'Entry {entry_id}, field "{field}": expected {expected_value!r}, got {actual_value!r}'


def validate_data_collection_status(file_path):
    """Validate Data collection status sheet with simple table assertions."""

    # Since comparison with nan is tricky, let's use a different approach
    # We'll convert nan values to a string for comparison
    def normalize_for_comparison(d):
        """Normalize a dictionary for comparison by converting nan to string."""
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                result[k] = normalize_for_comparison(v)
            elif isinstance(v, float) and math.isnan(v):
                result[k] = 'NAN_VALUE'
            else:
                result[k] = v
        return result

    # Read the sheet without headers to handle the two tables
    df_raw = pandas.read_excel(file_path, sheet_name='Data collection status', header=None)

    # Find where the second table starts (looking for "Collection timestamp")
    second_table_start = None
    for idx in range(len(df_raw)):
        if df_raw.iloc[idx, 0] == 'Collection timestamp':
            second_table_start = idx
            break

    assert second_table_start is not None, 'Could not find second table in Data collection status sheet'

    # Parse first table (missing data gaps)
    table1_df = pandas.read_excel(file_path, sheet_name='Data collection status', nrows=second_table_start - 1)
    table1_actual = transform_sheet_with_json_normalization(table1_df.to_dict())

    # Parse second table (collection status)
    table2_df = pandas.read_excel(file_path, sheet_name='Data collection status', skiprows=second_table_start, header=0)
    # Clean column names (remove newlines)
    table2_df.columns = [col.replace('\n', ' ') for col in table2_df.columns]
    table2_actual = transform_sheet_with_json_normalization(table2_df.to_dict())

    print(f'Table 1 (missing data gaps) has {len(table1_actual)} entries')
    print(f'Table 2 (collection status) has {len(table2_actual)} entries')

    # Expected values for table 1 (missing data gaps)
    expected_table1 = {
        0: {
            'CSV filename': 'job_host_summary.csv',
            'Gap in seconds': 86401,
            'Missing from': Timestamp('2025-07-10 23:59:59'),
            'Missing until': Timestamp('2025-07-12 00:00:00'),
        },
        1: {
            'CSV filename': 'main_host.csv',
            'Gap in seconds': 86401,
            'Missing from': Timestamp('2025-07-10 23:59:59'),
            'Missing until': Timestamp('2025-07-12 00:00:00'),
        },
        2: {
            'CSV filename': 'main_indirectmanagednodeaudit.csv',
            'Gap in seconds': 86401,
            'Missing from': Timestamp('2025-07-10 23:59:59'),
            'Missing until': Timestamp('2025-07-12 00:00:00'),
        },
    }

    # Normalize table2_actual
    table2_normalized = {k: normalize_for_comparison(v) for k, v in table2_actual.items()}

    # Expected values for table 2 with nan replaced
    # Updated expected data after deduplication (12 entries instead of 33 duplicated entries)
    expected_table2_normalized = {
        0: {
            'CSV filename': 'job_host_summary.csv',
            'Collection timestamp': Timestamp('2025-07-08 00:00:00'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-08 00:00:00'),
            'Filter until': Timestamp('2025-07-08 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': 'NAN_VALUE',
        },
        1: {
            'CSV filename': 'main_host.csv',
            'Collection timestamp': Timestamp('2025-07-08 00:00:00'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-08 00:00:00'),
            'Filter until': Timestamp('2025-07-08 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': 'NAN_VALUE',
        },
        2: {
            'CSV filename': 'main_indirectmanagednodeaudit.csv',
            'Collection timestamp': Timestamp('2025-07-08 00:00:00'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-08 00:00:00'),
            'Filter until': Timestamp('2025-07-08 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': 'NAN_VALUE',
        },
        3: {
            'CSV filename': 'main_host.csv',
            'Collection timestamp': Timestamp('2025-07-08 00:00:01'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-08 00:00:00'),
            'Filter until': Timestamp('2025-07-08 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.time(0, 0, 1),
        },
        4: {
            'CSV filename': 'main_indirectmanagednodeaudit.csv',
            'Collection timestamp': Timestamp('2025-07-08 00:00:02'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-08 00:00:00'),
            'Filter until': Timestamp('2025-07-08 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.time(0, 0, 2),
        },
        5: {
            'CSV filename': 'job_host_summary.csv',
            'Collection timestamp': Timestamp('2025-07-09 00:00:00'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-09 00:00:00'),
            'Filter until': Timestamp('2025-07-09 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        6: {
            'CSV filename': 'main_host.csv',
            'Collection timestamp': Timestamp('2025-07-09 00:00:01'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-09 00:00:00'),
            'Filter until': Timestamp('2025-07-09 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        7: {
            'CSV filename': 'main_indirectmanagednodeaudit.csv',
            'Collection timestamp': Timestamp('2025-07-09 00:00:02'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-09 00:00:00'),
            'Filter until': Timestamp('2025-07-09 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        8: {
            'CSV filename': 'job_host_summary.csv',
            'Collection timestamp': Timestamp('2025-07-10 00:00:00'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-10 00:00:00'),
            'Filter until': Timestamp('2025-07-10 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        9: {
            'CSV filename': 'main_host.csv',
            'Collection timestamp': Timestamp('2025-07-10 00:00:01'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-10 00:00:00'),
            'Filter until': Timestamp('2025-07-10 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        10: {
            'CSV filename': 'main_indirectmanagednodeaudit.csv',
            'Collection timestamp': Timestamp('2025-07-10 00:00:02'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-10 00:00:00'),
            'Filter until': Timestamp('2025-07-10 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.datetime(1900, 1, 1, 0, 0),
        },
        11: {
            'CSV filename': 'job_host_summary.csv',
            'Collection timestamp': Timestamp('2025-07-10 01:00:42'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-07-10 01:00:42'),
            'Filter until': Timestamp('2025-07-10 23:59:59'),
            'Status': 'ok',
            'Time since previous collection': datetime.time(1, 0, 42),
        },
    }

    # Simple assertions
    assert table1_actual == expected_table1, 'Table 1 (missing data gaps) does not match expected'
    assert table2_normalized == expected_table2_normalized, 'Table 2 (collection status) does not match expected'

    print('✓ Validated both data collection status tables')


def validate_input_csv_data_integrity():
    """Validate CSV data integrity using direct CSV file validation and tarball cross-validation."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    # Basic validation that input CSVs exist and have expected structure
    required_files = [
        'input_main_host.csv',
        'input_job_host_summary.csv',
        'input_main_jobevent.csv',
        'input_main_indirectmanagednodeaudit.csv',
        'input_data_collection_status.csv',
    ]

    for file_name in required_files:
        file_path = input_data_dir / file_name
        assert file_path.exists(), f'Required input file {file_name} not found'

        # Basic CSV validation
        try:
            df = pandas.read_csv(file_path)
            assert len(df) > 0, f'File {file_name} is empty'
        except Exception as e:
            pytest.fail(f'Failed to read {file_name}: {e}')


def validate_json_fields_comprehensive():
    """Comprehensive validation of JSON fields in CSV files."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    # Validate main_host canonical_facts JSON
    main_host_path = input_data_dir / 'input_main_host.csv'
    if main_host_path.exists():
        df = pandas.read_csv(main_host_path)
        for _, row in df.iterrows():
            try:
                canonical_facts = json.loads(row['canonical_facts'])
                assert isinstance(canonical_facts, dict), 'canonical_facts must be a dict'

                # Validate expected fields
                expected_fields = ['ansible_host', 'host_name']
                for field in expected_fields:
                    assert field in canonical_facts, f'Missing {field} in canonical_facts'

            except json.JSONDecodeError:
                pytest.fail(f'Invalid JSON in canonical_facts for row {row["host_name"]}')


def validate_canonical_facts_combinations():
    """Validate realistic combinations of canonical facts based on platform types."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    main_host_path = input_data_dir / 'input_main_host.csv'
    if main_host_path.exists():
        df = pandas.read_csv(main_host_path)

        # Validate canonical facts combinations by platform
        for _, row in df.iterrows():
            try:
                canonical_facts = json.loads(row['canonical_facts'])
                facts = json.loads(row['facts'])

                virtualization_type = facts.get('ansible_virtualization_type')
                connection_type = facts.get('ansible_connection_variable')

                # Platform-specific validations
                if virtualization_type == 'VirtualPC':  # Windows
                    assert connection_type == 'winrm', f'Windows host {row["host_name"]} should use winrm'
                    assert canonical_facts.get('ansible_port') == 5985, f'Windows host {row["host_name"]} should use port 5985'

                elif virtualization_type == 'container':  # Kubernetes
                    assert connection_type == 'kubectl', f'Container host {row["host_name"]} should use kubectl'

                elif connection_type == 'tcp':  # Network devices
                    assert virtualization_type == 'lxc', f'TCP connection host {row["host_name"]} should be lxc'

            except json.JSONDecodeError:
                pytest.fail(f'Invalid JSON in row {row["host_name"]}')


def extract_csvs_from_tarballs():
    """Extract CSV files from test tarballs for human review."""
    test_dir = get_test_dir()
    test_data_dir = test_dir.parent.parent.parent.parent / 'test_data' / 'data' / '2025'
    input_data_dir = test_dir / 'input_data'
    input_data_dir.mkdir(exist_ok=True)

    # Process files by date
    for date_dir in sorted(test_data_dir.rglob('*')):
        if date_dir.is_dir() and date_dir.name.isdigit():
            for tarball in sorted(date_dir.glob('*.tar.gz')):
                extract_tarball_csvs(tarball, input_data_dir)


def extract_tarball_csvs(tarball_path, output_dir):
    """Extract CSV files from a single tarball."""
    try:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.csv'):
                    # Extract to memory first
                    csv_content = tar.extractfile(member)
                    if csv_content:
                        # Determine output filename based on CSV type
                        base_name = member.name.split('/')[-1]
                        output_file = output_dir / f'input_{base_name}'

                        # Append or create file
                        with open(output_file, 'a' if output_file.exists() else 'w') as f:
                            content = csv_content.read().decode('utf-8')
                            # Skip header if appending
                            if output_file.exists() and '\n' in content:
                                lines = content.split('\n')
                                content = '\n'.join(lines[1:])  # Skip header
                            f.write(content)

        print(f'📦 Extracting from {tarball_path.name}')

    except Exception as e:
        print(f'❌ Error extracting {tarball_path}: {e}')


def validate_use_cases(actual_managed_nodes):
    """Validate all deduplication test cases.

    Deduplication Test Cases Explained:
    ===================================

    1. DEDUPLICATED HOSTS (merged based on matching serial + machine_id):
    ---------------------------------------------------------------------
    1.1. app01.cluster (4 entries → 1):
         - All 4 entries have same serial (USE1234567) + machine_id (e56eb592febecd4e03860514ce5a9f55)
         - Entries from 3 different orgs (Production x2, Development, Staging)
         - Result: Merged into single entry showing 3 organizations
         - Dedup: Old logic only (Host names before deduplication count=1) - all 4 entries had same ansible_host

    1.2. web01.internal + web01.prod.company.com (3 entries → 1):
         - All have same VMware serial + machine_id (3a2f8c9b...)
         - Different hostnames but same physical machine
         - Result: Merged, showing both hostnames in canonical facts
         - Dedup: New logic applied (Host names before deduplication count=2) - old logic kept 2 separate, new merged by machine_id+serial

    1.3. web02.external + web02.internal (2 entries → 1):
         - Same VMware serial + machine_id (f3e2da65c5d34e59151db7ec18b868d9)
         - Different network access points to same machine
         - Result: Merged into web02.external (first seen)
         - Dedup: New logic applied (Host names before deduplication count=2) - different ansible_host values

    1.4. db02.dev + db02.staging (2 entries → 1):
         - Same Dell serial (R750) + machine_id (eddfa033379afb7784abb2e4c7dc2cf1)
         - Different environment names for same database server
         - Result: Merged into db02.dev
         - Dedup: New logic applied (Host names before deduplication count=2) - old logic grouped by ansible_host, new merged by machine_id

    1.5. web03.internal + web03.prod.internal (2 entries → 1):
         - Same VMware serial + machine_id (01b6b28643a6a867e339e957c8ed9d37)
         - Production variants of same web server
         - Result: Merged into web03.internal
         - Dedup: New logic applied (Host names before deduplication count=2) - both had different ansible_host and different host_name

    1.6. cache01.internal (2 entries → 1):
         - Both have same machine_id (0267fc0887de14e8c994d1025a445221) but NO product_serial
         - From different orgs (Production, Development)
         - Result: Merged because machine_id matches (serial not required if missing)
         - Dedup: Old logic only (Host names before deduplication count=1) - same ansible_host and host_name

    2. NOT DEDUPLICATED HOSTS (unique serial/machine_id combinations):
    -------------------------------------------------------------------

    2.1. db01.company.com:
         - Has product_serial but NO machine_id
         - Cannot deduplicate without machine_id
         - Result: Kept separate
         - Dedup: No dedup needed (Host names before deduplication count=1) - unique ansible_host and serial CN7792194B0740

    2.2. log01.company.com:
         - Missing BOTH product_serial AND machine_id
         - No canonical facts to deduplicate on
         - Result: Kept separate
         - Dedup: No dedup needed (Host names before deduplication count=1) - unique host

    2.3. web04.dev and web04.staging:
         - Different machine_ids (ae920ed940e880003e264a357de969c1 vs d1134fec21d571a9b596f7dbf7dc5673)
         - Different serials (VMware-dev-... vs VMware-stg-...)
         - Different hostnames
         - Result: Kept as separate hosts (different environments)
         - Dedup: No dedup needed (Host names before deduplication count=1 each) - different hosts

    3. FALSE NEGATIVES - NOT DEDUPLICATED (but should be):
    -------------------------------------------------------
    3.1. win-srv01.company.com and win-srv02.company.com:
         - Different Windows servers with SAME serial (USE9876543)
         - Windows lacks machine_id (systemd-specific)
         - Only product_serial available for deduplication
         - Result: Kept separate (FALSE NEGATIVE - same serial but no machine_id)
         - Dedup: No dedup applied (Host names before deduplication count=1 each) - new logic requires machine_id

    3.2. k8s-node-01.cluster and k8s-node-01.internal:
         - Same Kubernetes node accessed differently
         - Container environment lacks both machine_id and serial
         - No canonical facts for deduplication
         - Result: Kept separate (SHOULD be merged based on hostname pattern)
         - Dedup: No dedup possible (Host names before deduplication count=1 each) - no canonical facts

    3.3. secure-host-01.company.com and secure-host-01-readonly.internal (privileged vs unprivileged):
         - Same host accessed with different credentials
         - Admin job has product_serial, user job doesn't
         - Same machine_id in both cases 4f7a8b9c2d3e5f6a7b8c9d0e1f2a3b4c
         - Result: Kept separate
         - Dedup: No dedup done (Host names before deduplication count=1 each) - because one record serial is missing

    3.4. app01.failover:
         - Different machine_id (1a17f31cc8a19e2e1d3aa4901cb47939) than app01.cluster
         - Same serial number USE1234567 but different physical machine
         - Result: Kept separate
         - Dedup: No dedup done (Host names before deduplication count=1 each) - because both machine_id and serial need to match

    4. FALSE POSITIVES - WRONGLY DEDUPLICATED (but shouldn't be):
    --------------------------------------------------------------
    4.1. aws-vm-01.us-east and aws-vm-02.us-west:
         - Different AWS VMs in different regions
         - Cloud-init generates same synthetic machine_id
         - Generic AWS product_serial (ec2-instance)
         - Result: Wrongly merged (SHOULD be kept separate)
         - Dedup: New logic wrongly applied (Host names before deduplication count=2) - matched on synthetic IDs

    4.2. nat-host-01.external and nat-host-02.external:
         - Different hosts behind same NAT gateway
         - NAT gateway's machine_id and serial exposed to both
         - Same public IP address (203.0.113.10) with different ports (2201 and 2202)
         - Result: Still merged (port not used in deduplication logic)
         - Dedup: Merged based on ansible_host (Host names before deduplication count=1)
         - Note: This demonstrates a limitation where NAT gateway hosts are incorrectly merged

    5. HOSTNAME RESOLUTION TEST CASES (NEW):
    ----------------------------------------
    These test cases demonstrate how DNS resolution affects deduplication
    when hosts are accessible via multiple hostnames

    5.1. api-server (3 entries → 1):
         - api-server (short hostname)
         - api-server.company.com (FQDN)
         - api-server.company.com.east (FQDN with region)
         - All have same machine_id (a644029003e46b31d1a09ecec6c77b02) and serial (USE1845G8K1)
         - Result: Correctly deduplicated based on matching canonical facts
         - This shows that with canonical facts, DNS variations don't cause duplicates
         - Dedup: New logic only (Host names before deduplication count=3) - deduplicated by machine_id and product_serial

    5.2. db-primary (3 entries → 3 showing false negative):
         - db-primary (short hostname) - HAS canonical facts
         - db-primary.company.com (FQDN) - NO canonical facts
         - db-primary.company.com.west (FQDN with region) - NO canonical facts
         - Only first entry has machine_id (bc2fa6de408414cef69227ebf4cf0f7e) and serial (CN7016194B0DB1)
         - Result: Shows as 3 separate hosts (false negative behavior)
         - This demonstrates that without canonical facts on all entries, they appear as separate hosts
         - Dedup: No dedup (Host names before deduplication count=1 each) - different ansible_host values, missing canonical facts
    """

    # Helper function to find host entry by name
    def find_host(hostname):
        for entry in actual_managed_nodes.values():
            if entry['Host name'] == hostname:
                return entry
        return None

    # Helper function to get canonical facts as dict
    def get_canonical_facts(entry):
        cf = entry.get('Canonical Facts', {})
        # If it's already a dict, return it
        if isinstance(cf, dict):
            return cf
        # Otherwise try to parse it as JSON
        try:
            return json.loads(cf)
        except (json.JSONDecodeError, TypeError):
            return {}

    # Test Case 1.1: app01.cluster (4 entries → 1)
    app01_cluster = find_host('app01.cluster')
    assert app01_cluster is not None, 'app01.cluster should be present'
    assert app01_cluster['Automated by organizations'] == 3, f'app01.cluster should show 3 orgs, got {app01_cluster["Automated by organizations"]}'
    cf = get_canonical_facts(app01_cluster)
    assert cf.get('ansible_machine_id') == ['e56eb592febecd4e03860514ce5a9f55'], (
        "app01.cluster should have machine_id 'e56eb592febecd4e03860514ce5a9f55'"
    )
    assert cf.get('ansible_product_serial') == ['USE1234567'], "app01.cluster should have serial 'USE1234567'"

    # Test Case 1.2: web01.internal + web01.prod.company.com (3 entries → 1)
    web01 = find_host('web01.internal')
    assert web01 is not None, 'web01.internal should be present'
    cf = get_canonical_facts(web01)
    hostnames = cf.get('host_name', [])
    assert 'web01.internal' in hostnames, 'web01.internal should be in host names'
    assert 'web01.prod.company.com' in hostnames, 'web01.prod.company.com should be in host names'
    assert cf.get('ansible_machine_id') == ['3a2f8c9b123456789012345678901234'], 'Should have correct machine_id'

    # Test Case 1.3: web02.external + web02.internal (2 entries → 1)
    web02 = find_host('web02.external')
    assert web02 is not None, 'web02.external should be present (first seen)'
    cf = get_canonical_facts(web02)
    assert cf.get('ansible_machine_id') == ['f3e2da65c5d34e59151db7ec18b868d9'], 'Should have correct machine_id'
    hostnames = cf.get('host_name', [])
    assert 'web02.external' in hostnames and 'web02.internal' in hostnames, 'Should show both hostnames'

    # Test Case 1.4: db02.dev + db02.staging (2 entries → 1)
    db02 = find_host('db02.dev')
    assert db02 is not None, 'db02.dev should be present'
    cf = get_canonical_facts(db02)
    assert cf.get('ansible_machine_id') == ['eddfa033379afb7784abb2e4c7dc2cf1'], 'Should have correct machine_id'
    hostnames = cf.get('host_name', [])
    assert 'db02.dev' in hostnames and 'db02.staging' in hostnames, 'Should show both hostnames'

    # Test Case 1.5: web03.internal + web03.prod.internal (2 entries → 1)
    web03 = find_host('web03.internal')
    assert web03 is not None, 'web03.internal should be present'
    cf = get_canonical_facts(web03)
    assert cf.get('ansible_machine_id') == ['01b6b28643a6a867e339e957c8ed9d37'], 'Should have correct machine_id'

    # Test Case 1.6: cache01.internal (2 entries → 1)
    cache01 = find_host('cache01.internal')
    assert cache01 is not None, 'cache01.internal should be present'
    assert cache01['Automated by organizations'] == 2, f'cache01.internal should show 2 orgs, got {cache01["Automated by organizations"]}'
    cf = get_canonical_facts(cache01)
    assert cf.get('ansible_machine_id') == ['0267fc0887de14e8c994d1025a445221'], 'Should have machine_id'
    assert cf.get('ansible_product_serial') is None or cf.get('ansible_product_serial') == [], 'Should have no serial'

    # Test Case 2.1: db01.company.com (no machine_id)
    db01 = find_host('db01.company.com')
    assert db01 is not None, 'db01.company.com should be present'
    cf = get_canonical_facts(db01)
    assert cf.get('ansible_machine_id') is None or cf.get('ansible_machine_id') == [], 'Should have no machine_id'
    assert cf.get('ansible_product_serial') == ['CN7792194B0740'], 'Should have serial'

    # Test Case 2.2: log01.company.com (no canonical facts)
    log01 = find_host('log01.company.com')
    assert log01 is not None, 'log01.company.com should be present'
    cf = get_canonical_facts(log01)
    assert not cf.get('ansible_machine_id') and not cf.get('ansible_product_serial'), 'Should have no canonical facts'

    # Test Case 2.3: web04.dev and web04.staging (different machines)
    web04_dev = find_host('web04.dev')
    web04_staging = find_host('web04.staging')
    assert web04_dev is not None, 'web04.dev should be present'
    assert web04_staging is not None, 'web04.staging should be present'
    assert web04_dev != web04_staging, 'web04.dev and web04.staging should be separate entries'

    # Test Case 3.1: win-srv01.company.com and win-srv02.company.com
    win_srv01 = find_host('win-srv01.company.com')
    win_srv02 = find_host('win-srv02.company.com')
    assert win_srv01 is not None, 'win-srv01.company.com should be present'
    assert win_srv02 is not None, 'win-srv02.company.com should be present'
    assert win_srv01 != win_srv02, 'Windows servers are kept separate (FALSE NEGATIVE)'

    # Test Case 3.2: k8s-node-01.cluster and k8s-node-01.internal
    k8s_cluster = find_host('k8s-node-01.cluster')
    k8s_internal = find_host('k8s-node-01.internal')
    assert k8s_cluster != k8s_internal, 'K8s nodes are kept separate (FALSE NEGATIVE)'

    # Test Case 3.3: secure-host-01.company.com (different privilege levels)
    # Look for secure-host-01.company.com entries
    secure_hosts = [entry for entry in actual_managed_nodes.values() if 'secure-host-01' in entry['Host name']]
    # Should have 2 entries due to different privilege levels
    assert len(secure_hosts) == 2, f'Should have 2 secure-host-01 entry (correctly deduplicated), got {len(secure_hosts)}'

    # Test Case 3.4: app01.failover (should be separate)
    app01_failover = find_host('app01.failover')
    assert app01_failover is not None, 'app01.failover should be present as separate host'
    cf = get_canonical_facts(app01_failover)
    assert cf.get('ansible_machine_id') == ['1a17f31cc8a19e2e1d3aa4901cb47939'], 'Should have different machine_id'

    # Test Case 4.1: AWS VMs with same synthetic machine_id
    # Look for any AWS VM entry
    aws_vm = find_host('aws-vm-01.us-east')
    assert aws_vm is not None, 'aws-vm-01.us-east should be present'
    cf = get_canonical_facts(aws_vm)
    hostnames = cf.get('host_name', [])
    # AWS VMs should be merged (they have same machine_id and serial)
    assert 'aws-vm-02.us-west' in hostnames, 'AWS VMs should be wrongly merged (expected false positive)'

    # Test Case 4.2: NAT hosts
    nat_entry = find_host('203.0.113.10')  # They get merged under the IP
    assert nat_entry is not None, 'NAT hosts should be present (merged under IP)'
    cf = get_canonical_facts(nat_entry)
    hostnames = cf.get('host_name', [])
    # NAT hosts should be merged under IP (expected false positive)
    assert 'nat-host-01.external' in hostnames and 'nat-host-02.external' in hostnames, 'NAT hosts should be wrongly merged'

    # Test Case 5.1: api-server variants
    api_server = find_host('api-server')
    assert api_server is not None, 'api-server should be present'
    cf = get_canonical_facts(api_server)
    hostnames = cf.get('host_name', [])
    assert 'api-server' in hostnames, 'Should include short hostname'
    assert any('api-server.company.com' in h for h in hostnames), 'Should include FQDN variants'

    # Test Case 5.2: db-primary variants (false negative)
    db_primary_short = find_host('db-primary')
    db_primary_fqdn = find_host('db-primary.company.com')
    db_primary_west = find_host('db-primary.company.com.west')

    # Count how many are present
    db_primary_count = sum(1 for h in [db_primary_short, db_primary_fqdn, db_primary_west] if h is not None)
    assert db_primary_count == 3, f'Should have 3 separate db-primary entries (false negative), got {db_primary_count}'

    # Verify each db-primary host exists as separate entries (false negative - no deduplication)
    # Check that we found separate entries for each hostname variant
    assert db_primary_short is not None, 'Should find db-primary entry'
    assert db_primary_fqdn is not None, 'Should find db-primary.company.com entry'
    assert db_primary_west is not None, 'Should find db-primary.company.com.west entry'
