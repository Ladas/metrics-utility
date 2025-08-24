import os
import random
import subprocess
import sys
import uuid

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest


@contextmanager
def temporary_env(new_env):
    """Temporarily update os.environ with new_env."""
    original = os.environ.copy()

    # os.environ.update(new_env), but removing keys with None as value
    for k, v in new_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


# Running a command as an external command, to test we can


def _run_ext(env, name, args):
    result = subprocess.run(
        [sys.executable, 'manage.py', name, *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={'AWX_LOGGING_MODE': 'stdout', **env},
    )

    status = result.returncode

    if status != 0:
        pytest.fail(result.stderr)

    assert status == 0

    return result


def run_build_ext(env, args):
    return _run_ext(env, 'build_report', args)


def run_gather_ext(env, args):
    return _run_ext(env, 'gather_automation_controller_billing_data', args)


# Running a command python way, so we can work with debugger in the code, and collect coverage


def run_build_int(env, options):
    from metrics_utility.management.commands.build_report import Command

    with temporary_env(env):
        Command().handle(**options)


def run_gather_int(env, options):
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    with temporary_env(env):
        Command().handle(**options)


def run_command_int(command_name, env, options):
    """Run any management command internally with environment and options."""
    if command_name == 'build_report':
        run_build_int(env, options)
    elif command_name == 'gather_automation_controller_billing_data':
        run_gather_int(env, options)
    elif command_name == 'compute_rollups':
        from metrics_utility.management.commands.compute_rollups import Command

        with temporary_env(env):
            Command().handle(**options)
    else:
        raise ValueError(f'Unsupported command: {command_name}')


def generate_renewal_guidance_dataframe(is_empty=False, current_datetime=None):
    """
    Generates a pandas DataFrame with specific, hardcoded mock renewal guidance
    report data for predefined test scenarios, or an empty DataFrame with defined columns.

    Args:
        is_empty (bool): If True, returns an empty DataFrame with correct columns.
                         Otherwise, returns the predefined rows.
        current_datetime (datetime.datetime, optional): A fixed datetime to use
                            for 'now'. If None, datetime.now(timezone.utc) is used.
    """
    column_names = [
        'host_id',
        'hostname',
        'first_automation',
        'last_automation',
        'automated_counter',
        'deleted_counter',
        'last_deleted',
        'deleted',
        'ansible_product_serial',
        'ansible_machine_id',
        'ansible_host_variable',
        'ansible_connection_variable',
        'days_automated',
    ]

    if is_empty:
        return pd.DataFrame(columns=column_names)

    # --- Helper to generate common datetime objects for consistency ---
    if current_datetime is None:
        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    else:
        now_utc = current_datetime.replace(microsecond=0)  # Use provided fixed datetime

    deleted_date_example_dt = now_utc - timedelta(days=5)

    rows = []

    # Row 1: localhost (Non-deleted, Non-ephemeral)
    first_auto_dt_1 = datetime(2025, 5, 7, 14, 45, 32, tzinfo=timezone.utc)
    last_auto_dt_1 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_1 = 'localhost'
    rows.append(
        {
            'host_id': 1001,
            'hostname': hostname_1,
            'first_automation': first_auto_dt_1.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_1.isoformat(timespec='seconds'),
            'automated_counter': 731,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_1 - first_auto_dt_1).days,
        }
    )

    # Row 2: localhost2 (Non-deleted, Non-ephemeral)
    first_auto_dt_2 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_2 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_2 = 'localhost2'
    rows.append(
        {
            'host_id': 1002,
            'hostname': hostname_2,
            'first_automation': first_auto_dt_2.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_2.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost2',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_2 - first_auto_dt_2).days,
        }
    )

    # Row 3: localhost3 (Non-deleted, Non-ephemeral)
    first_auto_dt_3 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_3 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_3 = 'localhost3'
    rows.append(
        {
            'host_id': 1003,
            'hostname': hostname_3,
            'first_automation': first_auto_dt_3.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_3.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost3',
            'ansible_connection_variable': 'winrm',
            'days_automated': (last_auto_dt_3 - first_auto_dt_3).days,
        }
    )

    # Row 4: localhost-duplicate (Deleted, Non-ephemeral)
    first_auto_dt_4 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_4 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_4 = 'localhost-duplicate'
    rows.append(
        {
            'host_id': 1004,
            'hostname': hostname_4,
            'first_automation': first_auto_dt_4.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_4.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost-duplicate',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_4 - first_auto_dt_4).days,
        }
    )

    # --- New Rows for Specific Test Scenarios ---

    # Row 5: Clearly Ephemeral (Short-lived)
    hostname_5 = 'ephemeral-dev-short-life'
    first_auto_dt_5 = now_utc - timedelta(days=40)  # Relative to fixed_now
    last_auto_dt_5 = first_auto_dt_5 + timedelta(days=5)  # 5 days automated
    rows.append(
        {
            'host_id': 1005,
            'hostname': hostname_5,
            'first_automation': first_auto_dt_5.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_5.isoformat(timespec='seconds'),
            'automated_counter': 10,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_5 - first_auto_dt_5).days,
        }
    )

    # Row 6: Clearly NON-Ephemeral (Long-lived)
    hostname_6 = 'stable-prod-long-life'
    first_auto_dt_6 = now_utc - timedelta(days=100)  # Relative to fixed_now
    last_auto_dt_6 = first_auto_dt_6 + timedelta(days=100)  # 100 days automated
    rows.append(
        {
            'host_id': 1006,
            'hostname': hostname_6,
            'first_automation': first_auto_dt_6.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_6.isoformat(timespec='seconds'),
            'automated_counter': 500,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'stable-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_6 - first_auto_dt_6).days,
        }
    )

    # Row 7: Boundary Ephemeral (days_automated exactly at threshold)
    hostname_7 = 'boundary-days-ephemeral'
    first_auto_dt_7 = now_utc - timedelta(days=60)  # Relative to fixed_now
    last_auto_dt_7 = first_auto_dt_7 + timedelta(days=30)  # 30 days automated
    rows.append(
        {
            'host_id': 1007,
            'hostname': hostname_7,
            'first_automation': first_auto_dt_7.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_7.isoformat(timespec='seconds'),
            'automated_counter': 50,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-boundary-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_7 - first_auto_dt_7).days,
        }
    )

    # Row 8: Boundary Ephemeral (first_automation exactly at threshold date)
    hostname_8 = 'boundary-date-ephemeral'
    first_auto_dt_8 = now_utc - timedelta(days=30)  # Exactly 30 days ago relative to fixed_now
    last_auto_dt_8 = first_auto_dt_8 + timedelta(days=5)  # 5 days automated
    rows.append(
        {
            'host_id': 1008,
            'hostname': hostname_8,
            'first_automation': first_auto_dt_8.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_8.isoformat(timespec='seconds'),
            'automated_counter': 10,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-boundary-2',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_8 - first_auto_dt_8).days,
        }
    )

    # Row 9: Deleted host, would be non-ephemeral if not deleted
    hostname_9 = 'long-lived-deleted'
    first_auto_dt_9 = now_utc - timedelta(days=100)  # Relative to fixed_now
    last_auto_dt_9 = first_auto_dt_9 + timedelta(days=90)  # 90 days automated
    rows.append(
        {
            'host_id': 1009,
            'hostname': hostname_9,
            'first_automation': first_auto_dt_9.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_9.isoformat(timespec='seconds'),
            'automated_counter': 200,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'deleted-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_9 - first_auto_dt_9).days,
        }
    )

    # Row 10: Deleted host, would be ephemeral if not deleted
    hostname_10 = 'short-lived-deleted'
    first_auto_dt_10 = now_utc - timedelta(days=40)  # Relative to fixed_now
    last_auto_dt_10 = first_auto_dt_10 + timedelta(days=10)  # 10 days automated
    rows.append(
        {
            'host_id': 1010,
            'hostname': hostname_10,
            'first_automation': first_auto_dt_10.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_10.isoformat(timespec='seconds'),
            'automated_counter': 5,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'deleted-ephemeral-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_10 - first_auto_dt_10).days,
        }
    )

    return pd.DataFrame(rows)
