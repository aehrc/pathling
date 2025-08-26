---
sidebar_position: 3
title: FHIR server synchronisation
description: Examples of synchronising your analytic data store with a FHIR server.
---

# FHIR server synchronisation

This guide demonstrates how to build a robust synchronisation pipeline that
periodically fetches data from a FHIR bulk export endpoint and maintains an
up-to-date set of Delta tables for analytics.

## Overview

The FHIR Bulk Data Access specification provides a standardised way to export
large volumes of healthcare data from FHIR servers. By combining this with
Pathling's built-in Delta Lake support, we can build efficient data pipelines
that:

- Perform initial full exports to establish baseline data
- Run incremental exports using the `since` parameter to capture only changes
- Merge updates automatically using Pathling's `SaveMode.MERGE`
- Handle failures gracefully with built-in retry logic
- Maintain data consistency with Delta Lake's ACID transactions

### Authentication

The script
uses [SMART Backend Services](https://www.hl7.org/fhir/smart-app-launch/backend-services.html)
with the client-confidential-asymmetric profile for secure authentication. This
approach is specifically designed for backend services that need autonomous
access to FHIR resources, such as:

- Analytics platforms performing bulk data imports
- Data integration services synchronising patient records
- Public health surveillance systems
- Utilisation tracking systems

The authentication flow uses asymmetric keys (public/private key pairs) rather
than shared secrets, providing enhanced security for automated systems that
access healthcare data.

## Complete synchronisation script

Here's a Python script that leverages Pathling's built-in bulk export and Delta
merge capabilities:

```python
#!/usr/bin/env python3
"""
FHIR Bulk Export to Delta Lake Synchronisation Script

This script performs periodic synchronisation of FHIR data from a bulk export
endpoint to Delta tables, supporting both initial loads and incremental updates.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

from pathling import PathlingContext
from pathling.datasink import SaveMode

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SyncState:
    """Tracks the state of synchronisation for incremental updates."""

    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.last_sync_time: Optional[datetime] = None
        self.load()

    def load(self):
        """Load sync state from JSON file."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                if data.get('last_sync_time'):
                    self.last_sync_time = datetime.fromisoformat(
                        data['last_sync_time']
                    )

    def save(self):
        """Save sync state to JSON file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump({
                'last_sync_time': self.last_sync_time.isoformat()
                if self.last_sync_time else None
            }, f, indent=2)


def sync_fhir_to_delta(
        fhir_endpoint: str,
        delta_path: str,
        state_file: Path,
        resource_types: Optional[List[str]] = None,
        group_id: Optional[str] = None,
        auth_config: Optional[Dict[str, Any]] = None,
        full_refresh: bool = False
):
    """
    Synchronise FHIR data from bulk export endpoint to Delta tables.
    
    Args:
        fhir_endpoint: FHIR server bulk export endpoint URL
        delta_path: Base path for Delta tables
        state_file: Path to store sync state
        resource_types: List of resource types to sync
        group_id: Optional group ID for group-level export
        auth_config: Authentication configuration for FHIR server
        full_refresh: Force a full export instead of incremental
    """
    start_time = datetime.now(timezone.utc)

    # Load sync state.
    state = SyncState(state_file)

    # Initialise Pathling context with Delta support.
    pc = PathlingContext.create(enable_delta=True)

    logger.info(f"Starting sync from {fhir_endpoint}")
    if state.last_sync_time and not full_refresh:
        logger.info(f"Incremental sync since {state.last_sync_time}")

    # Determine export parameters.
    since = None if full_refresh else state.last_sync_time

    # Perform bulk export and get data source.
    # The bulk client handles retries automatically.
    data_source = pc.read.bulk(
        fhir_endpoint_url=fhir_endpoint,
        group_id=group_id,
        types=resource_types,
        since=since,
        auth_config=auth_config,
        timeout=3600  # 1 hour timeout.
    )

    # Write to Delta tables using merge mode.
    # Pathling handles both initial creation and incremental updates.
    data_source.write.delta(delta_path, save_mode=SaveMode.MERGE)

    # Update sync state.
    state.last_sync_time = start_time
    state.save()

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"Sync completed successfully in {duration:.2f} seconds")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Sync FHIR data from bulk export to Delta tables"
    )
    parser.add_argument(
        "--fhir-endpoint",
        required=True,
        help="FHIR server bulk export endpoint URL"
    )
    parser.add_argument(
        "--delta-path",
        required=True,
        help="Base path for Delta tables"
    )
    parser.add_argument(
        "--state-file",
        default="/var/lib/fhir-sync/state.json",
        help="Path to sync state file"
    )
    parser.add_argument(
        "--resource-types",
        nargs="+",
        default=["Patient", "Condition", "Observation", "Encounter"],
        help="Resource types to sync"
    )
    parser.add_argument(
        "--group-id",
        help="Group ID for group-level export"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force a full export instead of incremental"
    )
    parser.add_argument(
        "--auth-client-id",
        help="OAuth2 client ID for authentication"
    )
    parser.add_argument(
        "--auth-private-key",
        help="Path to private key file for authentication"
    )

    args = parser.parse_args()

    # Build authentication configuration if provided.
    auth_config = None
    if args.auth_client_id:
        auth_config = {
            "enabled": True,
            "client_id": args.auth_client_id,
            "use_smart": True,
            "scope": "system/*.read"
        }

        if args.auth_private_key:
            with open(args.auth_private_key, 'r') as f:
                auth_config["private_key_jwk"] = f.read()

    # Run synchronisation.
    sync_fhir_to_delta(
        fhir_endpoint=args.fhir_endpoint,
        delta_path=args.delta_path,
        state_file=Path(args.state_file),
        resource_types=args.resource_types,
        group_id=args.group_id,
        auth_config=auth_config,
        full_refresh=args.full_refresh
    )
```

## Scheduling periodic synchronisation

### Using cron

The simplest way to schedule periodic synchronisation is using cron. Create a
shell script wrapper:

```bash
#!/bin/bash
# /usr/local/bin/fhir-sync.sh

# Set up environment.
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export SPARK_HOME=/opt/spark

# Run the sync script.
/usr/bin/python3 /opt/fhir-sync/sync.py \
    --fhir-endpoint "https://your-fhir-server.com/fhir" \
    --delta-path "/data/delta/fhir" \
    --state-file "/var/lib/fhir-sync/state.json" \
    --resource-types Patient Condition Observation Encounter \
    --auth-client-id "your-client-id" \
    --auth-private-key "/etc/fhir-sync/private-key.jwk" \
    >> /var/log/fhir-sync/sync.log 2>&1
```

Then add to crontab to run every hour:

```bash
# Run FHIR sync every hour at 15 minutes past.
15 * * * * /usr/local/bin/fhir-sync.sh
```

### Using Python scheduler

For more complex scheduling requirements, use the `schedule` library:

```python
import schedule
import time
from datetime import datetime
from pathlib import Path


def run_sync(full_refresh=False):
    """Execute the sync process."""
    logger.info(f"Starting scheduled sync at {datetime.now()}")

    sync_fhir_to_delta(
        fhir_endpoint="https://your-fhir-server.com/fhir",
        delta_path="/data/delta/fhir",
        state_file=Path("/var/lib/fhir-sync/state.json"),
        resource_types=["Patient", "Condition", "Observation"],
        full_refresh=full_refresh
    )


# Schedule different sync patterns.
schedule.every().hour.at(":15").do(run_sync)  # Hourly incremental.
schedule.every().sunday.at("03:00").do(
    lambda: run_sync(full_refresh=True)
)  # Weekly full refresh.

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Using Apache Airflow

For enterprise deployments, create an Airflow DAG:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fhir_bulk_sync',
    default_args=default_args,
    description='Sync FHIR data to Delta tables',
    schedule_interval='0 * * * *',  # Hourly.
    catchup=False
)


def sync_task(**context):
    """Airflow task to run sync."""
    from pathlib import Path

    sync_fhir_to_delta(
        fhir_endpoint="{{ var.value.fhir_endpoint }}",
        delta_path="{{ var.value.delta_path }}",
        state_file=Path("{{ var.value.state_file }}"),
        resource_types=["Patient", "Condition", "Observation"]
    )


sync = PythonOperator(
    task_id='sync_fhir_data',
    python_callable=sync_task,
    dag=dag
)

notify = EmailOperator(
    task_id='send_notification',
    to=['data-team@example.com'],
    subject='FHIR Sync Completed',
    html_content='Sync completed at {{ ds }}',
    trigger_rule='all_done',
    dag=dag
)

sync >> notify
```

## Monitoring and health checks

Implement health check endpoints for monitoring:

```python
from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/health')
def health_check():
    """Health check endpoint for monitoring systems."""
    try:
        state = SyncState(Path("/var/lib/fhir-sync/state.json"))

        # Check if sync is running regularly.
        if state.last_sync_time:
            time_since_sync = datetime.now(timezone.utc) - state.last_sync_time
            is_healthy = time_since_sync.total_seconds() < 7200  # 2 hours.
        else:
            is_healthy = False

        return jsonify({
            'status': 'healthy' if is_healthy else 'unhealthy',
            'last_sync': state.last_sync_time.isoformat() if state.last_sync_time else None
        }), 200 if is_healthy else 503

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 503
```

## Production considerations

### Security best practices

1. **Secure credential storage**: Use environment variables or secret management
   systems:

```python
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


def get_auth_config():
    """Retrieve authentication config from secure storage."""
    # Option 1: Environment variables.
    if os.getenv("FHIR_CLIENT_ID"):
        return {
            "enabled": True,
            "client_id": os.getenv("FHIR_CLIENT_ID"),
            "private_key_jwk": os.getenv("FHIR_PRIVATE_KEY"),
            "use_smart": True,
            "scope": "system/*.read"
        }

    # Option 2: Azure Key Vault.
    credential = DefaultAzureCredential()
    client = SecretClient(
        vault_url="https://your-vault.vault.azure.net/",
        credential=credential
    )

    return {
        "enabled": True,
        "client_id": client.get_secret("fhir-client-id").value,
        "private_key_jwk": client.get_secret("fhir-private-key").value,
        "use_smart": True,
        "scope": "system/*.read"
    }
```

### Handling large datasets

For very large datasets, consider tuning the bulk export parameters:

```python
# For large datasets, increase timeout and concurrent downloads.
data_source = pc.read.bulk(
    fhir_endpoint_url=fhir_endpoint,
    types=resource_types,
    since=since,
    auth_config=auth_config,
    timeout=7200,  # 2 hour timeout for large datasets.
    max_concurrent_downloads=10
)
```

## Summary

This guide demonstrates how to build a simple yet powerful synchronisation
pipeline between FHIR bulk export endpoints and Delta tables using Pathling's
built-in capabilities. The solution provides:

- **Incremental updates**: Leverages the `since` parameter for efficient sync
- **Automatic merging**: Uses Pathling's `SaveMode.MERGE` for conflict
  resolution
- **Built-in retry logic**: The bulk client handles transient failures
  automatically
- **Minimal code**: Just a few dozen lines of core synchronisation logic
- **Production ready**: Includes scheduling, monitoring, and security
  considerations

By leveraging Pathling's built-in bulk export and Delta merge functionality, you
can maintain an up-to-date analytical data store with minimal custom code while
ensuring robust, scalable synchronisation with your FHIR server.
