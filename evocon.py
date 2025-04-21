from typing import Any, Optional, Iterator
import os
from datetime import datetime, timedelta
import logging

# Import dlt first
import dlt
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment from dlt config, default to 'prod' for GitHub Actions
ENVIRONMENT = dlt.config.get('runtime.environment') or 'prod'

def date_range(start_date: str, end_date: str) -> Iterator[tuple[str, str]]:
    """Generate pairs of dates for each day in the range."""
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end:
        current_str = current.strftime("%Y-%m-%d")
        yield current_str, current_str
        current += timedelta(days=1)

@dlt.source(name="evocon")
def evocon_source(start_date: str, end_date: str) -> Any:
    """
    Evocon API source with date range parameters
    """
    # Get credentials directly from dlt.secrets
    api_key = dlt.secrets['sources.evocon.api_key']
    api_secret = dlt.secrets['sources.evocon.secret']
    
    if not api_key or not api_secret:
        logger.error("API key or secret is missing from secrets.toml")
        raise ValueError("API credentials are not set properly")
    
    logger.info(f"API Key: {api_key[:5]}...{api_key[-5:]}")
    logger.info(f"API Secret: {api_secret[:5]}...{api_secret[-5:]}")
    
    # Create base configuration
    base_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.evocon.com/api/reports/",
            "auth": {
                "type": "http_basic",
                "username": api_key,
                "password": api_secret,
            },
        },
        "resources": [
            {
                "name": "oee",
                "endpoint": {
                    "path": "oee_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["shift_id"]
            },
            {
                "name": "losses",
                "endpoint": {
                    "path": "losses_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["id"]
            },
            {
                "name": "scrap",
                "endpoint": {
                    "path": "scrap_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["shift_id", "date", "station", "product_code", "scrap_reason_name"]
            },
            {
                "name": "downtime",
                "endpoint": {
                    "path": "downtime_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["stop_instance_id"]
            },
            {
                "name": "checklists",
                "endpoint": {
                    "path": "checklists_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["shift_id", "date", "station", "name", "itemname"]
            },
            {
                "name": "quantity",
                "endpoint": {
                    "path": "quantity_json",
                    "params": {
                        "startTime": start_date,
                        "endTime": end_date,
                    }
                },
                "primary_key": ["id"]
            },
            #NO DATA SEEN ON THIS ENDPOINT
            # {
            #     "name": "client_metrics",
            #     "endpoint": {
            #         "path": "clientmetrics_json",
            #         "params": {
            #             "startTime": start_date,
            #             "endTime": end_date,
            #         }
            #     },
            # },
        ],
    }
    
    # Create source once with the full date range
    source = rest_api_source(base_config)
    
    # Yield all resources
    yield source.resources["oee"]
    yield source.resources["losses"]
    yield source.resources["scrap"]
    yield source.resources["downtime"]
    yield source.resources["checklists"]
    yield source.resources["quantity"]
    # yield source.resources["client_metrics"]

def load_evocon_data(start_date: Optional[str] = None, end_date: Optional[str] = None, write_disposition: str = "merge", environment: str = "dev") -> None:
    """
    Load data from Evocon API with a one-day overlap to catch retroactive updates
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format. Defaults to 2 days ago.
        end_date (str, optional): End date in YYYY-MM-DD format. Defaults to today.
        write_disposition (str): Write disposition for the pipeline. Defaults to "merge".
        environment (str): Environment to run the pipeline in ('dev' or 'prod'). Defaults to 'dev'.
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Add environment suffix for dev
    dataset_name = f"evocon{'_staging' if ENVIRONMENT == 'dev' else ''}"
    logger.info(f"Running pipeline in {ENVIRONMENT} environment using dataset {dataset_name}")
    
    pipeline = dlt.pipeline(
        pipeline_name="evocon_pipeline",
        destination='snowflake',
        dataset_name=dataset_name,  # This will create a separate schema in Snowflake
        export_schema_path="schemas/export"
    )

    load_info = pipeline.run(
        evocon_source(start_date, end_date),
        write_disposition=write_disposition
    )
    print(load_info)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--full-refresh', action='store_true', help='Perform a full refresh of all data')
    parser.add_argument('--environment', choices=['dev', 'prod'], default='dev', help='Environment to run the pipeline in')
    args = parser.parse_args()
    
    load_evocon_data(
        start_date=args.start_date,
        end_date=args.end_date,
        write_disposition="replace" if args.full_refresh else "merge",
        environment=args.environment
    )
