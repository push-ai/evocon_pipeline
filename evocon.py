from typing import Any, Optional
import os
from datetime import datetime, timedelta
import logging

# Import dlt first
import dlt
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Remove the dlt.config import since it's included in the main dlt package
# import dlt.config

# Remove this line since secrets are loaded automatically
# dlt.config.set_secrets_from_toml()

# Function to get the date range for the API call
def get_date_range():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return start_date, end_date

@dlt.source(name="evocon")
def evocon_source() -> Any:
    start_date, end_date = get_date_range()
    
    # Get credentials directly from dlt.secrets
    api_key = dlt.secrets['sources.evocon.api_key']
    api_secret = dlt.secrets['sources.evocon.secret']
    
    if not api_key or not api_secret:
        logger.error("API key or secret is missing from secrets.toml")
        raise ValueError("API credentials are not set properly")
    
    logger.info(f"API Key: {api_key[:5]}...{api_key[-5:]}")  # Log part of the key for verification
    logger.info(f"API Secret: {api_secret[:5]}...{api_secret[-5:]}")  # Log part of the secret for verification
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.evocon.com/api/reports/",
            "auth": {
                "type": "http_basic",
                "username": api_key,
                "password": api_secret,
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "startTime": start_date,
                    "endTime": end_date,
                },
            },
        },
        "resources": [
            {
                "name": "oee",
                "endpoint": {
                    "path": "oee_json",
                },
            },
            {
                "name": "losses",
                "endpoint": {
                    "path": "losses_json",
                },
            },
            {
                "name": "client_metrics",
                "endpoint": {
                    "path": "clientmetrics_json",
                },
            },
        ],
    }

    return rest_api_source(config)

def load_evocon_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="evocon_pipeline",
        destination='snowflake',
        dataset_name="evocon",
    )

    load_info = pipeline.run(evocon_source())
    print(load_info)

if __name__ == "__main__":
    load_evocon_data()
