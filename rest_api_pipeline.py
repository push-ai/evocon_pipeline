from typing import Any, Optional
import os
from datetime import datetime, timedelta

import dlt
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig

# Function to get the date range for the API call
def get_date_range():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return start_date, end_date

@dlt.source(name="evocon")
def evocon_source(api_key: Optional[str] = dlt.secrets.value) -> Any:
    start_date, end_date = get_date_range()
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.evocon.com/api/reports/",
            "auth": {
                "type": "http_basic",
                "username": dlt.secrets.get("sources.evocon.api_key"),
                "password": dlt.secrets.get("sources.evocon.secret"),
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
        dataset_name="evocon_data",
    )

    load_info = pipeline.run(evocon_source())
    print(load_info)

if __name__ == "__main__":
    load_evocon_data()
