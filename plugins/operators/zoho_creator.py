import time
from typing import Any, Dict

import pandas as pd
from pandas import json_normalize
from airflow.models import BaseOperator

from plugins.hooks.zoho_creator import ZohoCreatorHook

class ZohoCreatorOperator(BaseOperator):
    def __init__(
        self, dataset, app_name, view_name, table_name, bucket_name, *args, **kwargs
    ):
        super(ZohoCreatorOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.view_name = view_name
        self.app_name = app_name
        self.dataset = dataset
        self._creator_hook = None

    @property
    def creator_hook(self) -> ZohoCreatorHook:
        if self._creator_hook is None:
            self._creator_hook = ZohoCreatorHook()
        return self._creator_hook

    def paginate(self) -> pd.DataFrame:
        df = pd.DataFrame()
        status = 3000
        start = 0
        while status != 3100:
            # Make Api Call
            data = self.creator_hook.get_view(self.app_name, self.view_name, start)
            # Handle No Data
            if data["code"] == 3100:
                break
            # Parse Data
            data_df = json_normalize(data["data"])
            # Save Appended DF
            df = df.append(data_df, ignore_index=True)
            start = start + 200
            status = data["code"]
            time.sleep(3.5)
        return df

    def download_bulk_data(self):
        job = self.creator_hook.create_bulk_api_read_job(
            app_name=self.app_name, view_name=self.view_name
        )
        collector, download = pd.DataFrame(), 1
        cursor = "init"
        # Continue to Download Files until no more
        while cursor:
            self.creator_hook.log.info(f"Page: {download} Cursor: {cursor}")
            data = self.creator_hook.download_workflow(job)
            collector = collector.append(data.get("df"))
            cursor = data.get("cursor")
            job = self.creator_hook.create_bulk_api_read_job(
                app_name=self.app_name, view_name=self.view_name, cursor=cursor
            )
            download = download + 1
        return collector.drop_duplicates()


    def execute(self, context: Dict[str, Any]) -> str:
        message = (
            f"App_Name:: {self.app_name}, "
            f"View Name:: {self.view_name}, Table Name:: {self.table_name}"
        )
        self.creator_hook.log.info(message)
        creator_data = self.download_bulk_data()
        # Use your method for pushing data to Cloud Storage or database

        return message
