import io
import json
import time
import zipfile

from datetime import datetime

import pandas as pd
import requests
from airflow.hooks.base_hook import BaseHook
from pandas import json_normalize

report_creator = "username_of_report_creator"

class ZohoCreatorHook(BaseHook):
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.endpoint = "https://creator.zoho.eu/api/v2/"
        self.bulk_endpoint = "https://creator.zoho.eu/api/bulk/v2"
        self._token = {
            "token": None,
            "created_at": datetime(2019, 1, 1, 0, 0, 0, 0),  # init datetime object
        }

    def get_conn(self):
        """
        Use a Defined Airflow connection for Zoho Creator. 
        The following fields should be populated in Airflow Connection Definition
        login: client_id
        password: client_secret
        Extra: {"refresh_token": "XXXXXXXX"}
        """
        return self.get_connection(self.conn_id)

    def get_token(self) -> dict:
        """
        Check for Auth Token. Get an auth token if there is not valid token
        Generate an Auth Token with Zoho Creator documentation
        https://www.zoho.com/creator/help/api/v2/oauth-overview.html
        """
        if (datetime.utcnow() - self._token["created_at"]).total_seconds() <= 3600:
            return self._token
        conn = self.get_conn()
        params = {
            "refresh_token": json.loads(conn.get_extra())["refresh_token"],
            "client_secret": conn.password,
            "grant_type": "refresh_token",
            "client_id": conn.login,
        }
        url = "https://accounts.zoho.eu/oauth/v2/token"
        new_token = requests.post(url=url, params=params).json()
        self._token = {
            "token": new_token["access_token"],
            "created_at": datetime.utcnow(),
        }
        return self._token

    def get_headers(self) -> dict:
        token = self.get_token()["token"]
        return {"Authorization": f"Zoho-oauthtoken {token}"}

    def create_bulk_api_read_job(
        self, app_name: str, view_name: str, cursor: str = None
    ):
        """
        This method creates the Bulk API Read Job to be downloaded
        """
        bulk_url = f"{self.bulk_endpoint}/{report_creator}/{app_name}/report/{view_name}/read"
        query = {"max_records": 200000}
        if cursor:
            query["record_cursor"] = cursor
        response = requests.post(
            url=bulk_url, headers=self.get_headers(), json={"query": query}
        )
        response.raise_for_status()
        job_id = response.json().get("details").get("id")
        self.log.info(f"Zoho Creator Bulk Read Job Id: {job_id}")
        return {"url": bulk_url, "job_id": job_id, "cursor": cursor}

    def check_bulk_job(self, job):
        """
        Method checks the status of Bulk API Job.
        It takes about 60 seconds to be ready for download
        """
        response = requests.get(
            url=f"{job.get('url')}/{job.get('job_id')}",
            headers=self.get_headers(job.get("cursor")),
        ).json()
        return response

    def download_data(self, download_url):
        """
        Method downloads compressed file when ready.
        The compressed file is contain in response bytes
        """
        response = requests.get(
            url=f"https://creator.zoho.eu{download_url}",
            headers=self.get_headers(),
            stream=True,
        )
        response.raise_for_status()
        return response

    def download_workflow(self, job):
        """
        Workflow for checking the status of a bulk job. When download is ready.
        The workflow downloads file and returns bytes with record cursor
        """
        status = self.check_bulk_job(job)
        # Wait for file to be ready
        while status.get("details.status"):
            # Download if ready
            if status.get("details.status") == "Completed":
                self.log.info("Downloading Compressed File...")
                result = status.get("details.result")
                response = self.download_data(
                    download_url=result.get("download_url")
                )
                return {
                    "df": self.extract_zip_file_as_df(databytes=response.content),
                    "cursor": result.get("record_cursor"),
                }
            self.log.info("Waiting for File Status")
            time.sleep(30)
            status = self.check_bulk_job(job)
            self.log.info(status.get("details"))

    @staticmethod
    def extract_zip_file_as_df(databytes):
        """
        This Static method converts bytes to Zip File and loads as DataFrame
        param: databytes is the bytes returned from Zoho Creator API response.content
        """
        with tempfile.TemporaryDirectory():
            data = zipfile.ZipFile(io.BytesIO(databytes))
            location = tempfile.gettempdir()
            data.extractall(location)
            return pd.read_csv(f"{location}/{data.filelist[0].filename}")

    def get_view(self, app_name: str, view_name: str, start=0) -> dict:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.report.READ"
        view_url = self.endpoint + f"{report_creator}/{app_name}/report/{view_name}"
        response = requests.get(
            url=view_url, headers=headers, params={"from": start, "limit": 200}
        )
        self.log.info(response.url)
        return response.json()

    def get_apps(self) -> pd.DataFrame:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.dashboard.READ"
        data = requests.get(url=self.endpoint + "applications", headers=headers).json()
        return json_normalize(data["applications"])

    def get_forms(self, app_name: str) -> pd.DataFrame:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.meta.application.READ"
        url = f"{self.endpoint}{report_creator}/{app_name}/forms"
        data = requests.get(url=url, headers=headers).json()

        goods = list()
        for i in data["forms"]:
            details = (app_name, i.get("display_name"), i.get("link_name"))
            goods.append(details)
        df = pd.DataFrame(goods, columns=["App", "display_name", "link_name"])
        return df

    def get_reports(self, app_name: str) -> pd.DataFrame:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.meta.application.READ"
        url = f"{self.endpoint}{report_creator}/{app_name}/reports"
        data = requests.get(url=url, headers=headers).json()

        goods = list()
        for i in data["reports"]:
            details = (app_name, i.get("display_name"), i.get("link_name"))
            goods.append(details)
        df = pd.DataFrame(goods, columns=["App", "display_name", "link_name"])
        return df

    def create_record(self, app_name: str, form_name: str, params: dict) -> dict:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.form.CREATE"
        response = requests.post(
            url=self.endpoint + f"{report_creator}/{app_name}/form/{form_name}",
            headers=headers,
            json={"data": params},
        )
        self.log.info(response.url)
        return response.json()

    def update_record(
        self, app_name: str, report_name: str, record: str, params: dict
    ) -> dict:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.report.UPDATE"
        url = f"{self.endpoint}{report_creator}/{app_name}/report/{report_name}"

        response = requests.patch(
            url=url,
            headers=headers,
            json={"criteria": f"ID = {record}", "data": params},
        )
        self.log.info(response.url)
        return response.json()

    def delete_record(self, app_name: str, report_name: str, record: str) -> dict:
        headers = self.get_headers()
        headers["scope"] = "ZohoCreator.report.DELETE"
        url = f"{self.endpoint}{report_creator}/{app_name}/report/{report_name}"
        response = requests.delete(
            url=url, headers=headers, json={"criteria": f"ID = {record}"}
        )
        self.log.info(response.url)
        return response.json()

   
