"""
dashboard_data.py
-----------------
API helpers that power the dashboard KPI cards.
Each function is called from the /api/dashboard/counts Flask route
when the user opens the dashboard page.
"""

import requests
from flask import (
    Flask, render_template, request, jsonify, send_file,
    session, redirect, url_for, flash
)
import os
import boto3
import os
import re
import json
import pandas as pd
from salesforcecdpconnector.connection import SalesforceCDPConnection


SALESFORCE_LOGIN_URL = os.getenv("SALESFORCE_LOGIN_URL", "https://mimit.my.salesforce.com")
TOKEN_PATH           = "/services/oauth2/token"      




class Get_Dashboard_KPIS:
    def __init__(self,a,b):
        self.a = a
        self.b = b
    
    def get_secret(self,secret_name: str, region_name: str):
        """Fetch client_id, username, client_secret from AWS Secrets Manager."""
        boto_session = boto3.session.Session()
        client = boto_session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )
        response = client.get_secret_value(SecretId=secret_name)
        secret   = json.loads(response["SecretString"])

        pattern = r"-{5,}\n(.*?)\n-{5,}"

        client_id = secret["client_id"]
        match     = re.search(pattern, client_id, re.DOTALL)
        client_id = match.group(1)

        client_secret = secret["client_secret"]
        match         = re.search(pattern, client_secret, re.DOTALL)
        client_secret = match.group(1)

        username = secret["USERNAME"]
        return client_id, username, client_secret

    def get_data_stream_counts(self,client_id, username, client_secret):
        
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        total_datastream = 0
        next_url = f"{instance_url.rstrip('/')}/services/data/v64.0/ssot/data-streams"
        resp = requests.get(next_url, headers=headers, timeout=30)
        payload = resp.json()
        total_datastream =  payload.get("totalSize")
        return total_datastream
    
    def get_data_lakeobject_counts(self,client_id, username, client_secret):
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        total_datalakeobjects= 0
        next_url = f"{instance_url.rstrip('/')}/services/data/v64.0/ssot/data-lake-objects"
        resp = requests.get(next_url, headers=headers)
        payload = resp.json()
        total_datalakeobjects =  payload.get("totalSize")
        return total_datalakeobjects
    
    def get_calculated_insights_counts(self,client_id, username, client_secret):
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        total_calculated_insights= 0
        next_url = f"{instance_url.rstrip('/')}/services/data/v64.0/ssot/calculated-insights"
        resp = requests.get(next_url, headers=headers)
        payload = resp.json()
        total_calculated_insights = payload["collection"]["total"]
        return total_calculated_insights
    
    def get_unique_profile_counts(self,client_id, username, client_secret):
        conn = SalesforceCDPConnection(
        login_url='https://mimit.my.salesforce.com',
        client_id=client_id, 
        username=username, 
        client_secret=client_secret
        )
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) AS profile_count FROM UnifiedIndividual__dlm')
        result = cur.fetchone()
        #results = cur.fetchall()
        total_unique_profiles= result[0]
        return total_unique_profiles
    
    def get_total_segments(self,client_id, username, client_secret):
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        total_segments= 0
        next_url = f"{instance_url.rstrip('/')}/services/data/v64.0/ssot/segments"
        resp = requests.get(next_url, headers=headers)
        payload = resp.json()
        total_segments =  payload.get("totalSize")
        return total_segments
    
    def get_All_data_data_stream(self,client_id, username, client_secret):
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        next_url = f"{instance_url.rstrip('/')}/services/data/v64.0/ssot/data-streams?limit=200&offset=0"
        seen_urls = set()
        all_data_streams = []

        while next_url:
            # make relative nextPageUrl absolute before checking duplicates
            if next_url.startswith("/"):
                next_url = instance_url.rstrip("/") + next_url

            # stop if same URL comes again
            if next_url in seen_urls:
                print(f"Duplicate nextPageUrl detected, stopping loop: {next_url}")
                break

            seen_urls.add(next_url)
            print(next_url)

            resp = requests.get(next_url, headers=headers)
            resp.raise_for_status()
            page_data = resp.json()

            # append only the records
            all_data_streams.extend(page_data.get("dataStreams", []))

            # get next page only from current response
            next_url = page_data.get("nextPageUrl") or page_data.get("next") or None

            df = pd.DataFrame(all_data_streams)
            df.to_csv("DataStream.csv", index=False)
        return df

    
if __name__ == "__main__": 
    Get_Dashboard_KPI_obj = Get_Dashboard_KPIS("a","b")
    client_id, username, client_secret = Get_Dashboard_KPI_obj.get_secret("studycast-integration-access-secret","us-east-1")
    total_datastreams= Get_Dashboard_KPI_obj.get_data_stream_counts(client_id, username, client_secret)
    #total_datalakeobjects= Get_Dashboard_KPI_obj.get_data_lakeobject_counts(client_id, username, client_secret)
    total_calculated_insights = total_calculated_insights= Get_Dashboard_KPI_obj.get_calculated_insights_counts(client_id, username, client_secret)
    total_unique_profiles = Get_Dashboard_KPI_obj.get_unique_profile_counts(client_id, username, client_secret)
    total_segments = Get_Dashboard_KPI_obj.get_total_segments(client_id, username, client_secret)
    df = Get_Dashboard_KPI_obj.get_All_data_data_stream(client_id, username, client_secret)
    print("Data Streams",total_datastreams)
    #print(total_datalakeobjects)
    print("Calculated Insights", total_calculated_insights)
    print("Unique Profiles", total_unique_profiles)
    print("Total Segments", total_segments)


# while next_url:
    #     print(next_url)
       
    #     print(resp.status_code)
    #     

    #     items = (
    #         payload.get("items")
    #         or payload.get("data")
    #         or payload.get("records")
    #         or payload.get("dataStreams", [])
    #         or []
    #     )
    #     total += len(items)

    #     next_url = payload.get("nextPageUrl") or payload.get("next") or None
    #     if next_url and next_url.startswith("/"):
    #         next_url = instance_url.rstrip("/") + next_url