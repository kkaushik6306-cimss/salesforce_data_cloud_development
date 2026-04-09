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
import ast
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
        active_calculated_insights = payload["collection"]["count"]
        return active_calculated_insights,total_calculated_insights
    
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

    def create_dashboard_KPI_csv(self,total_datastreams,total_datalakeobjects,active_calculated_insights,
                                 total_calculated_insights,total_unique_profiles,total_segments):
        dashboard_KPIs = {}
        dashboard_KPIs["Total DS"] = total_datastreams
        dashboard_KPIs["Total DLO"] = 1350
        dashboard_KPIs["Total DMO"] = 1625
        dashboard_KPIs["Total CI"]= total_calculated_insights
        dashboard_KPIs["Active CI"]= active_calculated_insights
        dashboard_KPIs["Total UP"] = total_unique_profiles
        dashboard_KPIs["Total Seg"] = total_segments
        dashboard_KPIs["Total Connections"] = 10
        dashboard_df = pd.DataFrame([dashboard_KPIs])                       
        dashboard_df.to_csv('Dashboard.csv',index=False)
        return dashboard_df
        
    def get_KPIs(self):
        df = pd.read_csv('Dashboard.csv')
        total_ds = df['Total DS']
        total_dlo = df['Total DLO']
        total_dmo = df['Total DMO']
        total_ci = df['Total CI']
        active_ci = df['Active CI']
        total_up = df['Total UP']
        total_seg = df['Total Seg']
        total_conn = df['Total Connections']
        return total_ds,total_dlo,total_dmo,total_ci,active_ci,total_up,total_seg,total_conn

    def get_informationfrom_datastream_csv(self):
        df = pd.read_csv('DataStream.csv')
        active_datastream = df.loc[df['status']=='ACTIVE'].count()
        error_datastream = df.loc[df['status']=='ERROR'].count()
        df['lastRefreshDate'] = pd.to_datetime(df["lastRefreshDate"], format="mixed", errors='coerce').dt.date
        df['lastProcessedRecords'] = pd.to_numeric(df['lastProcessedRecords'], errors='coerce').fillna(0)
        # Get today's date
        today = pd.Timestamp.today().date()
        # Filter and sum
        today_sum = df.loc[df['lastRefreshDate'] == today, 'lastProcessedRecords'].sum()
        # Get Last 14 Days Volume Ingestion Summary
        last_14_days = today - pd.Timedelta(days=13)
        # Filter last 14 days
        df_filtered = df[df['lastRefreshDate'] >= last_14_days]
        # Group by date and sum volume
        result_df = (
            df_filtered
            .groupby(df_filtered['lastRefreshDate'])['lastProcessedRecords']
            .sum()
            .reset_index()
        )
        # Rename columns for clarity
        result_df.columns = ['Date', 'Total Volume']
        # Sort by date
        daily_ingestion_df = result_df.sort_values(by='Date')
        return active_datastream,error_datastream,today_sum,daily_ingestion_df
    
    def Get_category_datastream_dataframe(self):
        df = pd.read_csv('DataStream.csv')
        def Get_category(x):
            #catg_dict = json.loads(x)
            #catg_dict = dict(x)
            catg_dict = ast.literal_eval(x)
            category_name = catg_dict['category']
            return category_name
        df['Type'] = df['dataLakeObjectInfo'].apply(Get_category)
        filtered_datastream_df = df[['name','Type','totalRecords','status']]
        filtered_datastream_df.columns = ['Stream Name','Type','Records','Status']
        return filtered_datastream_df
    
if __name__ == "__main__": 
    Get_Dashboard_KPI_obj = Get_Dashboard_KPIS("a","b")
    client_id, username, client_secret = Get_Dashboard_KPI_obj.get_secret("studycast-integration-access-secret","us-east-1")
    total_datastreams= Get_Dashboard_KPI_obj.get_data_stream_counts(client_id, username, client_secret)
    total_datalakeobjects= Get_Dashboard_KPI_obj.get_data_lakeobject_counts(client_id, username, client_secret)
    active_calculated_insights,total_calculated_insights= Get_Dashboard_KPI_obj.get_calculated_insights_counts(client_id, username, client_secret)
    total_unique_profiles = Get_Dashboard_KPI_obj.get_unique_profile_counts(client_id, username, client_secret)
    total_segments = Get_Dashboard_KPI_obj.get_total_segments(client_id, username, client_secret)
    df = Get_Dashboard_KPI_obj.get_All_data_data_stream(client_id, username, client_secret)
    dashboard_df = Get_Dashboard_KPI_obj.create_dashboard_KPI_csv(total_datastreams,1350,active_calculated_insights,
                                 total_calculated_insights,total_unique_profiles,total_segments)

    total_ds,total_dlo,total_dmo,total_ci,active_ci,total_up,total_seg,total_conn = Get_Dashboard_KPI_obj.get_KPIs()                                                                              
    active_datastream,error_datastream,today_sum,daily_ingestion_df = Get_Dashboard_KPI_obj.get_informationfrom_datastream_csv()