import io
import os
import re
import json
import tempfile
from datetime import datetime
from functools import wraps

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from salesforcecdpconnector.connection import SalesforceCDPConnection
from flask import (
    Flask, render_template, request, jsonify, send_file,
    session, redirect, url_for, flash
)
from dashboard_data import Get_Dashboard_KPIS

app = Flask(__name__)
app.secret_key = "sfdc-datastream-secret-key"

# -----------------------------------------------------------------------------
# Version — update here to reflect the new version everywhere
# -----------------------------------------------------------------------------
APP_VERSION = "1.2"

@app.context_processor
def inject_version():
    return {"app_version": APP_VERSION}

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
SALESFORCE_LOGIN_URL = os.getenv("SALESFORCE_LOGIN_URL", "https://mimit.my.salesforce.com")
TOKEN_PATH           = "/services/oauth2/token"

APP_USERNAME = "admin"
APP_PASSWORD = "dc@6306"

# Mapping from UI object_type → Salesforce entityType query param
ENTITY_TYPE_MAP = {
    "DataLakeObject":     "DataLakeObject",
    "DataModelObject":    "DataModelObject",
    "CalculatedInsights": "CalculatedInsight",
    "Profiles":           "Profile",
}

CDP_BASE_URL       = "https://mq3gmzldh0ywgzlgm0ytqnbxgm.c360a.salesforce.com"
VALID_OBJECT_TYPES  = set(ENTITY_TYPE_MAP.keys())
VALID_CATEGORIES    = {"Profile", "Engagement", "Related"}


# -----------------------------------------------------------------------------
# Auth guard
# -----------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Please log in to continue.", "warning")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


# -----------------------------------------------------------------------------
# Salesforce helpers
# -----------------------------------------------------------------------------
class SalesforceAuthError(Exception):
    pass

def Get_Data_SFData_Cloud(client_id, username, client_secret, object_api_name):    
    try:
        conn = SalesforceCDPConnection(
            login_url='https://mimit.my.salesforce.com',
            client_id=client_id,
            username=username,
            client_secret=client_secret,
        )
        query = f'SELECT * FROM {object_api_name}'
        print('Here')
        df = conn.get_pandas_dataframe(query)
        print(df.head())
        return df
    except ClientError as exc:
        app.logger.error("ClientError in Get_Data_SFData_Cloud: %s", exc.response['Error']['Message'], exc_info=True)
        flash(f"Unable to Extract Data: {exc.response['Error']['Message']}", "danger")
        return redirect(url_for("aws_config"))


def get_secret(secret_name: str, region_name: str):
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


def get_access_token(client_id: str, client_secret: str):
    """OAuth 2.0 Client Credentials flow."""
    token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(token_url, data=payload, timeout=30)
    if resp.status_code != 200:
        raise SalesforceAuthError(
            f"Token request failed ({resp.status_code}): {resp.text}"
        )
    data = resp.json()
    if "access_token" not in data or "instance_url" not in data:
        raise SalesforceAuthError(f"Unexpected token response: {data}")
    return resp


def get_data360_token(client_id: str, client_secret: str):
    """Exchange a core Salesforce token for a Data Cloud (CDP) token."""
    core_resp         = get_access_token(client_id, client_secret)
    core_access_token = core_resp.json()["access_token"]
    core_instance_url = core_resp.json()["instance_url"]

    data = {
        "grant_type":         "urn:salesforce:grant-type:external:cdp",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token":      core_access_token,
    }
    cdp_url  = core_instance_url + "/services/a360/token"
    cdp_resp = requests.post(cdp_url, data=data, timeout=30)
    cdp_resp.raise_for_status()
    return cdp_resp


def get_data_streams(client_id: str, client_secret: str, api_url: str):
    """Fetch metadata objects from Data Cloud using the pre-resolved API URL."""
    dc_token     = get_data360_token(client_id, client_secret)
    access_token = dc_token.json()["access_token"]
    instance_url = dc_token.json()["instance_url"]

    headers = {"Authorization": f"Bearer {access_token}"}
    resp    = requests.get(api_url, headers=headers, timeout=30)

    if resp.status_code == 200:
        return {
            "working_url": api_url,
            "payload":     resp.json(),
            "token_info":  {"instance_url": instance_url},
        }
    raise Exception(
        f"Data 360 endpoint returned {resp.status_code}: {resp.text[:500]}"
    )


def dashboard_functions():
    kpi_obj = Get_Dashboard_KPIS("a", "b")
    total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up, total_seg, total_conn = kpi_obj.get_KPIs()
    active_datastream,error_datastream,today_sum,daily_ingestion_df = kpi_obj.get_informationfrom_datastream_csv()
    filtered_datastream_df = kpi_obj.Get_category_datastream_dataframe()
    refreshmode_counts_datastream = kpi_obj.refreshmode_counts_datastream()
    # Extract scalar from pandas Series if needed
    def _val(v):
        return v.iloc[0] if hasattr(v, 'iloc') else v
    active_rate = _val(active_datastream)/_val(total_ds)
    active_rate = f"{active_rate:.2%}"
    inactive_rate = _val(error_datastream)/_val(total_ds)
    inactive_rate = f"{inactive_rate:.2%}"
    active_ci_rate = _val(active_ci)/_val(total_ci)                                                    
    active_ci_rate = f"{active_ci_rate:.2%}"

    return total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up, total_seg,total_conn,active_datastream,error_datastream,today_sum,active_rate,inactive_rate,active_ci_rate,daily_ingestion_df,filtered_datastream_df,refreshmode_counts_datastream



# -----------------------------------------------------------------------------
# Routes — Authentication
# -----------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    if session.get("logged_in"):
        return redirect(url_for("aws_config"))
    return render_template("index.html")


@app.route("/login", methods=["POST"])
def login():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()

    if username == APP_USERNAME and password == APP_PASSWORD:
        session["logged_in"] = True
        session["app_user"]  = username
        return redirect(url_for("aws_config"))

    flash("Invalid username or password.", "danger")
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been signed out.", "info")
    return redirect(url_for("index"))


# -----------------------------------------------------------------------------
# Routes — AWS Config & Data
# -----------------------------------------------------------------------------
@app.route("/aws-config", methods=["GET"])
@login_required
def aws_config():
    return render_template("aws_config.html")


@app.route("/dashboard", methods=["GET"])
@login_required
def dashboard():
    total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up,total_seg,total_conn,active_datastream,error_datastream,today_sum,active_rate,inactive_rate,active_ci_rate,daily_ingestion_df,filtered_datastream_df,refreshmode_counts_datastream = dashboard_functions()
    print(filtered_datastream_df.head())
    def _val(v):
        return v.iloc[0] if hasattr(v, 'iloc') else v
    today_sum = f"{today_sum/1000:.1f}K"
    ingestion_dates = [str(d) for d in daily_ingestion_df['Date'].tolist()]
    ingestion_volumes = [float(v) for v in daily_ingestion_df['Total Volume'].tolist()]
    return render_template(
        "dashboard.html",
        ingestion_dates=ingestion_dates,
        ingestion_volumes=ingestion_volumes,
        total_ds=_val(total_ds),
        total_dlo=_val(total_dlo),
        total_dmo=_val(total_dmo),
        total_ci=_val(total_ci),
        active_ci = _val(active_ci),
        total_up=_val(total_up),
        total_seg=_val(total_seg),
        total_conn=_val(total_conn),
        active_datastream = int(_val(active_datastream)),error_datastream=int(_val(error_datastream)),
        active_rate = active_rate,active_ci_rate=active_ci_rate,
        inactive_rate = _val(inactive_rate),today_sum = _val(today_sum),
        datastream_records=filtered_datastream_df.to_dict('records'),
        refresh_mode_labels=[str(v) for v in refreshmode_counts_datastream['Refresh_Mode'].tolist()],
        refresh_mode_values=[int(v) for v in refreshmode_counts_datastream['count'].tolist()],
    )


# -----------------------------------------------------------------------------
# Routes — Create Data Objects
# -----------------------------------------------------------------------------
@app.route("/create-objects", methods=["POST"])
@login_required
def create_objects():
    return jsonify({
        "status":  "under_development",
        "message": "Functionality is under development. We will update you soon.",
    }), 501

# -----------------------------------------------------------------------------
# Routes — Delete Data Objects
# -----------------------------------------------------------------------------
@app.route("/delete-stream", methods=["POST"])
@login_required
def delete_stream():
    
    stream_name = request.form.get("stream_name", "").strip()
    delete_dlo  = request.form.get("delete_dlo") == "1"

    if not stream_name:
        return jsonify({"success": False, "title": "Validation Error",
                        "message": "Data Stream name is required."}), 400
    secret_name     = request.form.get("secret_name",     "").strip()
    region_name     = request.form.get("region_name",     "").strip()
    #client_id, username, client_secret = get_secret(secret_name, region_name)
    client_id     = session.get("client_id")
    client_secret = session.get("client_secret")
    if not client_id or not client_secret:
        return jsonify({"success": False, "title": "Session Expired",
                        "message": "No active Salesforce session. Please load your AWS secret first."}), 401

    try:
        token_url = SALESFORCE_LOGIN_URL.rstrip("/") + TOKEN_PATH
        payload = {
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
        resp = requests.post(token_url, data=payload, timeout=30)
        
        #dc_token     = get_data360_token(client_id, client_secret)
        access_token = resp.json()["access_token"]
        instance_url = resp.json()["instance_url"]
        
    except Exception as exc:
        app.logger.error("Authentication failed in delete_stream: %s", exc, exc_info=True)
        return jsonify({"success": False, "title": "Authentication Failed",
                        "message": str(exc)}), 500
    if delete_dlo:
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        # ── API path 1: Delete Data Stream AND its associated Data Lake Objects ──
        # Delete DMO First
        print(stream_name)
        DMO_NAME = stream_name.replace("RDS","amd_pm")
        DMO_NAME = DMO_NAME+'__dlm'
        #DMO_NAME = "amd_pm_vw_ODBC_actv_InstHealthInfo"
        api_url = f"{instance_url}/services/data/v64.0//ssot/data-model-objects/{DMO_NAME}"
        print(api_url)
        dmo_resp = requests.delete(
            api_url,
            headers=headers,data=payload
        )
        if dmo_resp.status_code == 500:
            DMO_NAME = stream_name.replace("RDS_","amd_pm-")
            DMO_NAME = DMO_NAME+'__dlm'
            api_url = f"{instance_url}/services/data/v64.0//ssot/data-model-objects/{DMO_NAME}"
            print(api_url)
            dmo_resp = requests.delete(
                api_url,
                headers=headers,data=payload
            )
        #Delete Data Stream & DLO
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        api_url = f"{instance_url}/services/data/v64.0/ssot/data-streams/{stream_name}?shouldDeleteDataLakeObject=True"
        stream_resp = requests.delete(
            api_url,
            headers=headers,data=payload
        )
        if stream_resp.status_code not in (200, 204):
            return jsonify({
                "success": False,
                "title":   "Stream Deletion Failed",
                "message": f"HTTP {stream_resp.status_code}: {stream_resp.text[:300]}",
            })
        return jsonify({
            "success": True,
            "title":   "Stream Deleted",
            "message": f"Data Stream & DMO '{stream_name}' has been deleted successfully.",
        })
    else:
        # ── API path 2: Delete Data Stream only ──
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        api_url = f"https://mimit.my.salesforce.com/services/data/v64.0/ssot/data-streams/{stream_name}?shouldDeleteDataLakeObject=True"
        stream_resp = requests.delete(
            api_url,
            headers=headers,
            timeout=30,
        )
        if stream_resp.status_code not in (200, 204):
            return jsonify({
                "success": False,
                "title":   "Stream Deletion Failed",
                "message": f"HTTP {stream_resp.status_code}: {stream_resp.text[:300]}",
            })

        return jsonify({
            "success": True,
            "title":   "Stream Deleted",
            "message": f"Data Stream '{stream_name}' has been deleted successfully.",
        })

# -----------------------------------------------------------------------------
# Routes — Load Secret keys from AWS
# -----------------------------------------------------------------------------
@app.route("/load-secret", methods=["POST"])
@login_required
def load_secret():
    secret_name     = request.form.get("secret_name",     "").strip()
    region_name     = request.form.get("region_name",     "").strip()
    object_type     = request.form.get("object_type",     "").strip()
    entity_category = request.form.get("entity_category", "").strip()
    action          = "get_data_schema"


    if not secret_name or not region_name:
        flash("Secret name and AWS region are required.", "danger")
        return redirect(url_for("aws_config"))

    if object_type not in VALID_OBJECT_TYPES:
        flash("Please select a valid object type.", "danger")
        return redirect(url_for("aws_config"))

    if entity_category not in VALID_CATEGORIES:
        flash("Please select a valid category.", "danger")
        return redirect(url_for("aws_config"))

    # -------------------------------------------------------------------------
    # Resolve the API URL based on the selected object type and category
    # -------------------------------------------------------------------------
    entity_type = ENTITY_TYPE_MAP[object_type]
    if entity_type == "Profile":
        api_url = (
            f"{CDP_BASE_URL}//api/v1/profile/metadata"
        )
    else:  
        api_url = (
            f"{CDP_BASE_URL}/api/v1/metadata"
            f"?entityType={entity_type}"
            f"&entityCategory={entity_category}"
        )
    app.logger.info(
        f"[load_secret] object_type={object_type!r}  category={entity_category!r}  url={api_url!r}"
    )
    # -------------------------------------------------------------------------

    try:
        client_id, username, client_secret = get_secret(secret_name, region_name)
    except ClientError as exc:
        app.logger.error("AWS ClientError in load_secret: %s", exc.response['Error']['Message'], exc_info=True)
        flash(f"AWS error: {exc.response['Error']['Message']}", "danger")
        return redirect(url_for("aws_config"))
    except Exception as exc:
        app.logger.error("Error retrieving secret in load_secret: %s", exc, exc_info=True)
        flash(f"Failed to retrieve secret: {exc}", "danger")
        return redirect(url_for("aws_config"))

    session["client_id"]       = client_id
    session["username"]        = username
    session["client_secret"]   = client_secret
    session["action"]          = action
    session["object_type"]     = object_type
    session["entity_category"] = entity_category
    session["api_url"]         = api_url
    return redirect(url_for("data_streams"))

# -----------------------------------------------------------------------------
# Routes — Extract MetaData
# -----------------------------------------------------------------------------
@app.route("/data-streams")
@login_required
def data_streams():
    if not session.get("client_id") or not session.get("client_secret"):
        if request.args.get("format") == "json":
            return jsonify({"success": False, "error": "Session expired. Please reconnect."}), 401
        flash("Please load your AWS secret first.", "warning")
        return redirect(url_for("aws_config"))

    # AJAX data request
    if request.args.get("format") == "json":\
    
        client_id     = session.get("client_id")
        client_secret = session.get("client_secret")
        #api_url       = session.get("api_url", f"{CDP_BASE_URL}/api/v1/metadata?entityType=DataLakeObject")
        api_url       = session.get("api_url")
        try:
            result  = get_data_streams(client_id, client_secret, api_url)
            payload = result["payload"]

            old_file = session.get("data_file")
            if old_file and os.path.exists(old_file):
                os.remove(old_file)
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(payload, tmp)
            tmp.close()
            session["data_file"] = tmp.name

            return jsonify({"success": True, "data": payload})
        except Exception as exc:
            app.logger.error("Error fetching data streams: %s", exc, exc_info=True)
            return jsonify({"success": False, "error": str(exc)}), 500

    # Page render
    return render_template(
        "streams.html",
        username=session.get("username", ""),
        action=session.get("action", "get_data_schema"),
        object_type=session.get("object_type", "DataLakeObject")
    )

# -----------------------------------------------------------------------------
# Routes — Download excel format
# -----------------------------------------------------------------------------
@app.route("/download/excel")
@login_required
def download_excel():
    data_file = session.get("data_file")
    if not data_file or not os.path.exists(data_file):
        flash("No data available — please load the data first.", "warning")
        return redirect(url_for("data_streams"))

    with open(data_file) as f:
        payload = json.load(f)

    records = (
        payload if isinstance(payload, list) else
        payload.get("metadata") or
        payload.get("data")     or
        payload.get("records")  or
        payload.get("dataLakeObjects") or
        payload.get("items")    or
        []
    )

    if not records:
        flash("No records to export.", "warning")
        return redirect(url_for("data_streams"))

    df = pd.DataFrame(records)
    for col in df.columns:
        df[col] = df[col].apply(
            lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
        )

    object_type = session.get("object_type", "DataLakeObject")
    sheet_name  = object_type[:31]

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)

    safe_name = object_type.lower().replace(" ", "_")
    filename  = f"{safe_name}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )

# -----------------------------------------------------------------------------
# Routes — Extract Data from Salesforce using Object API Name
# -----------------------------------------------------------------------------
@app.route("/extract-data", methods=["POST"])
@login_required
def extract_data():
    object_api_name = request.form.get("object_api_name", "").strip()
    print(object_api_name)
    if not object_api_name:
        return jsonify({"success": False, "message": "Object API name is required."}), 400
    
    secret_name     = request.form.get("secret_name",     "").strip()
    region_name     = request.form.get("region_name",     "").strip()
    if not secret_name or not region_name:
        flash("Secret name and AWS region are required.", "danger")
        return redirect(url_for("aws_config"))
    try:
        client_id, username, client_secret = get_secret(secret_name, region_name)
    except ClientError as exc:
        app.logger.error("AWS ClientError in extract_data: %s", exc.response['Error']['Message'], exc_info=True)
        flash(f"AWS error: {exc.response['Error']['Message']}", "danger")
        return redirect(url_for("aws_config"))
    except Exception as exc:
        app.logger.error("Error retrieving secret in extract_data: %s", exc, exc_info=True)
        flash(f"Failed to retrieve secret: {exc}", "danger")
        return redirect(url_for("aws_config"))

    if not client_id or not client_secret:
        return jsonify({"success": False, "message": "No active session. Please load your AWS secret first."}), 401

    try:
        df = Get_Data_SFData_Cloud(client_id, username, client_secret, object_api_name)
        print(df.head())
    except Exception as exc:
        app.logger.error("Error extracting data from Salesforce in extract_data: %s", exc, exc_info=True)
        return jsonify({"success": False, "message": str(exc)}), 500

    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    filename = f"{object_api_name}_{datetime.now().strftime('%Y-%m-%d')}.csv"
    return send_file(
        buf,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


# -----------------------------------------------------------------------------
# Routes — Data 360 Agent
# -----------------------------------------------------------------------------

CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "YOUR_CLAUDE_API_KEY_HERE")

DATA360_SYSTEM_PROMPT = """You are a Data 360 Expert AI Agent for the MIMIT Salesforce Production Organization (mimit.my.salesforce.com).

You help users understand, manage, and troubleshoot their Salesforce Data Cloud (Data 360) environment, and you can also query standard Salesforce CRM data using SOQL.

Key Data 360 Components you are knowledgeable about:
- Data Streams: Connectors that ingest data from source systems into Data Cloud. They define how data flows in.
- Data Lake Objects (DLO): Raw storage objects in the Data Lake layer — the landing zone for ingested data.
- Data Model Objects (DMO): Harmonized objects in the Data Model layer that map to standard or custom entities.
- Calculated Insights (CI): SQL-based computed metrics and analytics built on top of unified profiles and DMOs.
- Unified Profiles: Customer 360 profile objects that combine identity-resolved records from multiple sources.
- Segments: Audience filters built on unified profiles for targeted marketing and activation.
- Activations: Configuration to push segments to external destinations (e.g., Marketing Cloud, ad platforms).
- Identity Resolution: Rules to match and merge customer records across data sources.
- Data Transforms: Batch transformations to process or enrich data within the platform.
- Data Spaces: Logical partitions within the Data Cloud org for data governance and isolation.

You also have direct access to the MIMIT Salesforce org via live tool calls:
- Use query_salesforce to run any SOQL query against the org's standard and custom CRM objects.
- Use list_salesforce_objects to discover available Salesforce objects.
- Use describe_salesforce_object to inspect the fields and schema of any object before querying.

When answering, be concise, precise, and professional. When you have tool access, ALWAYS call the
appropriate tool to fetch real-time data before answering questions about counts, names, or status.
Always mention relevant Salesforce documentation paths or Setup menu navigation when applicable."""

AGENT_TOOLS = [
    {
        "name": "get_org_summary",
        "description": (
            "Get a high-level KPI summary of the MIMIT Salesforce Data Cloud org: "
            "total and active Data Streams, DLOs, DMOs, Calculated Insights, "
            "Unified Profiles, Segments, Connectors, and today's ingestion volume."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_data_streams",
        "description": (
            "Fetch the live list of Data Streams from the MIMIT org with their "
            "names, status, refresh mode, and metadata."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_data_lake_objects",
        "description": "Fetch Data Lake Objects (DLOs) from the MIMIT org. Optionally filter by category.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["Profile", "Engagement", "Related"],
                    "description": "Entity category to filter by. Defaults to Profile.",
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_data_model_objects",
        "description": "Fetch Data Model Objects (DMOs) from the MIMIT org. Optionally filter by category.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["Profile", "Engagement", "Related"],
                    "description": "Entity category to filter by. Defaults to Profile.",
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_calculated_insights",
        "description": "Fetch Calculated Insights (CIs) from the MIMIT org with names and configuration.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "enum": ["Profile", "Engagement", "Related"],
                    "description": "Entity category to filter by. Defaults to Profile.",
                }
            },
            "required": [],
        },
    },
    {
        "name": "query_salesforce",
        "description": (
            "Execute a SOQL query against the MIMIT Salesforce org to retrieve CRM data. "
            "Use this to query standard objects (Account, Contact, Case, Opportunity, etc.) "
            "or custom objects. Mirrors the MIMIT MCP mimit-sf-query capability."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "soql": {
                    "type": "string",
                    "description": "The SOQL query to execute, e.g. 'SELECT Id, Name FROM Account LIMIT 10'",
                }
            },
            "required": ["soql"],
        },
    },
    {
        "name": "list_salesforce_objects",
        "description": (
            "List all available Salesforce objects (standard and custom) in the MIMIT org. "
            "Use this to discover what CRM objects are available to query. "
            "Mirrors the MIMIT MCP mimit-list-objects capability."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "describe_salesforce_object",
        "description": (
            "Get the full schema and field list for a specific Salesforce object in the MIMIT org. "
            "Use this before querying to understand available fields and their types. "
            "Mirrors the MIMIT MCP mimit-describe-object capability."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "object_name": {
                    "type": "string",
                    "description": "API name of the Salesforce object, e.g. 'Account', 'Contact', 'MyCustom__c'",
                }
            },
            "required": ["object_name"],
        },
    },
]


def execute_agent_tool(tool_name: str, tool_input: dict, client_id: str, client_secret: str) -> str:
    """Execute a live Salesforce API call on behalf of the Data 360 Agent."""
    try:
        if tool_name == "get_org_summary":
            kpi_obj = Get_Dashboard_KPIS("a", "b")
            total_ds, total_dlo, total_dmo, total_ci, active_ci, total_up, total_seg, total_conn = kpi_obj.get_KPIs()
            active_ds, error_ds, today_sum, _ = kpi_obj.get_informationfrom_datastream_csv()
            def _v(x): return x.iloc[0] if hasattr(x, "iloc") else x
            return json.dumps({
                "total_data_streams":          int(_v(total_ds)),
                "active_data_streams":         int(_v(active_ds)),
                "error_data_streams":          int(_v(error_ds)),
                "total_data_lake_objects":     int(_v(total_dlo)),
                "total_data_model_objects":    int(_v(total_dmo)),
                "total_calculated_insights":   int(_v(total_ci)),
                "active_calculated_insights":  int(_v(active_ci)),
                "unified_profiles":            int(_v(total_up)),
                "total_segments":              int(_v(total_seg)),
                "total_connectors":            int(_v(total_conn)),
                "todays_ingestion_volume":     int(_v(today_sum)),
            })

        elif tool_name == "get_data_streams":
            core_resp    = get_access_token(client_id, client_secret)
            access_token = core_resp.json()["access_token"]
            instance_url = core_resp.json()["instance_url"]
            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            resp = requests.get(
                f"{instance_url}/services/data/v64.0/ssot/data-streams/",
                headers=headers, timeout=30,
            )
            resp.raise_for_status()
            return json.dumps(resp.json())

        elif tool_name in ("get_data_lake_objects", "get_data_model_objects", "get_calculated_insights"):
            entity_map = {
                "get_data_lake_objects":   "DataLakeObject",
                "get_data_model_objects":  "DataModelObject",
                "get_calculated_insights": "CalculatedInsight",
            }
            category    = tool_input.get("category", "Profile")
            entity_type = entity_map[tool_name]
            api_url = (
                f"{CDP_BASE_URL}/api/v1/metadata"
                f"?entityType={entity_type}&entityCategory={category}"
            )
            result = get_data_streams(client_id, client_secret, api_url)
            return json.dumps(result["payload"])

        elif tool_name == "query_salesforce":
            soql = tool_input.get("soql", "").strip()
            if not soql:
                return json.dumps({"error": "soql parameter is required"})
            core_resp    = get_access_token(client_id, client_secret)
            access_token = core_resp.json()["access_token"]
            instance_url = core_resp.json()["instance_url"]
            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            resp = requests.get(
                f"{instance_url}/services/data/v64.0/query",
                params={"q": soql},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "totalSize":  data.get("totalSize", 0),
                "done":       data.get("done", True),
                "records":    data.get("records", []),
            })

        elif tool_name == "list_salesforce_objects":
            core_resp    = get_access_token(client_id, client_secret)
            access_token = core_resp.json()["access_token"]
            instance_url = core_resp.json()["instance_url"]
            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            resp = requests.get(
                f"{instance_url}/services/data/v64.0/sobjects/",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            raw = resp.json()
            objects = [
                {"name": o["name"], "label": o["label"], "queryable": o.get("queryable", False)}
                for o in raw.get("sobjects", [])
            ]
            return json.dumps({"sobjects": objects, "count": len(objects)})

        elif tool_name == "describe_salesforce_object":
            object_name = tool_input.get("object_name", "").strip()
            if not object_name:
                return json.dumps({"error": "object_name parameter is required"})
            core_resp    = get_access_token(client_id, client_secret)
            access_token = core_resp.json()["access_token"]
            instance_url = core_resp.json()["instance_url"]
            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            resp = requests.get(
                f"{instance_url}/services/data/v64.0/sobjects/{object_name}/describe/",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            fields = [
                {"name": f["name"], "label": f["label"], "type": f["type"], "nillable": f.get("nillable", True)}
                for f in data.get("fields", [])
            ]
            return json.dumps({
                "name":       data.get("name"),
                "label":      data.get("label"),
                "queryable":  data.get("queryable"),
                "fields":     fields,
                "fieldCount": len(fields),
            })

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as exc:
        app.logger.error("Agent tool error (%s): %s", tool_name, exc, exc_info=True)
        return json.dumps({"error": str(exc)})


@app.route("/data-360-agent", methods=["GET"])
@login_required
def data_360_agent():
    return render_template("data_360_agent.html")


@app.route("/data-360-agent/chat", methods=["POST"])
@login_required
def data_360_agent_chat():
    body    = request.get_json(force=True, silent=True) or {}
    message = (body.get("message") or "").strip()
    history = body.get("history") or []

    if not message:
        return jsonify({"error": "Message is required."}), 400

    if CLAUDE_API_KEY == "YOUR_CLAUDE_API_KEY_HERE":
        return jsonify({"error": "Claude API key not configured. Please set the CLAUDE_API_KEY environment variable."}), 503

    client_id     = session.get("client_id")
    client_secret = session.get("client_secret")
    has_sf_creds  = bool(client_id and client_secret)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

        messages = [
            {"role": m["role"], "content": m["content"]}
            for m in history if m.get("role") in ("user", "assistant")
        ]
        messages.append({"role": "user", "content": message})

        system = DATA360_SYSTEM_PROMPT
        if has_sf_creds:
            system += (
                "\n\nIMPORTANT: You have live tool access to the MIMIT production org. "
                "When the user asks about counts, lists, names, or status of any Data Cloud "
                "component, ALWAYS call the appropriate tool first to fetch real-time data."
            )
        else:
            system += (
                "\n\nNOTE: No Salesforce credentials are active in this session. "
                "You cannot fetch live org data. Advise the user to load their AWS credentials "
                "via the Configure page first if they need live information."
            )

        call_kwargs = dict(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            messages=messages,
        )
        if has_sf_creds:
            call_kwargs["tools"] = AGENT_TOOLS

        for _ in range(5):
            response = client.messages.create(**call_kwargs)

            if response.stop_reason != "tool_use":
                text = "\n".join(b.text for b in response.content if hasattr(b, "text"))
                return jsonify({"response": text})

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_agent_tool(block.name, block.input, client_id, client_secret)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })

            call_kwargs["messages"] = call_kwargs["messages"] + [
                {"role": "assistant", "content": response.content},
                {"role": "user",      "content": tool_results},
            ]

        return jsonify({"error": "Agent reached maximum tool iterations. Please try again."}), 500

    except Exception as exc:
        app.logger.error("Data 360 Agent chat error: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
