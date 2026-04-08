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
from flask import (
    Flask, render_template, request, jsonify, send_file,
    session, redirect, url_for, flash
)
from dashboard_data import Get_Dashboard_KPIS
app = Flask(__name__)
app.secret_key = "sfdc-datastream-secret-key"

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
    # Extract scalar from pandas Series if needed
    def _val(v):
        return v.iloc[0] if hasattr(v, 'iloc') else v
    active_rate = _val(active_datastream)/_val(total_ds)
    active_rate = f"{active_rate:.2%}"
    inactive_rate = _val(error_datastream)/_val(total_ds)
    inactive_rate = f"{inactive_rate:.2%}"
    active_ci_rate = _val(active_ci)/_val(total_ci)
    active_ci_rate = f"{active_ci_rate:.2%}"

    return total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up, total_seg, total_conn,active_datastream,error_datastream,today_sum,active_rate,inactive_rate,active_ci_rate,daily_ingestion_df

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
    total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up,total_seg,total_conn,active_datastream,error_datastream,today_sum,active_rate,inactive_rate,active_ci_rate,daily_ingestion_df = dashboard_functions()
    def _val(v):
        return v.iloc[0] if hasattr(v, 'iloc') else v
    # kpi_obj = Get_Dashboard_KPIS("a", "b")
    # total_ds, total_dlo, total_dmo, total_ci,active_ci, total_up, total_seg, total_conn = kpi_obj.get_KPIs()
    # active_datastream,error_datastream,today_sum = kpi_obj.get_informationfrom_datastream_csv()
    # # Extract scalar from pandas Series if needed
    # def _val(v):
    #     return v.iloc[0] if hasattr(v, 'iloc') else v
    # active_rate = _val(active_datastream)/_val(total_ds)
    # active_rate = f"{active_rate:.2%}"
    # inactive_rate = _val(error_datastream)/_val(total_ds)
    # inactive_rate = f"{inactive_rate:.2%}"
    # active_ci_rate = _val(active_ci)/_val(total_ci)
    # active_ci_rate = f"{active_ci_rate:.2%}"
    #total_up = f"{total_up/1000:.1f}K"
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
        active_datastream = _val(active_datastream),error_datastream=_val(error_datastream),
        active_rate = active_rate,active_ci_rate=active_ci_rate,
        inactive_rate = _val(inactive_rate),today_sum = _val(today_sum)
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
        flash(f"AWS error: {exc.response['Error']['Message']}", "danger")
        return redirect(url_for("aws_config"))
    except Exception as exc:
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
            return jsonify({"success": False, "error": str(exc)}), 500

    # Page render
    return render_template(
        "streams.html",
        username=session.get("username", ""),
        action=session.get("action", "get_data_schema"),
        object_type=session.get("object_type", "DataLakeObject")
    )


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


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
