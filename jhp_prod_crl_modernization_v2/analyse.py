
import ast
from datetime import datetime
from pathlib import Path
import re
import statistics

import pandas as pd
from pydantic import SecretStr
import requests
from sqlalchemy_access import pyodbc
from datetime import datetime, timedelta, timezone

from analyse_file import count_claims_x12_strict, get_file_metadata_from_shared_drive
from automic_apis import get_job_latest_run, get_object_details, search_api
from database_util import add_job_rules_to_ms_access_db, add_jobs_to_ms_access_db, update_run_id_workflow_status
from main import AutomicConfig, analyse_job, get_job_execution_children, get_job_run_ids, get_job_stats
    

#from automic_apis import get_job_execution_ert, get_job_logs, get_job_remote_log,get_log_file_override_path, get_object_details, post_with_basic_auth, read_log_from_shared_drive, read_ms_db_by_query
def parse_access_extended(bdata):
    if isinstance(bdata, bytes):
        s = bdata.decode('ascii', errors='ignore').strip('\x00')
    else:
        s = str(bdata)
    
    # Pattern: YYYYMMDDHHNNSS.nnnnnnnn or similar
    match = re.match(r'(\d{8})(\d{6})(\d+)', s)
    if match:
        date_str, time_str, nano_str = match.groups()
        print( date_str, time_str, nano_str)
        dt_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        
        # Truncate nanoseconds to microseconds (Access max precision)
        nanos = int(nano_str[:6]) if len(nano_str) >= 6 else 0  # First 6 digits
        dt = dt.replace(microsecond=nanos * 1000)
        return dt
    return pd.NaT



from datetime import datetime, timedelta

def days_nanos_to_datetime(days, nanoseconds, tz=timezone.utc):
    epoch = datetime(1970, 1, 1, tzinfo=tz)
    return epoch + timedelta(days=days, seconds=nanoseconds / 1_000_000_000)


def ordinal_days_nanos_to_datetime(days, nanoseconds):
    base = datetime(1, 1, 1)
    return base + timedelta(days=days, seconds=nanoseconds / 1_000_000_000)
def access_bytes_to_timestamp(byte_data):
    """
    Decodes MS Access Date/Time Extended bytes: 
    b'0000000000000693593:0000000576000000000:7\x00'
    """
    try:
        # 1. Decode bytes to string and clean up null characters
        clean_str = byte_data.decode('utf-8').strip('\x00')
        
        # 2. Split by the colon
        # Parts: [Days since Epoch, Nanoseconds, Precision Flag]
        parts = clean_str.split(':')
        
        if len(parts) < 2:
            return None
            
        days_since_epoch = int(parts[0])
        nanoseconds = int(parts[1])
        print(days_since_epoch)
        print(nanoseconds)
        # 3. MS Access Epoch is December 30, 1899
        epoch = datetime(1899, 12, 30)
        
        # 4. Calculate the date
        target_date = epoch + timedelta(days=days_since_epoch)
        
        # 5. Add the fractional seconds (converted from nanoseconds)
        # 1 nanosecond = 1e-9 seconds
        final_timestamp = target_date + timedelta(seconds=nanoseconds / 1_000_000_000)
        
        return final_timestamp

    except Exception as e:
        print(f"Error decoding bytes: {e}")
        return None

def decode_access_extended_bytes(byte_data):
    """
    Decodes MS Access Date/Time Extended bytes specifically for:
    b'0000000000000739694:0000000258140000000:7\x00'
    Target: 2026-03-20 07:10:14
    """
    try:
        if byte_data:
            # 1. Clean and Split
            clean_str = byte_data.decode('utf-8').strip('\x00')
            parts = clean_str.split(':')
            
            # Part 0 is the "Tick" count
            ticks = int(parts[0]) 
            # Part 1 is the high-precision nanoseconds into the day
            nanoseconds_today = int(parts[1])
            
            # 2. Define the MS Access Base Epoch (1899-12-30)
            base_epoch = datetime(1, 1, 1)                        
            # 3. Calculation:            
            
            #final_dt = date_part + time_part
            days = int(parts[0]) 
            # Part 2: 10-nanosecond units into the day
            nano_seconds = int(parts[1]) *100
          
            ts =  ordinal_days_nanos_to_datetime(days,nano_seconds)
            return ts
        else:
            return None
 

    except Exception as e:
        return f"Error: {e}"
 
def get_data():
    
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"
    with pyodbc.connect(conn_str) as conn:
        # Use CStr to handle the 'Extended' timestamp issue we discussed
        query = "SELECT * FROM job_stats"
        df = pd.read_sql(query, conn)
    
    # Clean data
    #df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    #df = df.dropna(subset=['start_time']) # Remove invalid dates
    #df['start_time_str'] = df['start_time'].apply(decode_access_extended_bytes)
    #df = df[['run_id','object_type','start_time','end_time','start_time_str']]
    return df

def reprocess_ers():
    df = get_data()

import pyodbc
import pandas as pd

 
def update_access_from_df():
    """
    Processes each Run_ID in the DataFrame, applies logic, 
    and updates the Access Table.
    """
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
     
    hostname=os.getenv("HOSTNAME")
    port = os.getenv("PORT")
    client_id = os.getenv("CLIENT_ID")
    username = os.getenv("AUTOMIC_USER")
    password = os.getenv("AUTOMIC_PASS")   
    base_url= f"https://{hostname}:{port}/ae/api/v1/{client_id}"
    print(base_url)
    # Usage
    endpoint = f"https://{hostname}:{port}/ae/api/v1"
    automic_config = AutomicConfig(
        username=username,
        password=SecretStr(password),
        endpoint=endpoint,
        client_id=client_id,
        base_url=base_url     )
    
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_file};"
    df = get_data()
    # 1. Logic: Let's say we calculate a 'validation_status' based on RunID
    # (Example: Check if file exists or perform your EDI 837 check here)
    def validate_run(main_workflow_run_id):
        # Your custom logic here
        res = get_job_execution_children(base_url, automic_config, main_workflow_run_id)
        result_data= res["data"]
        if result_data:
            sorted_data = sorted( result_data, key=lambda x: ( datetime.strptime(x.get('start_time') or x.get('end_time') or "1900-03-20T07:10:14Z" , date_format) , x.get("line_number") ), reverse=False)
            for data in sorted_data:
                run_id = data.get("run_id")
                parent_run_id = data.get("workflow_id")
                object_type = data.get("type")
                status = data.get("status")
                status_text = data.get("status_text")
                name = data.get("name")
                start_time = data.get('start_time') 
                end_time  = data.get('end_time')
                resp_data = { "workflow_run_id":main_workflow_run_id,"parent_run_id":parent_run_id,"run_id": run_id,"object_type":object_type ,"name":name, "status":status,"status_text" : status_text, "start_time":start_time,"end_time":end_time}
                analyse_out = {}
                if object_type == "JOBP" and (status == 1900 or status == 1800 or status == 1850 or status == 1851):
                    #print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    sub_out_data = get_job_stats(automic_config,run_id=run_id,main_workflow_run_id=main_workflow_run_id)
                    output_data.append(resp_data |analyse_out)
                    output_data = output_data + sub_out_data
                else:    
                    if status == 1900 : # job ended ok - ENDED_OK - ended normally
                        print(f"{parent_run_id}|{run_id}|{object_type}{name}|{status_text}|{start_time}|{end_time}")
                        analyse_out  = analyse_job(data,base_url,automic_config)
                update_run_id_workflow_status(db_file, update_data) 

    df['val_status'] = df['workflow_run_id'].apply(validate_run)

    # 2. Connect to MS Access
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # 3. Process all rows using a dictionary loop
            # This is safer for Access than a bulk 'apply'
            records = df[['run_id', 'val_status']].to_dict('records')
            
            update_query = """
                UPDATE job_stats 
                SET [validation_status] = ?, 
                    [last_checked] = Now()
                WHERE [run_id] = ?
            """
            
            for row in records:
                cursor.execute(update_query, (row['val_status'], row['run_id']))
            
            conn.commit()
            print(f"✅ Successfully updated {len(records)} records in Access.")

    except pyodbc.Error as e:
        print(f"❌ Database Update Error: {e}")

# --- Example Usage ---
# conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\path\to\your\db.accdb;"
# update_access_from_df(df, conn_str)
#    
#print(get_data())
RET_RE = re.compile(r'\bRET=(\d+)\b')

# EXTERNAL_LOG_RE = re.compile(
#     r'(Log\s+)\s+"(?P<path>[^"]+\.log)"',
#     re.IGNORECASE
# )

EXTERNAL_LOG_RE = re.compile(
    r'\bLog\s+"(?P<path>[^"]+\.log)"',
    re.IGNORECASE
)

# CMD_RE = re.compile(
#     r'^(copy|move)\s+"(?P<src>.+?)\\\*\.(?P<ext>[^"]+)"\s+"(?P<dst>[^"]+)"',
#     re.IGNORECASE | re.MULTILINE
# )

COUNT_RE = re.compile(r'(?P<count>\d+)\s+file\(s\)\s+(copied|moved)', re.IGNORECASE)

def build_file_re(src_dir, ext):
    return re.compile(
        rf'{re.escape(src_dir)}\\(?P<filename>[^\\]+\.{re.escape(ext)})',
        re.IGNORECASE
    )

DOWNLOAD_RE = re.compile(
    r'Downloading to local file\s+"(?P<local>[^"]+)"',
    re.IGNORECASE
)

TRANSFER_OK_RE = re.compile(r'transfer succeeded', re.IGNORECASE)
FAILURE_RE = re.compile(r'Failure in command', re.IGNORECASE)

COPY_MOVE_CMD_RE = re.compile(
    r'^(copy|move)\s+"(?P<src>.+?)\\\*\.(?P<ext>[^"]+)"\s+"(?P<dst>[^"]+)"',
    re.IGNORECASE | re.MULTILINE
)

def build_file_re(src_dir, ext):
    return re.compile(
        rf'{re.escape(src_dir)}\\(?P<filename>[^\\]+\.{re.escape(ext)})',
        re.IGNORECASE
    )

COPY_MOVE_COUNT_RE = re.compile(
    r'(?P<count>\d+)\s+file\(s\)\s+(copied|moved)',
    re.IGNORECASE
)

FTP_GET_RE = re.compile(
    r'Downloading to local file\s+"(?P<local>[^"]+)"',
    re.IGNORECASE
)

FTP_PUT_RE = re.compile(
    r'Uploading file\s+"(?P<local>[^"]+)"',
    re.IGNORECASE
)

FTP_DELETE_RE = re.compile(
    r'Deleted\s+(?P<count>\d+)\s+files?',
    re.IGNORECASE
)

FTP_SUCCESS_RE = re.compile(r'transfer succeeded', re.IGNORECASE)
FTP_FAILURE_RE = re.compile(r'Failure in command', re.IGNORECASE)
 
COPY_MOVE_BLOCK_RE = re.compile(
    r'(?P<block>'
    r'^(?:[A-Za-z]:\\?>)?\s*(copy|move)\s+".*?"\s+".*?"'
    r'(?:\r?\n.+?)*?'
    r'\d+\s+file\(s\)\s+(copied|moved)\.'
    r')',
    re.IGNORECASE | re.MULTILINE
)

# CMD_RE = re.compile(
#     r'^(?:[a-zA-Z]:\\>)?\s*(copy|move)\s+'
#     r'"(?P<src>.+?)\\\*\.(?P<ext>[^"]+)"\s+'
#     r'"(?P<dst>[^"]+)"',
#     re.IGNORECASE
# )


CMD_RE = re.compile(
    r'^(?:[a-zA-Z]:\\>)?\s*'
    r'(?P<cmd>copy|move)\s+'
    r'"(?P<src>[^"]+)"\s+'
    r'"(?P<dst>[^"]+)"',
    re.IGNORECASE | re.MULTILINE
)

COUNT_RE = re.compile(
    r'(?P<count>\d+)\s+file\(s\)\s+(copied.|moved.)',
    re.IGNORECASE
)


def wildcard_to_regex(pattern: str) -> str:
    regex = re.escape(pattern)
    regex = regex.replace(r'\*', '.*')
    regex = regex.replace(r'\?', '.')
    return regex


def parse_copy_move_blocks_old(log_text):
    results = []
    print("inside parse_copy_move_blocks")
 
    for bm in COPY_MOVE_BLOCK_RE.finditer(log_text):
        print("copy  move block - available")
        block = bm.group("block")
  

        cmd = CMD_RE.search(block)
        if not cmd:
            continue

        src = cmd.group("src")
        dst = cmd.group("dst")
        #ext = cmd.group("ext")
        op = cmd.group(1).lower()

        
        _, ext = os.path.splitext(src.replace('*', ''))
        ext = ext.lstrip('.')   # zip
        src_dir, file_pattern = os.path.split(src)
        file_pattern_re = wildcard_to_regex(file_pattern)
        print(file_pattern_re)
    
        
        file_re = re.compile(
            rf'^{re.escape(src_dir)}\\(?P<filename>{file_pattern_re})$',
            re.IGNORECASE | re.MULTILINE
        )



        files = [m.group("filename") for m in file_re.finditer(block)]

        count_match = COUNT_RE.search(block)

        count = int(count_match.group("count")) if count_match else len(files)

        results.append({
            "operation": op,
            "source": src,
            "destination": dst,
            "file_pattern": file_pattern,
            "files": files,
            "file_count": count,
            "success": count == len(files)
        })

    return results


def parse_copy_move_blocks(log_text):
    results = []

    for bm in COPY_MOVE_BLOCK_RE.finditer(log_text):
        block = bm.group("block")

        cmd = CMD_RE.search(block)
        if not cmd:
            continue

        src = cmd.group("src")
        dst = cmd.group("dst")
        op = cmd.group(1).lower()

        src_dir, file_pattern = os.path.split(src)
        file_pattern_re = wildcard_to_regex(file_pattern)

        file_re = re.compile(
            rf'^{re.escape(src_dir)}\\(?P<filename>{file_pattern_re})$',
            re.IGNORECASE | re.MULTILINE
        )

        files = [m.group("filename") for m in file_re.finditer(block)]

        count_match = COUNT_RE.search(block)
        count = int(count_match.group("count")) if count_match else len(files)

        results.append({
            "operation": op,
            "source": src,
            "destination": dst,
            "file_pattern": file_pattern,
            "files": files,
            "file_count": count,
            "success": count == len(files)
        })

    return results


def parse_internal_log(log_text):
    result = {
        "copy_move_ops": [],
        "external_log": None
    }

    # External log reference
    ext = EXTERNAL_LOG_RE.search(log_text)
    if ext:
        result["external_log"] = ext.group("path")

    for cmd in COPY_MOVE_CMD_RE.finditer(log_text):
        src = cmd.group("src")
        dst = cmd.group("dst")
        extn = cmd.group("ext")
        op = cmd.group(1).lower()

        file_re = build_file_re(src, extn)
        files = [m.group("filename") for m in file_re.finditer(log_text)]

        count_match = COPY_MOVE_COUNT_RE.search(log_text)
        count = int(count_match.group("count")) if count_match else len(files)

        result["copy_move_ops"].append({
            "operation": op,
            "source": src,
            "destination": dst,
            "file_pattern": f"*.{extn}",
            "files": files,
            "count": count,
            "success": count == len(files)
        })

    return result

def parse_external_log(log_text):
    transfers = []

    for m in FTP_GET_RE.finditer(log_text):
        success = bool(FTP_SUCCESS_RE.search(log_text))
        failure = bool(FTP_FAILURE_RE.search(log_text))

        transfers.append({
            "operation": "ftp_get",
            "file": m.group("local").split("\\")[-1],
            "success": success and not failure
        })

    for m in FTP_PUT_RE.finditer(log_text):
        transfers.append({
            "operation": "ftp_put",
            "file": m.group("local").split("\\")[-1],
            "success": True
        })

    for m in FTP_DELETE_RE.finditer(log_text):
        transfers.append({
            "operation": "ftp_delete",
            "count": int(m.group("count"))
        })

    return transfers


def parse_job_log(log_text, base_url=None, config=None, external_log_loader=None):
    """
    external_log_loader: function(path) -> str
    """

    result = {
        "job_status": "UNKNOWN",
        "return_code": None,
        "transfer_mode": "NONE",
        "transfers": [],
        "errors": [],
        "external_log_path": None
    }

    # ---------- RETURN CODE ----------
    m = RET_RE.search(log_text)
    if m:
        result["return_code"] = m.group(1)
        result["job_status"] = "SUCCESS" if m.group(1) == "00000000" else "FAILED"

    # ---------- EXTERNAL LOG ----------
 
    ext_log = EXTERNAL_LOG_RE.search(log_text)
 
    if ext_log:
        path = ext_log.group("path")
        path = path.replace("D$",'')
        result["external_log_path"] = path
        result["transfer_mode"] = "EXTERNAL_LOG"

        if external_log_loader:
            log_text = external_log_loader(path)
      
        else:
            print("no external log loader")
        return parse_external_log(log_text)
    else:
     
        return parse_copy_move_blocks(log_text)
      

def get_workflow_job_details_old(base_url,config,job_name,parent_workflow=None):
    response = get_object_details(base_url,config,job_name)
    #data.jobp.workflow_definitions
    workflow_def = response.get("data").get("jobp").get("workflow_definitions")
    #print(workflow_def)
    sub_workflow = ""
    ext_job_name = ""
    leve2_sub_workflow = ""
    ls_out_data = []
    if  parent_workflow is None:
        parent_workflow = job_name
    else:
        sub_workflow = job_name

    excluded_jobs = ["START","END"]
    for job_def in workflow_def:
        object_type = job_def.get("object_type")
        object_name = job_def.get("object_name")
        print(f"{object_name}|{ext_job_name}")
        if str(object_name).startswith("JOBP."):
            if object_type == "<XTRNL>":
                ext_job_name = job_def.get("object_name")
                continue 
            sub_workflow = job_def.get("object_name")
            #.... recursive call
            ls_sub_out_data = get_workflow_job_details(base_url,config,sub_workflow,parent_workflow=parent_workflow)
            #return ls_sub_out_data
            ls_out_data.extend(ls_sub_out_data)             
        else:
            if object_name not in excluded_jobs:
                #Write-Host "$($job),,$($external_job),,$($definitions.object_name)"                 
                out_data = {"workflow_name":parent_workflow,"sub_workflow": sub_workflow ,"dependent_workflow":ext_job_name,"level2_sub_workflow":leve2_sub_workflow,"job_name":object_name}
                ls_out_data.append(out_data)
    #print_dict_list_table(ls_out_data)
    return ls_out_data


def get_workflow_job_details(
    base_url,
    config,
    jobp_name,
    root_workflow=None,
    parent_jobp=None,
    level2_jobp=None,
    depth=0,
    max_depth=2,inherited_external_deps=[]
):
    response = get_object_details(base_url, config, jobp_name)
    jobp = response["data"]["jobp"]

    workflow_defs = jobp.get("workflow_definitions", [])
    line_conditions = jobp.get("line_conditions", [])

    if root_workflow is None:
        root_workflow = jobp_name

    output = []

    # ---- external dependency mapping (unchanged) ----
    xtrnl_by_line = {
        n["line_number"]: n["object_name"]
        for n in workflow_defs
        if n.get("object_type") == "<XTRNL>"
    }

    external_deps = {}
    for lc in line_conditions:
        if lc.get("predecessor_line_number") in xtrnl_by_line:
            external_deps.setdefault(
                lc["workflow_line_number"], []
            ).append(xtrnl_by_line[lc["predecessor_line_number"]])

    # ---- walk workflow ----
    for node in workflow_defs:
        obj_type = node["object_type"]
        obj_name = node["object_name"]
        line_no = node["line_number"]

        if obj_type in ("<START>", "<END>", "<XTRNL>"):
            continue

        # ---- SUB-WORKFLOWS ----
        if obj_name.startswith("JOBP.") and depth < max_depth:

            # ✅ calculate hierarchy ONLY when descending
            if depth == 0:
                next_parent = obj_name
                next_level2 = None
            elif depth == 1:
                next_parent = parent_jobp
                next_level2 = obj_name
            else:
                next_parent = parent_jobp
                next_level2 = level2_jobp

            
            # ✅ propagate external deps
            propagated_external_deps = (
                inherited_external_deps + external_deps.get(line_no, [])
            )

            output.extend(
                get_workflow_job_details(
                    base_url,
                    config,
                    obj_name,
                    root_workflow=root_workflow,
                    parent_jobp=next_parent,
                    level2_jobp=next_level2,
                    depth=depth + 1,
                    max_depth=max_depth,
                    inherited_external_deps=propagated_external_deps
                )
            )
            continue

        # ---- JOBS ONLY ----
        # if not obj_name.startswith("JOBS.") and not obj_name.startswith("EVNT."):
        #     continue
        
        if not obj_name.startswith(("JOBS.", "EVNT.")):
            continue

        
        deps = set()

        # job's direct external deps
        deps.update(external_deps.get(line_no, []))

        # ✅ inherited from parent JOBP(s)
        deps.update(inherited_external_deps)

        output.append({
            "workflow_name": root_workflow,
            "sub_workflow": parent_jobp,
            "level2_sub_workflow": level2_jobp,
            "job_name": obj_name,
            "object_type": obj_type,
            "dependent_workflow": ",".join(sorted(deps))
        })

    return output


# METADATA_RE = re.compile(
#     r'(?i)(copy|move|del|erase|rename|pkzipc)'
#     r'(?:[\s-][-/a-z0-9=]+)*\s+'
#     r'(?:"([^"]+)"|([^\s]+))'
#     r'(?:\s+(?:"([^"]+)"|([^\s]+)))?'
# )

METADATA_RE = re.compile(
    r'(?i)^(copy|move|del|erase|rename|pkzipc)'
    r'(?:[ \t-][-/a-z0-9=]+)*[ \t]+'
    r'(?:"([^"]+)"|([^\s\r\n]+))'
    r'(?:[ \t]+(?:"([^"]+)"|([^\s\r\n]+)))?'
    , re.MULTILINE
)

def extract_metadata(text):
    if not text:
        return []

    text = "\n".join(
        line for line in text.replace("\r", "").split("\n")
        if not re.match(r'^\s*REM\b', line, re.I)
    )

    results = []
    for m in METADATA_RE.finditer(text):
        cmd = m.group(1).upper()
        p1 = m.group(2) or m.group(3)
        p2 = m.group(4) or m.group(5)

        src = p1 or ""
        tgt = p2 or ""

        src_parts = src.strip("\\").split("\\") if src else []
        tgt_parts = tgt.strip("\\").split("\\") if tgt else []

        results.append({
            "Command": cmd,
            "SourceServer": src_parts[0] if src_parts else "",
            "SourceFile": src_parts[-1] if src_parts else "",
            "SourcePath": src,
            "TargetServer": tgt_parts[0] if tgt_parts else "",
            "TargetPath": tgt,
        })

    return results

 

def process_job(base_url,config,job_name,job_type="jobs"):
    resp =  get_object_details(base_url,config,job_name)
    #print(resp)
    job = resp["data"][job_type]

    base_info = {
        "AJob": job["general_attributes"].get("name"),
        "estimated_time": job["general_attributes"].get("ert"),
        "job_type": job["general_attributes"].get("type"),
        "description": job["general_attributes"].get("description"),
        "platform": job["general_attributes"].get("platform"),
    }

    rows = []

    scripts = job.get("scripts") or [None]
    for s in scripts:
        process = "\n".join(filter(None, s.get("process", []))) if s else ""
        extracted = extract_metadata(process)
        # cmd_text = "\n".join(str(e) for e in extracted)
        cmd_text = extracted
        row = {
            **base_info,
            "Commands": cmd_text,
            "Process": process,
            "Pre_process": s.get("pre_process", "") if s else "",
            "post_process": s.get("post_process", "") if s else ""
        }

        rows.append(row)

    # Prompt set
    prompt_sets = job.get("prompt_set_defaults", [])
    # if prompt_sets:
    #     prompt_values = "\n".join(
    #         f"{p['variable_name']} = {p.get('value','')}"
    #         for p in prompt_sets
    #     )
    #     prompt_values += f"\nprompt_set = {prompt_sets[0].get('prompt_set','')}"
    # else:
    #     prompt_values = ""

    if prompt_sets:
        prompt_values = { p['variable_name'] : p.get('value','')  for p in prompt_sets   }
        prompt_values["prompt_set"]=  prompt_sets[0].get('prompt_set','')
    else:
        prompt_values = {}

    for r in rows:
        r["PromptSet"] = prompt_values

    return rows

def get_all_job_details(base_url,config, input_file="all_workflow_detail.csv",output_file="C:\\Users\\afe3356\\Job_Analysis_20260401_1.xlsx"):
    ALL_ROWS = []
    df = pd.read_csv(input_file)
   
    for index, row in df.iterrows():
        job_name = row["job_name"]
    
        print(job_name)
        if job_name in ['IF','FE'] or job_name.startswith("SCRI") or job_name.startswith("OBSOLETE.") or job_name.startswith("EVNT."):
            ALL_ROWS.extend({})
        else:
            try:
                ALL_ROWS.extend(process_job(base_url,config,job_name))
            except:
                print(f"error occurec for job -{job_name}")
                ALL_ROWS.extend({})


    # for job in JOB_NAMES:
    #     print(f"Processing job: {job}")
    #     ALL_ROWS.extend(process_job(base_url,config,job))

    df = pd.DataFrame(ALL_ROWS)
    df.to_excel(output_file, index=False)

    print("Excel file created successfully")


def get_ftp_n_copy_executions_count(base_url,config):
    start_date="2026-03-01T00:00:00Z"
    # out = get_job_run_ids(base_url,config, job_name,start_date)
    input_file =r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\ftp_copy_jobs_01.csv"
    output_file = "ftp_job_run_count_unique_march.xlsx"
    df = pd.read_csv(input_file)
   
    all_data = []
    total_count = 0 
    for index, row in df.iterrows():
        job_name = row["AJob"]
        start_date="2026-03-01T00:00:00Z"
        out = get_job_run_ids(base_url,config, job_name,start_date)
        number_of_execution = len(out)
        data = {"job_name": job_name , "execution_count":number_of_execution}
        all_data.append(data)
        total_count +=number_of_execution
    
    print(total_count)
    df_out = pd.DataFrame( all_data)
    df_out.to_excel(output_file, index=False)

def get_schedules_for_workflow(base_url,config, headers, workflow_name):
    payload = {
        "object_types": ["JSCH"],
        "search_term": workflow_name,
        "search_in": ["content"]
    }

    resp = requests.post(
        f"{base_url}/search",
        headers=headers,
        auth = config.auth,
        json=payload
    )
    print(resp.json())
    return resp.json().get("data", [])
 
def get_schedule_details(base_url, headers, schedule_name):
    resp = requests.get(
        f"{base_url}/ae/api/v1/objects/JSCH/{schedule_name}",
        headers=headers
    )
    return resp.json()

def get_workflow_details_with_stats(base_url,config):
    ALL_ROWS = []
    ls_process = ["JOBP.DAILY_IPLUS_837_INBOUND_OUTBOUND_PROCESS" ,"JOBP.DAILY_MEDICAID_834UPD_MEMBERSP_REDESIGNED" ,"JOBP.DAILY_CHIP_834_MEMBERSHIP_NEW_REDESIGN" ,"JOBP.DAILY_MHK_EXTRACTS_DATA_UPLOAD_PROCESS" ,"JOBP.DAILY_TD_BANK_PROCESS" ,"JOBP.DAILY_MEDICARE_ENCOUNTER_PROCESSING_PROCESS" ,"JOBP.MEDICIAD_ENCOUNTER_PROCESS" ,"JOBP.CVS_HEALTH_DAILY_CET_PCT_LOAD_PROCESSES" ,"JOBP.DAILY_ACA_SALESFORCE_ENROLLMENT" ,"JOBP.DAILY_BUILD_HEALTHTRIO_PROVIDER_DIRECTORY_DATA" ,"JOBP.DAILY_IPLUS_ZELIS_CLAIM_REPRICER_INBOUND_PROCESS" ,"JOBP.DAILY_IPLUS_ZELIS_CLAIM_REPRICER_OUTBOUND_PROCESS" ,"JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS" ,"JOBP.DAILY_IPLUS_SECONDARY_ZELIS_DOWNLOAD_PROCESS" ,"JOBP.WEEKLY_MATERNITY_BILLING_PROCESS" ,"JOBP.WEEKLY_MATERNITY_REMIT_INFORMATICA" ,"JOBP.MONTHLY_CAID_CHIP_820_REMITTANCE_MASTER" ,"JOBP.MONTHLY_CHIP_PREMIUM_BILLING_PROCESS" ,"JOBP.MANUAL_MONTHLY_MEDICARE_PREMIUM_BILLING_INVOICE_PROCESS" ,"JOBP.DPW_MONTHLY_MEMBERSHIP_REDESIGN" ,"JOBP.MONTHLY_CHIP_834_MEMBERSHIP_NEW_REDESIGN" ,"JOBP.MONTHLY_CAPITATION_EXTRACT_PROCESS" ,"JOBP.MONTHLY_BONUS_PAYMENT_ECHO_MASTER"]
    # ls_jobs =['JOBP.DPW_MONTHLY_MEMBERSHIP_REDESIGN']
    
    for jobp_name in ls_process:
        res = get_workflow_job_details(base_url,config,jobp_name)
        
        for job_detail in res:
            job_name = job_detail.get("job_name")
            object_type = job_detail.get("object_type")
            #print(job_name)
            job_config = process_job(base_url,config,job_name,object_type.lower())
            job_full_stats = job_detail |job_config[0]
            ALL_ROWS.append(job_full_stats)
            

    df = pd.DataFrame(ALL_ROWS)
    

    df["process_lc"] = df["Process"].str.lower()

    df["is_ftp"] = df["process_lc"].str.contains(
    ":inc jobi.ws_ftp.hpappworxts", na=False
    )

    # df["is_copy_move"] = df["process_lc"].str.contains(
    #     "copy|move", regex=True, na=False
    # )


    df["is_copy_move"] = df["process_lc"].str.contains(
    r"(^|\n)(copy|move)(\b)", 
    regex=True,
    na=False
    )

    
    # df["is_copy_move"] = df["process_lc"].str.contains(
    # r"(?:^|\n)\s*(?:copy|move)\s*(?:\n|$)",
    # regex=True,
    # na=False
    # )

    df.drop(columns=["process_lc"], inplace=True)
    df.drop(columns=["AJob"], inplace=True)

    
    workflow_stats = (
    df.groupby("workflow_name")
      .agg(
          total_jobs=("job_name", "count"),
          ftp_job_count=("is_ftp", "sum"),
          copy_move_job_count=("is_copy_move", "sum")
      )
      .reset_index()
    )
    df.to_csv("all_workflow_details.csv", index=False)
   
    summary = (
    df.assign(
        dependent_workflow=df["dependent_workflow"].replace("", pd.NA)
    )
    .groupby("workflow_name")
    .agg(
        job_count=("job_name", "count"),
        distinct_job_count=("job_name", "nunique"),
        sub_workflow_count=("sub_workflow", lambda x: x.nunique(dropna=True)),
        level2_sub_workflow_count=("level2_sub_workflow", lambda x: x.nunique(dropna=True)),
        external_dependency_count=("dependent_workflow", lambda x: x.nunique(dropna=True))
    )
    .reset_index()
    )
    
 
    df = summary.merge(     workflow_stats,
        on="workflow_name",
        how="left"
    )
    df.to_csv("all_workflow_stats_01.csv", index=False)

def analyse_sas_job_last_execution(base_url,config):
    start_date="2025-01-01T00:00:00Z"
    ls_jobs = ['JOBS.ACCORDANT_ACCORDANT_CLAIMRX', 'JOBS.ACCORDANT_CLAIMMHS', 'JOBS.ACCORDANT_CLAIMRXMC', 'JOBS.ARCHIVE_CLAIMS', 'JOBS.DIAGFMT', 'JOBS.EXPORTRENEWAL2CSV', 'JOBS.HCCLD00044694', 'JOBS.HCCLD00044694_CHIP', 'JOBS.HCCLM00077046', 'JOBS.HCCLQ021', 'JOBS.HCCLW00051808', 'JOBS.HCCLW003', 'JOBS.HCCLW003_CHIP', 'JOBS.HCFAM00170503', 'JOBS.HCFAM00382883', 'JOBS.HCFAQ00177568', 'JOBS.HCISM00159014', 'JOBS.HCISW022', 'JOBS.HCISW023', 'JOBS.HCMAQ004', 'JOBS.HCMEM310M', 'JOBS.HCMEM706', 'JOBS.HCMEW00077433', 'JOBS.HCMEW00088630', 'JOBS.HCMRQ00048141', 'JOBS.HCMRQ00048701', 'JOBS.HCMRQ096', 'JOBS.HCMRQ096_PRIOR', 'JOBS.HCMRQ097', 'JOBS.HCOPM00132014', 'JOBS.HCOPQ00132015', 'JOBS.HCOPW00132011', 'JOBS.HCPRD00084760', 'JOBS.HCPRM032', 'JOBS.HCPRM504_CHIP_PNF', 'JOBS.HCPRW00145679', 'JOBS.HCPRW00276345', 'JOBS.HCPRW504', 'JOBS.HCPRW504_MEDICARE', 'JOBS.HCQMM00060320', 'JOBS.HCQMQ0111772', 'JOBS.HCQMW00272955', 'JOBS.HCSW00458457', 'JOBS.HCURW011_P1', 'JOBS.HISM00431037', 'JOBS.HMCLQ00203849', 'JOBS.HMEDD001', 'JOBS.HMHED00147913', 'JOBS.HMISD004', 'JOBS.HMISM112', 'JOBS.HMISW00110368A', 'JOBS.HMISW00110368B', 'JOBS.HMISW00115855', 'JOBS.HMISW006', 'JOBS.HMMRM011', 'JOBS.HMOBM008', 'JOBS.HMOBW109', 'JOBS.HMPRM518', 'JOBS.HMQMM001', 'JOBS.HMQMQ00085083', 'JOBS.HMRXW001', 'JOBS.HPCOW110440', 'JOBS.HPMEM00057469', 'JOBS.HPPRM00069110', 'JOBS.HPPRW00056642', 'JOBS.HRDW2COBDATA', 'JOBS.HRDW2SAS', 'JOBS.HRISW00048726', 'JOBS.HSISD00447391', 'JOBS.IAPP2SAS_CLEANUPFORSAS', 'JOBS.IAPP2SAS_PBS', 'JOBS.IAPP2SAS_SAVE2HIST', 'JOBS.LOAD834RENEWALDATES', 'JOBS.LOAD_AVESIS_FILE_MEDICARE', 'JOBS.LOAD_AVESIS_FILE_PPO', 'JOBS.LOAD_SUPERIOR_FILE_ACA', 'JOBS.LOAD_SUPERIOR_FILE_MEDICARE', 'JOBS.LOAD_SUPERIOR_FILE_PPO', 'JOBS.LOADMAGELLANFILEI', 'JOBS.LOADMAGELLANFILEP', 'JOBS.MCBILLING', 'JOBS.MEMBER_WITH_INCORRECT_DATE_TO_SALESFORCE', 'JOBS.MEMPCP1DAYGAP', 'JOBS.MMCLW001', 'JOBS.MONTHLY_820_DPWREMIT', 'JOBS.RAP_DIAGNOSIS_MONTHLY', 'JOBS.RAP_MEMBERM', 'JOBS.RAP_PHARMACYM', 'JOBS.RAP_PROVIDER_MONTHLY', 'JOBS.RAP_PROVIDERGROUP_MONTHLY', 'JOBS.RAR_MEDICARE_CLAIMS', 'JOBS.RAR_MEDICARE_MEMBERS', 'JOBS.RAR_MEDICARE_PHARMACY', 'JOBS.RAR_MEDICARE_PROVIDER', 'JOBS.RASI_MONTHLY_FILE', 'JOBS.SCIO_CLAIMLAB', 'JOBS.SCIO_CLAIMMHS', 'JOBS.SCIO_CLAIMRX', 'JOBS.SCIO_CLAIMRX_MC', 'JOBS.SCIO_MEMBER_ELIGIBILITY', 'JOBS.SCIO_MEMBER_ELIGIBILITY_MC', 'JOBS.SECURECOMPARE', 'JOBS.TPLMCOER_FILE_MOVE', 'JOBS.UDDB2CAID834DAILYHIST', 'JOBS.UDDB2CLAIMS', 'JOBS.UDDB2FEE_SCHEDULE_HISTORY', 'JOBS.UDDB2SVCHOLDS', 'JOBS.UDDB2SVCLINES', 'JOBS.UDDB_AUTHS2SAS', 'JOBS.UDDB_CLAIMS2SAS', 'JOBS.UDDB_FUNDDATA2SAS', 'JOBS.UDDB_REF2SAS', 'JOBS.WEEKLY_835_CAID_COUNTS', 'JOBS.WEEKLY_COMPARE_SAS_DWHS', 'JOBS.WEEKLY_DPWMATERNITY_835', 'JOBS.AFFILIATION_SPECIALTY_LD_DLY', 'JOBS.CAPHIST', 'JOBS.DEMMAS2SQL', 'JOBS.ELIGIBILITY_BENEFIT_LD_DAILY', 'JOBS.ELIGIBILITY_LD_DAILY', 'JOBS.ELIGIBILITY_PROVIDER_LD_DAILY', 'JOBS.EMPLOYEE_LD_DAILY', 'JOBS.EMPLOYER_LOAD', 'JOBS.HCISD001_CHIP', 'JOBS.HCISD001_LOB', 'JOBS.HCISD001_MEDICARE', 'JOBS.HCISD001_PACAID', 'JOBS.HCISD002_LOB', 'JOBS.HCMAQ00052042', 'JOBS.HCMED00054102', 'JOBS.HCMEW00068326', 'JOBS.HCMEW320', 'JOBS.HCMRQ00048141_CHIP_PRIOR', 'JOBS.HCMRQ00048141_PRIOR', 'JOBS.HCMRQ00048435', 'JOBS.HCMRQ095', 'JOBS.HCMRQ096_PRIOR', 'JOBS.HCOBW110', 'JOBS.HCOPW00053452', 'JOBS.HCPRM00046628', 'JOBS.HCPRW504_MEDICARE', 'JOBS.HCRXB001', 'JOBS.HCRXD00061131', 'JOBS.HCSND00066881', 'JOBS.HCTPDM01', 'JOBS.HMCLD001', 'JOBS.HMCLW022', 'JOBS.HMCLW023', 'JOBS.HMCLW024', 'JOBS.HMCRD00082420', 'JOBS.HMISD004', 'JOBS.HMISD006_1', 'JOBS.HMISD006_2', 'JOBS.HMISD006_3', 'JOBS.HMISD006_4', 'JOBS.HMISD006_5', 'JOBS.HMPRD00190981', 'JOBS.HMPRD00197899', 'JOBS.HMQMD00076709', 'JOBS.HMURD00072556', 'JOBS.HPCRM00057948', 'JOBS.HPCRM00058090', 'JOBS.HPFM00152693', 'JOBS.HPISD00068046', 'JOBS.HPPRD00056645', 'JOBS.HRDW2SAS_TEST', 'JOBS.HRFAM00184283', 'JOBS.HRPRB000158165', 'JOBS.LOADAVESISFILE', 'JOBS.LOADSUPERIORFILE', 'JOBS.LOCATION_LD_DLY', 'JOBS.L_DIAGNOSIS_CODE_LD', 'JOBS.L_DRG_CODE_LD', 'JOBS.L_REVENUE_CODE_LD', 'JOBS.MEMBER21AGEDOUT', 'JOBS.MEMBERS2HPP_UDDB', 'JOBS.MEMBER_COB_LD_DAILY', 'JOBS.MEMBER_LANGUAGE_LD_DAILY', 'JOBS.MEMBER_LD_DAILY', 'JOBS.MEMBER_PROVIDER_LD_DAILY', 'JOBS.NAVINET_ID_SELECT_DLY', 'JOBS.NAVINET_LOCATION_DLY', 'JOBS.POLICY_BENEFIT_LD', 'JOBS.POLICY_LOAD', 'JOBS.PP_REFERENCE_TABLE_CREATE', 'JOBS.PROVIDER_ACCEPT_PATS_LD_DLY', 'JOBS.PROVIDER_ID_LD_DLY', 'JOBS.PROVIDER_ID_SELECTION_DLY', 'JOBS.PROVIDER_LANGUAGE_LD_DLY', 'JOBS.PROVIDER_LD_DLY', 'JOBS.PROVIDER_SPECIALTY_LD_DLY', 'JOBS.PROV_AFFILIATION_LD_DLY', 'JOBS.REPORT6D_MEDICAL_PRIOR', 'JOBS.REPORT_6D_PHARMACY', 'JOBS.STATICFILE_LD_DLY', 'JOBS.UDDB2CLAIM_PAYMENT_DETAIL', 'JOBS.UDDB_FUNDDATA2SAS_TEST', 'JOBS.VENDOR_PROVIDERFILE_CLEANSING'
    ]
    
    attr_required = ["name","type","queue","run_id","activation_time","start_time","status","status_text"]
    all_data = []
    for job_name in ls_jobs:
        print(job_name)
        out = get_job_latest_run(base_url,config, job_name,start_date,max_limit=4)
        if out:
            for job in out:
                #print(f'{job.get("name")}|{job.get("type")}|{job.get("queue")}|{job.get("run_id")}|{job.get("start_time")}|{job.get("activation_time")}')
                data_out = { attr: job.get(attr) for attr in attr_required }
                #print(data_out)
                all_data.append(data_out)
        else:
           all_data.append({"name":job_name,"type":"NO A VALID JOB NAME OR NO EXECUTION "})
    df = pd.DataFrame(all_data)
    df.to_csv("sas_job_last_run_info_01.csv", index=False)


def get_job_names_of_sas(base_url,config,input_file,output_file = "sas_jobs_details.csv"):
    # input_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\sas_job_names.csv"

    df = pd.read_csv(input_file)
    required_attr = [ 'name' , 'type' , 'sub_type', 'platform','id', 'title', 'archive_key1', 'archive_key2', 
                     'agent', 'login' , 'folder_path' , 'folder_id' , 'client', 'is_inactive', 'start_type', 
                     'calendar_event_name' , 'calendar_event_ctype', 'calendar_event_valid_from', 
                     'calendar_event_valid_to', 'calendar_event_child_calendar_names' ]
    all_records = []
    for index, row in df.iterrows():
        sno = row["sno"]
        name = row["name"]
        job_name = row["job_name"]
        out_data = {"sno":sno, "input_name":name , "input_job_name":job_name }		
        payload =  {
        "filters": [
                {
                "object_name": job_name.upper(),
                "filter_identifier": "object_name"
                },{ 
                    "object_types": [        "JOBS" ,"JOBP"       ],       "filter_identifier": "object_type"}
            ], "max_results": 10
    }
        
        resp = search_api(base_url,config, payload)
        if resp:
            data_out = resp.get("data",{})
              
            for data in data_out:
                job_out = { attr: data.get(attr) for attr in required_attr }
             
                combined = out_data | job_out
                all_records.append(combined)
    
    df = pd.DataFrame(all_records)
    
    df.to_csv( output_file, index=False)
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv

# 1. Explicitly point to the .env file location
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    date_format = "%Y-%m-%dT%H:%M:%SZ" #YYYY-MM-DDTHH:MM:SSZ
    print("Check process workflow started")
    #Checking process/workflow started 
    job_name = "JOBP.CVS_HEALTH_DAILY_CET_PCT_LOAD_PROCESSES"
    hostname=os.getenv("HOSTNAME")
    port = 8088 #os.getenv("PORT")
    client_id = os.getenv("CLIENT_ID")
    username = os.getenv("AUTOMIC_USER")
    password = os.getenv("AUTOMIC_PASS")    
    base_url= f"http://{hostname}:{port}/ae/api/v1/{client_id}"
    print(base_url)
    # Usage
    endpoint = f"http://{hostname}:{port}/ae/api/v1"
    config = AutomicConfig(
    username=username,
    password=SecretStr(os.getenv("AUTOMIC_PASS") ),
    endpoint=endpoint,
    client_id=client_id,
    base_url=base_url
    )

    # db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_support_modern.accdb"
    db_file = r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
    ls_run_id = [ 85739919,85742033,85748827, 85742036]
    #[85734066 ,85732533 ,85741651 ,85742034 ,85747221 ,85747234 ,85755892 ,85759152 ,85768792 ,85762632]
    #[ 85739919,85742033,85748827,85751194,85755887,85760099,85762595,85766811 ]
    #[ 85358968,85381763,85378841,85397430]
    
    #file_path = '\\\\hpappworxts\\Applications\\Archive\\Incoming\\FTPTODAY\\USTHP\\202603301114.PHXJHPI.X12'
    #print(count_claims_x12_strict(file_path))
    url ="http://hpappworx01:8088/ae/api/v1/3000/search"
    payload ={ "filters": [ { "object_types": [        "JOBS"        ],       "filter_identifier": "object_type"}      ],    "start_at": 5000,   "max_results": 5000     }

    payload2 ={   "filters": [     {        "object_types": [         "JOBP"        ],       "filter_identifier": "object_type"}   ],   "max_results": 1800  }
    
    payload =  {
        "filters": [
                {
                "object_name": "820_DPWREMIT",
                "filter_identifier": "object_name"
                }
            ], "max_results": 10
    }
   
    # resp = post_with_basic_auth(url,config,payload2)
    # #print(resp)
    # data = resp.get("data")
    # print(resp.get('total'))
    # print(resp.get('hasmore'))
    # df = pd.DataFrame(data)
    # def get_worflow_jobs(job_name,config):
    #     url = f"http://h#pappworx01:8088/ae/api/v1/3000/objects/{job_name}"
    #     resp = post_with_basic_auth(url,config,{})
    #     print(resp)
    
    #analyse_sas_job_last_execution(base_url, config )
    # job_name = "JOBP.REQUEST_SFTP_SPAP_UPLOAD_PROCESS"
    # job_name = "JOBP.DAILY_IPLUS_837_INBOUND_OUTBOUND_PROCESS"
    # job_name = "JOBP.DAILY_BUILD_HEALTHTRIO_PROVIDER_DIRECTORY_DATA"
    # all_data = []
    # start_date="2026-03-01T00:00:00Z"
    # print("staring get job details")
    
    #print(get_schedules_for_workflow(base_url,config,{},job_name))
    # get_workflow_details_with_stats(base_url,config)
    wf_details = ""
    wf_details_file=r"all_workflow_details.csv"
    # workflow_name="JOBP.DAILY_BUILD_HEALTHTRIO_PROVIDER_DIRECTORY_DATA"
    workflow_name="JOBP.DAILY_ACA_SALESFORCE_ENROLLMENT"
    workflow_id = 9
    # job_name="JOBS.MONTHLY_820_DPWREMIT"

    
    add_jobs_to_ms_access_db(db_file, wf_details_file, workflow_name, workflow_id)
    add_job_rules_to_ms_access_db(db_file, wf_details_file, workflow_name, workflow_id)
    
    # file_path = r"\\hpfs\sharedsecure$\Operations\IS\ProductionControl\SFTP Landing Zone\PROD\Outbound\HPP\ALL\Daily\MacessEnrollment\Macess_files\DAILY\Archive\HRP_Rplylist_ACA_20260414184001.036"
    
    #print(get_file_metadata_from_shared_drive(file_path) )
    #current = r"\\hpfs\sharedsecure$\Operations\IS\ProductionControl\SFTP Landing Zone\PROD\Outbound\HPP\ALL\Daily\MacessEnrollment\Macess_files\DAILY\HRP_Rplylist_ACA_20260414184001.036"
    #print(current, "->", os.path.exists(current))
    run_id=85902947
    run_id=85908535
    # log_resp = get_job_logs(base_url=base_url,config=config,run_id=run_id)
    # print(log_resp)
    # result = parse_job_log(log_resp, external_log_loader=read_log_from_shared_drive)
    # print(result)
    # out = get_job_run_ids(base_url,config, job_name,start_date,max_limit=5000)
    # print(len(out))
    
    # df = pd.DataFrame(out, columns=["run_id"])
    # print(df.head())
    # sql_query ="select run_id from workflow_stats  where workflow_id=14"
    # out_db_data = read_ms_db_by_query(db_file,sql_query,['run_id'])
    #print(out_db_data)
    #db_df = pd.DataFrame(out_db_data)

    
    # set_df1 = set(df["run_id"])
    # set_df2 = set(db_df["run_id"])

    # only_in_df1 = set_df1 - set_df2
    # only_in_df2 = set_df2 - set_df1
    # in_both = set_df1 & set_df2
    # print(only_in_df1)
    # print(only_in_df2)

     
    # for worklfow in data :
    #     job_name = worklfow.get("name")
    #     print(job_name)
    #     try:
    #         res = get_workflow_job_details(base_url,config,job_name)
    #         all_data.extend(res)
    #     except:
    #         print(f"failed to process job -{job_name}")

    # df = pd.DataFrame(all_data)
    # #print(df.head(100))
    # df.to_csv("all_workflow_detail.csv", index=False)
    
    # job_name = "JOBP.DPW_DAILY_MEMBERSHIP"
    # response = get_object_details(base_url,config,job_name)
    # #data.jobp.workflow_definitions
    # workflow_def = response.get("data").get("jobp").get("workflow_definitions")
    # print(workflow_def)
    

    # job_name  ="JOBP.DAILY_BUILD_HEALTHTRIO_PROVIDER_DIRECTORY_DATA"
    # job_name  ="JOBP.DAILY_MEDICARE_ENCOUNTER_PROCESSING_PROCESS"
    # job_name  ="JOBP.DAILY_ACA_SALESFORCE_ENROLLMENT"
    #res = get_workflow_job_details(base_url,config,job_name)
    #print_dict_list_table(res)
    # print(df.to_string(index=False))
    #print(df.head(100))
    # ALL_ROWS.extend(process_job(base_url,config,job_name))
    #df.to_csv("healthtrio_workflow_detail.csv", index=False)


    # for job in data:
    #     workflow_name = job.get("name")
    #     print(workflow_name)


    #df = df[[ 'name','title','type','sub_type','id' ,'folder_path','archive_key1','archive_key2','references','is_inactive']]
    #print(df.head())
    
    # df.to_csv("all_jobs_5001.csv", index=False)

    # print("CSV file written successfully")

    # for run_id in ls_run_id:
    #     log_resp = get_job_logs(base_url=base_url,config=config,run_id=run_id)
    #     #print(log_resp)
    #     match = re.search(r'\bRET=(\d+)\b', log_resp)
    #     print(parse_job_log(log_resp, external_log_loader=read_log_from_shared_drive))
        


