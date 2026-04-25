import ast
import codecs
import csv
import html
import os
from pathlib import Path
import re
import statistics
import struct
from pydantic import SecretStr
import requests
from requests.auth import HTTPBasicAuth
import json 
import urllib3
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

from analyse_file import FileDispatcher, UnsupportedFileTypeError, count_claims_837,  count_claims_x12_strict, count_text_file_rows_with_header, get_file_metadata_from_shared_drive
from automic_apis import get_job_execution_children, get_job_execution_details, get_job_run_ids, get_job_run_ids_status
from configuration import AutomicConfig
from database_util import add_data_to_job_stats, add_run_id_workflow_status, get_last_completed_workflow_stats_from_db, get_run_id_stats_from_db, read_ms_db_by_query_with_groupby, read_ms_db_workflow_table, update_ms_access_table, update_run_id_workflow_status, upsert_data_to_job_stats
from log_parser import parse_job_log
from dotenv import load_dotenv

from validation_rules import RuleEngine
# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

   

def main():
    #main()
    #file_path = r"C:\Users\afe3356\sample1_202506251230.PHXJHPP.X12"
    #header_info = inspect_x12_header(file_path)
    #print(header_info)
    #pyx12_parse(file_path)
    #url = "https://hpappworx01:8488/ae/api/v1/3000/objects/$($job)"
    api_url = "https://hpappworx01:8488/ae/api/v1/3000/search"
    payload ={
  "filters": [
    {
      "object_name": "JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSP_DOWNLOAD",
      "filter_identifier": "object_name"
    }
  ],
  "max_results": 10
    }
    #resp = post_with_basic_auth(api_url, payload)
    #print(json.dumps(resp, indent=2))
    job ="JOBS.DAILY_COPY_ACCUMS_CVS_ACA_HMO"
    job ="JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    job = "JOBP.REQUEST_SFTP_OPTUM_ECG_UPLOAD_PROCESS"
    #job = "JOBS.REQUEST_SFTP_CHANGEHEALTH_PCS_COBA_UPLOAD"
    #job = "JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSI_DOWNLOAD"
    ls_jobs = [ "JOBS.CAID_DHS_COPY_FIRST_820FILE_TO_LZ"    ]

    print("exuting get job details")
    #ls_run_ids = get_job_run_ids("https://hpappworx01:8488/ae/api/v1/3000/executions",job,"2026-03-1T00:00:00Z", )
    
    #print(ls_run_ids)
    res = get_job_execution_details("https://hpappworx01:8488/ae/api/v1/3000","85175973")
    print(res)
    ls_run_ids = [ "85175973","85182374","85180512","85175975","85174402"]
    ls_run_ids = []
    #print_dict_list_table([res])
    for run_id in ls_run_ids:
        res = get_job_execution_children("https://hpappworx01:8488/ae/api/v1/3000",run_id)
        #print_dict_list_table(res.get("data"))
        print(res)
        #print_dict_list_table(res)

    
    #for job in ls_jobs:
    #    failed_ids = get_failed_run_ids("https://hpappworx01:8488/ae/api/v1/3000/executions",job,"2025-01-01T00:00:00Z")
    #    
    #print(f"Found {len(failed_ids)} failures for {job}: {failed_ids}")
    #print(f'{job},{len(failed_ids)},"{failed_ids}"')
    base_url = "https://hpappworx01:8488/ae/api/v1/3000"
    
    job_failure_detail_file=r"C:\Users\afe3356\OneDrive - Health Partners Plans, Inc\JHP_PROD_JOb_Failures.csv"
    #failure_job_logs (base_url,job_failure_detail_file)
    #resp = get_job_logs(base_url,"84641496","REP")
    

    process_name = "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    process_name = "JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSP_DOWNLOAD"
    #process_name = "JSCH.BATCH.BATCH"
    filter = {
        "filters": [
            {
            "filter_identifier": "object_name",
            "object_name": "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS" 
            },
            {
            "filter_identifier": "object_type",
            "object_types": ["JOBP", "JOBS"]
            }
        ],
        "max_results": 20
    }

    # resp = get_object_details(base_url,process_name)
    # root_key = "jobs"
    # root_data = resp['data'][root_key]
    # print(root_data.get('schedule_tasks'))
    # general_attributes =root_data['general_attributes']

    # if 'workflow_definitions' in root_data:
    #     workflow_def = root_data['workflow_definitions']  
    #     print_dict_list_table(workflow_def)
    
    # print_dict_list_table([general_attributes])
    # print_dict_list_table([root_data['job_attributes']])
    
    # for key, value in root_data.items():
    #     print(key)

    # if 'condition_values' in root_data:
    #     print_dict_list_table(root_data['condition_values'])
    # if 'conditions' in root_data:
    #     print_dict_list_table(root_data['conditions'])
    # #read_ms_access_db()
    # process_name = "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    # process_name = "JOBS.DAILY_SFTP_FTPTODAY_USTHP_ZELIS_CLAIMSP_DOWNLOAD"
    # process_name = "JSCH.BATCH.BATCH"
    # Example usage for your validation wrapper
    #timing = get_workflow_timing_from_schedule(base_url, "JSCH.BATCH.BATCH", "JOBP.DAILY_IPLUS_ZELIS_CLAIM_REPRICER_INBOUND_PROCESS")
    #print_dict_list_table(timing)


def validate_run_time(runtime, estimated_run_time, percent_threshold,absolute_threshold=30):
    
    # IF estimated_runtime < 60 seconds:
    #     classify using absolute deviation (±30s)
    # ELSE:
    #     classify using percentage deviation (±10%)

    deviation = (runtime - estimated_run_time)  
    abs_deviation = abs(deviation)
    deviation_percent = abs_deviation/estimated_run_time
    ert_analysis_result = "Normal"
   
    if abs_deviation > absolute_threshold and deviation_percent > percent_threshold:
        if estimated_run_time > runtime  :
            ert_analysis_result ="Below Threshold" 
            #print(f"Estimated_run_time-{estimated_run_time} , real run time - {runtime},{abs_deviation},job run LESS than estimated (Underrun)")
        elif estimated_run_time < runtime:
            #print(f"Estimated_run_time-{estimated_run_time} , real run time - {runtime},{abs_deviation},job run More than estimated (Overrun)")
            ert_analysis_result = "Above Threshold"
    else:
        ert_analysis_result = "Normal"
        #print(f"Estimated_run_time-{estimated_run_time} , real run time - {runtime},{abs_deviation},job run with in range")

    return ert_analysis_result

def analyse_job(data:dict,base_url,config):
    run_id = data.get("run_id")
    activator = data.get("activator")
    parent = data.get("parent")
    activator_object_type = data.get("activator_object_type")
    reference_run_id = data.get("reference_run_id")
    runtime = data.get("runtime")
    estimated_runtime = data.get("estimated_runtime")
    analyse_out = {}
    trigger_by =""
    activator_id = ""
    if activator == parent and activator_object_type == "JOBP":
        print("job triggered by workflow process ")
        trigger_by ="auto_triggered"
        activator_id = activator_object_type
    elif  activator != parent :
        print("manually Triggered job")
        res = get_job_execution_details(base_url,config,activator)
        activator_id = res.get("name")
        if res.get("type") == "USER":
            print(f"invoked by user {activator_id}")
        if reference_run_id != 0 :
            print(f"job was reran by user for previos run - {reference_run_id} ")
        trigger_by ="manual_triggered"
    analyse_out["activator"] = trigger_by
    
    analyse_out["activator_id"] = activator_id
    #resp = get_job_execution_ert(base_url,config,run_id)
    #print(resp)
    #estimated_run_time = resp.get("ert")
    #print(f"############################# estimated_run_time run time {estimated_runtime}")
    #real_run_time = resp.get("rrt")
    estimated_erts = ""
    ert_analysis_result = "Not Applicable"
    THRESHOLD = 0.1
    absolute_threshold = 30 
    if estimated_runtime and runtime :
        ert_analysis_result  =  validate_run_time (runtime, estimated_runtime, percent_threshold=THRESHOLD,absolute_threshold=absolute_threshold)         
    # else:
    #     average_run_time = get_job_stats_ert(base_url,config,data.get("name"))
    #     print(f"#############################average run time {average_run_time}")
    #     if average_run_time:
    #         estimated_run_time = average_run_time
    #         ert_analysis_result  =  validate_run_time (runtime, estimated_runtime, THRESHOLD)
    analyse_out["estimated_runtime"] = estimated_runtime
    analyse_out["runtime"] = runtime
    #analyse_out["estimated_run_time"] = estimated_erts
    analyse_out["ert_analysis_result"] = ert_analysis_result
    return analyse_out

def check_run_id_is_processed(run_id,):
    pass

def get_job_stats(automic_config:AutomicConfig,job_name=None, run_id=None,main_workflow_run_id=None,start_time=None):
    base_url = automic_config.base_url
    if job_name:
        ls_run_ids = get_job_run_ids(base_url,automic_config,job_name,start_date=start_time )        
    else:
        ls_run_ids = [run_id]
    print(ls_run_ids)
    output_data = []

    print("if process started then get child process status ")
    for parent_run_id in ls_run_ids:
        #check id is already procesed 
        #if id not processed make an entry in status table then proceed with analysis 
        # if id already present then check the status , if its not complete then do analysis
        # if id exist and process_status complete then skip the id 
        res = get_job_execution_children(base_url,automic_config,parent_run_id)
        result_data= res["data"]
        if result_data:
            sorted_data = sorted( result_data, key=lambda x: ( datetime.strptime(x.get('start_time') or x.get('end_time') or "1900-03-20T07:10:14Z" , date_format) , x.get("line_number") ), reverse=False)
            for data in sorted_data:
                run_id = data.get("run_id")
                object_type = data.get("type")
                status = data.get("status")
                status_text = data.get("status_text")
                name = data.get("name")
                start_time = data.get('start_time') 
                end_time  = data.get('end_time')
                resp_data = { "workflow_run_id":main_workflow_run_id,"parent_run_id":parent_run_id,"run_id": run_id,"object_type":object_type ,"object_name":name, 
                             "status":status,"status_text" : status_text, "start_time":start_time,"end_time":end_time,
                             'line_number': data.get("line_number"), 'priority':data.get("priority"), 'agent':data.get("agent"), 'platform':data.get("platform")
                             ,'queue':data.get("queue") ,'activation_time':data.get('activation_time')}
                analyse_out = {}
                if object_type == "JOBP" and (status == 1900 or status == 1800 or status == 1850 or status == 1851):
                    print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    sub_out_data = get_job_stats(automic_config,run_id=run_id,main_workflow_run_id=main_workflow_run_id)
                    analyse_out  = analyse_job(data,base_url,automic_config)
                    output_data.append(resp_data |analyse_out)
                    output_data = output_data + sub_out_data
                else:    
                    if status == 1900 : # job ended ok - ENDED_OK - ended normally
                        print(f"{parent_run_id}|{run_id}|{object_type}{name}|{status_text}|{start_time}|{end_time}")
                        analyse_out  = analyse_job(data,base_url,automic_config)
                    elif status == 1800: #ENDED_NOT_OK - aborted  
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    elif status == 1700: # Waiting for predecessor
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    elif status == 1851: # ENDED_JP_CANCEL - Workflow canceled manually.
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    elif status == 1850: # ENDED_CANCEL - manually canceled.
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    elif status == 1560: #status - 1560 |Workflow is blocked
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    else:
                        print(f"unhandled job status - {status} |{status_text}")
                        print(f"{parent_run_id}|{run_id}|{object_type}|{name}|{status_text}|{start_time}|{end_time}")
                    
                    output_data.append(resp_data | analyse_out)
    return output_data 



def parse_access_extended(bdata):
    if isinstance(bdata, bytes):
        s = bdata.decode('ascii', errors='ignore').strip('\x00')
    else:
        s = str(bdata)
    
    # Pattern: YYYYMMDDHHNNSS.nnnnnnnn or similar
    match = re.match(r'(\d{8})(\d{6})(\d+)', s)
    if match:
        date_str, time_str, nano_str = match.groups()
        dt_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        
        # Truncate nanoseconds to microseconds (Access max precision)
        nanos = int(nano_str[:6]) if len(nano_str) >= 6 else 0  # First 6 digits
        dt = dt.replace(microsecond=nanos * 1000)
        return dt
    return pd.NaT



def get_job_stats_ert(base_url,config:AutomicConfig, job_name):
    endpoint = f"{base_url}/executions/"
    avg_runtime = 0
    # Query parameters: 
    # Filter by name, status 1900 (Ended OK), and sort by descending start time
    params = {
        "job_name": job_name,
        "status": 1900,
         "include_deactivated": "true",
         "sort": "startTime",
         "order": "desc",
        "max_results": 20  # Fetch more than 5 to ensure we have enough "Successful" ones
    }

    try:
        response = requests.get(
            endpoint, 
            auth=config.auth, 
            params=params,            
            verify=False
        )
        #response.raise_for_status()
        data = response.json()
        print(data)
        executions = data.get("data", [])
        
        # Filter for "ENDED_OK" and take the top 5
        successful_runs = [exe for exe in executions if exe.get("status") ==  1900][:5]
        
        if not successful_runs:
            print("No successful executions found for this job.")
            return None

        runtimes = []
        print(f"--- Last 5 Successful Executions for {job_name} ---")
        
        for exe in successful_runs:
            print(exe)
            run_id = exe.get("run_id")
            start = exe.get("activation_time")
            # Runtime is usually provided in seconds by the API
            duration = exe.get("runtime", 0) 
            
            runtimes.append(duration)
            #print(f"RunID: {run_id} | Started: {start} | Duration: {duration}s | exe.get("runtime"))

        # Calculate Average
        avg_runtime = statistics.mean(runtimes)
        
        print("-" * 40)
        print(f"Average Runtime (Last 5): {round(avg_runtime, 2)} seconds")
        return avg_runtime
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Automic API: {e}")


def run_analysis(db_file,config,start_time_from = "2026-03-19T00:00:00Z",max_result=5000):
    ls_workflow= read_ms_db_workflow_table(db_file)
    for workflow in ls_workflow:
        start_time = start_time_from
        job_name = workflow.get("object_name")
        object_type = workflow.get("object_type")
        workflow_id = workflow.get("workflow_id")
    
        last_run_job  = get_last_completed_workflow_stats_from_db( db_file,workflow_id)
        if last_run_job:
            last_start_time = last_run_job.get("start_time")
            #ts_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            # 2. Subtract 1 minute
            last_start_time = last_start_time - timedelta(minutes=1)
            start_time = last_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            print("No last complete job for this workflow")   

        result = get_job_run_ids_status(base_url, config, job_name, object_type,start_date=start_time,max_result=max_result)
        if len(result) == 0 :
            print(f"No run found for workflow id - {workflow_id} job_name -'{job_name}'")
            continue

        #db_result = get_run_id_stats_from_db(db_file,"85713356")
        sorted_result = sorted( result, key=lambda x: ( datetime.strptime(x.get('start_time') or x.get('end_time') or "2900-03-20T07:10:14Z" , date_format) ), reverse=False)
        # print(result)
        selected_keys = ['queue', 'run_id', 'status', 'status_text', 'activation_time', 'start_time','end_time', 'parent', 'reference_run_id',
                          'estimated_runtime', 'runtime','activator','activator_object_type','activation_time','priority','linenumber','user' ]
        for run_details in sorted_result:
            run_id = run_details.get("run_id")
            status = run_details.get("status")
            print(run_id)
            db_result = get_run_id_stats_from_db(db_file,run_id)
            if db_result is None:
                #add data to DB with stating processing
                print("add data to DB")
                #run_id, start_date,end_date,analysis_status
                start_timestamp_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                #print(start_timestamp_str)             
                details = {}
                #1300–1799   ACTIVE / WAITINGJob not finished yet
                #1800–1899   ANY_ABEND (FAILURE)Job ended abnormally
                #1900–1999   ANY_OK (SUCCESS)Job ended normally
                
                details = {"run_id":run_id}
                details["workflow_id"] = int(workflow_id)
                details["analysis_status"] = "Started"
                details["analysis_start_time"] = start_timestamp_str
                details["analysis_end_time"] = None
                 
                # Efficient extraction
                filtered_dict = {key: run_details[key] for key in selected_keys if key in run_details}
                details = details | filtered_dict
                #start procesing the run_id
                add_run_id_workflow_status(db_file,details)
                status_result = get_job_stats(config,run_id=run_id, main_workflow_run_id=run_id, start_time=None)
                add_data_to_job_stats(status_result,db_file) 
                if status >= 1800 and status <= 1999:
                    update_data = {"run_id":run_id}
                    update_data["analysis_status"] = "Completed"  
                    end_timestamp_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")           
                    update_data["analysis_end_time"] = end_timestamp_str
                    update_run_id_workflow_status(db_file, update_data) 
            else:
                print("Update data to DB")
                print(db_result)
                analysis_status = db_result.get("analysis_status")
                if analysis_status == "Completed":
                    continue
                else:
                    #Perform analysis 
                    print("perform analysis")
                    #start procesing the run_id
                    status_result = get_job_stats(config,run_id=run_id, main_workflow_run_id=run_id, start_time=None)
                    upsert_data_to_job_stats(status_result,db_file) 
                    if status >= 1800 and status <= 1999:
                        
                        update_data = {"run_id":run_id}
                        update_data["analysis_status"] = "Completed"  
                        end_timestamp_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")           
                        update_data["analysis_end_time"] = end_timestamp_str
                        filtered_dict = {key: run_details[key] for key in selected_keys if key in run_details}
                        update_data =  update_data | filtered_dict
                        print(update_data)
                        update_run_id_workflow_status(db_file, update_data) 


def run_validation_rules(db_path,base_url,config):

    query_1 = """ select  distinct run_id  from  (workflow_stats A
inner join jobs B on B.workflow_id = A.workflow_id )
inner join  job_rules C on C.job_id = B.job_id 
where A.rules_applied_Status  is NULL and A.analysis_status = 'Completed' and D.is_Active = True """
    query = """select D.rule_id,D.rule_param,D.execution_order , E.rule_name , A.job_id , A.workflow_id, A.object_name , 
    C.run_id , C.workflow_run_id , B.analysis_status ,B.rules_applied_Status ,C.line_number  
    from ( ( ( jobs A inner join workflow_stats B on B.workflow_id = A.Workflow_id)   
inner join job_stats C on C.object_name = A.object_name and  C.workflow_run_id = B.run_id )
inner join job_rules D on D.job_id = A.job_id  )
inner join rules E on E.rule_id = D.rule_id 
where B.rules_applied_Status  is NULL and B.analysis_status = 'Completed'  and B.status not in ( 1850,1550 )
and D.is_Active = True  order by C.workflow_run_id,C.line_number ,D.execution_order;"""

    #get worfklow run_ids 
    # ls_workflow_ids = read_ms_db_by_query(db_path,query_1,["run_id"])
    # for workflow_run_id in ls_workflow_ids:
    #     print(workflow_run_id)    
    grouped_data = read_ms_db_by_query_with_groupby(db_path,query,["rule_name","object_name","run_id","job_id","rule_id","rule_param","execution_order","workflow_run_id"],"workflow_run_id")
    
    
    for workflow_run_id, rows in grouped_data.items():
        print(f"Processing workflow_run_id: {workflow_run_id}")
        print(f"Rows: {len(rows)}")
        is_processing_success = True
        engine = RuleEngine(
            base_url=base_url,
            config=config,
            db_path=db_path
        )

       
        # for rule in rows:
        #     rule_name  = rule.get("rule_name")
        #     run_id  = rule.get("run_id")
        #     workflow_run_id  = rule.get("workflow_run_id")
        #     rule_id  = rule.get("rule_id")
        #     job_id  = rule.get("job_id")
        #     object_name  = rule.get("object_name")
        #     # print(object_name)
        #     try:
        #         if rule_name == "FILE_TRANSFER_STATS" :

        #             log_resp = get_job_logs(base_url=base_url,config=config,run_id=run_id)
        #             #print(log_resp)
        #             # match = re.search(r'\bRET=(\d+)\b', log_resp)
        #             result = parse_job_log(log_resp, external_log_loader=read_log_from_shared_drive)
        #             data = { "job_id" : job_id , "job_run_id": run_id, "workflow_run_id":workflow_run_id, "rule_id": rule_id,"result": str(result),"last_update_time":datetime.now()}
        #             add_ms_access_table(db_path,table_name="rules_result",data=data)
        #         elif rule_name == "FILE_METADATA":
        #             rule_param  = rule.get("rule_param")
        #             dict_rule_param = ast.literal_eval(rule_param)  
        #             file_path = dict_rule_param.get("file_path")
        #             operation_type = dict_rule_param.get("operation")
        #             ref_rule_id = dict_rule_param.get("ref_rule_id")
        #             query_3= "select result from rules_result where job_id = ? and workflow_run_id = ? and rule_id = ?"
        #             db_result = read_ms_db_by_query(db_path,query_3, ["result"],[job_id,workflow_run_id, ref_rule_id])
        #             output_result = ""
        #             if db_result:
        #                 job_result = db_result[0].get("result")
        #                 if job_result == "[]":
        #                     print("no file to get metadata")
        #                     output_result = "no file to get metadata"
        #                 else:
        #                     ls_result = ast.literal_eval(job_result)
        #                     ls_data = []
        #                     for res in ls_result :
        #                         command = res.get("operation")
        #                         files = res.get("files")
        #                         if command == operation_type:
        #                             if len(files) > 0 :
                                        
        #                                 for file_name in files:
        #                                     full_file_path= f"{file_path}{file_name}" 
        #                                     files_meta_data = get_file_metadata_from_shared_drive(full_file_path)
        #                                     print(files_meta_data)
        #                                     ls_data.append(files_meta_data)
        #                     output_result = str(ls_data)
                                
        #             data = { "job_id" : job_id , "job_run_id": run_id, "workflow_run_id":workflow_run_id, "rule_id": rule_id,"result": output_result ,"last_update_time":datetime.now()}
        #             add_ms_access_table(db_path,table_name="rules_result",data=data)
        #         elif rule_name == "GET_RECORD_COUNT":
        #             rule_param  = rule.get("rule_param")
        #             dict_rule_param = ast.literal_eval(rule_param)  
        #             file_path = dict_rule_param.get("file_path")
        #             operation_type = dict_rule_param.get("operation")
        #             file_type = dict_rule_param.get("file_type")
        #             query_3= "select result from rules_result where job_id = ? and workflow_run_id = ? and rule_id = ?"
        #             db_result = read_ms_db_by_query(db_path,query_3, ["result"],[job_id,workflow_run_id,1])
        #             output_result = ""
        #             if db_result:
        #                 job_result = db_result[0].get("result")
        #                 if job_result == "[]":
        #                     print("no file to get metadata")
        #                     output_result = "no file to get metadata"
        #                 else:
        #                     ls_result = ast.literal_eval(job_result)
        #                     ls_data = []
        #                     grouped_count = {}
        #                     dispatcher = FileDispatcher()
        #                     for res in ls_result :
        #                         command = res.get("operation")
        #                         files = res.get("files")
        #                         file_pattern = res.get("file_pattern") 
                                
        #                         if command == operation_type:
        #                             if len(files) > 0 :
                                        
        #                                 for file_name in files:
        #                                     full_file_path= f"{file_path}{file_name}" 
        #                                     try:
        #                                         row_count = dispatcher.dispatch(
        #                                                     file_type=file_type,
        #                                                     file_name=file_name,
        #                                                     full_file_path=full_file_path
        #                                                 )
                                                
        #                                         ls_data.append({file_name: row_count})
        #                                         grouped_count[file_pattern] = grouped_count.get(file_pattern, 0) + row_count
                                            
        #                                     except UnsupportedFileTypeError:
        #                                             print(f"Unsupported file: {file_name}")
        #                                             continue

        #                                     except Exception as e:
        #                                         print(f"Error processing {file_name}: {e}")
        #                                         continue


        #                                     # if  (file_type is None and str(file_name).lower().endswith(".x12")) or file_type == ".x12" :
        #                                     #     row_count = count_claims_x12_strict(full_file_path)     
        #                                     #     ls_data.append({file_name: row_count})
        #                                     #     if file_pattern in grouped_count:
        #                                     #         ct = grouped_count.get(file_pattern)
        #                                     #         ct = ct + row_count
        #                                     #         grouped_count[file_pattern] = ct
        #                                     #     else:
        #                                     #         grouped_count[file_pattern] = row_count
        #                                     # elif   str(file_name).lower().endswith(".txt"):
        #                                     #     row_count = count_text_file_rows_with_header(full_file_path)     
        #                                     #     ls_data.append({file_name: row_count})
        #                                     #     if file_pattern in grouped_count:
        #                                     #         ct = grouped_count.get(file_pattern)
        #                                     #         ct = ct + row_count
        #                                     #         grouped_count[file_pattern] = ct
        #                                     #     else:
        #                                     #         grouped_count[file_pattern] = row_count
        #                                     # elif str(file_name).lower().endswith(".837"):
        #                                     #     row_count = count_claims_837(full_file_path)     
        #                                     #     ls_data.append({file_name: row_count})
        #                                     #     if file_pattern in grouped_count:
        #                                     #         ct = grouped_count.get(file_pattern)
        #                                     #         ct = ct + row_count
        #                                     #         grouped_count[file_pattern] = ct
        #                                     #     else:
        #                                     #         grouped_count[file_pattern] = row_count
        #                                     # else:
        #                                     #     print(f"file format not yet supported - {file_pattern}")
                                                
        #                     out_data = {"grouped_count": grouped_count , "file_count" :ls_data}
        #                     output_result = str(out_data)
                                
        #             data = { "job_id" : job_id , "job_run_id": run_id, "workflow_run_id":workflow_run_id, "rule_id": rule_id,"result":output_result,"last_update_time":datetime.now() }
        #             add_ms_access_table(db_path,table_name="rules_result",data=data)
        #     except Exception as ex:
        #         print(f"Error while processing workflow_run_id = {workflow_run_id} and job_run_id ={run_id}")
        #         print(ex)
        #         is_processing_success = False
        #         break

        try:
            engine.run(rows)
        except Exception as exc:
            # Failure case
            update_data = {
                "run_id": workflow_run_id,
                "rules_applied_status": "Failed"
            }
            # optional but recommended
            print(f"Engine run failed for run_id={workflow_run_id}")
        else:
            # Success case
            update_data = {
                "run_id": workflow_run_id,
                "rules_applied_status": "Complete"
            }

        update_ms_access_table(
            db_path,
            table_name="workflow_stats",
            update_id="run_id",
            data=update_data
)


def load_config():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    
    hostname=os.getenv("HOSTNAME")
    port = os.getenv("PORT")
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
    return config
if __name__ == "__main__":
    

# 1. Explicitly point to the .env file location
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    date_format = "%Y-%m-%dT%H:%M:%SZ" #YYYY-MM-DDTHH:MM:SSZ
    print("Check process workflow started")
    #Checking process/workflow started 
    job_name = "JOBP.CVS_HEALTH_DAILY_CET_PCT_LOAD_PROCESSES"
    hostname=os.getenv("HOSTNAME")
    port = os.getenv("PORT")
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

 
    db_file = r"C:\Users\nlr9894\OneDrive - Health Partners Plans, Inc\Documents\prod_crl_modern.accdb"
    run_analysis( db_file, config, "2026-02-01T00:00:00Z", max_result=5000)
    run_validation_rules(db_file, base_url, config)
    # log_resp = get_job_logs(base_url=base_url,config=config,run_id=85743099)
    #                 #print(log_resp)
    #                 # match = re.search(r'\bRET=(\d+)\b', log_resp)
    # result = parse_job_log(log_resp, external_log_loader=read_log_from_shared_drive)
    # print(result)
                   
    #log_resp = get_job_logs(base_url=base_url,config=config,run_id="85610467")
    #print(log_resp)
    #result = parse_job_log(log_resp, external_log_loader=read_log_from_shared_drive)
    #print(result)
     
    

