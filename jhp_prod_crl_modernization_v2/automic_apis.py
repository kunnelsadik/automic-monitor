
import csv
import html
import os
import re

import requests

from configuration import AutomicConfig
 


def post_with_basic_auth(url,config:AutomicConfig, payload):
    
    try:
        response = requests.post(
            url, 
            json=payload, 
            auth=config.auth,
            timeout=10,
            verify=False
        )
        #print(response.content)
        # Raises an error for 4xx or 5xx status codes
        response.raise_for_status()
        
        return response.json()
    except requests.exceptions.HTTPError as err:
        return f"HTTP error occurred: {err}"
    except Exception as err:
        return f"An error occurred: {err}"


def search_api(base_url,config:AutomicConfig, payload):

    try:
        url = f"{base_url}/search"
        response = requests.post(
            url, 
            json=payload, 
            auth=config.auth,
            timeout=10,
            verify=False
        )
        #print(response.content)
        # Raises an error for 4xx or 5xx status codes
        response.raise_for_status()
        
        return response.json()
    except requests.exceptions.HTTPError as err:
        return f"HTTP error occurred: {err}"
    except Exception as err:
        return f"An error occurred: {err}"
    

def write_to_file(file_name,data,mode="w"):
     

    # 'w' stands for write
    with open(file_name, mode) as file:
        file.write(data)
    print("Data written successfully.")




def normalize_automic_log(log_text: str) -> str:
    if not log_text:
        return ""
    
    # 1️⃣ Decode HTML entities (&gt; &lt; &amp; etc.)
    log_text = html.unescape(log_text)

    # 2️⃣ Decode JSON-style escaped backslashes (\\ → \)
    # log_text = codecs.decode(log_text, "unicode_escape")

    # 1. Normalize line endings
    log_text = log_text.replace("\r\n", "\n").replace("\r", "\n")

   # log_text = html.unescape(log_text)
    # 2. Remove BOM if present
    log_text = log_text.lstrip("\ufeff")

    
    # ✅ 4. REMOVE TRAILING WHITESPACE ON EACH LINE (THIS IS CRITICAL)
    log_text = "\n".join(line.rstrip() for line in log_text.splitlines())


    # 3. Remove non-printable control chars (except newline & tab)
    log_text = re.sub(r"[^\x09\x0A\x20-\x7E]", "", log_text)

    # 4. Fix broken command prompts (API often splits lines)
    # Ensures "c:>copy" is on a fresh line
    log_text = re.sub(r'(?<!\n)([A-Za-z]:\\?>\s*(copy|move)\s+")',
                      r'\n\1', log_text, flags=re.IGNORECASE)

    return log_text
    
def get_job_logs(base_url,config,run_id,report_type="REP"):
    """
    Endpoint: GET /ae/api/v1/{client}/executions/{run_id}/reports/{report_type}
    Common Report Types:
    REP: The standard job log (STDOUT).
    ACT: The activation log.
    PLOG: The agent log.
    """
   
    
    url = f"{base_url}/executions/{run_id}/reports/{report_type}"
    #print(f"URL = {url}" )   
    response = requests.get(
        url,          
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()       
        page = 0 
        for item in res_data.get("data", []):
            content = item.get("content", "")
            if content:
                full_log += content + "\n"

        full_log = normalize_automic_log(full_log)
          
        hasmore = res_data['hasmore']  
        if hasmore :
            print(f"More data to extratct. execte api for further page, current page {page}")
        return full_log

    elif response.status_code == 404:  
        url = f"{base_url}/executions/{run_id}/reports/LOG"
        response = requests.get(
        url,          
        auth=config.auth,
        verify=False    )
        
        full_log = ""
        res_data = {}
        if response.status_code == 200:
            res_data = response.json()
        
            ls_data = res_data['data']
        
            page = 0 
            
            for item in res_data.get("data", []):
                content = item.get("content", "")
                if content:
                    full_log += content + "\n"

            full_log = normalize_automic_log(full_log)
            hasmore = res_data['hasmore']
            if hasmore :
                print(f"More data to extratct. execte api for further page, current page {page}")
        
        return full_log
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def get_ext_logs_path(base_url,config,run_id,report_type="REP"):
    log_text  = get_job_logs(base_url,config,run_id,report_type)
    return log_text


def get_job_execution_details(base_url,config,run_id ):
    """
    Endpoint: GET /ae/api/v1/{client}/executions/{run_id}
     
    """
 
    
    url = f"{base_url}/executions/{run_id}"
    #print(f"URL = {url}" )   
    response = requests.get(
        url,          
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()
        # Extracting just the run_ids from the list of executions
        #run_ids = [task['run_id'] for task in data.get('data', [])]
        #ls_data = res_data['data']
        
        page = 0 
        # for data in ls_data:
        #     full_log = full_log + "\n" + data["content"] 
        #     page=data["page"]
           
           
        # hasmore = res_data['hasmore']  
         
        return res_data
     
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []    

def get_job_execution_ert(base_url,config, run_id ):
    """
    Endpoint: GET /ae/api/v1/{client}/executions/{run_id}/ert
     
    """
 
    
    url = f"{base_url}/executions/{run_id}/ert"
    #print(f"URL = {url}" )   
    response = requests.get(
        url,          
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()
         
        return res_data
     
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return {}
  
def get_job_execution_children(base_url,config:AutomicConfig,run_id ):
    """
    Endpoint: GET /ae/api/v1/{client}/executions/{run_id}/children
     
    """
    
    url = f"{base_url}/executions/{run_id}/children"
    #print(f"URL = {url}" )   
    response = requests.get(
        url,          
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()
        # Extracting just the run_ids from the list of executions
        #run_ids = [task['run_id'] for task in data.get('data', [])]
        #ls_data = res_data['data']
        
        page = 0 
        # for data in ls_data:
        #     full_log = full_log + "\n" + data["content"] 
        #     page=data["page"]
           
           
        # hasmore = res_data['hasmore']  
         
        return res_data
     
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []    

def get_job_execution_variables(base_url,config:AutomicConfig,run_id ):
    """
    Endpoint: GET /ae/api/v1/{client}/executions/{run_id}/variables
     
    """ 
    
    url = f"{base_url}/executions/{run_id}/variables"
    #print(f"URL = {url}" )   
    response = requests.get(
        url,          
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()
        # Extracting just the run_ids from the list of executions
        #run_ids = [task['run_id'] for task in data.get('data', [])]
        #ls_data = res_data['data']
        
        page = 0 
        # for data in ls_data:
        #     full_log = full_log + "\n" + data["content"] 
        #     page=data["page"]
           
           
        # hasmore = res_data['hasmore']  
         
        return res_data
     
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []    
    
def get_failed_run_ids(base_url,config:AutomicConfig ,job_name,start_date):
    #base_url = "https://your-automic-server:8088/ae/api/v1/1000/executions"
 
    # Status 1800 = ENDED_NOT_OK, 1822 = FAULT_OTHER
    failed_statuses = "1800,1801,1802,1810,1815,1820,1821,1822,1823,1824,1825,1826,1827,1828,1829,1830,1856"
    sucess_status ="1900"
    params = {
        "name": job_name,
        "status": failed_statuses, 
        "include_deactivated": "true",
        "time_frame_from": start_date, # Format: YYYY-MM-DDTHH:MM:SSZ
    }
    
    response = requests.get(
        base_url, 
        params=params, 
        auth=config.auth,
        verify=False
    )
    
    if response.status_code == 200:
        data = response.json()
        # Extracting just the run_ids from the list of executions
        run_ids = [task['run_id'] for task in data.get('data', [])]
        return run_ids
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def get_job_latest_run(base_url:str,config:AutomicConfig, job_name,start_date, start_at_run_id=None,max_limit=1000 ):
    #base_url = "https://your-automic-server:8088/ae/api/v1/1000/executions"
 
    # Status 1800 = ENDED_NOT_OK, 1822 = FAULT_OTHER
    failed_statuses = "1800,1801,1802,1810,1815,1820,1821,1822,1823,1824,1825,1826,1827,1828,1829,1830,1856"
    sucess_status ="1900"
    params = {
        "name": job_name,
        "include_deactivated": "true",
        "max_results": max_limit,
        #"status": sucess_status
        "time_frame_from": start_date # Format: YYYY-MM-DDTHH:MM:SSZ
    }
    if start_at_run_id:
        params["start_at_run_id"] = start_at_run_id
    url = f"{base_url}/executions" 
    response = requests.get(
        url, 
        params=params, 
        auth=config.auth,
        verify=False
    )
    #print(response.json())
    if response.status_code == 200:
        data = response.json()
        # Extracting just the run_ids from the list of executions
        # run_ids = [task['run_id'] for task in data.get('data', [])]
        #print_dict_list_table(data.get("data"))
        return data.get('data', [])
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []


def get_job_run_ids(base_url:str,config:AutomicConfig, job_name,start_date, start_at_run_id=None,max_limit=1000 ):
    #base_url = "https://your-automic-server:8088/ae/api/v1/1000/executions"
 
    # Status 1800 = ENDED_NOT_OK, 1822 = FAULT_OTHER
    failed_statuses = "1800,1801,1802,1810,1815,1820,1821,1822,1823,1824,1825,1826,1827,1828,1829,1830,1856"
    sucess_status ="1900"
    params = {
        "name": job_name,
        "include_deactivated": "true",
        "max_results": max_limit,
        #"status": sucess_status
        "time_frame_from": start_date # Format: YYYY-MM-DDTHH:MM:SSZ
    }
    if start_at_run_id:
        params["start_at_run_id"] = start_at_run_id
    url = f"{base_url}/executions" 
    response = requests.get(
        url, 
        params=params, 
        auth=config.auth,
        verify=False
    )
    #print(response.json())
    if response.status_code == 200:
        data = response.json()
        # Extracting just the run_ids from the list of executions
        run_ids = [task['run_id'] for task in data.get('data', [])]
        #print_dict_list_table(data.get("data"))
        return run_ids
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def get_job_run_ids_status(base_url:str,config:AutomicConfig, job_name, object_type, start_date, start_at_run_id=None,max_result=30):
    #base_url = "https://your-automic-server:8088/ae/api/v1/1000/executions"
 
    # Status 1800 = ENDED_NOT_OK, 1822 = FAULT_OTHER
    failed_statuses = "1800,1801,1802,1810,1815,1820,1821,1822,1823,1824,1825,1826,1827,1828,1829,1830,1856"
    sucess_status ="1900"
    params = {
        "name": job_name,
        "include_deactivated": "true",
        "type": object_type,
        "max_results": max_result,
        #"status": sucess_status
        "time_frame_from": start_date # Format: YYYY-MM-DDTHH:MM:SSZ
    }
    if start_at_run_id:
        params["start_at_run_id"] = start_at_run_id
    url = f"{base_url}/executions" 
    response = requests.get(
        url, 
        params=params, 
        auth=config.auth,
        verify=False
    )
    #print(response.json())
    if response.status_code == 200:
        data = response.json()
        # Extracting just the run_ids from the list of executions
        #run_id_status = [{"run_id":task['run_id'],"status":task['status'] ,"status_text":task['status_text']
        #                  ,"start_time":task['start_time'] ,"end_time":task.get('end_time')} for task in data.get('data', [])]
        #print_dict_list_table(data.get("data"))
        return data.get('data', [])
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []

def failure_job_logs (base_url,job_failure_detail_file):
    # Open the file
    with open(job_failure_detail_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # Iterate through each row
        for row in reader:            
            # Access data using the column names
            #print(f"JOB_NAME: {row['JOB_NAME']}, ExecutionIds: {row['Failed ExecutionIds']}")            
            ##logic to get failed job logs 
            job_name =  row['JOB_NAME']
            print(f"processing job -{job_name}")            
            ls_run_ids = eval( row['Failed ExecutionIds'] )
            all_logs = ""
            for run_id in ls_run_ids:
                #run_id = "84241321"
                resp = get_job_logs(base_url,run_id,"REP")
                #print(run_id)
                file_name = f"{job_name}_{run_id}.log"
                if resp:
                    write_to_file(file_name,resp)
                    all_logs = all_logs +f"\n***************\n {run_id}\n *************** \n " +resp
                else:
                    print(f"empty data :{file_name}")
                    
            file_name_all_log = f"{job_name}_all_failed_logs.log"
            if resp:
                write_to_file(file_name_all_log,all_logs)
 
def read_log_from_shared_drive(log_path):
    try:
        #if log_path.start
        if os.path.exists(log_path):
            # with open(log_path, 'r', encoding='utf-8', errors='ignore') as log_file:
            #     # Read the last 100 lines (common for failure analysis)
            #     lines = log_file.readlines()
            #     return lines
                # last_lines = lines[-100:] 
                
                # for line in last_lines:
                #     if "ERROR" in line.upper() or "FAILED" in line.upper():
                #         print(f"Found Issue: {line.strip()}")
            
            with open(log_path, "r", encoding='utf-8', errors="ignore") as f:
                content = f.read()
                return content

        else:
            print(f"Log file not found at: {log_path}")
            raise Exception(f"file not found -{log_path}")
             
    except Exception as ex:
        print(f"error happened - {ex}")
        raise


def get_log_file_override_path(base_url,config, run_id ):
    resp = get_job_execution_variables(base_url,config, run_id )
 
    log_path = resp["LOG_FILE_OVERRIDE#"]
    log_path = log_path.replace("D$",'')
    #print(log_path)

    return log_path

def get_job_remote_log(base_url, config, run_id):
    resp = get_job_execution_variables(base_url,config, run_id )
    #print(json.dumps(resp))
    log_path = resp["LOG_FILE_OVERRIDE#"]
    log_path = log_path.replace("D$",'')
    print(log_path)
    log_data = read_log_from_shared_drive(log_path)
    # for log in log_data:
    #     if log != '\n':
    #         print(log)
    return log_data
    
   # /{client_id}/objects/{object_name}/usage

def get_object_details(base_url,config:AutomicConfig , object_name,filter={}):
 
    url = f"{base_url}/objects/{object_name}/"
  
    
     
    #print(f"URL = {url}" )   
    response = requests.get(
        url, 
        params=filter,         
        auth=config.auth,
        verify=False
    )
    full_log = ""
    #print(response.json())
    if response.status_code == 200:
        res_data = response.json()
        
        return res_data
     
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []    

def get_workflow_timing_from_schedule(base_url,config,schedule_name, target_workflow):
    #base_url = "https://your-automic-server:8088/ae/api/v1"
     
    url = f"{base_url}/objects/{schedule_name}"
    
    # Authenticate with User/Dept and Password
    response = requests.get(url, auth=config.auth,verify=False)
    
    if response.status_code != 200:
        print(f"Error fetching schedule: {response.status_code}")
        return None

    data = response.json()
    
    # 'data' contains the object definition. We look into the tasks.
    # Note: Key names might vary slightly by version (e.g., 'tasks' vs 'schedule_tasks')
    print(data.get('data').get('jsch').keys())
    print(len(data.get('data').get('jsch').get('calendar_conditions')))
    print(len(data.get('data').get('jsch').get('general_attributes')))
    print(len(data.get('data').get('jsch').get('schedule_attributes')))
    print(len(data.get('data').get('jsch').get('workflow_definitions')))
    tasks = data.get('data', {}).get('jsch',{}).get('workflow_definitions', [])
    result = []
    for task in tasks:
        print(task)
        if task.get('object_name') == target_workflow and task.get('active') == 1:
            result.append( {
                "object_name": task.get('object_name'),
                "type":task.get('object_type'),
                "earliest_start_time": task.get('earliest_start_time'),
                "start_offset": task.get('start_offset'),
                "active": task.get('active')
            })
    
    return result
    #return f"Workflow {target_workflow} not found in {schedule_name}."
