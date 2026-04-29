import ast
from collections import defaultdict
import os
from pathlib import PurePosixPath
import pandas as pd
from pathlib import Path
# from main import print_dict_list_table

from collections import defaultdict
import ntpath
import posixpath
import fnmatch

from utils import print_dict_list_table


 
from collections import defaultdict
import ntpath
import posixpath

EXTERNAL = "__EXTERNAL__"  # initial state owner

def split_path_exact_old(path):
    """
    Split path WITHOUT collapsing parents.
    Works for Windows UNC and Unix paths.
    """
    if not path:
        return None, None

    if path.startswith("\\\\"):
        return ntpath.dirname(path), ntpath.basename(path)
    return posixpath.dirname(path), posixpath.basename(path)


def derive_initial_state_old(jobs):
    """
    Automatically infer initial filesystem state based on
    first usage of COPY / MOVE / DELETE commands.
    """
    produced_folders = set()
    initial_state = defaultdict(set)

    for job in jobs:
        for cmd in job["commands"]:
            action = cmd["command"].upper()
            src_folder, src_pattern = split_path_exact(cmd.get("source"))
            tgt_folder = cmd.get("target")

            # File producers
            if action in ("MGET", "MPUT") and tgt_folder:
                produced_folders.add(tgt_folder)

            # File consumers
            elif action in ("COPY", "MOVE", "DELETE", "ERASE"):
                if src_folder and src_folder not in produced_folders:
                    initial_state[src_folder].add(src_pattern)

    return dict(initial_state)


# def process_jobs(jobs,initial_state = None):
#     # Final filesystem: folder -> set of file patterns
#     fs = defaultdict(set)

#     # Track folders touched by each job
#     job_folders = defaultdict(set)

    
#     # Inject inferred initial state
#     if initial_state :
#         for folder, patterns in initial_state.items():
#             fs[folder].update(patterns)
#     else:
#         out = derive_initial_state(jobs)
#         for folder, patterns in out.items():
#             fs[folder].update(patterns)


#     # ---------------------------------
#     # PASS 1: execute all jobs
#     # ---------------------------------
#     for job in jobs:
#         job_name = job["job_name"]

#         for cmd in job["commands"]:
#             action = cmd["command"].upper()

#             src_folder, src_pattern = split_path_exact(cmd.get("source"))
#             tgt_folder = cmd.get("target")

#             # Track folders this job touches
#             if src_folder:
#                 job_folders[job_name].add(src_folder)
#                 fs[src_folder] = fs[src_folder]

#             if tgt_folder:
#                 job_folders[job_name].add(tgt_folder)
#                 fs[tgt_folder] = fs[tgt_folder]

#             # -------- simulate filesystem ----------
#             if action == "MGET":
#                 fs[tgt_folder].add(src_pattern)

#             elif action == "COPY":
#                 if src_folder in fs and src_pattern in fs[src_folder]:
#                     fs[tgt_folder].add(src_pattern)

#             elif action == "MOVE":
#                 if src_folder in fs and src_pattern in fs[src_folder]:
#                     fs[src_folder].remove(src_pattern)
#                     fs[tgt_folder].add(src_pattern)

#             elif action == "MPUT":
#                 if src_folder in fs and src_pattern in fs[src_folder]:
#                     fs[tgt_folder].add(src_pattern)

#             elif action in ("DELETE", "ERASE"):
#                 fs[src_folder].discard(src_pattern)

#     # ---------------------------------
#     # PASS 2: final-status per job
#     # ---------------------------------
#     results = []

#     for job in jobs:
#         job_name = job["job_name"]

#         folders_status = {
#             folder: {
#                 "has_files": bool(fs[folder]),
#                 "files": sorted(fs[folder])
#             }
#             for folder in job_folders[job_name]
#         }

#         results.append({
#             "job_name": job_name,
#             "final_folders_status": folders_status
#         })

#     return results


# -------------------------------------------------------
# Utility: split path safely
# -------------------------------------------------------
# def split_path_exact(path):
#     if not path:
#         return None, None
#     if path.startswith("\\\\"):
#         return ntpath.dirname(path), ntpath.basename(path)
#     return posixpath.dirname(path), posixpath.basename(path)

VAR_CANONICAL_MAP = {
    "%year4%": "%year%",
    "${year4}": "%year%",
    "<<YEAR4>>": "%year%",
    "%yyyy%": "%year%",
}

def canonicalize_pattern(p):
    if not p:
        return p
    p = p.lower()
    for k, v in VAR_CANONICAL_MAP.items():
        p = p.replace(k, v)
    return p

def normalize_path(p):
    if not p:
        return p
    return ntpath.normpath(p).lower()


def split_path_exact(path):
    if not path:
        return None, None

    # UNC / Windows path
    if path.startswith("\\\\"):
        folder = ntpath.dirname(path)
        pattern = ntpath.basename(path)
    else:
        folder = posixpath.dirname(path)
        pattern = posixpath.basename(path)

    # ✅ normalize ONLY the folder
    folder = normalize_path(folder)

    # ✅ pattern must stay just the filename/wildcard
    return folder, pattern

 
# -------------------------------------------------------
# Infer initial state
# -------------------------------------------------------
def derive_initial_state(jobs):
    produced_folders = set()
    initial_state = defaultdict(set)

    for job in jobs:
        for cmd in job["commands"]:
            action = cmd["command"].upper()
            src_folder, src_pattern = split_path_exact(cmd.get("source"))
            tgt_folder = cmd.get("target")
            
            src_folder = normalize_path(src_folder)
            tgt_folder = normalize_path(tgt_folder)


            if action in ("MGET", "MPUT") and tgt_folder:
                produced_folders.add(tgt_folder)

            elif action in ("COPY", "MOVE", "DELETE", "ERASE"):
                if src_folder and src_folder not in produced_folders:
                    initial_state[src_folder].add(src_pattern)

    return dict(initial_state)



def pattern_matches(src_pattern, existing_pattern):
    src = canonicalize_pattern(src_pattern)
    existing = canonicalize_pattern(existing_pattern)

    return (
        fnmatch.fnmatch(existing, src) or
        fnmatch.fnmatch(src, existing)
    )

# -------------------------------------------------------
# Main simulation
# -------------------------------------------------------
def process_jobs(jobs, initial_state=None):
    """
    folder -> owner -> set(patterns)
    """
    fs = defaultdict(lambda: defaultdict(set))
    job_folders = defaultdict(set)

    # -------------------------
    # Initial state
    # -------------------------
    if initial_state is None:
        initial_state = derive_initial_state(jobs)

    for folder, patterns in initial_state.items():
        fs[folder][EXTERNAL].update(patterns)

    # -------------------------
    # PASS 1: execute jobs
    # -------------------------
    for job in jobs:
        job_name = job["job_name"]
        # print(job_name)
        for cmd in job["commands"]:
            action = cmd["command"].upper()
            # print(action)
            # src_folder, src_pattern = split_path_exact(cmd.get("source"))
            # tgt_folder = cmd.get("target")
            
            src_folder, src_pattern = split_path_exact(cmd.get("source"))
            tgt_folder = normalize_path(cmd.get("target"))


            if src_folder:
                job_folders[job_name].add(src_folder)
            if tgt_folder:
                job_folders[job_name].add(tgt_folder)

            # ---------- MGET ----------
            if action == "MGET":
                fs[tgt_folder][job_name].add(src_pattern)

            # ---------- COPY ----------
            elif action in ("COPY", "MPUT", "PKZIPC"):
                # for owner, patterns in fs[src_folder].items():
                #     # if src_pattern in patterns:                    
                #     for existing_pattern in patterns:
                #         if pattern_matches(src_pattern, existing_pattern):
                #             fs[tgt_folder][job_name].add(src_pattern)
                is_pattern_found = False
                for owner, patterns in list(fs[src_folder].items()):
                        
                        if any(pattern_matches(src_pattern, p) for p in patterns):
                            fs[tgt_folder][job_name].add(src_pattern)
                            is_pattern_found = True
                            break
                #Assuming file was created by intermediate ETL job. so simulating file creation and file movement
                if not is_pattern_found and action == "MPUT":
                    fs[tgt_folder][job_name].add(src_pattern)
                    fs[src_folder][job_name].add(src_pattern)



            # ---------- MOVE ----------
            elif action == "MOVE":                
                file_present = False
                # for owner in list(fs[src_folder].keys()):
                #     if src_pattern in fs[src_folder][owner]:
                #         fs[src_folder][owner].remove(src_pattern)
                #         file_present = True
                #         if not fs[src_folder][owner]:
                #             del fs[src_folder][owner]
                
                for owner in list(fs[src_folder].keys()):
                    to_remove = set()

                    for existing_pattern in fs[src_folder][owner]:
                        if pattern_matches(src_pattern, existing_pattern):
                            to_remove.add(existing_pattern)
                            file_present = True

                    for pat in to_remove: 
                        fs[src_folder][owner].discard(pat)

                if not fs[src_folder][owner]:
                    del fs[src_folder][owner]

                if file_present:
                    fs[tgt_folder][job_name].add(src_pattern)


            # ---------- DELETE ----------
            elif action in ("DELETE", "ERASE"):
                for owner in list(fs[src_folder].keys()):
                    if src_pattern in fs[src_folder][owner]:
                        fs[src_folder][owner].remove(src_pattern)
                        if not fs[src_folder][owner]:
                            del fs[src_folder][owner]

            # ---------- RENAME (logical move) ----------
           # ---------- RENAME (logical move inside same folder) ----------
            elif action == "RENAME":
                new_name = canonicalize_pattern(
                    ntpath.basename(cmd.get("target"))
                )

                for owner in list(fs[src_folder].keys()):
                    removed = set()
                    for existing_pattern in fs[src_folder]:
                        if pattern_matches(src_pattern, existing_pattern):
                            removed.add(existing_pattern)

                    if removed:
                        # Remove matched source files
                        fs[src_folder][owner] -= removed

                        # Clean up owner only if empty
                        if not fs[src_folder]:
                            del fs[src_folder][owner]

                        # Add renamed file under this job
                        fs[src_folder][job_name].add(new_name)


    # -------------------------
    # PASS 2: per-job final view
    # -------------------------
    results = []

    for job in jobs:
        job_name = job["job_name"]
        folders_status = {}
        #print(f"job_name - {job_name}")
        #print(job_folders)
        for folder in job_folders[job_name]:

            #print(f"folder {folder} - has files -{fs[folder].get(job_name, set())} ")
            job_files = fs[folder].get(job_name, set())
            other_files = {
                f
                for owner, patterns in fs[folder].items()
                if owner != job_name
                for f in patterns
            }

            folders_status[folder] = {
                "has_job_files": bool(job_files),
                "job_files": sorted(job_files),
                "other_job_files": sorted(other_files),
            }

        results.append({
            "job_name": job_name,
            "final_folders_status": folders_status
        })

    return results

def ensure_trailing_slash(path):
    # If it's already there, it won't add a second one
    # If it's missing, it adds the OS-specific slash
    return os.path.join(path, '')

def add_consistent_slash(path):
    # Determine which slash is being used
    if "\\" in path:
        slash = "\\"
    else:
        slash = "/"
    
    # Check if the path already ends with that slash
    if not path.endswith(slash):
        return path + slash
    
    return path

def get_list_of_file_transfer_jobs(jobp_name, wf_details_file):
    df = pd.read_csv(wf_details_file )
    
    filtered_df  = df[(df['workflow_name'] == jobp_name )& ( (df['is_ftp'] == True) |  (df['is_copy_move'] == True)) ]
    filtered_df = df[['workflow_name','job_name','job_type','is_ftp','is_copy_move']]

    return filtered_df
     

def get_filetransfer_cmd(jobp_name,wf_details_file):

    df = pd.read_csv(wf_details_file )
    
    filtered_df  = df[(df['workflow_name'] == jobp_name )& ( (df['is_ftp'] == True) |  (df['is_copy_move'] == True)) ]
    # print(filtered_df.head())
    # axis=1 applies the function to each row
    # df['total'] = df.apply(lambda row: row['col1'] + row['col2'], axis=1)
    ls_job_data = []
    for row in filtered_df.itertuples(index=True, name='Pandas'):
        # print(row.Index, row.workflow_name, row.is_ftp, row.is_copy_move)   
        job_name =row.job_name
        # "job_name"
        if row.is_ftp:
        
            prompt = row.PromptSet
            prompt = ast.literal_eval(prompt)
            prompt["command"] = prompt.pop('CMD#')

            all_cmd = []
            
            prompt.pop('LOG_FILE_OVERRIDE#')
            if prompt["command"] == 'mput':
                local_dir = prompt.pop('LOCAL_DIR#')
                local_dir = add_consistent_slash(local_dir)
                prompt["source"] = f"{local_dir}{prompt.get('SOURCE#')}"
                prompt["target"] = prompt.pop('REMOTE_DIR#')
            elif prompt["command"] == 'mget':
                prompt["target"] = f"{prompt.pop('LOCAL_DIR#')}"
                remote_dir = prompt.pop('REMOTE_DIR#')
                # remote_dir = str(Path(remote_dir) / "")
                remote_dir = add_consistent_slash(remote_dir)
                prompt["source"] = f"{remote_dir}{prompt.get('SOURCE#')}"
            prompt.pop('CONNECTION#')
            
            
            prompt.pop('DESTINATION#')
            #prompt.pop('LOCAL_DIR#')
            prompt.pop('SOURCE#')
            # prompt.pop('REMOTE_DIR#')
            prompt.pop('prompt_set')
            all_cmd.append(prompt)
            if prompt.get('DELETE#') == "Y":
               del_command =  {'command': 'DELETE',   'source': prompt["source"]}
               all_cmd.append(del_command)
               

            if 'DELETE#' in prompt.keys():
                prompt.pop('DELETE#')

            data = {"job_name":job_name, "commands": all_cmd }
            #print(row.PromptSet)
            ls_job_data.append(data)
        elif row.is_copy_move:
            # print(row.Commands)
            command = ast.literal_eval(row.Commands)
            ls_out = []
            for cmd in command:
                temp_cmd = {}
                cmd['command'] = cmd.pop('Command')
                cmd['source'] = cmd.pop('SourcePath')
                cmd['target'] = cmd.pop('TargetPath')
                cmd.pop('SourceFile')
                cmd.pop('SourceServer')
                cmd.pop('TargetServer')


            data = {"job_name":job_name, "commands":command }
            ls_job_data.append(data)

    return ls_job_data

def normalize_folder_for_slash(path):
    if not path:
        return path
    path = ntpath.normpath(path)
    return path + ntpath.sep if not path.endswith(ntpath.sep) else path
    
def get_data_for_job_rules_old(final_status_of_jobs,ls_job_data):
    rows = []
    for job in final_status_of_jobs:
        job_name = job["job_name"],
        commands = []
        for job_commands in ls_job_data:
            job_name1 = job_commands.get("job_name")
            if job_name[0] == job_name1:
                commands = job_commands.get("commands")
                break

            
        for folder, info in job.get("final_folders_status", {}).items():
          
            if info.get("has_job_files"):
                ftp_folder = False 
                for cmd in commands:
                    cmd_name = cmd.get("command")                 

                    if cmd_name == 'mget':
                        if folder.lower() == normalize_path(cmd.get('source')):
                            ftp_folder = True
                            break
                    if cmd_name =='mput': 
                          
                        if folder.lower() == normalize_path(cmd.get('target')):
                            ftp_folder = True
                            break
                if not ftp_folder:
                     
                   
                    command_name = None
                    for cmd in commands:
                        cmd_name = cmd.get("command") 
                        if folder.lower() == normalize_path(cmd.get('source')) or folder.lower() == normalize_path(cmd.get('target')):
                            command_name = cmd_name
                            break
                    if folder.startswith(r"\\hpappworxts01"):
                        folder = folder.replace("d$",'Applications')
                    elif  folder.startswith(r"\\hpappworxts"):
                        folder = folder.replace("d$",'Applications')
                    else:
                        folder = folder.replace("d$",'')
                    folder =  normalize_folder_for_slash(folder)
                    file_ext = set()
                    files = info.get("job_files", [])
                    for file_name in files:
                       ext =  Path(file_name).suffix.lower() 
                       file_ext.add(ext)
                    rows.append( {"job_name": job_name[0], "folder":folder, "files_ext":list(file_ext) , "files": files,"command": command_name})
                else:
                    rows.append( {"job_name": job_name[0], "folder":None , "files":None })
    return rows



def get_data_for_job_rules(final_status_of_jobs, ls_job_data):
    rows = []

    for job in final_status_of_jobs:
        job_name = job["job_name"]

        # Find commands for this job
        commands = []
        for job_commands in ls_job_data:
            if job_commands.get("job_name") == job_name:
                commands = job_commands.get("commands", [])
                break

        job_record = {
            "job_name": job_name,
            "folders": []
        }

        for folder, info in job.get("final_folders_status", {}).items():

            if not info.get("has_job_files"):
                continue

            ftp_folder = False
            command_name = None

            # Identify command and ftp folders
            for cmd in commands:
                cmd_name = cmd.get("command")

                if cmd_name == "mget" and folder.lower() == normalize_path(cmd.get("source")):
                    ftp_folder = True
                    break

                if cmd_name == "mput" and folder.lower() == normalize_path(cmd.get("target")):
                    ftp_folder = True
                    break

                if (
                    folder.lower() == normalize_path(cmd.get("source")) or
                    folder.lower() == normalize_path(cmd.get("target"))
                ):
                    command_name = cmd_name

            if ftp_folder:
                continue

            # Normalize folder path
            if folder.startswith(r"\\hpappworxts01") or folder.startswith(r"\\hpappworxts"):
                folder = folder.replace("d$", "Applications")
            else:
                folder = folder.replace("d$", "")

            folder = normalize_folder_for_slash(folder)

            # Collect file extensions
            files = info.get("job_files", [])
            file_ext = {Path(file).suffix.lower() for file in files}

            job_record["folders"].append(
                {
                    "folder": folder,
                    "files_ext": list(file_ext),
                    "files": files,
                    "command": command_name
                }
            )

        # if job_record["folders"]:
        #     rows.append(job_record)
        rows.append(job_record)

    return rows
if __name__ == '__main__':
 
    # jobp_name = "JOBP.MANUAL_MONTHLY_MEDICARE_PREMIUM_BILLING_INVOICE_PROCESS"
    # jobp_name = "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    # df =pd.read_csv(r"C:\Users\afe3356\Code\jhp_prod_crl_modernization\all_workflow_details.csv" )
     
    jobp_name = "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    jobp_name = "JOBP.MONTHLY_CHIP_834_MEMBERSHIP_NEW_REDESIGN"
    jobp_name = "JOBP.MONTHLY_CHIP_PREMIUM_BILLING_PROCESS"
    # jobp_name = "JOBP.DAILY_ACA_SALESFORCE_ENROLLMENT"
    # jobp_name = "JOBP.DAILY_IPLUS_SECONDARY_ZELIS_UPLOAD_PROCESS"
    # wf_details_file=r"C:\Users\afe3356\Code\jhp_prod_crl_modernization\all_workflow_details.csv"
    wf_details_file=r"all_workflow_details.csv"
    ls_job_data = get_filetransfer_cmd(jobp_name,wf_details_file)
    print(ls_job_data)
    out = process_jobs(ls_job_data)
    #print_dict_list_table(out)
    print("*******************************")
    # print(out)
    # for details in out:
    #     print(details.get("job_name"))
         
    #     folder_status = details.get("final_folders_status")
    rows = get_data_for_job_rules(out,ls_job_data)
    print_dict_list_table(rows)