from collections import defaultdict
import os
import re


RET_RE = re.compile(r'\bRET=(\d+)\b')
 

EXTERNAL_LOG_RE = re.compile(
    r'\bLog\s+"(?P<path>[^"]+\.log)"',
    re.IGNORECASE
)

ERROR_PATTERNS = [
    # Most specific first
    ("DUPLICATE", re.compile(
        r'duplicate file name exists', re.IGNORECASE
    )),
    ("NOT_FOUND", re.compile(
        r'cannot be found|file not found', re.IGNORECASE
    )),
    ("ACCESS_DENIED", re.compile(
        r'access is denied|permission denied', re.IGNORECASE
    )),
    ("PATH_NOT_FOUND", re.compile(
        r'path not found|system cannot find the path', re.IGNORECASE
    )),
]


ERROR_SEVERITY_MAP = {
    "DUPLICATE": "WARNING",
    "NOT_FOUND": "WARNING",
    "PATH_NOT_FOUND": "ERROR",
    "ACCESS_DENIED": "CRITICAL",
    "UNKNOWN": "ERROR"
}

ERROR_RETRYABLE_MAP = {
    "DUPLICATE": False,        # Retry would fail again unless target is cleaned
    "NOT_FOUND": True,         # Upstream timing issue; file may appear later
    "PATH_NOT_FOUND": False,   # Misconfiguration or missing mount
    "ACCESS_DENIED": False,    # Permissions issue; retry is useless
    "UNKNOWN": False           # Safer to not retry blindly
}

# CMD_RE = re.compile(
#     r'^(copy|move)\s+"(?P<src>.+?)\\\*\.(?P<ext>[^"]+)"\s+"(?P<dst>[^"]+)"',
#     re.IGNORECASE | re.MULTILINE
# )

# COUNT_RE = re.compile(r'(?P<count>\d+)\s+file\(s\)\s+(copied|moved)', re.IGNORECASE)

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

 

FTP_DELETE_RE = re.compile(
    r'Deleted\s+(?P<count>\d+)\s+files?',
    re.IGNORECASE
)

FTP_SUCCESS_RE = re.compile(r'transfer succeeded', re.IGNORECASE)
FTP_FAILURE_RE = re.compile(r'Failure in command', re.IGNORECASE)
 
# COPY_MOVE_BLOCK_RE = re.compile(
#     r'(?P<block>'
#     r'^(?:[A-Za-z]:\\?>)?\s*(copy|move)\s+".*?"\s+".*?"'
#     r'(?:\r?\n.+?)*?'
#     r'\d+\s+file\(s\)\s+(copied|moved)\.'
#     r')',
#     re.IGNORECASE | re.MULTILINE
# )


# COPY_MOVE_BLOCK_RE = re.compile(
#     r'(?P<block>'
#     r'^(?:[A-Za-z]:\\?>)?\s*(copy|move)\s+".*?"\s+".*?"'
#     r'(?:\r?\n(?![A-Za-z]:\\?>).+?)*'
#     r')',
#     re.IGNORECASE | re.MULTILINE
# )


COPY_MOVE_BLOCK_RE = re.compile(
    r'(?P<block>'
    r'^(?:[A-Za-z]:\\?>)?\s*'
    r'(copy|move)\s+"[^"]+"\s+"[^"]+"'
    r'(?:\r?\n(?![A-Za-z]:\\?>).*)*'
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

FTP_DOWNLOAD_RE = re.compile(
    r'Opening remote file\s+"(?P<remote_file>[^"]+)"\s+for reading\s*'
    r'Size\s+(?P<size>\d+)\s*'
    r'Downloading to local file\s+"(?P<local_file>[^"]+)"\s*'
    r'# transferred\s+(?P<bytes>\d+)\s+bytes\s+in\s+'
    r'(?P<time>[\d.]+)\s+seconds.*?transfer\s+succeeded\.',
    re.IGNORECASE | re.DOTALL
)
 
MGET_RE = re.compile(
    r'Processing Line \d+\s+\[mget\s+(?P<pattern>[^\]]+)\]',
    re.IGNORECASE
)


FTP_DELETE_RE = re.compile(
    r'Removing remote file\s+"(?P<remote_file>[^"]+)"',
    re.IGNORECASE
)

DIR_HEADER_RE = re.compile(
    r'Directory of\s+(?P<path>/[^\r\n]+)',
    re.IGNORECASE
)

DIR_FILE_RE = re.compile(
    r'(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+'
    r'(?P<time>\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s+'
    r'(?P<size>[\d,]+)\s+'
    r'(?P<filename>\S+)',
    re.IGNORECASE
)



FTP_PUT_RE = re.compile(
    r'Opening remote file\s+"(?P<remote_file>[^"]+)"\s+for writing\s*'
    r'Uploading local file\s+"(?P<local_file>[^"]+)"\s*'
    r'# transferred\s+(?P<bytes>\d+)\s+bytes\s+in\s+'
    r'(?P<time>[\d.]+)\s+seconds.*?transfer\s+succeeded\.',
    re.IGNORECASE | re.DOTALL
)


MPUT_COMMAND_RE = re.compile(
    r'Processing Line \d+\s+\[mput\s+(?P<pattern>[^\]]+)\]',
    re.IGNORECASE
)


def read_log_from_shared_drive(log_path):
    if os.path.exists(log_path):
        with open(log_path, "r", encoding='utf-8', errors="ignore") as f:
            content = f.read()
            return content

    else:
        print(f"Log file not found at: {log_path}")
        
        return None
    
# def parse_copy_move_blocks(log_text):
#     results = []
#     print("inside parse_copy_move_blocks")
 
#     for bm in COPY_MOVE_BLOCK_RE.finditer(log_text):
       
#         block = bm.group("block")  
#         cmd = CMD_RE.search(block)
#         if not cmd:
#             continue

#         src = cmd.group("src")
#         dst = cmd.group("dst")
#         ext = cmd.group("ext")
#         op = cmd.group(1).lower()

#         file_re = re.compile(
#             rf'^(?!.*\*){re.escape(src)}\\(?P<filename>[^\\]+\.{re.escape(ext)})$',
#             re.IGNORECASE | re.MULTILINE
#         )


#         files = [m.group("filename") for m in file_re.finditer(block)]

#         count_match = COUNT_RE.search(block)

#         count = int(count_match.group("count")) if count_match else len(files)

#         results.append({
#             "operation": op,
#             "source": src,
#             "destination": dst,
#             "file_pattern": f"*.{ext}",
#             "files": files,
#             "file_count": count,
#             "success": count == len(files)
#         })

#     return results

def wildcard_to_regex(pattern: str) -> str:
    regex = re.escape(pattern)
    regex = regex.replace(r'\*', '.*')
    regex = regex.replace(r'\?', '.')
    return regex


def normalize_error_type(error_message: str | None) -> str | None:
    if not error_message:
        return None

    for error_type, pattern in ERROR_PATTERNS:
        if pattern.search(error_message):
            return error_type

    return "UNKNOWN"


def resolve_retryable(error_type: str | None) -> bool | None:
    if not error_type:
        return None  # success case

    return ERROR_RETRYABLE_MAP.get(error_type, False)


def resolve_error_severity(error_type: str | None) -> str | None:
    if not error_type:
        return None  # success case

    return ERROR_SEVERITY_MAP.get(error_type, "ERROR")


def extract_error_message(block: str) -> str | None:
    lines = [line.strip() for line in block.splitlines()]

    error_lines = []
    for line in lines:
        if not line:
            continue

        # Skip command line
        if re.match(r'^(?:[A-Za-z]:\\?>)?\s*(copy|move)\b', line, re.IGNORECASE):
            continue

        # Skip success lines
        if re.search(r'\d+\s+file\(s\)\s+(copied|moved)\.', line, re.IGNORECASE):
            continue

        # Skip prompt echo
        if re.match(r'^[A-Za-z]:\\?>$', line):
            continue

        error_lines.append(line)

    return " ".join(error_lines) if error_lines else None

# def parse_copy_move_blocks(log_text):
#     results = []

#     for bm in COPY_MOVE_BLOCK_RE.finditer(log_text):
#         block = bm.group("block")

#         cmd = CMD_RE.search(block)
#         if not cmd:
#             continue

#         src = cmd.group("src")
#         dst = cmd.group("dst")
#         op = cmd.group(1).lower()

#         src_dir, file_pattern = os.path.split(src)
#         # file_pattern_re = wildcard_to_regex(file_pattern)

#         # file_re = re.compile(
#         #     rf'^{re.escape(src_dir)}\\(?P<filename>{file_pattern_re})$',
#         #     re.IGNORECASE | re.MULTILINE
#         # )

#         # files = [m.group("filename") for m in file_re.finditer(block)]

#         count_match = COUNT_RE.search(block)
#         success = count_match is not None
#         # count = int(count_match.group("count")) if count_match else len(files)
#         count = int(count_match.group("count")) if success else 0
#         files = []
#         error_message = None

        
#         if success:
#             # Extract filenames printed by wildcard COPY/MOVE
#             file_pattern_re = wildcard_to_regex(file_pattern)
#             file_re = re.compile(
#                 rf'^{re.escape(src_dir)}\\(?P<filename>{file_pattern_re})$',
#                 re.IGNORECASE | re.MULTILINE
#             )

#             files = [m.group("filename") for m in file_re.finditer(block)]

#             # Exact-file fallback
#             if not files and count == 1 and file_pattern:
#                 files = [file_pattern]

#         else:
#             error_message = extract_error_message(block)


#         results.append({
#             "operation": op,
#             "source": src,
#             "destination": dst,
#             "file_pattern": file_pattern,
#             "files": files,
#             "file_count": count,
#             "success": count == len(files),
#             "error_message": error_message
#         })

#     return results


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

        count_match = COUNT_RE.search(block)
        success = count_match is not None
        count = int(count_match.group("count")) if success else 0

        files = []
        error_message = None
        error_type = None
        error_severity = None

        if success:
            file_pattern_re = wildcard_to_regex(file_pattern)
            file_re = re.compile(
                rf'^{re.escape(src_dir)}\\(?P<filename>{file_pattern_re})$',
                re.IGNORECASE | re.MULTILINE
            )

            files = [m.group("filename") for m in file_re.finditer(block)]

            # Exact-file fallback
            if not files and count == 1:
                files = [file_pattern]

        else:
            error_message = extract_error_message(block)
            error_type = normalize_error_type(error_message)
            error_severity = resolve_error_severity(error_type)

        results.append({
            "operation": op,
            "source": src,
            "destination": dst,
            "file_pattern": file_pattern,
            "files": files,
            "file_count": count,
            "success": success,
            "error_type": error_type,
            "error_severity": error_severity,
            "error_message": error_message
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
    
    results = []
    
# ---- Parse MGET Downloads ----
    for m in FTP_DOWNLOAD_RE.finditer(log_text):
        remote_file = m.group("remote_file")
        local_file = m.group("local_file")

        results.append({
            "operation": "mget",
            "remote_path": os.path.dirname(remote_file),
            "local_path": os.path.dirname(local_file),
            "filename": os.path.basename(remote_file),
            "remote_size": int(m.group("size")),
            "bytes_transferred": int(m.group("bytes")),
            "transfer_time_sec": float(m.group("time")),
            "success": True
        })

    # ---- Parse MDELETE ----
    for m in FTP_DELETE_RE.finditer(log_text):
        remote_file = m.group("remote_file")

        results.append({
            "operation": "mdelete",
            "remote_path": os.path.dirname(remote_file),
            "filename": os.path.basename(remote_file),
            "success": True
        })

    # ---- Parse DIR Listing ----
    dir_header = DIR_HEADER_RE.search(log_text)
    if dir_header:
        current_path = dir_header.group("path")

        for f in DIR_FILE_RE.finditer(log_text):
            results.append({
                "operation": "dir",
                "remote_path": current_path,
                "filename": f.group("filename"),
                "size_bytes": int(f.group("size").replace(",", "")),
                "modified_date": f.group("date"),
                "modified_time": f.group("time")
            })

    return results

def normalize_ftp_to_copy_move(ftp_result):
    normalized = []

    # ---------- MGET ----------
    if "mget" in ftp_result:
        entry = ftp_result["mget"]
        files = [f["filename"] for f in entry["files"]]

        normalized.append({
            "operation": "mget",
            "source": entry["remote_path"],
            "destination": entry["local_path"],
            "file_pattern": "*",
            "files": files,
            "file_count": len(files),
            "success": True,
            "error_type": None,
            "error_severity": None,
            "error_message": None
        })

    # ---------- PUT ----------
    if "put" in ftp_result:
        files = ftp_result["put"]["files"]

        if files:
            normalized.append({
                "operation": "mput",
                "source": files[0]["local_path"],
                "destination": files[0]["remote_path"],
                "file_pattern": "*",
                "files": [f["filename"] for f in files],
                "file_count": len(files),
                "success": True,
                "error_type": None,
                "error_severity": None,
                "error_message": None
            })

    # ---------- MDELETE ----------
    if "mdelete" in ftp_result:
        entry = ftp_result["mdelete"]
        files = entry["files"]

        normalized.append({
            "operation": "delete",
            "source": entry["remote_path"],
            "destination": None,
            "file_pattern": "*",
            "files": files,
            "file_count": len(files),
            "success": True,
            "error_type": None,
            "error_severity": None,
            "error_message": None
        })

    return normalized

def parse_ftp_transactions_grouped(log_text):
    result = defaultdict(dict)

    # ---------- MGET ----------
    mget_files = []
    remote_path = local_path = None

    for m in FTP_DOWNLOAD_RE.finditer(log_text):
        remote_file = m.group("remote_file")
        local_file = m.group("local_file")

        remote_path = os.path.dirname(remote_file)
        local_path = os.path.dirname(local_file)

        mget_files.append({
            "filename": os.path.basename(remote_file),
            "size_bytes": int(m.group("bytes")),
            "transfer_time_sec": float(m.group("time")),
            "success": True
        })

    if mget_files:
        result["mget"] = {
            "remote_path": remote_path,
            "local_path": local_path,
            "files": mget_files
        }

    # ---------- PUT ----------
    put_files = []

    for m in FTP_PUT_RE.finditer(log_text):
        local_file = m.group("local_file")
        remote_file = m.group("remote_file")

        put_files.append({
            "filename": os.path.basename(remote_file),
            "local_path": os.path.dirname(local_file),
            "remote_path": os.path.dirname(remote_file),
            "size_bytes": int(m.group("bytes")),
            "transfer_time_sec": float(m.group("time")),
            "success": True
        })

    if put_files:
        result["put"] = {
            "files": put_files
        }

    # ---------- MDELETE ----------
    delete_files = []
    delete_path = None

    for m in FTP_DELETE_RE.finditer(log_text):
        remote_file = m.group("remote_file")
        delete_path = os.path.dirname(remote_file)
        delete_files.append(os.path.basename(remote_file))

    if delete_files:
        result["mdelete"] = {
            "remote_path": delete_path,
            "files": delete_files
        }

    # ---------- DIR ----------
    dir_match = DIR_HEADER_RE.search(log_text)
    if dir_match:
        dir_files = []
        dir_path = dir_match.group("path")

        for f in DIR_FILE_RE.finditer(log_text):
            dir_files.append({
                "filename": f.group("filename"),
                "size_bytes": int(f.group("size").replace(",", "")),
                "modified_date": f.group("date"),
                "modified_time": f.group("time")
            })

        result["dir"] = {
            "remote_path": dir_path,
            "files": dir_files
        }

    return dict(result)

 
def get_external_log_path(log_text):
    path = None
    ext_log = EXTERNAL_LOG_RE.search(log_text)
 
    if ext_log:
        path = ext_log.group("path")
        if path.startswith(r"\\hpappworxts01"):
            path = path.replace("D$",'Applications')
        else:
            path = path.replace("D$",'')  
    return path

def load_external_log( log_text=None):
    if log_text:
        path = get_external_log_path(log_text)
 
        if path: 
            log_text = read_log_from_shared_drive(path)
        
            return log_text
        else:
            print("no external log") 
            return "no external log"
    return "no external log"


def parse_job_log(log_text, external_log_loader=None):
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
        if path.startswith(r"\\hpappworxts01"):
            path = path.replace("D$",'Applications')
        else:
            path = path.replace("D$",'')
        result["external_log_path"] = path
        result["transfer_mode"] = "EXTERNAL_LOG"

        if external_log_loader:
            log_text = external_log_loader(path)
      
        else:
            print("no external log loader")
        # return parse_external_log(log_text)
        result =  parse_ftp_transactions_grouped(log_text)
        return normalize_ftp_to_copy_move(result)
    else:
     
        return parse_copy_move_blocks(log_text)
      

