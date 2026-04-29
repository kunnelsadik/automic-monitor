from ftplib import FTP
from datetime import datetime
import os



def pyx12_parse(file_path):
	# 1. Setup error handler (required)
	errh = pyx12.error_handler.NullErrorHandler()

	# 2. Open and read the file
	# Note: You usually need to point this to your HIPAA IG definitions
	with open(file_path, "rb") as fd_in:
		# X12ContextReader processes the raw EDI
		reader = pyx12.x12context.X12ContextReader(None, errh, fd_in)
		
		# 3. Navigate the tree
		# Iterating over '2300' finds all the Claim Information loops
		for datatree in reader.iter_segments('2300'):
			print(f"Found Claim Loop: {datatree.get_value('CLM01')}")
			
			# 4. Access child nodes (e.g., Service Line 2400)
			for loop2400 in datatree.select('2400'):
				# Accessing a specific element (e.g., Procedure Code)
				# You need to know the segment/element ID (e.g., SV101)
				print(f"  Service Line: {loop2400.get_value('SV101')}")


def inspect_x12_header(file_path):
    with open(file_path, 'r') as f:
        # Read the first 200 characters to catch ISA and GS
        header = f.read(500)
        
        if not header.startswith("ISA"):
            return "Error: File does not start with a valid ISA segment."

        # ISA is a fixed-width segment (106 chars)
        # Element separator is the 4th character
        element_sep = header[3]
        
        # Segment terminator is the 106th character
        segment_term = header[105]
        
        segments = header.split(segment_term)
        
        info = {
            "Element Separator": f"'{element_sep}'",
            "Segment Terminator": f"'{segment_term}'",
            "Segments Found": []
        }

        for seg in segments[:5]: # Check first 5 segments
            if not seg.strip(): continue
            elements = seg.split(element_sep)
            seg_id = elements[0]
            
            # ST*837 identifies the transaction
            if seg_id == "ST":
                info["Transaction Type"] = elements[1]
            
            info["Segments Found"].append(seg_id)

        return info

def get_file_metada_from_ftp(ftp_server, username, password, file_name):
    ftp = FTP(ftp_server)
    ftp.login(user=username, passwd=password)
 

    # File size
    file_size = ftp.size(file_name)

    # Last modified time (YYYYMMDDHHMMSS)
    modified_raw = ftp.sendcmd(f"MDTM {file_name}")[4:]
    last_modified = datetime.strptime(modified_raw, "%Y%m%d%H%M%S")

    print(f"File size      : {file_size} bytes")
    print(f"Last modified  : {last_modified}")

    ftp.quit()
    return {"file_name":file_name , "size":file_size , "last_modified_date":last_modified}

def get_file_metadata_from_shared_drive(file_path):
    try:
        stats = os.stat(file_path)
        file_size = stats.st_size
        raw_last_modified = stats.st_mtime
        
        last_modified = datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")
        created_time = datetime.fromtimestamp(stats.st_ctime)
        last_accessed = datetime.fromtimestamp(stats.st_atime)

        
        print(f"File size      : {file_size} bytes")
        print(f"Last modified  : {last_modified}")
    except Exception as ex:
        print(f"error occured while get file metadata from drive - {file_path} ")
        return {"error": str(ex)}

    return {"file_name":file_path , "size":file_size , "last_modified_date":last_modified}


def count_claims_x12_strict(file_path):
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # ISA segment is fixed-width (106 chars)
        isa_segment = content[:106]
        segment_terminator = isa_segment[-1]

        segments = content.split(segment_terminator)
        count =  sum(1 for seg in segments if seg.startswith("CLM*"))
        return count
    except Exception as ex:

        print(f"error happened in reading file from -{file_path} ")
        print(ex)
        return 0
    
def count_text_file_rows(file_path):
    row_count = 0
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            row_count = sum(1 for _ in f)
    
    except FileNotFoundError as e:
        print("Error:", e)

    return row_count

def count_text_file_rows_with_header(file_path):
    row_count = count_text_file_rows(file_path)
    if row_count > 0:
        row_count = row_count - 1

    return row_count



def count_claims_837(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        segments = f.read().split("~")

    return sum(1 for seg in segments if seg.startswith("CLM*"))


from abc import ABC, abstractmethod

class FileHandler(ABC):
    registry = {}
    extension: str = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.extension:
            FileHandler.registry[cls.extension.lower()] = cls()

    @abstractmethod
    def count_rows(self, file_path: str) -> int:
        pass

class X12Handler(FileHandler):
    extension = ".x12"

    def count_rows(self, file_path: str) -> int:
        return count_claims_x12_strict(file_path)


class TxtHandler(FileHandler):
    extension = ".txt"

    def count_rows(self, file_path: str) -> int:
        return count_text_file_rows_with_header(file_path)


class EDI837Handler(FileHandler):
    extension = ".837"

    def count_rows(self, file_path: str) -> int:
        return count_claims_837(file_path)
    

class UnsupportedFileTypeError(Exception):
    pass

from pathlib import Path

class FileDispatcher:

    @staticmethod
    def resolve_extension(file_type, file_name):
        if file_type:
            return file_type.lower()
        return Path(file_name).suffix.lower() if file_name else None

    def dispatch(self, file_type, file_name, full_file_path):
        ext = self.resolve_extension(file_type, file_name)

        handler = FileHandler.registry.get(ext)
        if not handler:
            raise UnsupportedFileTypeError(ext)

        return handler.count_rows(full_file_path)
