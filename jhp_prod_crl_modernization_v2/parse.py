 
import pyx12.x12context
import pyx12.error_handler


def pyx12_parse():
	# 1. Setup error handler (required)
	errh = pyx12.error_handler.NullErrorHandler()

	# 2. Open and read the file
	# Note: You usually need to point this to your HIPAA IG definitions
	with open("your_837_file.x12", "rb") as fd_in:
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

# Usage
header_info = inspect_x12_header(r"C:\Users\afe3356\sample1_202506251230.PHXJHPP.X12")
print(header_info)