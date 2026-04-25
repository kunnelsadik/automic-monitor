import pyodbc

db = r"C:\Users\nlr9894\Downloads\prod_crl_modern.accdb"
conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db};"

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print("✅ Connection OK")
conn.close()