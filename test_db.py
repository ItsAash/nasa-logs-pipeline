import sys
import threading
from databricks import sql
import traceback
import dashboard.config as conf

def query():
    try:
        config = conf.get_databricks_config()
        print("Connecting to Databricks...")
        conn = sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            access_token=config.access_token
        )
        print("Connected! Executing query...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            print("Query 1 Result:", cursor.fetchall())
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM READ_FILES('{conf.ANOMALIES_PATH}', format => 'delta')")
                print("READ_FILES anomalies COUNT:", cursor.fetchall())
            except Exception as e:
                print("READ_FILES Failed:", e)
                
            try:
                cursor.execute(f"SELECT COUNT(*) FROM delta.`{conf.ANOMALIES_PATH}`")
                print("DELTA anomalies COUNT:", cursor.fetchall())
            except Exception as e:
                print("DELTA syntax Failed:", e)
                
    except Exception as e:
        print("Fatal:", traceback.format_exc())

th = threading.Thread(target=query)
th.start()
th.join(timeout=300)
print("Test completed.")
