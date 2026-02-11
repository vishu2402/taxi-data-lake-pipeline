import duckdb
import sys
import os

access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
secret_key = os.getenv("MINIO_SECRET_KEY", "password")
endpoint = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")

con = duckdb.connect()

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{endpoint.replace('http://', '')}';")
con.execute(f"SET s3_access_key_id='{access_key}';")
con.execute(f"SET s3_secret_access_key='{secret_key}';")
con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

path = "s3://silver-clean/orders_parquet_table/**/*.parquet"

reports = {
    "summary": f"""
        SELECT 
            processing_date, 
            count(*) as trips, 
            round(sum(cast(total_amount as double)), 2) as revenue,
            round(avg(cast(discount_percent as double)), 2) as avg_discount
        FROM read_parquet('{path}', union_by_name=True) 
        GROUP BY 1 ORDER BY 1""",
    
    "hours": f"""
        SELECT 
            hour(
                CASE 
                    WHEN tpep_pickup_datetime LIKE '%-%-% %:%' AND length(tpep_pickup_datetime) > 16 
                    THEN CAST(tpep_pickup_datetime AS TIMESTAMP)
                    ELSE strptime(tpep_pickup_datetime, '%d-%m-%Y %H:%M') 
                END
            ) as hour, 
            count(*) as trips 
        FROM read_parquet('{path}') 
        GROUP BY 1 ORDER BY 1""",
    
    "vendors": f"""
        SELECT 
            vendorid, 
            count(*) as trips, 
            round(avg(cast(tip_amount as double)), 2) as avg_tip 
        FROM read_parquet('{path}') 
        GROUP BY 1""",
    
    "payments": f"""
        SELECT 
            payment_type, 
            count(*) as trips 
        FROM read_parquet('{path}') 
        GROUP BY 1"""
}

arg = sys.argv[1] if len(sys.argv) > 1 else "summary"

if arg in reports:
    print(f"\n--- Running {arg.upper()} Report ---")
    try:
        df = con.execute(reports[arg]).df()
        print(df.to_string(index=False))
        print("-" * 50)
    except Exception as e:
        print(f"Query Failed: {e}")
else:
    print(f"Invalid report! Try: {', '.join(reports.keys())}")