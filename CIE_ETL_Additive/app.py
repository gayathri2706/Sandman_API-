import json
import os
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Load config
cd = os.getcwd()
config_dir = os.path.join(cd, "config")
config_file_path = os.path.join(config_dir, "config.json")
with open(config_file_path, "r") as config_file:
    config = json.load(config_file)

# Database config
db_config = config["database"]
DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]
DB_NAME = db_config["database_name"]

# SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

@app.route('/api/cie-mixer-report', methods=['GET'])
def get_cie_mixer_report():
    try:
        limit = request.args.get('limit', default=100, type=int)
        offset = request.args.get('offset', default=0, type=int)
        start_date = request.args.get('start_date', default=None, type=str)
        end_date = request.args.get('end_date', default=None, type=str)

        # WHERE clause
        where_clause = ""
        if start_date and end_date:
            where_clause = f"WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            where_clause = f"WHERE timestamp >= '{start_date}'"
        elif end_date:
            where_clause = f"WHERE timestamp <= '{end_date}'"

        table_name = "additive_report_dummy_rename_mcie"

        # Total count
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
        total_records = pd.read_sql(count_query, con=engine).iloc[0]['total']

        # Min and max timestamps
        time_range_query = f"""
            SELECT 
                MIN(timestamp) as start_time, 
                MAX(timestamp) as end_time 
            FROM {table_name}
            {where_clause}
        """
        time_range = pd.read_sql(time_range_query, con=engine)
        start_time = time_range.iloc[0]['start_time']
        end_time = time_range.iloc[0]['end_time']

        # Data
        data_query = f"""
            SELECT * FROM {table_name}
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit} OFFSET {offset}
        """
        df = pd.read_sql(data_query, con=engine)

        # Format datetime/timedelta
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_timedelta64_dtype(df[col]):
                df[col] = df[col].astype(str)

        data = df.to_dict(orient='records')
        response = {
            'status': 'success',
            'metadata': {
                'total_records': int(total_records),
                'start_time': str(start_time),
                'end_time': str(end_time),
                'limit': limit,
                'offset': offset
            },
            'data': data
        }
        return jsonify(response), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    print(f"Starting CIE API server at {datetime.now()}")
    app.run(debug=True, port=5050, host='0.0.0.0')  # You can change port if needed
