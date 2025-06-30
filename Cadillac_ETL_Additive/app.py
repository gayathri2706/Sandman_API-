from flask import Flask, request, jsonify
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

# === Path to your Excel file ===
EXCEL_FILE = r"C:\Users\MPM Infosoft\Downloads\Cadillac_ETL_Additive\data\processed_data.xlsx"

@app.route('/api/cadilac-mixer-report', methods=['GET'])
def get_excel_data():
    try:
        if not os.path.exists(EXCEL_FILE):
            return jsonify({"status": "error", "message": "Excel file not found."}), 404

        df = pd.read_excel(EXCEL_FILE)

        if df.empty:
            return jsonify({"status": "error", "message": "No data found in the Excel file."}), 404

        # Ensure consistent column names (optional but safer)
        df.columns = df.columns.str.strip()

        # Optional date filtering
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors='coerce')

            if start_date:
                df = df[df["Date"] >= pd.to_datetime(start_date, errors='coerce')]
            if end_date:
                df = df[df["Date"] <= pd.to_datetime(end_date, errors='coerce')]

        # Pagination
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))

        total = len(df)
        paginated = df.iloc[offset:offset + limit].copy()

        # Convert datetime columns to string
        for col in ["Date", "Time"]:
            if col in paginated.columns:
                paginated[col] = paginated[col].astype(str)

        return jsonify({
            "status": "success",
            "metadata": {
                "total_records": total,
                "limit": limit,
                "offset": offset
            },
            "data": paginated.fillna("").to_dict(orient="records")
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    print(f"Running API server on Excel at {datetime.now()}")
    app.run(debug=True, port=5050)
