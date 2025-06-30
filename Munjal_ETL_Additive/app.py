from flask import Flask, request, jsonify
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

# === Path to your Excel file ===
EXCEL_FILE = r"C:\Users\MPM Infosoft\Downloads\Munjal_ETL_Additive\munjal_output.xlsx"

# === Desired columns ===
COLUMNS_TO_INCLUDE = [
    "Date", "Time", "Shift", "Mixer Name", "Batch Counter", "Component ID",
    "Recycle sand Actual", "Bentonite Actual", "Coal Dust Actual", "FSS Actual",
    "Water Actual", "Compactability SMC (%)", "COSP Percentage (%)",
    "Temperature (C)", "Total Seconds (seconds)", "Total Water (ltr)",
    "Moisture SMC (%)", "WD1 (ltr)", "CO1 (%)"
]

@app.route('/api/munjal-mixer-report', methods=['GET'])
def get_excel_data():
    try:
        if not os.path.exists(EXCEL_FILE):
            return jsonify({"status": "error", "message": "Excel file not found."}), 404

        df = pd.read_excel(EXCEL_FILE)

        # Check for missing columns
        missing = [col for col in COLUMNS_TO_INCLUDE if col not in df.columns]
        if missing:
            return jsonify({"status": "error", "message": f"Missing columns: {missing}"}), 400

        df = df[COLUMNS_TO_INCLUDE]

        # Optional date filtering — assumes Date is datetime
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        if start_date:
            df = df[df["Date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["Date"] <= pd.to_datetime(end_date)]

        # Pagination
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))

        total = len(df)
        paginated = df.iloc[offset:offset + limit]

        # Convert date/time to string for JSON
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
            "data": paginated.to_dict(orient="records")
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    print(f"Running API server on Excel at {datetime.now()}")
    app.run(debug=True, port=5050)
