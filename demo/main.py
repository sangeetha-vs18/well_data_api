import pandas as pd
import sqlite3
from flask import Flask, request, jsonify
import os

file_path = os.path.join('product.xls')
df = pd.read_excel(file_path)

df.columns = df.columns.str.strip()
print("Column names:", df.columns.tolist())

annual_data = df.groupby('API WELL  NUMBER').agg({ 
    'OIL': 'sum',
    'GAS': 'sum',
    'BRINE': 'sum'
}).reset_index()

annual_data.columns = ['api_well_number', 'oil', 'gas', 'brine']
conn = sqlite3.connect('production_data.db')
annual_data.to_sql('production', conn, if_exists='replace', index=False)
conn.commit()


app = Flask(__name__)

@app.route('/data', methods=['GET'])
def get_data():
    well_number = request.args.get('well')
    conn = sqlite3.connect('production_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT oil, gas, brine FROM production WHERE api_well_number = ?", (well_number,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return jsonify({
            "oil": row[0],
            "gas": row[1],
            "brine": row[2]
        })
    else:
        return jsonify({"error": "API Well Number not found"}), 404

if __name__ == '__main__':
    app.run(port=8080)
