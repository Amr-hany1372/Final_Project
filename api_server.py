from flask import Flask, request, jsonify
import pandas as pd
import os

app = Flask(__name__)
@app.route('/data', methods=['GET'])
def get_csv_data():
    filename = request.args.get('filename')
    
    if not filename:
        return jsonify({'error': 'Filename is required as a query parameter'}), 400

    if not filename.endswith('.csv'):
        return jsonify({'error': 'Only .csv files are allowed'}), 400

    if not os.path.exists(filename):
        return jsonify({'error': f'File "{filename}" not found'}), 404

    try:
        df = pd.read_csv(filename)
        data = df.to_dict(orient='records')
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
