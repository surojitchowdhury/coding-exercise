from flask import Flask, jsonify
from db import Database, TableNotFoundException
import logging

app = Flask(__name__)

#Basic Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Example Route. This can be changed at your convenience
@app.route('/')
def index():
    try:
        with Database() as con:
            sales_tbl=Database.getTable('raw_sales')
            result = con.execute(f"SELECT * FROM {sales_tbl} LIMIT 10").fetch_df()
            data = result.to_dict(orient='records')
            print(data)
            return jsonify(data)
    except TableNotFoundException as e:
        logger.error(e)
        return jsonify({
            'error': 'Internal Server Error',
            'message': f'Raw data source for sales is not properly configured on the server. {e}'
            }), 500
    except Exception as e:
        logger.error(f"Undefined error accessing the database: {e}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'Generic error. Please contact Support'
            }), 500

if __name__ == '__main__':
    app.run(debug=True)