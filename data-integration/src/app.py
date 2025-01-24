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

@app.route('/data_load')
def fact_dim_table_loads():
    """Function to load fact dimension tables"""
    try:
        with Database() as con:
            con.execute("DROP TABLE IF EXISTS sales_activity")
            qry1 = """CREATE TABLE IF NOT EXISTS sales_activity  AS
                        SELECT 
                            uuid,
                            tenant,
                            exec_time,
                            uuid() as sales_id,
                            uuid() as activity_id
                        FROM raw_sales
            """
            con.execute(qry1)

            con.execute("DROP TABLE IF EXISTS country")
            qry2 = """CREATE TABLE IF NOT EXISTS country  AS
                        SELECT distinct
                            gen_random_uuid()  as country_id,
                            activity__country_code as country_code
                        FROM raw_sales
                        WHERE activity__country_code IS NOT NULL
            """
            con.execute(qry2)

            con.execute("DROP TABLE IF EXISTS location")
            qry3 = """CREATE TABLE IF NOT EXISTS location  AS
                        SELECT distinct
                            activity__location_id  as location_id,
                            activity__location as location
                        FROM raw_sales
            """
            con.execute(qry3)
            result = con.execute(f"SELECT * FROM location LIMIT 10").fetch_df()
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