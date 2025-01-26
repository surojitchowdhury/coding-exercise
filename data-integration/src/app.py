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

            con.execute("DROP TABLE IF EXISTS associate")
            qry4 = """CREATE TABLE IF NOT EXISTS associate  AS
                        SELECT distinct
                            sale__assigned_associate_id  as associate_id,
                            sale__assigned_associate as associate
                        FROM raw_sales
            """
            con.execute(qry4)

            con.execute("DROP TABLE IF EXISTS product")
            qry5 = """CREATE TABLE IF NOT EXISTS product  AS
                        SELECT distinct
                            gen_random_uuid()  as product_id,
                            sale__product_name as product_name,
                            sale__product_sku as product_sku
                        FROM raw_sales
            """
            con.execute(qry5)

            con.execute("DROP TABLE IF EXISTS currency")
            qry5 = """CREATE TABLE IF NOT EXISTS currency  AS
                        SELECT distinct
                            gen_random_uuid()  as currency_id,
                            sale__currency_consumer as currency
                        FROM raw_sales
            """
            con.execute(qry5)

            con.execute("DROP TABLE IF EXISTS order_tab")
            qry6 = """CREATE TABLE IF NOT EXISTS order_tab AS
                        SELECT distinct
                            gen_random_uuid()  as order_id,
                            p.product_id as product_id,
                            r.sale__order_status as order_status,
                            r.sale__item_status as item_status,
                            r.sale__item_type as item_type,
                            r.sale__channel_type as channel_type
                        FROM raw_sales r
                        JOIN product p on (r.sale__product_name = p.product_name)
            """
            con.execute(qry6)

            con.execute("DROP TABLE IF EXISTS activity")
            qry6 = """CREATE TABLE IF NOT EXISTS activity AS
                        SELECT distinct
                            gen_random_uuid()  as activity_id,
                            activity__associate_id as associate_id,
                            activity__country_code as country_code,
                            activity__date as activity__date,
                            activity__date_local as activity__date_local,
                            activity__location_id as activity__location_id
                        FROM raw_sales r
            """
            con.execute(qry6)

            con.execute("DROP TABLE IF EXISTS sales")
            qry7 = """CREATE TABLE IF NOT EXISTS sales AS
                        SELECT distinct
                            gen_random_uuid() as sales_id,
                            sale__assigned_associate_id as associate_id,
                            c.currency_id as sale_currency_id,
                            sale__date_local as sale__date_local,
                            sale__external_id as sale__external_id,
                            sale__is_historical as sale__is_historical,
                            l.location_id as sale__location_id,
                            o.order_id as sale__order_id,
                            sale__price_net as sale__price_net,
                            sale__price_tax as sale__price_tax,
                            sale__tax_method as sale__tax_method,
                            sale__store_id as sale__store_id
                        FROM raw_sales r
                        JOIN currency c on (r.sale__currency_consumer=c.currency)
                        JOIN location l on (r.sale__location=l.location)
                        JOIN product p on (p.product_sku = r.sale__product_sku)
                        JOIN order_tab o on (p.product_id=o.product_id)
            """
            con.execute(qry7)

            con.execute("DROP TABLE IF EXISTS sales_activity")
            qry1 = """CREATE TABLE IF NOT EXISTS sales_activity  AS
                        SELECT 
                            uuid,
                            tenant,
                            exec_time,
                            s.sales_id as sales_id,
                            a.activity_id as activity_id
                        FROM raw_sales r
                        JOIN activity a on (r.activity__location_id = a.activity__location_id and r.activity__date=a.activity__date)
                        JOIN sales s on (r.sale__external_id = s.sale__external_id and r.sale__store_id=s.sale__store_id)
            """
            con.execute(qry1)


            return jsonify({
            'message': 'Data loading complete'
            }), 200
        
    
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