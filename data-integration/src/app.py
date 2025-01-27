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
            #con.execute("PRAGMA max_temp_directory_size='40GiB'")

            con.execute("DROP TABLE IF EXISTS country")
            qry2 = """CREATE TABLE IF NOT EXISTS country  AS
                        SELECT distinct
                            gen_random_uuid()  as country_id,
                            country_code as country_code
                        FROM (SELECT distinct
                            activity__country_code as country_code
                        FROM raw_sales
                        WHERE activity__country_code IS NOT NULL
                        ) r
            """
            con.execute(qry2)

            con.execute("DROP TABLE IF EXISTS location")
            qry3 = """
                        CREATE TABLE IF NOT EXISTS location  AS
                        SELECT 
                            uuid()  as location_id,
                            location as location
                        FROM (
                                SELECT distinct
                                 activity__location as location
                                FROM raw_sales
                            ) l
            """
            con.execute(qry3)

            con.execute("DROP TABLE IF EXISTS associate")
            qry4 = """CREATE TABLE IF NOT EXISTS associate  AS
                        SELECT 
                            uuid()  as associate_id,
                            associate as associate
                        FROM (SELECT distinct
                            sale__assigned_associate as associate
                        FROM raw_sales) r
            """
            con.execute(qry4)

            con.execute("DROP TABLE IF EXISTS product")
            qry5 = """CREATE TABLE IF NOT EXISTS product  AS
                        SELECT distinct
                            gen_random_uuid()  as product_id,
                            product_name as product_name,
                            product_sku as product_sku
                        FROM (SELECT distinct
                            sale__product_name as product_name,
                            sale__product_sku as product_sku
                        FROM raw_sales
                        ) r
            """
            con.execute(qry5)

            con.execute("DROP TABLE IF EXISTS currency")
            qry5 = """CREATE TABLE IF NOT EXISTS currency  AS
                        SELECT 
                            gen_random_uuid()  as currency_id,
                            currency as currency
                        FROM (SELECT distinct
                            sale__currency_consumer as currency
                        FROM raw_sales
                        ) dc
            """
            con.execute(qry5)

            con.execute("DROP TABLE IF EXISTS order_tab")
            qry6 = """CREATE TABLE IF NOT EXISTS order_tab AS
                        SELECT 
                            gen_random_uuid()  as order_id,
                            p.product_id as product_id,
                            r.order_status as order_status,
                            r.item_status as item_status,
                            r.item_type as item_type,
                            r.channel_type as channel_type
                        FROM (SELECT distinct 
                            r.sale__order_status as order_status,
                            r.sale__item_status as item_status,
                            r.sale__item_type as item_type,
                            r.sale__channel_type as channel_type,
                            sale__product_name
                            FROM raw_sales r
                            ) r
                        LEFT JOIN product p on (r.sale__product_name = p.product_name)
            """
            con.execute(qry6)

            con.execute("DROP TABLE IF EXISTS activity")
            qry6 = """CREATE TABLE IF NOT EXISTS activity AS
                        SELECT 
                            gen_random_uuid()  as activity_id,
                            associate_id as associate_id,
                            country_code as country_code,
                            activity__date as activity__date,
                            activity__date_local as activity__date_local,
                            activity__location_id as activity__location_id
                        FROM (SELECT distinct
                            activity__associate_id as associate_id,
                            activity__country_code as country_code,
                            activity__date as activity__date,
                            activity__date_local as activity__date_local,
                            activity__location_id as activity__location_id
                        FROM raw_sales r
                        ) r
            """
            con.execute(qry6)

            con.execute("DROP TABLE IF EXISTS sales")
            qry7 = """  CREATE TABLE IF NOT EXISTS sales AS
                        SELECT distinct
                            gen_random_uuid() as sales_id,
                            associate_id as associate_id,
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
                        FROM 
                        ( SELECT distinct
                                sale__assigned_associate_id as associate_id,
                                sale__date_local as sale__date_local,
                                sale__external_id as sale__external_id,
                                sale__is_historical as sale__is_historical,
                                sale__price_net as sale__price_net,
                                sale__price_tax as sale__price_tax,
                                sale__tax_method as sale__tax_method,
                                sale__store_id as sale__store_id ,
                                sale__location,
                                sale__currency_consumer,
                                sale__product_sku,
                                sale__order_status,
                                sale__channel_type,
                                sale__item_status,
                                sale__item_type
                            FROM raw_sales r
                        ) r
                        LEFT JOIN currency c on (r.sale__currency_consumer=c.currency)
                        LEFT JOIN location l on (trim(r.sale__location)=trim(coalesce(l.location,'unknown')))
                        LEFT JOIN 
                        (SELECT DISTINCT
                            order_id,
                            o.product_id,
                            p.product_sku,
                            order_status,
                            channel_type,
                            item_status,
                            item_type
                        FROM order_tab o 
                        INNER JOIN product p on (p.product_id=o.product_id)
                        ) o on (o.product_sku = r.sale__product_sku and o.order_status = r.sale__order_status and o.channel_type=r.sale__channel_type
                                and o.item_status = r.sale__item_status and r.sale__item_type = o.item_type)
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
                        LEFT JOIN activity a on (r.activity__location_id = a.activity__location_id and r.activity__date=a.activity__date)
                        LEFT JOIN sales s on (r.sale__external_id = s.sale__external_id and r.sale__store_id=s.sale__store_id)
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


@app.route('/get_total_sales_last_year')
def get_total_sales_last_year():
    """Function to get_total_sales_last_year"""
    try:
        with Database() as con:
            qry = """
            SELECT 
                CAST(SUM(sale__price_net) AS DECIMAL(10,2)) as total_sales
            FROM
                sales
            WHERE CAST(sale__date_local AS DATE) > current_date - INTERVAL 365 DAY
            AND CAST(sale__date_local AS DATE) <= current_date
            """
            result = con.execute(qry).fetch_df()
            data = result.to_dict(orient='records')
            print(f"Total sales last year: {data[0]['total_sales']}")
            return jsonify(data[0]["total_sales"])


    except Exception as e:
        logger.error(f"Undefined error accessing the database: {e}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'Generic error. Please contact Support'
            }), 500

@app.route('/get_total_net_sales_next10_years')
def get_total_net_sales_next10_years():
    """Function to get_total_net_sales_next10_years"""
    try:
        with Database() as con:
            qry = """
            SELECT (total_sales-total_tax) AS net_sales
            FROM (
            SELECT 
                CAST(SUM(sale__price_net) AS DECIMAL(10,2)) as total_sales,
                CAST(SUM(sale__price_tax) AS DECIMAL(10,2)) as total_tax
            FROM
                sales
            WHERE CAST(sale__date_local AS DATE) <= current_date + INTERVAL 10 YEAR
            AND CAST(sale__date_local AS DATE) > current_date
            ) temp
            """
            result = con.execute(qry).fetch_df()
            data = result.to_dict(orient='records')
            print(f"Total net sales for next 10 years: {data[0]['net_sales']}")
            return jsonify(data[0]["net_sales"])


    except Exception as e:
        logger.error(f"Undefined error accessing the database: {e}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'Generic error. Please contact Support'
            }), 500

if __name__ == '__main__':
    app.run(debug=True)