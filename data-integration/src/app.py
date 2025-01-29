from flask import Flask, jsonify, request
from db import Database, TableNotFoundException
import logging
import pandas as pd
from datetime import datetime, timedelta

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
                            uuid() as location_id,
                            location as location
                        FROM (
                                SELECT distinct
                                --activity__location_id as  location_id,
                                 activity__location as location
                                FROM raw_sales
                                WHERE activity__location is not null
                            ) l
            """
            con.execute(qry3)

            con.execute("DROP TABLE IF EXISTS associate")
            qry4 = """CREATE TABLE IF NOT EXISTS associate  AS
                        SELECT 
                            associate_id  as associate_id,
                            associate as associate
                        FROM (SELECT distinct
                            sale__assigned_associate_id as associate_id,
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


@app.route('/data_load_from_file')
def table_loads():
    """Function to load fact dimension tables"""
    try:
        # Open and read the file as a single buffer
        fd = open('/Users/surojitchowdhury/Documents/dev/surojit_repo/coding-exercise/data-integration/models/data_loads.sql', 'r')
        sqlFile = fd.read()
        fd.close()
    except Exception as e:
        logger.error(f"Error accessing the SQL File: {e}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'Generic error. Please contact Support'
            }), 500

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        try:
            with Database() as con:
                con.execute(command)

        except Exception as e:
            logger.error(f"Error while running data loads: {e}")
            return jsonify({
                'error': 'Internal Server Error',
                'message': 'Generic error. Please contact Support'
                }), 500
    
    return jsonify({
            'message': 'Data loading complete'
            }), 200

@app.route('/get_total_sales_last_year')
def get_total_sales_last_year():
    """Function to get_total_sales_last_year"""
    try:
        with Database() as con:
            qry = """
            SELECT 
                CAST(SUM(sale__price_net) AS DECIMAL(20,2)) as total_sales
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
                CAST(SUM(sale__price_net) AS DECIMAL(20,2)) as total_sales,
                CAST(SUM(sale__price_tax) AS DECIMAL(20,2)) as total_tax
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

def generate_fiscal_calendar(start_month, calendar_type="4-4-5", years=3):
    """
        Generate calendar for last specific number of years for given calendar type where fiscal period start from given month

        :param start_month: Fiscal year start month (e.g. 7 for July)
        :param calendar_type: Type of calendar (e.g. "4-4-5","4-5-4" or "5-4-4")
        :param years: Number of past years to generate (defaul 3)
        :return : DataFrame with columns as per data model
    """

    if calendar_type not in ["4-4-5","4-5-4" ,"5-4-4"]:
        raise ValueError("Invalid calendar type")

    today = datetime.today()
    start_year = today.year - years

    fiscal_data = []
    quarter_patterns = {
        "4-4-5": [4,4,5],
        "4-5-4": [4,5,4],
        "5-4-4": [5,4,4]
    }

    for year in range(start_year, today.year + 1):
        start_date = datetime(year,start_month, 1)
        fiscal_year = year
        fiscal_quarter = 1
        weeks_per_quarter = quarter_patterns[calendar_type]

        current_date = start_date

        for q in range(1, 5): #4 quarters
            for period, weeks in enumerate(weeks_per_quarter, start = 1):
                for week in range(weeks):
                    for day in range(7):
                        fiscal_data.append(
                            {
                                "standard_date": current_date.strftime("%Y-%m-%d"),
                                "fiscal_year": fiscal_year,
                                "fiscal_quarter": fiscal_quarter,
                                "fiscal_period": period,
                                "week_number": week+1 
                            }
                        )
                        current_date += timedelta(days=1)
            fiscal_quarter += 1

    return pd.DataFrame(fiscal_data)

def load_fiscal_table(start_month, calendar_type="4-4-5", years=3):
    """
        Generate DuckDB table for last specific number of years for given calendar type where fiscal period start from given month

        :param start_month: Fiscal year start month (e.g. 7 for July)
        :param calendar_type: Type of calendar (e.g. "4-4-5","4-5-4" or "5-4-4")
        :param years: Number of past years to generate (defaul 3)
        :return : DuckDB table
    """
    fiscal_df = generate_fiscal_calendar(start_month, calendar_type, years)

    try:
        with Database() as con:
            con.execute("DROP TABLE IF EXISTS fiscal_calendar")
            query = """CREATE TABLE IF NOT EXISTS fiscal_calendar AS
                        SELECT * FROM fiscal_df
                    """
            con.execute(query)
    except Exception as e:
        logger.error(f"Undefined error while loading fiscal_calendar table: {e}")

def load_fact_dim_tables():
    """
        Load fact dimensional tables from raw table
    """

    try:
        with Database() as con:
            con.execute("DROP TABLE IF EXISTS tenants")
            con.execute("""CREATE TABLE IF NOT EXISTS tenants AS
                            SELECT 
                                uuid() as tenant_id,
                                tenant
                            FROM 
                                (SELECT DISTINCT
                                    tenant
                                FROM raw_sales
                                ) temp
                        """)
            con.execute("""COPY (
                            SELECT 
                                t.tenant_id as tenant_id,
                                date_part('year', sale__date_local) as sale_year,
                                sale__date_local,
                                sale__store_id,
                                sale__price_net,
                                sale__price_tax,
                            FROM 
                                (SELECT DISTINCT
                                    sale__date_local,
                                    sale__store_id,
                                    sale__price_net,
                                    sale__price_tax,
                                    tenant
                                FROM raw_sales
                                ) t1
                                LEFT JOIN tenants t on (t1.tenant=t.tenant)
                            ) TO 'sales_tab' (FORMAT PARQUET, PARTITION_BY (tenant_id, sale_year), COMPRESSION SNAPPY, OVERWRITE)
                        """)
    except Exception as e:
        logger.error(f"Undefined error while loading fact dimensional tables: {e}")


@app.route('/v1/get_total_sales')
def get_total_sales():
    """Function to get total sales last year"""
    tenant = request.args.get("tenant")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    print(f"tenant: {tenant}, start_date: {start_date}, end_date: {end_date}")
    logger.info(f"tenant: {tenant}, start_date: {start_date}, end_date: {end_date}")
    if not tenant:
        return jsonify({"error": "tenant field is mandatory"}), 400

    try:
        with Database() as con:
            query = f"""SELECT
                            tenant,
                            sale_year,
                            SUM(sale__price_net) as total_sales
                        FROM
                            read_parquet('sales_tab/*/*/*.parquet', hive_partitioning = true) as sales
                        JOIN
                            tenants t ON (t.tenant_id=sales.tenant_id)
                    
                        WHERE tenant = '{tenant}'
            """
            
            if start_date:
                query += f" AND CAST(sale__date_local AS DATE) >= CAST('{start_date}' AS DATE)"

            if end_date:
                query += f" AND CAST(sale__date_local AS DATE) <= CAST('{end_date}' AS DATE)"

            query += "GROUP BY tenant,sale_year"
            result = con.execute(query).fetch_df()
            return jsonify(result.to_dict(orient='records'))
    
    except Exception as e:
        logger.error(f"Undefined error accessing the database: {e}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'Generic error. Please contact Support'
            }), 500

if __name__ == '__main__':
    app.run(debug=True)