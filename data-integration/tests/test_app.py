from app import app, generate_fiscal_calendar, load_fiscal_table, load_fact_dim_tables
from db import Database, TableNotFoundException
from unittest.mock import patch
from pandas import DataFrame
from pytest import raises
from config import DATA_PATH

def test_root_route():
    """Testing root route"""
    response = app.test_client().get('/')
    
    assert response.status_code == 200
    assert len(response.json) == 10
    assert isinstance(response.json, list)

@patch("app.Database.getTable", side_effect= TableNotFoundException)
def test_root_exception(mock_get_table):
    """Testing root route"""
    response = app.test_client().get('/')
    assert response.status_code == 500
    assert 'Raw data source for sales is not properly configured' in response.json['message'] 
    
@patch.object(Database, "__enter__",side_effect= Exception)
def test_root_exception2(mock_get_table):
    """Testing root route"""
    response = app.test_client().get('/')
    assert response.status_code == 500
    assert 'Generic error' in response.json['message'] 

def test_table_creations():
    """Testing table creations"""

    response = app.test_client().get('/data_load_from_file')
    with Database() as con:
        result = con.execute("SHOW TABLES").fetch_df()
    
        data = result.to_dict(orient='records')
        tables = [item['name'] for item in data]
        assert set(["sales_activity","country","location", "associate", "currency", "product", "order_tab", "activity", "sales"]).issubset(set(tables))


        result = con.execute("SELECT * FROM location").fetch_df()
        data = result.to_dict(orient='records')
        locations = [item['location'] for item in data]
        assert set(["Cervantesmouth", "Rochaside"]).issubset(set(locations))

        result = con.execute("SELECT COUNT(*) as cnt FROM location").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt = [item['cnt'] for item in data]
        #assert row_cnt[0]==0

        result = con.execute("SELECT COUNT(*) as cnt FROM currency").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt = [item['cnt'] for item in data]
        #assert row_cnt[0]==0

        result = con.execute("SELECT COUNT(*) as cnt FROM order_tab").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt = [item['cnt'] for item in data]
        assert row_cnt[0]>0

        result = con.execute("SELECT COUNT(*) as cnt FROM activity").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt = [item['cnt'] for item in data]
        assert row_cnt[0]>0

        result = con.execute("SELECT COUNT(*) as cnt FROM sales").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt = [item['cnt'] for item in data]
        assert row_cnt[0]>0

        result = con.execute("SELECT COUNT(*) as cnt FROM sales").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt_sales = [item['cnt'] for item in data]
        result = con.execute("SELECT COUNT(*) as cnt FROM raw_sales").fetch_df()
        data = result.to_dict(orient='records')
        row_cnt_raw_sales = [item['cnt'] for item in data]
        assert row_cnt_sales[0]==row_cnt_raw_sales[0]

    
def test_get_total_sales_last_year():
    """Testing total sales last year"""

    response = app.test_client().get('/get_total_sales_last_year')
    assert response.status_code == 200
    assert response.json > 0


def test_get_total_net_sales_next10_years():
    """Testing total net sales for next 10 year"""

    response = app.test_client().get('/get_total_net_sales_next10_years')
    assert response.status_code == 200
    assert response.json > 0

def test_generate_fiscal_calendar():
    """Testing fiscal calendar generate function"""
    response = generate_fiscal_calendar(start_month = 4,calendar_type = "4-4-5", years=2)
    assert isinstance(response, DataFrame)
    assert response.query('standard_date=="2024-04-01"')['fiscal_period'].iloc[0] == 1

def test_generate_fiscal_calendar_exception():
    """Testing fiscal calendar generate function"""
    with raises(ValueError):
        response = generate_fiscal_calendar(start_month = 4,calendar_type = "5-4-5", years=2)


def test_load_fiscal_table():
    """Testing fiscal calendar duckdb load"""
    load_fiscal_table(start_month = 4,calendar_type = "4-4-5", years=2)
    
    with Database() as con:
        result = con.execute("SELECT COUNT(*) as cnt FROM fiscal_calendar").fetch_df()
        data = result.to_dict(orient = 'records')
        assert data[0]['cnt'] > 0

        result = con.execute("SELECT fiscal_period FROM fiscal_calendar WHERE standard_date='2024-04-01'").fetch_df()
        data = result.to_dict(orient = 'records')
        assert data[0]['fiscal_period'] == 1


def test_load_fact_dim_tables():
    """Testing fact dim duckdb load"""
    load_fact_dim_tables()
    
    with Database() as con:
        result = con.execute("SELECT COUNT(*) as cnt FROM tenants").fetch_df()
        data = result.to_dict(orient = 'records')
        assert data[0]['cnt'] > 0

        result = con.execute(f"SELECT COUNT(*) as cnt FROM read_parquet('{DATA_PATH}/sales_tab/*/*/*.parquet', hive_partitioning = true)").fetch_df()
        data = result.to_dict(orient = 'records')
        assert data[0]['cnt'] > 0


def test_get_total_sales():
    """Testing get_total_sales"""
    response = app.test_client().get('/v1/get_total_sales?tenant=acme_industries&start_date=2024-01-01&end_date=2024-12-31')
    
    assert response.status_code == 200
    assert len(response.json) > 0
    assert "tenant" in response.json[0]

def test_get_total_sales_error():
    """Testing get_total_sales"""
    response = app.test_client().get('/v1/get_total_sales?start_date=2024-01-01&end_date=2024-12-31')
    
    assert response.status_code == 400
    assert response.json["error"] == "tenant field is mandatory"

@patch.object(Database, "__enter__", side_effect = Exception("connection error"))
def test_get_total_sales_error1(mock_db):
    """Testing get_total_sales"""
    response = app.test_client().get('/v1/get_total_sales?tenant=acme_industries&start_date=2024-01-01&end_date=2024-12-31')
    
    assert response.status_code == 500
    assert "Generic error" in response.json["message"] 


def test_get_fiscal_sales_yoy():
    """Testing get_fiscal_sales_yoy"""
    response = app.test_client().get('/v1/get_fiscal_sales_yoy?tenant=acme_industries&fiscal_year=2024&fiscal_quarter=3&fiscal_period=1')
    
    assert response.status_code == 200
    assert len(response.json) > 0
    assert "tenant" in response.json[0]

def test_get_fiscal_sales_yoy_error():
    """Testing get_fiscal_sales_yoy error"""
    response = app.test_client().get('/v1/get_fiscal_sales_yoy?fiscal_year=2024&fiscal_quarter=3&fiscal_period=1')
    
    assert response.status_code == 400
    assert response.json["error"] == "tenant field is mandatory"

@patch.object(Database, "__enter__", side_effect = Exception("connection error"))
def test_get_fiscal_sales_yoy_error1(mock_db):
    """Testing get_fiscal_sales_yoy"""
    response = app.test_client().get('/v1/get_fiscal_sales_yoy?tenant=acme_industries&fiscal_quarter=3&fiscal_period=1')
    
    assert response.status_code == 500
    assert "Generic error" in response.json["message"] 