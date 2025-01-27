from app import app, fact_dim_table_loads
from db import Database, TableNotFoundException

def test_root_route():
    """Testing root route"""
    response = app.test_client().get('/')
    
    assert response.status_code == 200
    assert len(response.json) == 10
    assert isinstance(response.json, list)

def test_table_creations():
    """Testing table creations"""

    response = app.test_client().get('/data_load')
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

    
