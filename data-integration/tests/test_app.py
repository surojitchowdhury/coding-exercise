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
        assert set(["sales_activity","country","location"]).issubset(set(tables))


        result = con.execute("SELECT * FROM location").fetch_df()
        data = result.to_dict(orient='records')
        locations = [item['location'] for item in data]
        assert set(["Cervantesmouth", "Rochaside"]).issubset(set(locations))


    
