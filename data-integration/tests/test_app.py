from app import app

def test_root_route():
    """Testing root route"""
    response = app.test_client().get('/')

    assert response.status_code == 200