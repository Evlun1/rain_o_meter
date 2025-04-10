# from api import app

# client = TestClient(app)


# def test_read_root():
#     response = client.get("/")
#     assert response.status_code == 200
#     assert response.json() == {"Hello": "World"}


# def test_read_item():
#     input_id = "99"
#     response = client.get(f"/items/{input_id}")
#     assert response.status_code == 200
#     assert response.json() == {"item_id": int(input_id), "q": None}
