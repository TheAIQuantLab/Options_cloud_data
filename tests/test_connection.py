# test_connection.py
import requests


def test_meff_connection():
    url = "https://www.meff.es/esp/Derivados-Financieros/Ficha/FIEM_MiniIbex_35"
    response = requests.get(url)
    assert response.status_code == 200, f"Failed with status {response.status_code}"
