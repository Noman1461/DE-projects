from validate import validate_row   # after pytest.ini

def test_valid_row():
    row = {
        "Transaction ID": "TXN_1234567",
        "Item": "Coffee",
        "Quantity": "2",
        "Price Per Unit": "5.0",
        "Total Spent": "10.0",
        "Payment Method": "Cash",
        "Location": "In-store",
        "Transaction Date": "2023-01-01"
    }

    is_valid, result = validate_row(row)

    assert is_valid is True
