# test_rejected_logging.py
from validate import log_rejected_row
import uuid

# Generate a run_id
run_id = str(uuid.uuid4())[:8]

# Create a test row (simulating a bad row from your CSV)
test_row = {
    'Transaction ID': 'TXN_999',
    'Item': 'Coffee',
    'Quantity': 'abc',  # This should cause validation error
    'Price Per Unit': '2.50',
    'Total Spent': '5.00',
    'Payment Method': 'Cash',
    'Location': 'In-store',
    'Transaction Date': '2024-01-15'
}

# Log it as rejected
log_rejected_row(test_row, "Quantity must be a number", run_id)

print("Check your logs folder - what do you see?")