from datetime import datetime
import uuid
import os
import csv
import re

def validate_row(row, run_id=None):
    if run_id is None:
        run_id = str(uuid.uuid4())
    try:
        # transacton id validation
        transaction_id = row['Transaction ID']
        if not transaction_id or not re.match(r"^TXN_\d{7}$", transaction_id):
            return False, f"Invalid transaction_id"
        
        #quantity validation
        quantity = int(row['Quantity'])
        if quantity <= 0:
            return False, f"Invalid Quanity"
        
        #validate price per unit
        price_per_unit = float(row["Price Per Unit"])
        if price_per_unit <= 0:
            return False, f"Invalid price_per_unit"
        
        total_spent = float(row["Total Spent"])
        if round(total_spent,2) != round(quantity*price_per_unit):
            return False, f"Total mismatch"
        
        #validate date
        transaction_date = datetime.strptime(
            row['Transaction Date'], "%Y-%m-%d"
        ).date()

        cleaned_row = {
            "transaction_id": transaction_id,
            "item": row['Item'].strip() or "UNKNOWN",
            "quantity": quantity,
            "price_per_unit":price_per_unit,
            "total_spent":total_spent,
            "payment_method":row["Payment Method"].strip() or "UNKNOWN",
            "location":row["Location"].strip() or "UNKNOWN",
            "transaction_date":transaction_date
        }

        return True, cleaned_row
             
    except Exception as e:
        return False, f"Validation error: {str(e)}"
    
def log_rejected_row(original_row, error_message, run_id):
    """Args:
        original_row: The raw dictionary from CSV
        error_message: Why it was rejected
        run_id: Unique identifier for this pipeline run
    """
    log_dirs = 'logs'
    if not os.path.exists(log_dirs):
        os.makedirs(log_dirs)
        print(f"directory created: {log_dirs}")
        
    today = datetime.now().strftime("%Y%m%d")
    file_path = f"logs/rejected_rows_{today}.csv"

    # cheking if file exist
    file_exists = os.path.isfile(file_path)

    enriched_row = original_row.copy()
    enriched_row['error_message'] = error_message
    enriched_row['run_id'] = run_id
    enriched_row['time_stamp'] = datetime.now().isoformat()

    fieldnames = list(original_row.keys()) + ['error_message','run_id','time_stamp']

    try:
        with open(file_path,'a',newline='',encoding='utf-8') as f:
            writer= csv.DictWriter(f,fieldnames)

            if not file_exists:
                writer.writeheader()
                print(f"DEBUG: Created new file with headers: {file_path}")
            
            writer.writerow(enriched_row)
            #print(f"DEBUG: row rejected to  {file_path}")

    except Exception as e:
        print(f"CRITICAL: Could not write rejected row to {file_path}: {str(e)}")
    
                 
        





