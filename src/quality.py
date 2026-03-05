import json 
from datetime import datetime
import os

def save_quality_checks(valid, invalid, run_id, rejected_file_path):
    time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"quality_report_{time}.json"

    report_dir = "quality_reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
        print("quality reports/ dir created.")
    
    filepath = os.path.join(report_dir, filename)

    total = valid + invalid
    rejection_rate = (invalid/total)* 100.0 if total> 0 else 0

    report_data = {
        "run_id":run_id,
        "valid_rows":valid,
        "invalid_rows":invalid,
        "total_rows":total, 
        "rejection_percentage": round(rejection_rate,2),
        "rejected_rows_file":rejected_file_path,
        "run_timestamp":time
    } 

    print(f"Quality report prepared: {filename}")
    print(f"Data: {report_data} ")

    with open(filepath,"w") as file:
        json.dump(report_data,file, indent=4)
        print("JSON File created successfully.")

# if __name__ == "__main__":
#     save_quality_checks(1123, 112)

