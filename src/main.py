from extract import extract_data
from load import load_raw_data, load_clean_data
from validate import validate_row, log_rejected_row
from transform import transform_data
import uuid
from quality import save_quality_checks
import pandas as pd
from datetime import datetime

def main():
    run_id = str(uuid.uuid4())
    print(f"Pipeline run id: {run_id}")

    file_path  = "data/dirty_cafe_sales.csv"
    rows = extract_data(file_path)
    print(f"Extracted {len(rows)} rows")

    valid_rows = []
    invalid_count = 0

    for i, row in enumerate(rows):
        is_valid, result = validate_row(row, run_id)

        if is_valid:
            #insert into database
            valid_rows.append(result)
        else:
            #log the invalid rows into the csv file
            invalid_count += 1
            log_rejected_row(row, result, run_id)
            #print(f"Row {i} rejected: {result}...")
    
    print(f"\nValidation complete!")
    print(f"Valid rows: {len(valid_rows)}")
    print(f"Invalid rows: {invalid_count}")

    # use a valid strftime format (was incorrect previously)
    today = datetime.now().strftime("%Y%m%d")

    save_quality_checks(
        valid=len(valid_rows),
        invalid=invalid_count,
        run_id=run_id,
        rejected_file_path=f"logs/rejected_rows_{today}.csv"
    )
        
    if valid_rows:
        load_raw_data(valid_rows)
        print(f"Loaded {len(valid_rows)} rows into database")
    else:
        print("No valid rows to load")
    
    valid_df = pd.DataFrame(valid_rows)

    clean_df = transform_data(valid_df)

    clean_df.to_csv("data/clean_cafe_sales.csv", index=False)
    
    clean_rows = clean_df.to_dict(orient='records')

    load_clean_data(clean_rows) #clean layer


if __name__ == "__main__":
    main()