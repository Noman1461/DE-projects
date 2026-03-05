try:
    import psycopg2
except ImportError as e:
    raise ImportError(
        "psycopg2 is required to load data into Postgres. "
        "Install it with `pip install psycopg2-binary` or add it to your requirements."
    ) from e

from config import DB_CONFIG

def load_raw_data(rows) :
    conn = psycopg2.connect(**DB_CONFIG) #the bridge vs connects to database

    cursor = conn.cursor()

    # NOTE: the original implementation used ``ON CONFLICT (transaction_id)``
    # but this produces an error if the database table has no unique/exclusion
    # constraint defined on that column.  You can either add a UNIQUE index in
    # the database or leave the clause out.  For simplicity we omit it here and
    # rely on the caller to ensure the data is deduplicated if necessary.
    insert_query = """
        INSERT INTO cafe_sales_raw (
            transaction_id,
            item, 
            quantity, 
            price_per_unit, 
            total_spent, 
            payment_method, 
            location, 
            transaction_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    for row in rows:
        # rows should be the "cleaned" dictionaries returned by validate_row,
        # which use snake_case keys and proper types
        try:
            cursor.execute(insert_query, (
                row['transaction_id'],
                row['item'],
                row['quantity'],
                row['price_per_unit'],
                row['total_spent'],
                row['payment_method'],
                row['location'],
                row['transaction_date'],
            ))
        except KeyError as ke:
            # If the row doesn't have the expected keys, raise a more helpful error
            raise KeyError(
                f"load_raw_data received row with unexpected format: missing {ke}"
            ) from ke

    conn.commit()
    cursor.close()
    conn.close()

def load_clean_data(rows):

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO cafe_sales_clean (
            transaction_id,
            item,
            quantity,
            price_per_unit,
            total_spent,
            payment_method,
            location,
            transaction_date,
            spending_category,
            is_high_value_order,
            processed_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in rows:
        try:
            cursor.execute(insert_query, (
                row['transaction_id'],
                row['item'],
                row['quantity'],
                row['price_per_unit'],
                row['total_spent'],
                row['payment_method'],
                row['location'],
                row['transaction_date'],
                row['spending_category'],
                row['is_high_value_order'],
                row['processed_timestamp'],
            ))
        except KeyError as ke:
            raise KeyError(
                f"load_clean_data received row with unexpected format: missing {ke}"
            ) from ke

    conn.commit()
    cursor.close()
    conn.close()
