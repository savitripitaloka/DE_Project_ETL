#!python ETL

import pandas as pd
import sqlalchemy as sa

from config import oltp_conn_string, warehouse_conn_string, oltp_tables, warehouse_tables, dimension_columns, ddl_statements, ddl_marts

def create_tables():
    """Create tables in the data warehouse if they do not exist."""
    engine = sa.create_engine(warehouse_conn_string)
    with engine.connect() as conn:
        for ddl in ddl_statements.values():
            conn.execute(ddl)
            
def extract_data(table_name):
    """Extract data from a table in the OLTP database."""
    engine = sa.create_engine(oltp_conn_string)
    query = f"SELECT * FROM {oltp_tables[table_name]}"
    df = pd.read_sql(query, engine)
    print(f'Extract Data {oltp_tables[table_name]} Success')
    return df

def transform_data(df, target_table):
    """Transform the extracted data to match the schema of the target dimension table."""
    columns = dimension_columns.get(target_table)
    if columns:
        df = df[columns]
    print(f'Transform Data {target_table} Success')
    return df

def transform_fact_orders():
    """Transform data for the fact_orders table."""
    dataframes = {table: extract_data(table) for table in oltp_tables.keys()}

    df_orders = dataframes['orders']
    df_orders = df_orders.merge(dataframes['users'], on='user_id')
    df_orders = df_orders.merge(dataframes['payments'], on='payment_id')
    df_orders = df_orders.merge(dataframes['shippers'], on='shipper_id')
    df_orders = df_orders.merge(dataframes['ratings'], on='rating_id')
    df_orders = df_orders.merge(dataframes['vouchers'], how='left', on='voucher_id')
    df_orders.rename(columns={'user_id_x': 'user_id'}, inplace=True)
    
    fact_orders_columns = dimension_columns.get('fact_orders')
    return df_orders[fact_orders_columns]

def transform_fact_order_items():
    """Transform data for the fact_order_items table."""
    dataframes = {table: extract_data(table) for table in oltp_tables.keys()}

    df_order_items = dataframes['order_items']
    df_order_items = df_order_items.merge(dataframes['orders'], on='order_id')
    df_order_items = df_order_items.merge(dataframes['products'], on='product_id')
    
    fact_order_items_columns = dimension_columns.get('fact_order_items')
    return df_order_items[fact_order_items_columns]

def load_data(df, table_name):
    """Load the transformed data into the target table in the data warehouse."""
    engine = sa.create_engine(warehouse_conn_string)
    with engine.connect() as conn:
        # Cek kunci unique
        unique_key = get_unique_key(table_name)  # Misalnya user_id untuk tabel dim_user
        existing_data = pd.read_sql(f"SELECT {unique_key} FROM {table_name}", conn)
        
        # Deduplikasi data
        df = deduplicate_data(df, existing_data, unique_key)
        
        # Masukkan data baru
        df.to_sql(table_name, conn, index=False, if_exists='append', method='multi')
        print(f'Load Data {table_name} Success')
        
def deduplicate_data(new_data, existing_data, unique_key):
    """Remove duplicates from new data based on existing data."""
    existing_keys = existing_data[unique_key].tolist()
    unique_rows = new_data[~new_data[unique_key].isin(existing_keys)]
    return unique_rows

def get_unique_key(table_name):
    """Retrieve the unique key of the table."""
    if table_name == 'dim_user':
        return 'user_id'
    elif table_name == 'dim_payment':
        return 'payment_id'
    elif table_name == 'dim_shipper':
        return 'shipper_id'
    elif table_name == 'dim_rating':
        return 'rating_id'
    elif table_name == 'dim_voucher':
        return 'voucher_id'
    elif table_name == 'dim_product_category':
        return 'product_category_id'
    elif table_name == 'dim_product':
        return 'product_id'
    elif table_name == 'fact_orders':
        return 'order_id'
    elif table_name == 'fact_order_items':
        return 'order_item_id'
    # Tambahkan kondisi lain jika ada tabel lain
    else:
        raise ValueError("Table name not recognized.")

def validate_foreign_keys(df, engine, foreign_key_column, referenced_table, referenced_column):
    # Get unique foreign key values from the dataframe
    foreign_keys = df[foreign_key_column].unique()
    # Query the referenced table to get existing keys
    existing_keys_query = f"SELECT {referenced_column} FROM {referenced_table} WHERE {referenced_column} IN :keys"
    existing_keys = pd.read_sql(existing_keys_query, engine, params={"keys": tuple(foreign_keys)})

    # Find missing keys
    missing_keys = set(foreign_keys) - set(existing_keys[referenced_column])
    
    if missing_keys:
        raise ValueError(f"Missing foreign keys: {missing_keys}")
        
def create_and_insert_dm_sales():
    """Create dm_sales table and insert data into it."""
    engine = sa.create_engine(warehouse_conn_string)
    with engine.connect() as conn:
        # Create dm_sales table
        conn.execute(ddl_marts['dim_sales'])

        # Insert data into dm_sales table
        conn.execute(ddl_marts['insert_dm_sales'])
    print(f'Data Mart Has Create Success')
    
def etl_process():
    """Run the entire ETL process."""
    # Create tables
    create_tables()

    # Process dimension tables
    for dim_table, target_table in warehouse_tables.items():
        if dim_table != 'fact_orders':
            source_table = dim_table
            df = extract_data(source_table)
            transformed_df = transform_data(df, dim_table)
            load_data(transformed_df, target_table)
        else:
            # Process fact table
            df_fact_orders = transform_fact_orders()
            load_data(df_fact_orders, target_table)

    # proses mart table
    create_and_insert_dm_sales()

if __name__ == "__main__":
    etl_process()
