from sqlalchemy import create_engine, Column, Integer, String, VARBINARY, DATE, text, event
from sqlalchemy.orm import sessionmaker, declarative_base
import numpy as np
from datetime import datetime
import pandas as pd
import hashlib
from dotenv import dotenv_values
from tqdm import tqdm
import logging
from logging.handlers import RotatingFileHandler

config = dotenv_values(".env")

# Create a logger for SQLAlchemy
sqlalchemy_logger = logging.getLogger('sqlalchemy')

# Set the logging level to ERROR to only log errors
sqlalchemy_logger.setLevel(logging.ERROR)

# Create a rotating file handler to log errors to a file
file_handler = RotatingFileHandler('sqlalchemy_errors.log', maxBytes=1024*1024*5, backupCount=5)
file_handler.setLevel(logging.ERROR)

# Create a formatter for the log messages
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
sqlalchemy_logger.addHandler(file_handler)

# Define the model
Base = declarative_base()

class RecipientInfo(Base):
    __tablename__ = 'recipient_info'
    __table_args__ = {'schema': 'test'}
    recipient_key = Column(Integer, primary_key=True)
    tin_type = Column(Integer)
    customer_id = Column(String)
    name1 = Column(String)
    name2 = Column(String)
    address1 = Column(String)
    address2 = Column(String)
    city = Column(String)
    state_province = Column(String)
    postal_code = Column(String)
    add_batch_id = Column(Integer)
    update_batch_id = Column(Integer)
    source_file = Column(String)
    freeze_electronic_consent_ind = Column(String)
    entity_type = Column(String)
    foreign_address_ind = Column(String)
    user_field_1 = Column(String)
    status = Column(String)
    status_tax_year = Column(Integer)
    created_by = Column(String)
    tin_hash = Column(VARBINARY)
    audit_hash = Column(VARBINARY)
    last_updated_by = Column(String)
    last_updated = Column(DATE)

def calculate_audit_hash_vectorized(row):
    concat_fields = " : ".join(
        row[['tin_type',
            'customer_id',
            'name1',
            'name2',
            'address1',
            'address2',
            'city',
            'state_province',
            'postal_code',
            'tin',
            'source_file',
            'freeze_electronic_consent_ind',
            'entity_type',
            'foreign_address_ind',
            'status',
            'status_tax_year',
            ]].fillna('').astype(str))
    
    return hashlib.sha256(concat_fields.encode("utf-16le")).digest()
    

def add_audit_hash_to_df(df):
    print(f'Creating {len(df)} Audit Hashes')
    
    # df["audit_hash"] = [None] * len(df)
    audit_hashes = []
    for idx in tqdm(range(len(df)), desc="Creating Audit Hashes"):
        audit_hashes.append(calculate_audit_hash_vectorized(df.iloc[idx]))

    assert list(set(audit_hashes)) != [None]
    return audit_hashes


def add_tin_hash_to_df(df, session):
    print(f'Encrypting {len(df)} TINs')

    session.execute(text("OPEN SYMMETRIC KEY PIIKey DECRYPTION BY CERTIFICATE GTInhouse"))
    encryptbykey_query = text("""
        SELECT tin, ENCRYPTBYKEY(KEY_GUID('PIIKey'), CONVERT(VARCHAR, tin)) AS tin_hash 
        FROM (SELECT value AS tin FROM STRING_SPLIT(CAST(:tins AS NVARCHAR(MAX)), ',')) AS T
    """)
    tins = ','.join(df.tin.dropna().astype(str).tolist())
    result = session.execute(encryptbykey_query, {"tins": tins}).fetchall()
    session.execute(text("CLOSE SYMMETRIC KEY PIIKey"))
    
    tin_hashes = df.tin.map(dict(result)).tolist()
    assert list(set(tin_hashes)) != [None]

    return tin_hashes

def add_user_name_to_df(df, session):
    select_user_name_query = text("""select user_name()""")
    result = session.execute(select_user_name_query).fetchone()
    user_name = result[0]
    df['last_updated_by'] = user_name
    return df

def check_if_map_tin_type(df):
    tin_type_values = df.tin_type.unique()

    ### if 1 or 2 is not in, then take lower case tin types and make 1 for ssn and 2 for anything else
    if not set([1, 2]).issubset(set(tin_type_values)):
        df['tin_type'] = df['tin_type'].str.lower().apply(lambda x: 1 if x == 'ein' else 2)

    return df

if __name__ == "__main__":

    engine = create_engine(
        config.get('SQLALCHEMY_URL'),
        connect_args = {
            "TrustServerCertificate": "yes"
        }, echo=False)
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    Session = sessionmaker(engine)

    with Session() as session:
        csv_path = input("Enter the path to the csv file: ")

        try:
            df = pd.read_csv(csv_path, dtype=str)
        except FileNotFoundError:
            print("File not found, please try again.")
            exit()

        tin_hashes = add_tin_hash_to_df(df, session)
        audit_hashes = add_audit_hash_to_df(df)

        df = (
            df
            .pipe(check_if_map_tin_type)
            .pipe(add_user_name_to_df, session = session)
            .assign(
                tin_hash = tin_hashes,
                audit_hash = audit_hashes,
                last_updated = datetime.now(),
            )
            .replace(np.nan, None)
        )

        df = df.drop('tin', axis = 1)
        data_list = df.to_dict(orient='records')

        insert_sql = text(f'''
            insert into test.recipient_info ({', '.join(df.columns)}) 
            values ({', '.join([f':{x}' for x in df.columns])})'''
        )

        print('Inserting data into recipient_info')
        try:
            session.execute(insert_sql, data_list)
           
        except Exception as e:
            sqlalchemy_logger.exception("An error occurred: %s", e)
            raise

        session.commit()

        print(f'Inserted {len(data_list)} rows into recipient_info')

    #  ../../../../downloads/test_subset.csv

    #  ../../../../downloads/recipient_info_subset_4.csv