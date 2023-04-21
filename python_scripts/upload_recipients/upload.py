from sqlalchemy import create_engine, Column, Integer, String, VARBINARY, text
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import hashlib
# from memoryview import memoryview

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

# def calculate_audit_hash(row, session):
#     audit_hash_query = text("SELECT HASHBYTES('SHA2_256', "
#                         "CONVERT(NVARCHAR, ISNULL(:tin_type, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:customer_id, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:name1, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:name2, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:address1, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:address2, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:city, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:state_province, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:postal_code, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:tin, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:source_file, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:freeze_electronic_consent_ind, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:entity_type, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:foreign_address_ind, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:status, '')) + ' : ' + "
#                         "CONVERT(NVARCHAR, ISNULL(:status_tax_year, '')))")
#     result = session.execute(
#         audit_hash_query, 
#          {
#             'tin': row['tin'],
#             'tin_type': row['tin_type'],
#             'customer_id': row['customer_id'],
#             'name1': row['name1'],
#             'name2': row['name2'],
#             'address1': row['address1'],
#             'address2': row['address2'],
#             'city': row['city'],
#             'state_province': row['state_province'],
#             'postal_code': row['postal_code'],
#             'source_file': row['source_file'],
#             'freeze_electronic_consent_ind': row['freeze_electronic_consent_ind'],
#             'entity_type': row['entity_type'],
#             'foreign_address_ind': row['foreign_address_ind'],
#             'status': row['status'],
#             'status_tax_year': row['status_tax_year']
#         }
#     )
    
#     audit_hash = result.scalar()
#     return audit_hash

# def create_tin_hash(tin, session):
#     encryptbykey_query = text("SELECT ENCRYPTBYKEY(KEY_GUID('PIIKey'), CONVERT(VARCHAR, :tin))")
#     result = session.execute(encryptbykey_query, {'tin': tin})

#     row = result.fetchone()
#     return row[0]
    
# def add_tin_hash_to_df(df, session):
#     print(f'Creating {len(df)} TIN Hashes')
#     session.execute(text("OPEN SYMMETRIC KEY PIIKey DECRYPTION BY CERTIFICATE GTInhouse"))

#     df = (
#         df
#         .assign(
#             tin_hash = lambda df_: df_.tin.apply(lambda x: create_tin_hash(x, session))
#         )
#     )
#     ### check that tin hashes are not None
#     assert list(df.tin_hash.unique()) != [None]
    
#     session.execute(text("CLOSE SYMMETRIC KEY PIIKey"))
    
#     return df

# def add_audit_hash_to_df(df, session):
#     print(f'Creating {len(df)} Audit Hashes')
#     df = (
#         df
#         .assign(
#             audit_hash = lambda df_: df_.apply(lambda x: calculate_audit_hash(x, session), axis = 1)
#         )
#     )
#     ### check that tin hashes are not None
#     assert list(df.audit_hash.unique()) != [None]
    
#     return df
def calculate_audit_hash_vectorized(row):
    concat_fields = " : ".join(
        row[['tin',
            'tin_type',
            'customer_id',
            'name1',
            'name2',
            'address1',
            'address2',
            'city',
            'state_province',
            'postal_code',
            'source_file',
            'freeze_electronic_consent_ind',
            'entity_type',
            'foreign_address_ind',
            'status',
            'status_tax_year']])
    
    return hashlib.sha256(concat_fields.encode("utf-8")).digest()

def add_audit_hash_to_df_vectorized(df):
    print(f'Creating {len(df)} Audit Hashes')
    df = df.fillna('')
    df["audit_hash"] = df.apply(calculate_audit_hash_vectorized, axis=1)
    assert list(df.audit_hash.unique()) != [None]
    return df

def create_tin_hashes(df, session):
    encryptbykey_query = text("SELECT tin, ENCRYPTBYKEY(KEY_GUID('PIIKey'), CONVERT(VARCHAR, tin)) AS tin_hash FROM (VALUES (:tins)) AS T(tin)")
    tins = [{"tins": tin} for tin in df['tin']]
    result = session.execute(encryptbykey_query, tins)
    tin_hash_dict = {row[0]: row[1] for row in result}
    return tin_hash_dict


def add_tin_hash_to_df_optimized(df, session):
    print(f'Creating {len(df)} TIN Hashes')
    session.execute(text("OPEN SYMMETRIC KEY PIIKey DECRYPTION BY CERTIFICATE GTInhouse"))

    tin_hash_dict = create_tin_hashes(df, session)
    df['tin_hash'] = df['tin'].map(tin_hash_dict)
    assert list(df.tin_hash.unique()) != [None]

    session.execute(text("CLOSE SYMMETRIC KEY PIIKey"))
    return df


sqlalchemy_url = 'mssql+pyodbc:///?odbc_connect=Driver%3D%7BODBC+Driver+18+for+SQL+Server%7D%3BServer%3DPHX2-TIR-DB01%3BDatabase%3Dus_tax_reporting%3BTrusted_Connection%3Dyes%3B'

engine = create_engine(
    sqlalchemy_url,
    connect_args = {
        "TrustServerCertificate": "yes"
    }, echo=False)

Session = sessionmaker(engine)

with Session() as session:
    csv_path = input("Enter the path to the csv file: ")

    ### read csv file from path, catch error and respond to user if does not exist
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except FileNotFoundError:
        print("File not found, please try again.")
        exit()


    # df = (
    #     df
    #     .fillna('')
    #     .pipe(add_tin_hash_to_df, session = session)
    #     .pipe(add_audit_hash_to_df, session = session)
    # )
    df = (
        df
        .fillna('')
        .pipe(add_tin_hash_to_df_optimized, session=session)
        .pipe(add_audit_hash_to_df_vectorized)
    )

    data_list = df.drop('tin', axis = 1).to_dict(orient='records')

    session.bulk_insert_mappings(RecipientInfo, data_list)
    session.commit()


#  ../../../../downloads/test_subset.csv