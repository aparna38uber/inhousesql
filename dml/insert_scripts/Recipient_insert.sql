-- Insert rows into table 'TableName' in schema '[dbo]'
OPEN SYMMETRIC KEY PIIKey DECRYPTION   
    BY CERTIFICATE GTInhouse 
INSERT INTO [test].[recipient_info]
( -- Columns to insert data into
    [tin_type]
    ,[customer_id]
    ,[name1]
    ,[name2]
    ,[address1]
    ,[address2]
    ,[city]
    ,[state_province]
    ,[postal_code]
    ,[add_batch_id]
    ,[update_batch_id]
    ,[source_file]
    ,[freeze_electronic_consent_ind]
    ,[entity_type]
    ,[foreign_address_ind]
    ,[user_field_1]
    ,[status]
    ,[status_tax_year]
    ,[created_by]
    ,[tin_hash]
    ,[audit_hash]
)
SELECT  
    case when [tin_type] = 'ein' then 1 when [tin_type] = 'ssn' then 2 else null end as tin_type
    ,[customer_id]
    ,[name1]
    ,[name2]
    ,[address1]
    ,[address2]
    ,[city]
    ,[state_province]
    ,[postal_code]
    ,[add_batch_id]
    ,[update_batch_id]
    ,[source_file]
    ,[freeze_electronic_consent_ind]
    ,[entity_type]
    ,[foreign_address_ind]
    ,[user_field_1]
    ,[status]
    ,[status_tax_year]
    ,user_name()
    ,ENCRYPTBYKEY(
        KEY_GUID('PIIKey'),
        convert(VARCHAR,[TIN])
    )
    ,HASHBYTES(
        'SHA2_256', 
        (
            CONVERT(NVARCHAR,[tin_type]) + ' : ' 
            + CONVERT(NVARCHAR,[customer_id]) + ' : ' 
            + CONVERT(NVARCHAR,[name1]) + ' : ' 
            + CONVERT(NVARCHAR,[name2]) + ' : ' 
            + CONVERT(NVARCHAR,[address1]) + ' : ' 
            + CONVERT(NVARCHAR,[address2]) + ' : ' 
            + CONVERT(NVARCHAR,[city]) + ' : ' 
            + CONVERT(NVARCHAR,[state_province]) + ' : ' 
            + CONVERT(NVARCHAR,[postal_code]) + ' : ' 
            + CONVERT(NVARCHAR,[tin])
        )
    )
FROM dbo.[recipient_info_subset_1]
GO
CLOSE SYMMETRIC KEY PIIKey;
            GO