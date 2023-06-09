USE us_tax_reporting 
CREATE MASTER KEY ENCRYPTION   
BY PASSWORD='GT-InHouseReporting@2023'  
GO  

CREATE CERTIFICATE GTInhouse
WITH SUBJECT ='PII_InHouseReporting',  
EXPIRY_DATE='2025-04-30'  
GO  

CREATE SYMMETRIC KEY PIIKey  
WITH ALGORITHM= AES_256 ENCRYPTION   
BY CERTIFICATE GTInhouse  
GO  