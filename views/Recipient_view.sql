CREATE VIEW vw_Recipient 
AS  

SELECT  
name1,
address1,
city,
state_province,
postal_code,
CONVERT(VARCHAR(50),DECRYPTByKeyAutoCert(cert_id('GTInhouse'),NULL,tin_hash)) AS TIN
FROM test.recipient_info 

