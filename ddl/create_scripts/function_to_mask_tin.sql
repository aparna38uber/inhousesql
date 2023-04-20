
CREATE FUNCTION [dbo].[ufn_TIN_mask](@input NVARCHAR(100))  
RETURNS NVARCHAR(100)  
AS   
BEGIN  
        DECLARE @data NVARCHAR(100)  
        SELECT @data= 'XXXXXXXX'+SUBSTRING(@input,8,2)  
        RETURN @data  
END  
