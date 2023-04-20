SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [test].[update_recipient] ON [us_tax_reporting].[test].[recipient_info]
AFTER UPDATE
AS
BEGIN
SET NOCOUNT ON;
UPDATE us_tax_reporting.test.recipient_info SET last_updated = GETDATE(), last_updated_by = user_name()
FROM us_tax_reporting.test.recipient_info r
INNER JOIN inserted i on r.recipient_key=i.recipient_key
END
GO
ALTER TABLE [test].[recipient_info] ENABLE TRIGGER [update_recipient]
GO
