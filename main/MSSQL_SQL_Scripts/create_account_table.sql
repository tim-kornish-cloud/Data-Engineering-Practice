USE [Data_Engineering]
GO

/****** Object:  Table [dbo].[Accounts_test_1]    Script Date: 8/17/2025 1:12:06 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF EXISTS [dbo].[Accounts_test_2]
GO

CREATE TABLE [dbo].[Accounts_test_2](
	[AccountNumber] [nvarchar](50) NULL,
	[Name] [nvarchar](50) NULL,
	[NumberOfEmployees] [nvarchar](50) NULL,
	[NumberOfLocations__c] [nvarchar](50) NULL,
	[Phone] [nvarchar](50) NULL,
	[SLA__c] [nvarchar](50) NULL,
	[SLASerialNumber__c] [nvarchar](50) NULL,
	[Account_Number_ExternaL_ID__c] [nvarchar](50) NULL,
	[IsActive] [bit] NULL,
	[CreatedDate] DATE NULL,
	[AmountPaid] [nvarchar](50) NULL
) ON [PRIMARY]
GO
