DROP TABLE IF EXISTS Accounts_test
GO

CREATE TABLE Accounts_test(
	AccountNumber varchar(50) primary key,
	Name varchar(50) NULL,
	NumberOfEmployees varchar(50) NULL,
	NumberOfLocations__c varchar(50) NULL,
	Phone varchar(50) NULL,
	SLA__c varchar(50) NULL,
	SLASerialNumber__c varchar(50) NULL,
	Account_Number_ExternaL_ID__c varchar(50) NULL,
	IsActive bit NULL,
	CreatedDate DATE NULL,
	AmountPaid varchar(50) NULL
)
