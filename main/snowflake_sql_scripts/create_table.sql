DROP TABLE IF EXISTS Accounts_test;

CREATE TABLE Accounts_test(
	AccountNumber VARCHAR(50) primary key,
	Name VARCHAR(50) NULL,
	NumberOfEmployees VARCHAR(50) NULL,
	NumberOfLocations__c VARCHAR(50) NULL,
	Phone VARCHAR(50) NULL,
	SLA__c VARCHAR(50) NULL,
	SLASerialNumber__c VARCHAR(50) NULL,
	Account_Number_ExternaL_ID__c VARCHAR(50) NULL,
	IsActive BOOLEAN NULL,
	CreatedDate DATE NULL,
	AmountPaid VARCHAR(50) NULL
);
