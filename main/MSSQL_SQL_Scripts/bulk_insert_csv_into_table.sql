BULK INSERT [Data_Engineering].[dbo].[Accounts_test_1]
    FROM 'C:\Users\John Cena\Documents\GitHub\Data-Engineering-Practice\main\MockData\MOCK_DATA_Accounts_with_IDs.csv'
    WITH
    (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    ERRORFILE = 'C:\Users\John Cena\Documents\GitHub\Data-Engineering-Practice\main\MockData\MOCK_DATA_Accounts_with_IDsErrorRows.csv',
    TABLOCK
    )
