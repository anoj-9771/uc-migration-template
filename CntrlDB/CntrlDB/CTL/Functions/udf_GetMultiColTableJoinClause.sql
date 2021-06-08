CREATE FUNCTION [CTL].[udf_GetMultiColTableJoinClause] (
	@Column varchar(255),
	@SourceAlias varchar(100),
	@TargetAlias varchar(100)
)
RETURNS VARCHAR(MAX)
AS
/**************************************************************
This function returns the column list with the source and target table alias for table join
**************************************************************/
BEGIN

	DECLARE @SQLClause VARCHAR(MAX) = ''

	IF CHARINDEX (',', @Column) > 0
	BEGIN
		--Temporary table to store the list of columns 	
		DECLARE @ColTable TABLE 
		(
			ColName varchar(100),
			RowID INT identity(1,1)
		)

		--Split the column list based on comma and store in the temporary table
		INSERT INTO @ColTable (ColName)
		SELECT VALUE FROM STRING_SPLIT(@Column, ',')

		DECLARE @TotalRows INT, @CurrentRow INT

		--Get the total number of rows
		SELECT @TotalRows = COUNT(1) FROM @ColTable

		--Initial Counter
		SET @CurrentRow = 1

		--Loop through all the records
		WHILE @CurrentRow <= @TotalRows
		BEGIN
			DECLARE @Row varchar(100), @CurrentCol varchar(100)
			SELECT @CurrentCol = ColName FROM @ColTable WHERE RowID = @CurrentRow
			SET @CurrentCol = TRIM(@CurrentCol)
			--Alias the columns
			SET @Row = @SourceAlias + '.' + @CurrentCol + ' = ' + @TargetAlias + '.' + @CurrentCol

			--Add the and clause from the second part onwards
			IF @SQLClause = ''
				SET @SQLClause = @SQLClause + @Row
			ELSE
				SET @SQLClause = @SQLClause + ' AND ' + @Row

			--Increment the counter, so that we are not in infinite loop
			SET @CurrentRow = @CurrentRow + 1
		END
	END
	ELSE
	BEGIN
		--If there are no comma, it means there is only one column
		SET @SQLClause = @SourceAlias + '.' + @Column + ' = ' + @TargetAlias + '.' + @Column
	END
	RETURN @SQLClause

END