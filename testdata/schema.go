package testdata

var (
	CreateTableStatements = []string{
		"CREATE TABLE users (`UserId` STRING(36) NOT NULL, `Name` STRING(36), `Age` INT64, `CreatedAt` TIMESTAMP, `UpdatedAt` TIMESTAMP) PRIMARY KEY (`UserId`)",
	}
)
