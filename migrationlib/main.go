/*举例:
 	migrationlib.NewMigrateLib(migrationlib.Config{
		DatabaseDriver: migrationlib.PostgresDriver,
		DatabaseURL:    "postgres://postgres:postgres@localhost:5432/migrationlib?sslmode=disable",
		SourceDriver:   migrationlib.FileDriver,
		SourceURL:      "file://migrations",
		TableName:      "migration_versions",
	}).Up()
*/
package migrationlib

import (
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

//对Commad接口的二次封装
type MigrationLib interface {
	Command
}

func NewMigrateLib(c Config) MigrationLib {
	return newMigrateCmd(c)
}
