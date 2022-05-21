package migrationlib

import (
	"database/sql"
	"errors"
	"log"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source"

	"github.com/golang-migrate/migrate/v4/source/file"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
)

type Config struct {
	DatabaseDriver DatabaseDriver
	DatabaseURL    string
	SourceDriver   SourceDriver
	SourceURL      string //指定数据库的存放路径

	TableName string
}

//二次封装，实现了Command接口
type MigrateCommand struct {
	config Config
}

func newMigrateCmd(c Config) *MigrateCommand {
	return &MigrateCommand{
		config: c,
	}
}

//配置数据库
func (m *MigrateCommand) prepare() (*migrate.Migrate, error) { //指针接收者
	//连接数据库
	db, err := sql.Open(string(m.config.DatabaseDriver), m.config.DatabaseURL) //使用go自带database/sql;注意第一个参数必须转化成string类型，否则报错
	if err != nil {
		return nil, err
	}

	//配置资源驱动
	var sourceDriver source.Driver
	switch m.config.SourceDriver {
	case FileDriver:
		sourceDriver = &file.File{}
	default:
		return nil, errors.New("not supported source driver")
	}
	sourceDriver, err = sourceDriver.Open(m.config.SourceURL)
	if err != nil {
		return nil, err
	}

	//配置数据库驱动
	var databaseDriver database.Driver
	switch m.config.DatabaseDriver {
	case PostgresDriver:
		databaseDriver, err = postgres.WithInstance(db, &postgres.Config{MigrationsTable: m.config.TableName})
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("not supported database driver")
	}

	//调用外部库 migrate.NewWithInstance返回*migrate.Migrate
	mi, err := migrate.NewWithInstance(string(m.config.SourceDriver),
		sourceDriver,
		string(m.config.DatabaseDriver),
		databaseDriver,
	)
	if err != nil {
		return nil, err
	}
	return mi, nil
}

//启动数据库
func (m MigrateCommand) Up() error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Up()
}

//
func (m MigrateCommand) UpTo(limit uint) error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Steps(int(limit))
}

func (m MigrateCommand) Down() error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Down()
}

func (m MigrateCommand) DownTo(limit int) error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	if limit > 0 {
		return errors.New("limit should be less than 0")
	}
	return mi.Steps(limit)
}

func (m MigrateCommand) Drop() error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Drop()
}

func (m MigrateCommand) GoTo(version uint) error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Migrate(version)
}

func (m MigrateCommand) Force(version uint) error {
	mi, err := m.prepare()
	if err != nil {
		return err
	}
	return mi.Force(int(version))
}

func (m MigrateCommand) CurrentVersion() (uint, bool, error) {
	mi, err := m.prepare()
	if err != nil {
		return 0, false, err
	}
	return mi.Version()
}

func (m MigrateCommand) PrintUsageInfo() {
	log.Println(`
Usage: migrate OPTIONS COMMAND [arg...]
migrate [ -version | -help ]

Options:
  -source          Location of the migrations (driver://url)
  -path            Shorthand for -source=file://path
  -database        Run migrations against this database (driver://url)
  -prefetch N      Number of migrations to load in advance before executing (default 10)
  -lock-timeout N  Allow N seconds to acquire database lock (default 15)
  -verbose         Print verbose logging
  -version         Print version
  -help            Print usage

Commands:
  create [-ext E] [-dir D] [-seq] [-digits N] [-format] NAME
	   Create a set of timestamped up/down migrations titled NAME, in directory D with extension E.
	   Use -seq option to generate sequential up/down migrations with N digits.
	   Use -format option to specify a Go time format string. Note: migrations with the same time cause "duplicate migration version" error. 

  goto V       Migrate to version V
  up [N]       Apply all or N up migrations
  down [N]     Apply all or N down migrations
  drop [-f] [-all]    Drop everything inside database
	Use -f to bypass confirmation
	Use -all to apply all down migrations
  force V      Set version V but don't run migration (ignores dirty state)
  version      Print current migration version

Source drivers: godoc-vfs, gcs, file, bitbucket, gitlab, github-ee, go-bindata, s3, github
Database drivers: firebirdsql, mysql, redshift, sqlserver, stub, spanner, cockroachdb, crdb-postgres, firebird, mongodb, postgres, postgresql, cassandra, clickhouse, cockroach, mongodb+srv, neo4j`)
}
