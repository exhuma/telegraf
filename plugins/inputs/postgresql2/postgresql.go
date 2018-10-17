package postgresql2

import (
	_ "fmt" // XXX

	// register in driver.
	_ "github.com/lib/pq"

	"github.com/exhuma/gopgstats"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Postgresql struct {
	Service
	Databases        []string
	IgnoredDatabases []string
}

var sampleConfig = `
  ## specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\
  ##       ?sslmode=[disable|verify-ca|verify-full]
  ## or a simple string:
  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional.
  ##
  ## Without the dbname parameter, the driver will default to a database
  ## with the same name as the user. This dbname is just for instantiating a
  ## connection with the server and doesn't restrict the databases we are trying
  ## to grab metrics for.
  ##
  address = "host=localhost user=postgres sslmode=disable"
  ## A custom name for the database that will be used as the "server" tag in the
  ## measurement output. If not specified, a default one generated from
  ## the connection address is used.
  # outputaddress = "db01"

  ## connection configuration.
  ## maxlifetime - specify the maximum lifetime of a connection.
  ## default is forever (0s)
  max_lifetime = "0s"

  ## A  list of databases to explicitly ignore.  If not specified, metrics for all
  ## databases are gathered.  Do NOT use with the 'databases' option.
  # ignored_databases = ["postgres", "template0", "template1"]

  ## A list of databases to pull metrics about. If not specified, metrics for all
  ## databases are gathered.  Do NOT use with the 'ignored_databases' option.
  # databases = ["app_production", "testing"]
`

func (p *Postgresql) SampleConfig() string {
	return sampleConfig
}

func (p *Postgresql) Description() string {
	return "Read metrics from one or many postgresql servers"
}

func (p *Postgresql) AccumulateLocks(acc telegraf.Accumulator, locks []gopgstats.LocksRow) {
	for _, value := range locks {
		fields := map[string]interface{}{
			"lock_count": value.Count,
		}
		var isGranted string
		if value.Granted {
			isGranted = "true"
		} else {
			isGranted = "false"
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
			"mode":          value.Mode,
			"type":          value.Type,
			"granted":       isGranted,
		}
		acc.AddFields("postgresql2-locks", fields, tags)
	}
}

func (p *Postgresql) AccumulateDiskIOs(acc telegraf.Accumulator, ios []gopgstats.DiskIOsRow) {
	for _, value := range ios {
		fields := map[string]interface{}{
			"HeapBlocksRead":       value.HeapBlocksRead,
			"HeapBlocksHit":        value.HeapBlocksHit,
			"IndexBlocksRead":      value.IndexBlocksRead,
			"IndexBlocksHit":       value.IndexBlocksHit,
			"ToastBlocksRead":      value.ToastBlocksRead,
			"ToastBlocksHit":       value.ToastBlocksHit,
			"ToastIndexBlocksRead": value.ToastIndexBlocksRead,
			"ToastIndexBlocksHit":  value.ToastIndexBlocksHit,
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
		}
		acc.AddFields("postgresql2-disk-ios", fields, tags)
	}
}

func (p *Postgresql) AccumulateDiskSizes(acc telegraf.Accumulator, ios []gopgstats.DiskSizesRow) {
	for _, value := range ios {
		fields := map[string]interface{}{
			"Size": value.Size,
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
		}
		acc.AddFields("postgresql2-disk-size", fields, tags)
	}
}

func (p *Postgresql) AccumulateConnections(acc telegraf.Accumulator, connections []gopgstats.ConnectionsRow) {
	for _, value := range connections {
		fields := map[string]interface{}{
			"Idle":              value.Idle,
			"IdleInTransaction": value.IdleInTransaction,
			"Unknown":           value.Unknown,
			"QueryActive":       value.QueryActive,
			"Waiting":           value.Waiting,
		}
		tags := map[string]string{
			"username": value.Username,
		}
		acc.AddFields("postgresql2-connections", fields, tags)
	}
}

func (p *Postgresql) AccumulateQueryAges(acc telegraf.Accumulator, ages []gopgstats.QueryAgesRow) {
	for _, value := range ages {
		fields := map[string]interface{}{
			"QueryAge":       value.QueryAge,
			"TransactionAge": value.TransactionAge,
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
		}
		acc.AddFields("postgresql2-query-ages", fields, tags)
	}
}

func (p *Postgresql) AccumulateTransactions(acc telegraf.Accumulator, transactions []gopgstats.TransactionsRow) {
	for _, value := range transactions {
		fields := map[string]interface{}{
			"Committed":  value.Committed,
			"Rolledback": value.Rolledback,
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
		}
		acc.AddFields("postgresql2-query-transactions", fields, tags)
	}
}
func (p *Postgresql) AccumulateTempBytes(acc telegraf.Accumulator, tempBytes []gopgstats.TempBytesRow) {
	for _, value := range tempBytes {
		fields := map[string]interface{}{
			"TemporaryBytes": value.TemporaryBytes,
		}
		tags := map[string]string{
			"database_name": value.DatabaseName,
		}
		acc.AddFields("postgresql2-query-temp-bytes", fields, tags)
	}
}

func (p *Postgresql) Gather(acc telegraf.Accumulator) error {
	var err error

	fetcher := gopgstats.MakeDefaultFetcher(p.DB)

	// global stats
	locks, err := fetcher.Locks()
	if err != nil {
		return err
	}
	p.AccumulateLocks(acc, locks)

	diskSizes, err := fetcher.DiskSize()
	if err != nil {
		return err
	}
	p.AccumulateDiskSizes(acc, diskSizes)

	connections, err := fetcher.Connections()
	if err != nil {
		return err
	}
	p.AccumulateConnections(acc, connections)

	ages, err := fetcher.QueryAges()
	if err != nil {
		return err
	}
	p.AccumulateQueryAges(acc, ages)

	transactions, err := fetcher.Transactions()
	if err != nil {
		return err
	}
	p.AccumulateTransactions(acc, transactions)

	tempBytes, err := fetcher.TempBytes()
	if err != nil {
		return err
	}
	p.AccumulateTempBytes(acc, tempBytes)

	// db-local stats
	diskio, err := fetcher.DiskIOAll(p.Address)
	if err != nil {
		return err
	}
	p.AccumulateDiskIOs(acc, diskio)

	return nil
}

func init() {
	inputs.Add("postgresql2", func() telegraf.Input {
		return &Postgresql{
			Service: Service{
				MaxIdle: 1,
				MaxOpen: 1,
				MaxLifetime: internal.Duration{
					Duration: 0,
				},
				IsPgBouncer: false,
			},
		}
	})
}
