package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/kr/pretty"
	"github.com/spf13/cobra"
	"gopkg.in/ini.v1"
	"gopkg.in/yaml.v2"
)

// Pgbouncer

type PgbouncerConfigFile struct {
	Pgbouncer *PgbouncerOptionsConfig `ini:"pgbouncer"`
	Databases map[string]string       `ini:"databases"`
}

func (c *PgbouncerConfigFile) Marshal() ([]byte, error) {
	cfg := ini.Empty()

	// Write pgbouncer options
	err := cfg.Section("pgbouncer").ReflectFrom(c.Pgbouncer)
	if err != nil {
		return nil, err
	}

	// Write databases
	for db, opts := range c.Databases {
		cfg.Section("databases").Key(db).SetValue(fmt.Sprintf(`"%s"`, opts))
	}

	// Write to bytes
	buf := &bytes.Buffer{}
	_, err = cfg.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Poppy
type Config struct {
	PoppyConfig     *PoppyConfig     `yaml:"pgpoppy"`
	PgbouncerConfig *PgbouncerConfig `yaml:"pgbouncer"`
}

func (c *Config) getDatabase(dbname string) *DatabaseConfig {
	for _, database := range c.PgbouncerConfig.Databases {
		if database.DatabaseName == dbname {
			return database
		}
	}
	return nil
}

func (c *Config) ToPgbouncerConfigFile() (*PgbouncerConfigFile, error) {
	pgbouncerOpts := c.PgbouncerConfig.Options

	databases := map[string]string{}
	for _, database := range c.PgbouncerConfig.Databases {
		dbname := database.DatabaseName
		dbnameTrace := dbname

		fallbackDatabaseName := database.FallbackDatabase
		options, isValid := database.GetOptions()

		// If options are not valid, we need to check a fallback database.
		for !isValid {
			if len(fallbackDatabaseName) == 0 {
				return nil, fmt.Errorf("Database %s has no healthy servers and did not specify a fallback database.", dbnameTrace)
			}

			database = c.getDatabase(fallbackDatabaseName)
			if database == nil {
				return nil, fmt.Errorf("Database %s has no healthy servers and listed a fallback database that does not exist.", dbnameTrace)
			}

			// Try again.
			dbnameTrace = dbnameTrace + " --(falling back to)--> " + fallbackDatabaseName
			fallbackDatabaseName = database.FallbackDatabase
			options, isValid = database.GetOptions()
		}

		databases[dbname] = options
	}

	return &PgbouncerConfigFile{
		Pgbouncer: pgbouncerOpts,
		Databases: databases,
	}, nil
}

type PoppyConfig struct {
	PgbouncerConnectionOptions string `yaml:"pgbouncer_connection_options"`
	PgbouncerConfigPath        string `yaml:"pgbouncer_config_path"`
}

type PgbouncerConfig struct {
	Databases []*DatabaseConfig       `yaml:"databases"`
	Options   *PgbouncerOptionsConfig `yaml:"options"`
}

type PgbouncerOptionsConfig struct {
	AdminUsers  string `yaml:"admin_users" ini:"admin_users,omitempty"`
	AuthFile    string `yaml:"auth_file" ini:"auth_file,omitempty"`
	AuthHbaFile string `yaml:"auth_hba_file" ini:"auth_hba_file,omitempty"`
	AuthType    string `yaml:"auth_type" ini:"auth_type,omitempty"`

	ClientIdleTimeout  int64  `yaml:"client_idle_timeout" ini:"client_idle_timeout,omitempty"`
	ClientTlsCaFile    string `yaml:"client_tls_ca_file" ini:"client_tls_ca_file,omitempty"`
	ClientTlsCertFile  string `yaml:"client_tls_cert_file" ini:"client_tls_cert_file,omitempty"`
	ClientTlsCiphers   string `yaml:"client_tls_ciphers" ini:"client_tls_ciphers,omitempty"`
	ClientTlsKeyFile   string `yaml:"client_tls_key_file" ini:"client_tls_key_file,omitempty"`
	ClientTlsProtocols string `yaml:"client_tls_protocols" ini:"client_tls_protocols,omitempty"`
	ClientTlsSslmode   string `yaml:"client_tls_sslmode" ini:"client_tls_sslmode,omitempty"`

	ServerConnectTimeout int64  `yaml:"server_connect_timeout" ini:"server_connect_timeout,omitempty"`
	ServerIdleTimeout    int64  `yaml:"server_idle_timeout" ini:"server_idle_timeout,omitempty"`
	ServerTlsCaFile      string `yaml:"server_tls_ca_file" ini:"server_tls_ca_file,omitempty"`
	ServerTlsCertFile    string `yaml:"server_tls_cert_file" ini:"server_tls_cert_file,omitempty"`
	ServerTlsCiphers     string `yaml:"server_tls_ciphers" ini:"server_tls_ciphers,omitempty"`
	ServerTlsKeyFile     string `yaml:"server_tls_key_file" ini:"server_tls_key_file,omitempty"`
	ServerTlsProtocols   string `yaml:"server_tls_protocols" ini:"server_tls_protocols,omitempty"`
	ServerTlsSslmode     string `yaml:"server_tls_sslmode" ini:"server_tls_sslmode,omitempty"`
	ServerCheckQuery     string `yaml:"server_check_query" ini:"server_check_query,omitempty"`

	DefaultPoolSize         int64  `yaml:"default_pool_size" ini:"default_pool_size,omitempty"`
	IgnoreStartupParameters string `yaml:"ignore_startup_parameters" ini:"ignore_startup_parameters,omitempty"`
	ListenAddr              string `yaml:"listen_addr" ini:"listen_addr,omitempty"`
	ListenPort              string `yaml:"listen_port" ini:"listen_port,omitempty"`
	MaxClientConn           int64  `yaml:"max_client_conn" ini:"max_client_conn,omitempty"`
	MinPoolSize             int64  `yaml:"min_pool_size" ini:"min_pool_size,omitempty"`
	PoolMode                string `yaml:"pool_mode" ini:"pool_mode,omitempty"`
	ReservePoolSize         int64  `yaml:"reserve_pool_size" ini:"reserve_pool_size,omitempty"`
	ReservePoolTimeout      string `yaml:"reserve_pool_timeout" ini:"reserve_pool_timeout,omitempty"`
	RestartOnChange         bool   `yaml:"restart_on_change" ini:"restart_on_change,omitempty"`
}

type DatabaseConfig struct {
	DatabaseName     string                 `yaml:"database"`
	DatabaseOptions  *DatabaseOptionsConfig `yaml:"options"`
	Servers          []*ServerConfig        `yaml:"servers"`
	Healthcheck      *HealthcheckConfig     `yaml:"healthcheck"`
	FallbackDatabase string                 `yaml:"fallback_database"`
}

func (dc *DatabaseConfig) pickServer() *ServerConfig {
	// Look for a healthy server
	for _, server := range dc.Servers {
		if server.isHealthy {
			return server
		}
	}

	// Or, look for a default server.
	for _, server := range dc.Servers {
		if server.Default {
			return server
		}
	}

	// Otherwise, no server is available.
	return nil
}

func (dc *DatabaseConfig) GetOptions() (string, bool) {
	options := []string{}
	if dc.DatabaseOptions.PoolSize != 0 {
		options = append(options, fmt.Sprintf("pool_size=%d", dc.DatabaseOptions.PoolSize))
	}
	if dc.DatabaseOptions.PoolMode != "" {
		options = append(options, fmt.Sprintf("pool_mode=%s", dc.DatabaseOptions.PoolMode))
	}
	if dc.DatabaseOptions.Port != 0 {
		options = append(options, fmt.Sprintf("port=%d", dc.DatabaseOptions.Port))
	}
	if dc.DatabaseOptions.ClientEncoding != "" {
		options = append(options, fmt.Sprintf("client_encoding=%s", dc.DatabaseOptions.ClientEncoding))
	}
	if dc.DatabaseOptions.Dbname != "" {
		options = append(options, fmt.Sprintf("dbname=%s", dc.DatabaseOptions.Dbname))
	}

	server := dc.pickServer()
	if server == nil {
		// No server found, try looking for a fallback database?
		return "", false
	}
	options = append(options, fmt.Sprintf("host=%s", server.ServerHost))

	return strings.Join(options, " "), true
}

type DatabaseOptionsConfig struct {
	PoolSize       int64  `yaml:"pool_size"`
	PoolMode       string `yaml:"pool_mode"`
	Port           int64  `yaml:"port"`
	ClientEncoding string `yaml:"client_encoding"`
	Dbname         string `yaml:"dbname"`
}

type ServerConfig struct {
	ServerName string `yaml:"name"`
	ServerHost string `yaml:"host"`
	Default    bool   `yaml:"default"`

	isHealthy bool
}

type HealthcheckConfig struct {
	Port int64  `yaml:"port"`
	URI  string `yaml:"uri"`
}

func ParseConfig(file string) (*Config, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func main() {
	cfg, err := ParseConfig("./examples/config.yaml")
	if err != nil {
		panic(err)
	}

	bootstrapCmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap a pgbouncer config via poppy yaml config",
		Run: func(cmd *cobra.Command, args []string) {
			pretty.Println(cfg)

			pgbouncerCfgFile, err := cfg.ToPgbouncerConfigFile()
			if err != nil {
				panic(err)
			}
			cfgFile, err := pgbouncerCfgFile.Marshal()
			if err != nil {
				panic(err)
			}
			fmt.Println(string(cfgFile))

			err = ioutil.WriteFile(cfg.PoppyConfig.PgbouncerConfigPath, cfgFile, 0644)
			if err != nil {
				panic(err)
			}
		},
	}

	rootCmd := &cobra.Command{
		Use:   "poppy",
		Short: "Poppy is a pgbouncer operator",
	}
	rootCmd.AddCommand(bootstrapCmd)
	rootCmd.Execute()
}
