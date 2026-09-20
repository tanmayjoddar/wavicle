package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Storage StorageConfig `yaml:"storage"`
	Engine  EngineConfig  `yaml:"engine"`
	DB      DBConfig      `yaml:"db"`
	Metrics MetricsConfig `yaml:"metrics"`
	Auth    AuthConfig    `yaml:"auth"`
	Logging LoggingConfig `yaml:"logging"`
}

type AuthConfig struct {
	Password string `yaml:"requirepass"`
}

type ServerConfig struct {
	Listen       string        `yaml:"listen"`
	MaxConns     int           `yaml:"max_connections"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type StorageConfig struct {
	DataDir        string `yaml:"data_dir"`
	Engine         string `yaml:"engine"` // "crystal" or "postgres" or "mysql"
	MaxFrontier    int    `yaml:"max_frontier"`
	MaxMemoryBytes int64  `yaml:"max_memory_bytes"`
}

type EngineConfig struct {
	ProofCache ProofCacheConfig `yaml:"proof_cache"`
}

type ProofCacheConfig struct {
	MaxEntries int   `yaml:"max_entries"`
	MaxBytes   int64 `yaml:"max_bytes"`
	Shards     int   `yaml:"shards"`
}

type DBConfig struct {
	Type            string         `yaml:"type"`
	DSN             string         `yaml:"dsn"`
	ReplicationSlot string         `yaml:"replication_slot"`
	Publication     string         `yaml:"publication"`
	TableMappings   []TableMapping `yaml:"table_mappings"`
}

type TableMapping struct {
	Table       string   `yaml:"table"`
	KeyTemplate string   `yaml:"key_template"`
	Columns     []string `yaml:"columns"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"` // "json" or "text"
}

func Default() *Config {
	c := &Config{
		Server: ServerConfig{
			Listen:       ":6379",
			MaxConns:     10000,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		},
		Storage: StorageConfig{
			DataDir:        "data",
			Engine:         "crystal",
			MaxFrontier:    10000000,
			MaxMemoryBytes: 4 * 1024 * 1024 * 1024, // 4GB default
		},
		Engine: EngineConfig{
			ProofCache: ProofCacheConfig{
				MaxEntries: 100000,
				MaxBytes:   200 * 1024 * 1024, // 200 MB
				Shards:     64,
			},
		},
		DB: DBConfig{
			Type:            "crystal",
			DSN:             "",
			ReplicationSlot: "wavicle_slot",
			Publication:     "wavicle_proofs",
		},
		Metrics: MetricsConfig{
			Enabled: false,
			Listen:  ":8080",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
	}
	c.LoadFromEnv()
	return c
}

func (c *Config) LoadFromEnv() {
	if v := os.Getenv("WAVICLE_SERVER_LISTEN"); v != "" {
		c.Server.Listen = v
	}
	if v := os.Getenv("WAVICLE_MAX_CONNS"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Server.MaxConns = i
		}
	}
	if v := os.Getenv("WAVICLE_STORAGE_ENGINE"); v != "" {
		c.Storage.Engine = v
	}
	if v := os.Getenv("WAVICLE_STORAGE_DATA_DIR"); v != "" {
		c.Storage.DataDir = v
	}
	if v := os.Getenv("WAVICLE_MAX_MEMORY"); v != "" {
		if bytes, err := ParseBytes(v); err == nil {
			c.Storage.MaxMemoryBytes = bytes
		}
	}
	if v := os.Getenv("WAVICLE_DB_TYPE"); v != "" {
		c.DB.Type = v
	}
	if v := os.Getenv("WAVICLE_DB_DSN"); v != "" {
		c.DB.DSN = v
	}
	if v := os.Getenv("WAVICLE_DB_REPLICATION_SLOT"); v != "" {
		c.DB.ReplicationSlot = v
	}
	if v := os.Getenv("WAVICLE_DB_PUBLICATION"); v != "" {
		c.DB.Publication = v
	}
	if v := os.Getenv("WAVICLE_METRICS_ENABLED"); v != "" {
		c.Metrics.Enabled = (v == "true")
	}
	if v := os.Getenv("WAVICLE_METRICS_LISTEN"); v != "" {
		c.Metrics.Listen = v
	}
	if v := os.Getenv("WAVICLE_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("WAVICLE_AUTH_PASSWORD"); v != "" {
		c.Auth.Password = v
	}

	// Internal limits
	if v := os.Getenv("WAVICLE_CACHE_ENTRIES"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			c.Engine.ProofCache.MaxEntries = i
		}
	}
}

// ParseBytes parses a human-readable byte string (e.g., "500MB", "4GB", "1024KB", "1048576").
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty byte string")
	}
	multiplier := int64(1)
	valStr := s
	if strings.HasSuffix(s, "TB") || strings.HasSuffix(s, "T") {
		multiplier = 1024 * 1024 * 1024 * 1024
		valStr = strings.TrimRight(s, "TB")
	} else if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		valStr = strings.TrimRight(s, "GB")
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		valStr = strings.TrimRight(s, "MB")
	} else if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		multiplier = 1024
		valStr = strings.TrimRight(s, "KB")
	} else if strings.HasSuffix(s, "B") {
		valStr = strings.TrimRight(s, "B")
	}
	valStr = strings.TrimSpace(valStr)
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func (c *Config) Validate() error {
	if c.Server.Listen == "" {
		return fmt.Errorf("server.listen is required")
	}
	if c.Server.MaxConns <= 0 {
		return fmt.Errorf("server.max_connections must be positive")
	}
	if c.Storage.Engine != "crystal" && c.Storage.Engine != "postgres" && c.Storage.Engine != "mysql" {
		return fmt.Errorf("storage.engine must be 'crystal', 'postgres', or 'mysql'")
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
	}
	if c.Engine.ProofCache.MaxEntries <= 0 {
		return fmt.Errorf("engine.proof_cache.max_entries must be positive")
	}
	if c.Engine.ProofCache.Shards <= 0 {
		return fmt.Errorf("engine.proof_cache.shards must be positive")
	}
	return nil
}

func (c *Config) DataPath(name string) string {
	return filepath.Join(c.Storage.DataDir, name)
}

func (c *Config) WALPath() string {
	return c.DataPath("crystal.log")
}
