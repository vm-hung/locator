package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/hunkvm/locator/pkg/types"
	"github.com/spf13/viper"
)

var (
	validLogLevels = map[string]bool{
		"debug": true, "info": true,
		"warn": true, "error": true,
	}
	validLogFormats = map[string]bool{
		"json": true, "text": true,
	}
)

func setIfConfigured[T any](v *viper.Viper, key string, assign func(T), get func(string) T) {
	if v.IsSet(key) {
		assign(get(key))
	}
}

func Load(path string) (ServerConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("LOCATOR")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		format := "read config file: %w"
		return ServerConfig{}, fmt.Errorf(format, err)
	}

	conf := defaultServerConfig()
	decodeServerConfig(v, &conf)

	errorList := validateServerConfig(conf)
	if len(errorList) > 0 {
		dotMap := make(map[string]any)
		flattenValue("", reflect.ValueOf(conf), dotMap)
		var sb strings.Builder
		sb.WriteString("invalid config:\n")
		keys := make([]string, 0, len(dotMap))
		for k := range dotMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&sb, "  %s: %v\n", k, dotMap[k])
		}
		sb.WriteString("errors:\n")
		for i, e := range errorList {
			fmt.Fprintf(&sb, "  [%d] %s\n", i+1, e)
		}
		return ServerConfig{}, errors.New(sb.String())
	}

	return conf, nil
}

func decodeServerConfig(v *viper.Viper, cfg *ServerConfig) {
	setIfConfigured(v, "node", func(value uint64) { cfg.Node = value }, v.GetUint64)
	setIfConfigured(v, "data_dir", func(value string) { cfg.DataDir = value }, v.GetString)

	setIfConfigured(v, "raft.peer_addrs", func(value string) { cfg.Raft.PeerAddrs = value }, v.GetString)
	setIfConfigured(v, "raft.election_tick", func(value int) { cfg.Raft.ElectionTick = value }, v.GetInt)
	setIfConfigured(v, "raft.heartbeat", func(value time.Duration) { cfg.Raft.Heartbeat = value }, v.GetDuration)
	setIfConfigured(v, "raft.snapshot_count", func(value uint64) { cfg.Raft.SnapshotCount = value }, v.GetUint64)
	setIfConfigured(v, "raft.catchup_entries", func(value uint64) { cfg.Raft.CatchupEntries = value }, v.GetUint64)

	decodeEndpoint(v, "transport.address", &cfg.Transport.Address)
	decodeTLSConfig(v, "transport.tls", &cfg.Transport.TLS)
	decodeEndpoint(v, "registry.address", &cfg.Registry.Address)
	decodeTLSConfig(v, "registry.tls", &cfg.Registry.TLS)

	setIfConfigured(v, "health_check.min_backoff_duration", func(value time.Duration) { cfg.HealthCheck.MinBackoffDuration = value }, v.GetDuration)
	setIfConfigured(v, "health_check.max_backoff_duration", func(value time.Duration) { cfg.HealthCheck.MaxBackoffDuration = value }, v.GetDuration)
	setIfConfigured(v, "health_check.min_connect_timeout", func(value time.Duration) { cfg.HealthCheck.MinConnectTimeout = value }, v.GetDuration)
	setIfConfigured(v, "health_check.max_backoff_attempts", func(value int) { cfg.HealthCheck.MaxBackoffAttempts = value }, v.GetInt)

	setIfConfigured(v, "logger.level", func(value string) { cfg.Logger.Level = value }, v.GetString)
	setIfConfigured(v, "logger.format", func(value string) { cfg.Logger.Format = value }, v.GetString)
}

func decodeEndpoint(v *viper.Viper, prefix string, endpoint *types.Endpoint) {
	setIfConfigured(v, prefix+".host", func(value string) { endpoint.Host = value }, v.GetString)
	setIfConfigured(v, prefix+".port", func(value int) { endpoint.Port = value }, v.GetInt)
}

func decodeTLSConfig(v *viper.Viper, prefix string, cfg *TLSConfig) {
	setIfConfigured(v, prefix+".enabled", func(value bool) { cfg.Enabled = value }, v.GetBool)
	setIfConfigured(v, prefix+".ca_certificate_file", func(value string) { cfg.CaCertificateFile = value }, v.GetString)
	setIfConfigured(v, prefix+".certificate_file", func(value string) { cfg.CertificateFile = value }, v.GetString)
	setIfConfigured(v, prefix+".private_key_file", func(value string) { cfg.PrivateKeyFile = value }, v.GetString)
	setIfConfigured(v, prefix+".refresh_interval", func(value time.Duration) { cfg.RefreshInterval = value }, v.GetDuration)
}

func flattenValue(prefix string, v reflect.Value, result map[string]any) {
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		val := v.Field(i)

		key := prefix
		if prefix != "" {
			key += "."
		}
		key += toSnakeCase(field.Name)

		if val.Kind() == reflect.Struct {
			flattenValue(key, val, result)
		} else {
			result[key] = val.Interface()
		}
	}
}

func toSnakeCase(s string) string {
	var builder strings.Builder
	for i, r := range s {
		if i > 0 && unicode.IsUpper(r) {
			builder.WriteRune('_')
		}
		builder.WriteRune(unicode.ToLower(r))
	}
	return builder.String()
}

func validateServerConfig(cfg ServerConfig) []string {
	var errorList []string

	if cfg.Node == 0 {
		errorList = append(errorList, "node must be > 0")
	}

	if cfg.DataDir == "" {
		errorList = append(errorList, "data_dir is required")
	}

	if cfg.Raft.PeerAddrs == "" {
		errorList = append(errorList, "raft.peer_addrs is required")
	} else {
		for peer := range strings.SplitSeq(cfg.Raft.PeerAddrs, ",") {
			peerErrs := validatePeerAddrs(strings.TrimSpace(peer))
			errorList = append(errorList, peerErrs...)
		}
	}

	transAddr, regAddr := cfg.Transport.Address, cfg.Registry.Address
	errorList = append(errorList, validateEndpoint("transport.address", transAddr)...)
	errorList = append(errorList, validateEndpoint("registry.address", regAddr)...)

	if transAddr.Host == regAddr.Host && transAddr.Port == regAddr.Port {
		errorList = append(errorList, "transport and registry addresses must be different")
	}

	errorList = append(errorList, validateLogConfig(cfg.Logger)...)
	errorList = append(errorList, validateTLSConfig("transport.tls", cfg.Transport.TLS)...)
	errorList = append(errorList, validateTLSConfig("registry.tls", cfg.Registry.TLS)...)

	return errorList
}

func validateLogConfig(cfg LogConfig) []string {
	var errorList []string

	if !validLogLevels[strings.ToLower(cfg.Level)] {
		format := "log.level must be one of [debug, info, warn, error], got %q"
		errorList = append(errorList, fmt.Sprintf(format, cfg.Level))
	}

	if !validLogFormats[strings.ToLower(cfg.Format)] {
		format := "log.format must be one of [json, text], got %q"
		errorList = append(errorList, fmt.Sprintf(format, cfg.Format))
	}

	return errorList
}

func validatePeerAddrs(peer string) []string {
	var errorList []string
	parts := strings.SplitN(peer, "@", 2)
	if len(parts) != 2 {
		format := "raft.peer_addrs: %q: expected format <id>@<host>:<port>"
		errorList = append(errorList, fmt.Sprintf(format, peer))
		return errorList
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || id == 0 {
		format := "raft.peer_addrs: %q: node id must be a positive integer"
		errorList = append(errorList, fmt.Sprintf(format, peer))
	}
	if parts[1] == "" {
		format := "raft.peer_addrs: %q: address is empty"
		errorList = append(errorList, fmt.Sprintf(format, peer))
	}
	return errorList
}

func validateTLSConfig(field string, cfg TLSConfig) []string {
	if !cfg.Enabled {
		return nil
	}
	var errorList []string
	for _, pair := range []struct{ name, path string }{
		{"ca_certificate_file", cfg.CaCertificateFile},
		{"certificate_file", cfg.CertificateFile},
		{"private_key_file", cfg.PrivateKeyFile},
	} {
		if pair.path == "" {
			errorList = append(errorList, fmt.Sprintf("%s.%s is required when tls.enabled is true", field, pair.name))
			continue
		}
		if _, err := os.ReadFile(pair.path); err != nil {
			errorList = append(errorList, fmt.Sprintf("%s.%s: cannot read file %q: %v", field, pair.name, pair.path, err))
		}
	}
	if cfg.RefreshInterval > 0 && cfg.RefreshInterval < time.Second {
		errorList = append(errorList, fmt.Sprintf("%s.refresh_interval must be >= 1s when non-zero, got %s", field, cfg.RefreshInterval))
	}
	return errorList
}

func validateEndpoint(field string, endpoint types.Endpoint) []string {
	var errorList []string
	if strings.TrimSpace(endpoint.Host) == "" {
		format := "%s.host cannot be empty or contain only whitespace"
		errorList = append(errorList, fmt.Sprintf(format, field))
	}
	if port := endpoint.Port; port < 1 || port > 65535 {
		format := "%s.port must be in range 1-65535, got %d"
		errorList = append(errorList, fmt.Sprintf(format, field, port))
	}
	return errorList
}
