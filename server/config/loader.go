package config

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/go-viper/mapstructure/v2"
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

func normalizeKey(s string) string {
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, ".", "")
	return strings.ToLower(s)
}

func Load(path string) (ServerConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("LOCATOR")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")

	if err := v.ReadInConfig(); err != nil {
		return ServerConfig{}, fmt.Errorf("read config file: %w", err)
	}

	var cfg ServerConfig
	decoderConfig := &mapstructure.DecoderConfig{
		Result:           &cfg,
		WeaklyTypedInput: true,
		Squash:           true,
		MatchName: func(mapKey, fieldName string) bool {
			return normalizeKey(mapKey) == normalizeKey(fieldName)
		},
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.TextUnmarshallerHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
		),
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("create decoder: %w", err)
	}
	if err := decoder.Decode(v.AllSettings()); err != nil {
		return ServerConfig{}, fmt.Errorf("decode config: %w", err)
	}

	errorList := validateServerConfig(cfg)
	if len(errorList) > 0 {
		dotMap := make(map[string]any)
		flattenValue("", reflect.ValueOf(cfg), dotMap)
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

	return cfg, nil
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
