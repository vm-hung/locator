package types

import "fmt"

type HealthStatus int32

const (
	HealthStatus_UNSPECIFIED HealthStatus = 0
	HealthStatus_HEALTHY     HealthStatus = 1
	HealthStatus_UNHEALTHY   HealthStatus = 2
)

var (
	HealthStatus_name = map[int32]string{
		0: "UNSPECIFIED",
		1: "HEALTHY",
		2: "UNHEALTHY",
	}
	HealthStatus_value = map[string]int32{
		"UNSPECIFIED": 0,
		"HEALTHY":     1,
		"UNHEALTHY":   2,
	}
)

func (s HealthStatus) String() string {
	if name, ok := HealthStatus_name[int32(s)]; ok {
		return name
	}
	return HealthStatus_name[0]
}

func (s HealthStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *HealthStatus) UnmarshalText(data []byte) error {
	val, ok := HealthStatus_value[string(data)]
	if !ok {
		return fmt.Errorf("invalid health status: %s", data)
	}
	*s = HealthStatus(val)
	return nil
}

type HealthConfig struct {
	Service string
	Address Endpoint
}
