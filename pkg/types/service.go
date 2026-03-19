package types

import (
	"fmt"
	"time"
)

const (
	DefaultNamespace = "default"
	DefaultRegion    = "local"
	DefaultZone      = "default"
	DefaultSubZone   = "primary"
)

type Locality struct {
	Region  string
	Zone    string
	SubZone string
}

func (l Locality) String() string {
	return fmt.Sprintf(
		"%s/%s/%s", l.Region,
		l.Zone, l.SubZone,
	)
}

type ServiceMetadata map[string]string

func (m ServiceMetadata) GetLocality() Locality {
	region := DefaultRegion
	if r, ok := m["Region"]; ok {
		region = r
	}
	zone := DefaultZone
	if z, ok := m["Zone"]; ok {
		zone = z
	}
	subzone := DefaultSubZone
	if sz, ok := m["SubZone"]; ok {
		subzone = sz
	}
	return Locality{
		Region:  region,
		Zone:    zone,
		SubZone: subzone,
	}
}

func (m ServiceMetadata) GetNamespace() string {
	if name, ok := m["Namespace"]; ok {
		return name
	}
	return DefaultNamespace
}

type Service struct {
	ID           string
	Name         string
	Version      string
	Address      Endpoint
	HealthStatus HealthStatus
	HealthCheck  *HealthConfig
	Enabled      bool
	Metadata     ServiceMetadata
	LastChecked  *time.Time
	RegisteredAt time.Time
}
