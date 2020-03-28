package utils

import (
	"time"
)

type (
	// Meta description
	// a metadata is an indicator send to API
	Meta struct {
		Name      string      `json:"name"`
		Timestamp int64       `json:"timestamp"`
		Value     interface{} `json:"value"`
	}

	// Metas contains all metadata for the collector itself, the configured source and configured sink
	Metas struct {
		Agent   map[string]Meta            `json:"agent"`
		Sources map[string]map[string]Meta `json:"sources"`
		Sinks   map[string]map[string]Meta `json:"sinks"`
	}
)

// NewMeta return new meta with timestamp automatically set
func NewMeta(name string, value interface{}) Meta {
	return Meta{
		Timestamp: time.Now().Unix(),
		Name:      name,
		Value:     value,
	}
}

// NewMetas init meta struct
func NewMetas() (meta Metas) {
	meta = Metas{
		Agent:   make(map[string]Meta),
		Sources: make(map[string]map[string]Meta),
		Sinks:   make(map[string]map[string]Meta),
	}
	return
}

// SetMetaSources add a metadata to a source within the Metas struct
func (m *Metas) SetMetaSources(sourceName string, meta Meta) {
	if _, ok := m.Sources[sourceName]; !ok {
		m.Sources[sourceName] = make(map[string]Meta)
	}

	m.Sources[sourceName][meta.Name] = meta
}

// SetMetaSinks Add a metadata to a sink within the Metas struct
func (m *Metas) SetMetaSinks(sinkName string, meta Meta) {
	if _, ok := m.Sinks[sinkName]; !ok {
		m.Sinks[sinkName] = make(map[string]Meta)
	}

	m.Sinks[sinkName][meta.Name] = meta
}
