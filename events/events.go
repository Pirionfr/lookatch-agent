package events

type (
	// Offset events format
	Offset struct {
		Source string `json:"source"`
		Agent  string `json:"agent"`
	}

	// ColumnsMeta metadata of column
	ColumnsMeta struct {
		Type     string `json:"type"`
		Position int    `json:"position"`
	}

	// SQLEvent events format
	SQLEvent struct {
		Tenant       string                 `json:"tenant,omitempty"`
		Environment  string                 `json:"environment"`
		Timestamp    string                 `json:"timestamp"`
		Database     string                 `json:"database"`
		Schema       string                 `json:"schema"`
		Table        string                 `json:"table"`
		Method       string                 `json:"method"`
		PrimaryKey   string                 `json:"primary_key,omitempty"`
		Offset       *Offset                `json:"offset,omitempty"`
		ColumnsMeta  map[string]ColumnsMeta `json:"columns_meta,omitempty"`
		Statement    map[string]interface{} `json:"statement"`
		OldStatement map[string]interface{} `json:"old_statement,omitempty"`
	}

	// GenericEvent events format
	GenericEvent struct {
		Environment string      `json:"environment"`
		Timestamp   string      `json:"timestamp"`
		Offset      *Offset     `json:"offset,omitempty"`
		Value       interface{} `json:"value"`
	}

	// LookatchTenantInfo tenant info header
	LookatchTenantInfo struct {
		ID  string `json:"id"`
		Env string `json:"env"`
	}

	// LookatchHeader header format
	LookatchHeader struct {
		EventType string
		Tenant    LookatchTenantInfo
	}

	// LookatchEvent wire message format
	LookatchEvent struct {
		Header  LookatchHeader
		Payload interface{}
	}

	// ErrorEvent error format
	ErrorEvent struct {
		Timestamp int64
		Level     int
		Host      string
		ShortMsg  string
		FullMsg   string
	}
)
