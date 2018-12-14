package events

type (
	// Offset events format
	Offset struct {
		Database string `json:"database"`
		Agent    string `json:"agent"`
	}

	// SQLEvent events format
	SQLEvent struct {
		Tenant       string  `json:"tenant,omitempty"`
		Environment  string  `json:"environment"`
		Timestamp    string  `json:"timestamp"`
		Database     string  `json:"database"`
		Schema       string  `json:"schema"`
		Table        string  `json:"table"`
		Method       string  `json:"method"`
		PrimaryKey   string  `json:"primaryKey,omitempty"`
		Offset       *Offset `json:"offset,omitempty"`
		Statement    string  `json:"statement"`
		StatementOld string  `json:"statementOld,omitempty"`
	}

	// GenericEvent events format
	GenericEvent struct {
		Tenant      string      `json:"tenant"`
		Environment string      `json:"environment"`
		Timestamp   string      `json:"timestamp"`
		AgentID     string      `json:"agentID"`
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
		Header  *LookatchHeader
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
