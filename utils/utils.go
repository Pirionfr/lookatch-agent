package utils

import (
	"encoding/hex"

	"github.com/spf13/viper"
)

// IsStandalone check if collector is in standalone mode
func IsStandalone(v *viper.Viper) bool {
	test := v.Get("controller")
	return test == nil
}

// EscapeCtrl escape ctrl character
func EscapeCtrl(ctrl []byte) (esc []byte) {
	u := []byte(`\u0000`)
	for i, ch := range ctrl {
		if ch <= 31 {
			if esc == nil {
				esc = append(make([]byte, 0, len(ctrl)+len(u)), ctrl[:i]...)
			}
			esc = append(esc, u...)
			hex.Encode(esc[len(esc)-2:], ctrl[i:i+1])
			continue
		}
		if esc != nil {
			esc = append(esc, ch)
		}
	}
	if esc == nil {
		return ctrl
	}
	return esc
}

// FindInSlice check is val is in slice
func InSlice(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func IsNumDot(s string) bool {
	for _, v := range s {
		if v == '.' {
			return true
		} else if v < '0' || v > '9' {
			return false
		}
	}
	return false
}
