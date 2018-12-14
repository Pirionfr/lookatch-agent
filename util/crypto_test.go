package util

import (
	"testing"
)

func TestEnc(t *testing.T) {
	data := []struct {
		input string
		key   string
	}{
		{"Foo", "Boo"},
		{"Bar", "Car"},
		{"Bar", ""},
		{"", "Car"},
		{"Long input with more than 16 characters", "Car"},
	}
	for _, d := range data {

		enc, err := EncryptString(d.input, d.key)
		if err != nil {
			t.Errorf("Unable to encrypt '%v' with key '%v': %v", d.input, d.key, err)
			continue
		}
		dec, err := DecryptString(enc, d.key)
		if err != nil {
			t.Errorf("Unable to decrypt '%v' with key '%v': %v", enc, d.key, err)
			continue
		}
		if dec != d.input {
			t.Errorf("Decrypt Key %v\n  Input: %v\n  Expect: %v\n  Actual: %v", d.key, enc, d.input, enc)
		}
	}
}

func BenchmarkEncryptString(b *testing.B) {
	for n := 0; n < b.N; n++ {
		EncryptString("The secular cooling that must someday overtake our planet has already gone far indeed with our neighbour.", "testcharacters")
	}
}

func BenchmarkDecryptString(b *testing.B) {
	for n := 0; n < b.N; n++ {
		DecryptString("The secular cooling that must someday overtake our planet has already gone far indeed with our neighbour.", "testcharacters")
	}
}
