package core

import (
	"testing"

	"io"
	"net/http"
	"net/http/httptest"
)

func TestNewAuth(t *testing.T) {
	auth := newAuth("tenant", "uuid", "secret", "host", "url")

	if auth.tenant != "tenant" {
		t.Fail()
	}
	if auth.uuid != "uuid" {
		t.Fail()
	}
	if auth.secretkey != "secret" {
		t.Fail()
	}
	if auth.hostname != "host" {
		t.Fail()
	}

	if auth.authURL != "url" {
		t.Fail()
	}
}

func TestAuth_GetToken(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `{"token":"test"}`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := newAuth("tenant", "uuid", "secret", "host", server.URL)

	token, err := auth.GetToken()
	if err != nil {
		t.Fail()
	}
	if token != "test" {
		t.Fail()
	}
}

func TestAuth_GetTokenError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `{"error":"test"}`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := newAuth("tenant", "uuid", "secret", "host", server.URL)

	token, err := auth.GetToken()

	if err == nil {
		t.Fail()
	}

	if token != "" {
		t.Fail()
	}
}

func TestAuth_GetUnmarshallError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `error`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := newAuth("tenant", "uuid", "secret", "host", server.URL)

	token, err := auth.GetToken()

	if err == nil {
		t.Fail()
	}

	if token != "" {
		t.Fail()
	}
}
