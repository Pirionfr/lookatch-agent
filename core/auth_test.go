package core

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewAuth(t *testing.T) {
	auth := NewAuth("uuid", "", "url")

	if auth.uuid != "uuid" {
		t.Fail()
	}

	if auth.authURL != "url"+AuthPath {
		t.Fail()
	}
}

func TestNewError(t *testing.T) {
	auth := NewAuth("uuid", "", "url%%2")

	if auth != nil {
		t.Fail()
	}
}

func TestNewAuthPwd(t *testing.T) {
	auth := NewAuth("uuid", "password", "url")

	if auth.uuid != "uuid" {
		t.Fail()
	}
	if auth.password != "password" {
		t.Fail()
	}

	if auth.authURL != "url"+AuthPath {
		t.Fail()
	}
}

func TestAuthAuthenticate(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HeaderDcc) != "1" {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `test`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := NewAuth("uuid", "", server.URL)

	err := auth.authenticate()
	if err != nil {
		t.Fail()
	}
	if auth.token != "test" {
		t.Fail()
	}
}

func TestAuthAuthenticatePwd(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `test`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := NewAuth("uuid", "password", server.URL)

	err := auth.authenticate()
	if err != nil {
		t.Fail()
	}
	if auth.token != "test" {
		t.Fail()
	}
}

func TestAuthAuthenticateError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":"test"}`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := NewAuth("uuid", "", server.URL)

	err := auth.authenticate()

	if err == nil {
		t.Fail()
	}

	if auth.token != "" {
		t.Fail()
	}
}

func TestAuthAuthenticateBodyError(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := NewAuth("uuid", "", server.URL)

	auth.GetToken()

	if auth.GetToken() != "" {
		t.Fail()
	}

}

func TestGetTokenFirstError(t *testing.T) {
	nbcall := 0

	handler := func(w http.ResponseWriter, r *http.Request) {

		if nbcall == 0 {
			w.WriteHeader(http.StatusUnauthorized)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("token"))
		}
		nbcall++
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth := NewAuth("uuid", "", server.URL)

	WaitAuth = time.Millisecond

	auth.GetToken()

	if auth.GetToken() != "token" {
		t.Fail()
	}

}
