package core

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Pirionfr/lookatch-agent/sources"
	"github.com/Pirionfr/lookatch-agent/utils"

	"github.com/spf13/viper"
)

const (
	token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiJ9.eyJpbnRlcm5hbCI6dHJ1ZSwicGVybWlzc2lvbnMiOnsicGVybWlzc2lvbnNBZ2VudHMiOlsiYWdlbnQtbWFuYWdlOnIiLCJhZ2VudC1tYW5hZ2U6dyIsImFnZW50czpyIl19LCJ1c2VyIjoiMjYxNmFjZGMtYzM0MS00YTU4LTg5MDQtNzRjZmE5NTZlMzJkIiwiaWF0IjoxNTI2MjAzNDE5fQ.JLfZKl3jNSrKhfE3YZ3_usDFlBY-MQR9WFmgCVTlqCxv4rT94uGHcMEmbTPZNjnLpwZDpfQikHqvlb0iWyesXEVrNqTtIMB03GhzzJCAK7YUNkD-oKFWhUTMZ8OED6yDCSjLei4pWqsaR2t21pLzpJV287k40wOz66Oj58YSIeck0LB9ZRGw7oz9XU9ZS0uDYCFkGObs_VfYXNuCuuPVNIoF_trMTQF3Onr24F1ywyaw2VCj_ECpQEcdF0sQsZ0RVJ_wHAByl51M53PpSpl4dDOpKAANU7KKiLV-5T_wmhzkjXX8fe_CGajrQEoEsDvkQA8UaU7urUt-eUgqFitSPg"
	UUID  = "bfb08000-cae5-4bd1-9cd5-b01662c300f2"
)

var (
	vCtrl *viper.Viper
	auth  *Auth
)

func init() {
	vCtrl = viper.New()
	vCtrl.Set("controller", map[string]interface{}{
		"address": "localhost",
	})
	auth = &Auth{
		uuid:    UUID,
		authURL: token,
	}
}

func TestNewControllerClient(t *testing.T) {
	crtl := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtl == nil {
		t.Fail()
	}
}

func TestCall(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		val := r.URL.Query().Get(metaParameter)
		if val != "test" {
			t.Fail()
		}

		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	_, err := ctrl.call("GET", "/collectors/"+UUID,
		nil,
		map[string]string{metaParameter: "test"},
		[]byte{})

	if err != nil {
		t.Fail()
	}

}

func TestConfiguration(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"sinks":{"default":{"enabled":true,"type":"stdout"}},"sources":{"default":{"autostart":true,"enabled":true,"type":"random","sinks":["default"],"wait":"1s"}}}`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)
	config, _ := ctrl.GetConfiguration()
	res := make(map[string]interface{})

	if err := json.Unmarshal(config, &res); err != nil {
		t.Fail()
	}

	if _, ok := res["sinks"]; !ok {
		t.Fail()
	}

	if _, ok := res["sources"]; !ok {
		t.Fail()
	}
}

func TestSendMeta(t *testing.T) {
	meta := utils.NewMetas()

	meta.Sources["default"] = map[string]utils.Meta{
		"status": {
			Timestamp: 1562253951,
			Name:      "status",
			Value:     AgentStatusOnline,
		},
		"offset": {
			Timestamp: 1562253951,
			Name:      "offset",
			Value:     "test",
		},
	}

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		returnBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fail()
		}
		res := utils.Metas{}
		if err := json.Unmarshal(returnBody, &res); err != nil {
			t.Fail()
		}

		if !reflect.DeepEqual(meta, res) {
			t.Fail()
		}

		w.Header().Set("X-DCC-TASKS", "2")
		w.WriteHeader(http.StatusNoContent)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	err := ctrl.SendMeta(meta)

	if err != nil {
		t.Fail()
	}

	if ctrl.PendingTask != 2 {
		t.Fail()
	}

}

func TestGetMeta(t *testing.T) {

	metas := utils.NewMetas()

	metas.SetMetaSources("default", utils.NewMeta("offset", "testvalue"))

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		body, _ := json.Marshal(metas)
		io.WriteString(w, string(body))
	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	result, _ := ctrl.GetMeta("offset")

	if !reflect.DeepEqual(result, metas) {
		t.Fail()
	}

}

func TestSendCapabilities(t *testing.T) {

	paramDesc := utils.ParametersDescription{
		Description: "parameter",
		Name:        "aParam",
		Type:        "string",
		Required:    false,
	}

	capabilities := make(map[string]*utils.TaskDescription)
	capabilities["aTask"] = &utils.TaskDescription{
		Description: "test task",
		Parameters: []*utils.ParametersDescription{
			&paramDesc,
		},
	}

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		returnBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fail()
		}
		res := make(map[string]*utils.TaskDescription)
		if err := json.Unmarshal(returnBody, &res); err != nil {
			t.Fail()
		}

		if ok := reflect.DeepEqual(res, capabilities); !ok {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusNoContent)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	err := ctrl.SendCapabilities(capabilities)

	if err != nil {
		t.Fail()
	}

}

func TestSendSourcesCapabilities(t *testing.T) {

	paramDesc := utils.ParametersDescription{
		Description: "parameter",
		Name:        "aParam",
		Type:        "string",
		Required:    false,
	}

	capabilities := make(map[string]*utils.TaskDescription)
	capabilities["aTask"] = &utils.TaskDescription{
		Description: "test task",
		Parameters: []*utils.ParametersDescription{
			&paramDesc,
		},
	}

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		returnBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fail()
		}
		res := make(map[string]*utils.TaskDescription)
		if err := json.Unmarshal(returnBody, &res); err != nil {
			t.Fail()
		}

		if ok := reflect.DeepEqual(res, capabilities); !ok {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusNoContent)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	if ctrl.SendSourcesCapabilities("default", capabilities) != nil {
		t.Fail()
	}

}

func TestGetOneTasks(t *testing.T) {

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		io.WriteString(w, `[{"end_date":null,"taskType":"QuerySource","created_at":1548686030754,"description":null,"id":"a0bebf3b-2db1-46d2-9af2-5991a47dca45","params":{"Query":"SELECT * FROM truc"},"steps":[],"start_date":null,"status":"TODO","target":"source:9f1c0563-2219-4297-be8a-ca75dfb4e682"}]`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	result, err := ctrl.GetTasks(1)
	if err != nil {
		t.Fail()
	}

	if result[0].Parameters["Query"] != "SELECT * FROM truc" {
		t.Fail()
	}

}

func TestGetTasks(t *testing.T) {

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		io.WriteString(w, `[{"end_date":null,"taskType":"QuerySource","created_at":1548686030754,"description":null,"id":"a0bebf3b-2db1-46d2-9af2-5991a47dca45","params":{"Query":"SELECT * FROM truc"},"steps":[],"start_date":null,"status":"TODO","target":"source:9f1c0563-2219-4297-be8a-ca75dfb4e682"}]`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	result, err := ctrl.GetTasks(-1)
	if err != nil {
		t.Fail()
	}

	if result[0].Parameters["Query"] != "SELECT * FROM truc" {
		t.Fail()
	}

}

func TestUpdateTasks(t *testing.T) {

	task := utils.Task{
		ID:      "a0bebf3b-2db1-46d2-9af2-5991a47dca45",
		EndDate: 0,
		Parameters: map[string]interface{}{
			"Query": "SELECT * FROM truc",
		},
		TaskType:  "QuerySource",
		Target:    "source:default",
		StartDate: 0,
		Status:    "TODO",
		CreatedAt: 1562253951,
	}

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		returnBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fail()
		}
		res := utils.Task{}
		if err := json.Unmarshal(returnBody, &res); err != nil {
			t.Fail()
		}

		if ok := reflect.DeepEqual(res, task); !ok {
			t.Fail()
		}

		w.WriteHeader(http.StatusNoContent)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	err := ctrl.UpdateTasks(task)
	if err != nil {
		t.Fail()
	}

}

func TestSendSchema(t *testing.T) {
	schema := map[string]map[string]*sources.Column{
		"ramdom": {
			"line": &sources.Column{
				Column:       "line",
				ColumnOrdPos: 0,
				Nullable:     true,
				DataType:     "string",
				ColumnType:   "string",
			},
		},
	}

	handler := func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get(authHeader) != "Bearer "+token {
			t.Fail()
		}
		if !strings.Contains(r.URL.EscapedPath(), UUID) {
			t.Fail()
		}

		returnBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fail()
		}

		if r.Method == http.MethodPut {
			res := Schema{}
			if err := json.Unmarshal(returnBody, &res); err != nil {
				t.Fail()
			}

			if res.Key != "ramdom" {
				t.Fail()
			}

			if !res.Values["line"].Nullable {
				t.Fail()
			}
		}

		w.WriteHeader(http.StatusNoContent)

	}

	server := httptest.NewServer(http.HandlerFunc(handler))

	vCtrl.Set("controller.base_url", server.URL)
	auth.token = token
	ctrl := NewControllerClient(vCtrl.Sub("controller"), auth)

	err := ctrl.SendSchema("default", schema)

	if err != nil {
		t.Fail()
	}

}
