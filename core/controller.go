package core

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/remeh/sizedwaitgroup"

	"github.com/Pirionfr/lookatch-agent/sources"
	"github.com/Pirionfr/lookatch-agent/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	agentIDParamPath       = "{agentId}"
	sourceIDParamPath      = "{sourceId}"
	taskIDParamPath        = "{taskId}"
	configurationPath      = "/collectors/" + agentIDParamPath + "/controller/configuration"
	metaPath               = "/collectors/" + agentIDParamPath + "/controller/meta"
	capabilitiesPath       = "/collectors/" + agentIDParamPath + "/controller/capabilities"
	sourceCapabilitiesPath = "/collectors/" + agentIDParamPath + "/controller/sources/" + sourceIDParamPath + "/capabilities"
	taskPath               = "/collectors/" + agentIDParamPath + "/controller/tasks/" + taskIDParamPath
	tasksPath              = "/collectors/" + agentIDParamPath + "/controller/tasks"
	schemaPath             = "/collectors/" + agentIDParamPath + "/controller/sources/" + sourceIDParamPath + "/schema"
	metaParameter          = "name"
	authHeader             = "Authorization"
	DefaultTimeOut         = 60
)

type (
	// ControllerConfig representation of controller config
	ControllerConfig struct {
		BaseURL      string `mapstructure:"base_url" json:"base_url"`
		PollerTicker string `mapstructure:"poller_ticker" json:"poller_ticker"`
		Worker       int    `json:"worker"`
	}

	// Controller allow the collector to be controlled by API
	Controller struct {
		conf        *ControllerConfig
		auth        *Auth
		PendingTask int
	}

	// Schema use to send schema to the API
	Schema struct {
		Key    string                     `json:"key"`
		Values map[string]*sources.Column `json:"values"`
	}
)

// NewControllerClient create new controller from configuration
// return an initialized controller
func NewControllerClient(conf *viper.Viper, auth *Auth) *Controller {
	ctrlConf := &ControllerConfig{}
	err := conf.Unmarshal(ctrlConf)
	if err != nil {
		log.WithError(err).Error("Unmarshal err")
		return nil
	}
	if ctrlConf.Worker < 1 {
		ctrlConf.Worker = 1
	}
	ctrl := &Controller{
		conf: ctrlConf,
		auth: auth,
	}

	return ctrl
}

// GetConfiguration get Configuration from server
// return sources and sinks
func (c *Controller) GetConfiguration() (config []byte, err error) {
	config, err = c.call(http.MethodGet, configurationPath, nil, nil, nil)
	if err != nil {
		err = errors.Annotate(err, "error while getting configuration")
	}
	return
}

// SendMeta send meta to the API
func (c *Controller) SendMeta(meta utils.Metas) (err error) {
	body, _ := json.Marshal(meta)
	_, err = c.call(http.MethodPost, metaPath, nil, nil, body)
	if err != nil {
		err = errors.Annotate(err, "error while sending metadata")
	}
	return
}

// SendSchema send the schema to the API
func (c *Controller) SendSchema(sourceName string, schema map[string]map[string]*sources.Column) (err error) {
	path := strings.Replace(schemaPath, sourceIDParamPath, sourceName, 1)
	_, err = c.call(http.MethodDelete, path, nil, nil, nil)
	if err != nil {
		if !strings.Contains(err.Error(), "source.schema.doesNotExist") {
			return errors.Annotate(err, "error while deleting old schema")
		}
		err = nil
	}

	wg := sizedwaitgroup.New(c.conf.Worker)

	for k, v := range schema {
		wg.Add()
		schemaBody := Schema{
			Key:    k,
			Values: v,
		}
		go func(schemaBody Schema) {
			body, _ := json.Marshal(schemaBody)
			_, errSend := c.call(http.MethodPut, path, nil, nil, body)
			if errSend != nil {
				log.WithError(errSend).WithField("key", schemaBody.Key).Error("error while sending schema")
			}
			wg.Done()
		}(schemaBody)

	}
	wg.Wait()
	return
}

// GetMeta get metadata by name from API
// get meta from name if metaName is empty get all metas
// return meta as Metas object
func (c *Controller) GetMeta(metaName string) (meta utils.Metas, err error) {
	var params map[string]string
	if metaName != "" {
		params = map[string]string{
			metaParameter: metaName,
		}
	}

	result, err := c.call(http.MethodGet, metaPath, nil, params, nil)
	if err != nil {
		return
	}

	if err = json.Unmarshal(result, &meta); err != nil {
		err = errors.Annotate(err, "error while getting schema")
		return
	}

	return
}

// SendCapabilities send the currently supported capabilities of this collector to the API
func (c *Controller) SendCapabilities(tasks map[string]*utils.TaskDescription) (err error) {
	body, _ := json.Marshal(tasks)
	_, err = c.call(http.MethodPost, capabilitiesPath, nil, nil, body)
	if err != nil {
		err = errors.Annotate(err, "error while sending capabilities")
	}
	return
}

// SendSourcesCapabilities send the currently supported capabilities of the configured source to the API
func (c *Controller) SendSourcesCapabilities(sourceName string, tasks map[string]*utils.TaskDescription) (err error) {
	body, _ := json.Marshal(tasks)

	path := strings.Replace(sourceCapabilitiesPath, sourceIDParamPath, sourceName, 1)
	_, err = c.call(http.MethodPost, path, nil, nil, body)
	if err != nil {
		err = errors.Annotate(err, "error while sending sources capabilities")
	}
	return
}

// GetTasks get the list of tasks the collector needs to execute from the API
// if limit is set to -1 return all task else return limit set
func (c *Controller) GetTasks(limit int) (task []utils.Task, err error) {
	params := map[string]string{
		"orderbydate": "asc",
	}

	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}

	body, err := c.call(http.MethodGet, tasksPath, nil, params, nil)
	if err != nil {
		err = errors.Annotate(err, "error while getting tasks")
		return nil, err
	}
	task = []utils.Task{}
	err = json.Unmarshal(body, &task)

	return
}

// UpdateTasks update the status (and results if needed) of a task that has been executed by sending it to the API
func (c *Controller) UpdateTasks(task utils.Task) (err error) {
	path := strings.Replace(taskPath, taskIDParamPath, task.ID, 1)
	body, err := json.Marshal(task)
	if err != nil {
		return
	}

	_, err = c.call(http.MethodPatch, path, nil, nil, body)
	if err != nil {
		err = errors.Annotate(err, "error while updating tasks")
	}
	return
}

// call function used to actually call the API
// handle token expiration
func (c *Controller) call(method string, path string, headers map[string]string, parameters map[string]string, body []byte) (returnBody []byte, err error) {
	returnBody, err = c.callAPI(method, path, headers, parameters, body)
	if errors.IsUnauthorized(err) {
		c.auth.token = ""
		returnBody, err = c.callAPI(method, path, headers, parameters, body)
	}
	return returnBody, err
}

// callApi function used to actually call the API
// return an error if HTTP status code (299>=) is not Successful
// check if header response return pending task pending.
// Task header is used to know if API have task for collector
func (c *Controller) callAPI(method string, path string, headers map[string]string, parameters map[string]string, body []byte) (returnBody []byte, err error) {
	client := &http.Client{
		Timeout: time.Second * DefaultTimeOut,
	}
	path = strings.Replace(path, agentIDParamPath, c.auth.uuid, 1)
	req, err := http.NewRequest(method, c.conf.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return
	}
	log.WithFields(
		log.Fields{
			"method": method,
			"path":   path,
		}).Debug("call")

	req.Header.Set("Accept", "application/json")
	req.Header.Set(authHeader, "Bearer "+c.auth.GetToken())

	// add header
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	//add parameters
	if parameters != nil {
		q := req.URL.Query()
		for key, value := range parameters {
			q.Set(key, value)
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	//check auth and reconnect if needed
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, errors.NewUnauthorized(err, "")
	}

	if resp.StatusCode >= 299 {
		err = errors.New(resp.Status)
		if resp.Body != nil {
			errorBody, _ := ioutil.ReadAll(resp.Body)
			err = errors.Annotate(err, string(errorBody))
		}
		return
	}

	//check if header response return pending task number
	pendingTask := resp.Header.Get("X-DCC-TASKS")
	if pendingTask != "" {
		c.PendingTask, err = strconv.Atoi(pendingTask)
		if err != nil {
			return nil, err
		}
	}

	//read body
	returnBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return returnBody, nil
}
