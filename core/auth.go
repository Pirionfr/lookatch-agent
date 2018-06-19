package core

import (
	"crypto/tls"
	"encoding/json"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Auth struct {
	tenant    string
	uuid      string
	secretkey string
	hostname  string
	authUrl   string
	client    *http.Client
}

func newAuth(tenant string, uuid string, secretkey string, hostname string, authUrl string) *Auth {

	u, err := url.Parse(authUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Debug("parsing url")
		return nil
	}

	ht := &http.Transport{
		TLSClientConfig: &tls.Config{ServerName: u.Host},
	}

	return &Auth{
		tenant:    tenant,
		uuid:      uuid,
		secretkey: secretkey,
		hostname:  hostname,
		authUrl:   authUrl,
		client: &http.Client{
			Transport: ht,
		},
	}

}

func (a *Auth) GetToken() (token string, err error) {

	req, err := http.NewRequest("GET", a.authUrl, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Debug("get Token")
		return token, err
	}

	req.Header.Add("X-OVH-TENANT", a.tenant)
	req.Header.Add("X-OVH-UUID", a.uuid)
	req.Header.Add("X-OVH-KEY", a.secretkey)
	req.Header.Add("X-OVH-HOST", a.hostname)

	resp, err := a.client.Do(req)
	if err != nil {
		return token, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return token, errors.Annotate(err, "Unable to read Body")
	}

	var auth map[string]string
	if err = json.Unmarshal(body, &auth); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"body":  string(body),
		}).Error("unmarshal error")
		return token, errors.Annotate(err, "error unmarshaling response body")
	}

	if auth["error"] != "" {
		return "", errors.New(auth["error"])
	}
	token = auth["token"]
	return
}
