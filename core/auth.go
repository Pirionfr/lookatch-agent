package core

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// Auth representation of auth
type Auth struct {
	uuid     string
	password string
	authURL  string
	client   *http.Client
	token    string
}

// AuthPath path of the authentication endpoint
const AuthPath = "/auth/token"
const HeaderDcc = "X-Dcc-Auth"

// WaitAuth number of second to wait if authentication failed
var WaitAuth = time.Second * 5

// NewAuth creates a new Auth using the given collector uuid, password and remote base url
func NewAuth(uuid string, password string, baseURL string) *Auth {
	u, err := url.Parse(baseURL + AuthPath)
	if err != nil {
		log.WithError(err).Error("parsing url")
		return nil
	}

	return &Auth{
		uuid:     uuid,
		password: password,
		authURL:  u.String(),
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}
}

// GetToken get token from server
func (a *Auth) authenticate() (err error) {
	req, err := http.NewRequest(http.MethodPost, a.authURL, nil)
	if err != nil {
		return
	}

	//set specific header for auth
	req.Header.Set(HeaderDcc, "1")
	req.SetBasicAuth(a.uuid, a.password)

	resp, err := a.client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.Annotate(err, "Unable to read Body")
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.New(resp.Status)
		return
	}

	log.WithField("url", a.authURL).Println("Connected")

	a.token = strings.Replace(string(body), "\"", "", -1)

	return nil
}

// GetToken return the current token if it exists, get a new one otherwise
func (a *Auth) GetToken() string {
	if a.token == "" {
		err := a.authenticate()
		for err != nil {
			log.WithError(err).Error("Error while authenticating. Waiting " + WaitAuth.String() + " before retrying...")
			time.Sleep(WaitAuth)
			err = a.authenticate()
		}
	}

	return a.token
}
