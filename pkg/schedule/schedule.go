package schedule

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/portworx/px-backup-baas-notifier/pkg/types"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type TokenConfig struct {
	ClientID      string
	Password      string
	TokenDuration string
	TokenURL      string
	UserName      string
}

type SchedulerStatus struct {
	SUCCESS bool
}

type Schedule struct {
	ScheduleURL     string
	TokenConfig     TokenConfig
	ScheduleTimeout int64
	logger          logr.Logger
}

func NewSchedule(scheduleURL string, scheduleTimeout int64, config TokenConfig,
	logger logr.Logger) *Schedule {
	s := &Schedule{
		ScheduleURL:     scheduleURL,
		ScheduleTimeout: scheduleTimeout,
		TokenConfig:     config,
		logger:          logger,
	}
	return s
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func (s *Schedule) GetStatus(creationTime v1.Time, ns string) (types.Status, error) {
	isReady, err := s.isReady(ns)
	if err != nil {
		return types.FAILED, err
	}
	if !isReady {
		if time.Since(creationTime.Time) > time.Duration(s.ScheduleTimeout)*time.Minute { //TODO: make configurable backup timeout
			return types.FAILED, nil
		}
		return types.PENDING, nil
	}
	return types.AVAILABLE, nil
}

func (s *Schedule) isReady(ns string) (bool, error) {

	// Get Token
	token, err := s.getToken()
	if err != nil {
		return false, err
	}

	// Get Schedule
	tokenPrefix := "Bearer"
	request, err := http.NewRequest("GET", s.ScheduleURL, nil)
	if err != nil {
		return false, err
	}
	q := request.URL.Query()
	q.Add("name", ns)
	request.URL.RawQuery = q.Encode()
	request.Header.Set("Authorization", tokenPrefix+" "+token)

	client := &http.Client{}

	var status SchedulerStatus
	resp, err := client.Do(request)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Schedule request returned: %v", resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&status)
	if err != nil {
		return false, err
	}
	return status.SUCCESS, nil
}

func (s *Schedule) getToken() (string, error) {
	client := &http.Client{}
	tokenResponse := &TokenResponse{}
	params := url.Values{}
	params.Add("grant_type", "password")
	params.Add("client_id", s.TokenConfig.ClientID)
	params.Add("username", s.TokenConfig.UserName)
	params.Add("password", s.TokenConfig.Password)
	params.Add("token-duration", s.TokenConfig.TokenDuration)
	body := strings.NewReader(params.Encode())

	request, err := http.NewRequest("POST", s.TokenConfig.TokenURL, body)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err != nil {
		return "", fmt.Errorf("Error in token request: %v", err)
	}

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("Error in token request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Token request returned: %v", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(tokenResponse)
	return tokenResponse.AccessToken, errors.Wrap(err, "Error marshalling token response")
}
