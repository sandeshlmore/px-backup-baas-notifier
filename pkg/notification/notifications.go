package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// StatesAndNotificationsMapping represents all possible states and corresponding notification
// Format >>  {"BackupStatus" : {"MongoStatus": "NotificationToBeSent"}UnReachable
// Note: Only state transition that is possible after backup is "Available" is "UnReachable"
var StatesAndNotificationsMapping = map[string]map[string]string{
	"Available": {
		"Available":   "Success",
		"UnReachable": "UnReachable",
		"Pending":     "Provisioning",
		"Failed":      "Failed",
		"NotFound":    "UnReachable",
	},
	"UnReachable": { // does not matter status of mongo, if backup is in Unreachable notification will be UnReachable
		"Available":   "UnReachable",
		"UnReachable": "UnReachable",
		"Pending":     "UnReachable",
		"Failed":      "UnReachable",
		"NotFound":    "Unreachable",
	},
	"Pending": { // does not matter status of mongo, if backup is in Unreachable notification will be Provisioning
		"Available":   "Provisioning",
		"UnReachable": "Provisioning",
		"Pending":     "Provisioning",
		"Failed":      "Provisioning",
		"NotFound":    "Provisioning",
	},
	"Failed": { // does not matter status of mongo, if backup is in Unreachable notification will be Failed
		"Available":   "Failed",
		"UnReachable": "Failed",
		"Pending":     "Failed",
		"Failed":      "Failed",
		"NotFound":    "Failed",
	},
	"NotFound": { // does not matter status of mongo, if backup is in Unreachable notification will be Failed
		"Available":   "Deleted",
		"UnReachable": "Deleted",
		"Pending":     "Deleted",
		"Failed":      "Deleted",
		"NotFound":    "Deleted",
	},
}

type Note struct {
	State          string `json:"state"`
	FailureMessage string `json:"failure_message"`
	Namespace      string `json:"namespace"`
}

type Client struct {
	WebhookURL string
}

func (n *Client) Send(note Note) error {
	data, err := json.Marshal(note)
	if err != nil {
		return err
	}
	request, err := http.NewRequest("POST", n.WebhookURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("non 200 Response from Webhook. Actual Status: %s", resp.Status)
	}
	return err
}
