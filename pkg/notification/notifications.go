package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// StatesAndNotificationsMapping represents all possible states and corresponding notification
// Format >>  {"BackupStatus" : {"MongoStatus": "NotificationToBeSent"}NotReachable
// Note: Only state transition that is possible after backup is "Available" is "NotReachable"
var StatesAndNotificationsMapping = map[string]map[string]string{
	"Available": {
		"Available":    "Success",
		"NotReachable": "NotReachable",
		"Pending":      "Provisioning",
		"Failed":       "Failed",
		"NotFound":     "NotReachable",
	},
	"NotReachable": { // does not matter status of mongo, if backup is in Unreachable notification will be NotReachable
		"Available":    "NotReachable",
		"NotReachable": "NotReachable",
		"Pending":      "NotReachable",
		"Failed":       "NotReachable",
		"NotFound":     "NotReachable",
	},
	"Pending": { // does not matter status of mongo, if backup is in Unreachable notification will be Provisioning
		"Available":    "Provisioning",
		"NotReachable": "Provisioning",
		"Pending":      "Provisioning",
		"Failed":       "Provisioning",
		"NotFound":     "Provisioning",
	},
	"Failed": { // does not matter status of mongo, if backup is in Unreachable notification will be Failed
		"Available":    "Failed",
		"NotReachable": "Failed",
		"Pending":      "Failed",
		"Failed":       "Failed",
		"NotFound":     "Failed",
	},
	"NotFound": { // does not matter status of mongo, if backup is in Unreachable notification will be Failed
		"Available":    "Failed",
		"NotReachable": "Failed",
		"Pending":      "Provisioning",
		"Failed":       "Failed",
		"NotFound":     "Deleted",
	},
}

var BackupAndSchedulerStatusMapping = map[string]map[string]string{
	"Available": {
		"Pending":   "Provisioning",
		"Failed":    "Failed",
		"Available": "Success",
	},
}

var NOTIFICATION_NON_200_RESPONSE string = "non 200 Response from Webhook."

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
		return fmt.Errorf(NOTIFICATION_NON_200_RESPONSE+" Actual Status: %s", resp.Status)
	}
	return err
}
