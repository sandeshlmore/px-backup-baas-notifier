package envvars

import (
	"net/url"
	"os"
	"strconv"

	"github.com/portworx/px-backup-baas-notifier/pkg/log"
)

var (
	ClientID          string
	UserName          string
	Password          string
	TokenDuration     string
	TokenURL          string
	SchedulerUrl      string
	RetryDelaySeconds int
	WebhookURL        string
	NamespaceLabel    string
)

func init() {
	loadEnv()
}

var Logger = log.Logger

func loadEnv() {

	var err error
	if ClientID = os.Getenv("CLIENT_ID"); ClientID == "" {
		Logger.Fatal("CLIENT_ID should not be empty")
	}

	if UserName = os.Getenv("USERNAME"); UserName == "" {
		Logger.Fatal("USERNAME should not be empty")
	}

	if Password = os.Getenv("PASSWORD"); Password == "" {
		Logger.Fatal("PASSWORD should not be empty")
	}

	if TokenDuration = os.Getenv("TOKEN_DURATION"); TokenDuration == "" {
		TokenDuration = "365d"
	}

	if TokenURL = os.Getenv("TOKEN_URL"); !isUrl(TokenURL) {
		Logger.Fatal("Invalid TOKEN_URL configured as env")
	}

	if SchedulerUrl = os.Getenv("SCHEDULE_URL"); !isUrl(SchedulerUrl) {
		Logger.Fatal("Invalid SCHEDULE_URL configured as env")
	}

	if WebhookURL = os.Getenv("WEBHOOK_URL"); !isUrl(WebhookURL) {
		Logger.Fatal("Invalid WEBHOOK_URL configured as env")
	}

	if temp := os.Getenv("RETRY_DELAY"); temp == "" {
		RetryDelaySeconds = 8
	} else {
		if RetryDelaySeconds, err = strconv.Atoi(temp); err != nil {
			Logger.Fatal("Failed to Parse RETRY_DELAY env")
		}
	}

	NamespaceLabel = "" //os.Getenv("nsLabel")
	// if nsLabel == "" {
	// 	Logger.Error(errors.New("nsLabel env not found"), "Namespace Identifier label 'nsLabel' must be set as env")
	// 	os.Exit(1)
	// } else {
	// 	if res := strings.Split(nsLabel, ":"); len(res) != 2 {
	// 		Logger.Error(errors.New("Invalid env 'nsLabel'"), "Namespace Identifier label 'nsLabel' must be in `key:val` format ")
	// 		os.Exit(1)
	// 	}
	// }

}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
