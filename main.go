package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	"github.com/portworx/px-backup-baas-notifier/pkg/schedule"
	"go.uber.org/zap/zapcore"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	Logger              logr.Logger
	nsLabel             string
	retryDelaySeconds   int
	retryMaxBackoff     int
	retryDefaultBackoff int
)

const (
	ScheduleTimeout = 20
)

func init() {
	var err error

	opts := zap.Options{
		Development: true,
		TimeEncoder: zapcore.TimeEncoderOfLayout(time.RFC3339),
	}
	Logger = zap.New(zap.UseFlagOptions(&opts))

	nsLabel = "" //os.Getenv("nsLabel")
	// if nsLabel == "" {
	// 	Logger.Error(errors.New("nsLabel env not found"), "Namespace Identifier label 'nsLabel' must be set as env")
	// 	os.Exit(1)
	// } else {
	// 	if res := strings.Split(nsLabel, ":"); len(res) != 2 {
	// 		Logger.Error(errors.New("Invalid env 'nsLabel'"), "Namespace Identifier label 'nsLabel' must be in `key:val` format ")
	// 		os.Exit(1)
	// 	}
	// }

	t := os.Getenv("RETRY_DELAY")
	if t == "" {
		retryDelaySeconds = 8
	} else {
		retryDelaySeconds, err = strconv.Atoi(t)
		if err != nil {
			log.Fatalf("Failed to Parse retryDelaySeconds env")
		}
	}
	t = os.Getenv("RETRY_MAX_BACKOFF")
	if t == "" {
		retryMaxBackoff = 10
	} else {
		retryMaxBackoff, err = strconv.Atoi(t)
		if err != nil {
			log.Fatalf("Failed to Parse RETRY_MAX_BACKOFF env")
		}
	}
	t = os.Getenv("DEFAULT_RETRY_BACKOFF")
	if t == "" {
		retryDefaultBackoff = 2
	} else {
		retryDefaultBackoff, err = strconv.Atoi(t)
		if err != nil {
			log.Fatalf("Failed to Parse DEFAULT_RETRY_BACKOFF env")
		}
	}
}

func main() {

	config, err := kubeconfig()
	if err != nil {
		Logger.Error(err, "Could not load config from kubeconfig")
		os.Exit(1)
	}

	schedule := createSchdeule(Logger)

	webhookURL := os.Getenv("WEBHOOK_URL")
	if !isUrl(webhookURL) {
		Logger.Info("Invalid webhook url configured", "webhookUrl", webhookURL)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		Logger.Error(err, "error creating clientset")
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		Logger.Error(err, "Error getting dyn client")
		os.Exit(1)
	}

	infFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0*time.Minute)

	stopch := make(<-chan struct{})
	notifyClient := notification.Client{WebhookURL: webhookURL}

	c := newController(clientset, dynClient, infFactory, stopch, notifyClient, *schedule)

	c.run(stopch)

}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func createSchdeule(logger logr.Logger) *schedule.Schedule {
	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		log.Fatal("ClientID should not be empty")
	}

	userName := os.Getenv("USERNAME")
	if userName == "" {
		log.Fatal("UserName should not be empty")
	}

	password := os.Getenv("PASSWORD")
	if password == "" {
		log.Fatal("Password should not be empty")
	}

	tokenDuration := os.Getenv("TOKEN_DURATION")
	if tokenDuration == "" {
		tokenDuration = "365d"
	}

	tokenURL := os.Getenv("TOKEN_URL")
	if tokenURL == "" {
		log.Fatal("TokenURL should not be empty")
	}

	schedulerUrl := os.Getenv("SCHEDULE_URL")
	if schedulerUrl == "" {
		log.Fatal("SchedulerUrl should not be empty")
	}
	schdeule := schedule.NewSchedule(schedulerUrl, int64(ScheduleTimeout), schedule.TokenConfig{
		ClientID:      clientID,
		UserName:      userName,
		Password:      password,
		TokenDuration: tokenDuration,
		TokenURL:      tokenURL,
	}, logger)

	return schdeule
}

func kubeconfig() (*rest.Config, error) {
	var kubeconfigfile string = os.Getenv("kubeconfigfile")
	if kubeconfigfile == "" {
		kubeconfigfile = os.Getenv("HOME") + "/.kube/config"
	}

	kubeconfig := flag.String("kubeconfig", kubeconfigfile, "absolute path to the kubeconfig file")
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		Logger.Info("Error loading kube configuration from directory")
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		Logger.Info("Loaded config from cluster sucessfully.")
	}
	Logger.Info("Loaded config from kubeconfig file.")
	return config, nil
}
