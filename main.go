package main

import (
	"errors"
	"flag"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	"go.uber.org/zap/zapcore"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	Logger            logr.Logger
	nsLabel           string
	schedulerUrl      string
	RetryDelaySeconds int
	BackupTimeout     int
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

	schedulerUrl = os.Getenv("SCHEDULER_URL")
	if schedulerUrl == "" || !isUrl(schedulerUrl) {
		Logger.Error(errors.New("Invalid backupSchedulerUrl configured"), "")
		os.Exit(1)
	}
	t := os.Getenv("BackupTimeout")
	if t == "" {
		BackupTimeout = 15
	} else {
		BackupTimeout, err = strconv.Atoi(t)
		if err != nil {
			log.Fatalf("Failed to Parse BackupTimeout env")
		}
	}
	t = os.Getenv("RetryDelaySeconds")
	if t == "" {
		RetryDelaySeconds = 8
	} else {
		RetryDelaySeconds, err = strconv.Atoi(t)
		if err != nil {
			log.Fatalf("Failed to Parse retryDelaySeconds env")
		}
	}
}

func main() {
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
			Logger.Error(err, "Colud not load config from inclusterconfig")
			os.Exit(1)
		}
		Logger.Info("Loading config from cluster sucessful.")
	}

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

	c := newController(clientset, dynClient, infFactory, stopch, notifyClient)

	c.run(stopch)

}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
