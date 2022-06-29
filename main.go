package main

import (
	"flag"
	"net/url"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	"go.uber.org/zap/zapcore"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var Logger logr.Logger

func init() {
	opts := zap.Options{
		Development: true,
		TimeEncoder: zapcore.TimeEncoderOfLayout(time.RFC3339),
	}
	Logger = zap.New(zap.UseFlagOptions(&opts))
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

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		Logger.Error(err, "Error getting dyn client")
	}

	infFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0*time.Minute)

	stopch := make(<-chan struct{})
	notifyClient := notification.Client{WebhookURL: webhookURL}

	c := newController(dynClient, infFactory, stopch, notifyClient)

	c.run(stopch)

}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
