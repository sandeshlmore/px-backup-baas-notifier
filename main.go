package main

import (
	"flag"

	"os"
	"time"

	"github.com/portworx/px-backup-baas-notifier/pkg/log"
	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	"github.com/portworx/px-backup-baas-notifier/pkg/schedule"

	"github.com/portworx/px-backup-baas-notifier/pkg/controllers"
	"github.com/portworx/px-backup-baas-notifier/pkg/envvars"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var Logger = log.Logger

const (
	ScheduleTimeout = 20
)

func main() {

	config, err := kubeconfig()
	if err != nil {
		Logger.Error(err, "Could not load config from kubeconfig")
		os.Exit(1)
	}

	schedule := createSchdeule()

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
	notifyClient := notification.Client{WebhookURL: envvars.WebhookURL}

	c := controllers.NewController(clientset, dynClient, infFactory, stopch, notifyClient, *schedule)

	c.Run(stopch)

}

func createSchdeule() *schedule.Schedule {
	schdeule := schedule.NewSchedule(envvars.SchedulerUrl, int64(ScheduleTimeout), schedule.TokenConfig{
		ClientID:      envvars.ClientID,
		UserName:      envvars.UserName,
		Password:      envvars.Password,
		TokenDuration: envvars.TokenDuration,
		TokenURL:      envvars.TokenURL,
	})
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
