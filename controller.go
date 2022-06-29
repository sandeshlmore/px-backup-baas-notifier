package main

import (
	"sync"
	"time"

	"github.com/portworx/px-backup-baas-notifier/pkg/notification"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type controller struct {
	client              dynamic.Interface
	backupinformer      cache.SharedIndexInformer
	mongoinformer       cache.SharedIndexInformer
	dynInformer         dynamicinformer.DynamicSharedInformerFactory
	backupLister        cache.GenericLister
	mongoLister         cache.GenericLister
	stopChannel         <-chan struct{}
	fullCacheSyncedOnce bool
	stateHistory        *StateHistory
	notifyClient        notification.Client
}

var backupGVR = schema.GroupVersionResource{
	Group:    "backup.purestorage.com",
	Version:  "v1alpha1",
	Resource: "backups",
}

var mongoGVR = schema.GroupVersionResource{
	Group:    "backup.purestorage.com",
	Version:  "v1alpha1",
	Resource: "mongos",
}

type NamespaceStateHistory struct {
	backupStatus string
	mongoStatus  string
	notification string
	lastUpdate   time.Time
}

type StateHistory struct {
	sync.RWMutex
	perNamespaceHistory map[string]*NamespaceStateHistory
}

const NOTFOUND string = "NotFound"

// Create Informers and add eventhandlers
func newController(client dynamic.Interface, dynInformer dynamicinformer.DynamicSharedInformerFactory,
	stopch <-chan struct{}, notifyClient notification.Client) *controller {
	Backupinf := dynInformer.ForResource(backupGVR).Informer()
	Mongoinf := dynInformer.ForResource(mongoGVR).Informer()
	c := &controller{
		client:         client,
		backupinformer: Backupinf,
		mongoinformer:  Mongoinf,
		dynInformer:    dynInformer,
		backupLister:   dynInformer.ForResource(backupGVR).Lister(),
		mongoLister:    dynInformer.ForResource(mongoGVR).Lister(),
		stopChannel:    stopch,
		stateHistory: &StateHistory{
			perNamespaceHistory: map[string]*NamespaceStateHistory{},
		},
		notifyClient: notifyClient,
	}
	Backupinf.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				Logger.Info("Backup CREATE event")
				c.SyncInformerCache()
				c.handleBackupAndMongoStatus(obj)
			},
			UpdateFunc: func(old, new interface{}) {
				Logger.Info("Backup UPDATE event")
				c.handleBackupAndMongoStatus(new)
			},
			DeleteFunc: func(obj interface{}) {
				Logger.Info("Backup is deleted")
				c.handleBackupAndMongoStatus(obj)
			},
		},
	)

	Mongoinf.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				c.SyncInformerCache()
				Logger.Info("Mongo CREATE event")
				c.handleBackupAndMongoStatus(obj)
			},
			UpdateFunc: func(old, new interface{}) {
				Logger.Info("Mongo UPDATE event")
				c.handleBackupAndMongoStatus(new)
			},
			DeleteFunc: func(obj interface{}) {
				Logger.Info("Mongo is deleted")
				c.handleBackupAndMongoStatus(obj)
			},
		},
	)
	return c
}

func (c *controller) SyncInformerCache() {
	if !c.fullCacheSyncedOnce {
		if !cache.WaitForNamedCacheSync("px-backup-notifier", c.stopChannel, c.backupinformer.HasSynced, c.mongoinformer.HasSynced) {
			Logger.Info("Timedout waiting for cache to be synced")
			c.fullCacheSyncedOnce = false
			return
		}
	}
	c.fullCacheSyncedOnce = true
}

// start the controller
func (c *controller) run(stopch <-chan struct{}) {
	Logger.Info("Started notification controller")

	c.dynInformer.Start(stopch)

	<-stopch

	Logger.Info("Shutting down notification controller")

}

func getCRStatus(lister cache.GenericLister, ns string) string {
	cr, _ := lister.ByNamespace(ns).List(labels.NewSelector())
	if len(cr) != 0 {
		u := cr[0].(*unstructured.Unstructured)
		status := extractStateFromCRStatus(u.Object)
		return status
	}
	return NOTFOUND
}

func extractStateFromCRStatus(obj map[string]interface{}) string {
	var state string

	if obj["status"] == nil {
		state = "Pending"
	} else {
		state = obj["status"].(map[string]interface{})["state"].(string)
	}
	return state
}

func (c *controller) handleBackupAndMongoStatus(obj interface{}) {
	var mongoStatus, backupStatus string

	u := obj.(*unstructured.Unstructured)
	ns := u.Object["metadata"].(map[string]interface{})["namespace"].(string)
	creationTime := u.GetCreationTimestamp()

	backupStatus = getCRStatus(c.backupLister, ns)
	mongoStatus = getCRStatus(c.mongoLister, ns)

	state := notification.StatesAndNotificationsMapping[backupStatus][mongoStatus]

	if ((backupStatus == NOTFOUND && mongoStatus != NOTFOUND) ||
		(backupStatus != NOTFOUND && mongoStatus == NOTFOUND)) &&
		(time.Since(creationTime.Time) < time.Duration(2*time.Minute)) {
		// It might happen that Mongo Cr is created and backup is not created yet or vice versa,
		// In this case we dont want to send unreachable or deleted straightway
		// following e.g. scenarios
		// Mongo --> Pending/Available/UnReachable   AND   Backup --> NotFound
		// then we send Provisioning till creationTime < 2 min
		// same goes for below case
		// Backup --> Pending/Available/ and Mongo --> NotFound
		// Idea here is to wait for 2 minutes before we send notification as defined in StatesAndNotificationsMapping \
		// because CR creation might be delayed or cache has not been sync properly
		state = "Provisioning"
	}

	c.stateHistory.Lock()

	previousState, ok := c.stateHistory.perNamespaceHistory[ns]
	if ok {
		if previousState.notification == state {
			// skip sending notification because current state matches previous state
			Logger.Info("Skipping notification", "Namespace", ns)
			c.stateHistory.Unlock()
			return
		} else {
			// update new state in history
			previousState.backupStatus = backupStatus
			previousState.mongoStatus = mongoStatus
			previousState.lastUpdate = time.Now()
			previousState.notification = state
		}

	} else {
		// add entry in history
		c.stateHistory.perNamespaceHistory[ns] = &NamespaceStateHistory{
			backupStatus: backupStatus,
			mongoStatus:  mongoStatus,
			notification: state,
			lastUpdate:   time.Now(),
		}
	}

	c.stateHistory.Unlock()

	Logger.Info("Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "Notification", state)

	note := notification.Note{
		State:          state, //TODO: handle unknown state transition error
		Namespace:      ns,
		FailureMessage: "",
	}

	err := c.notifyClient.Send(note) //TODO: check if notification send failed and retry in case of non 200
	if err != nil {
		Logger.Error(err, "Failed to send notification")
	}
}
