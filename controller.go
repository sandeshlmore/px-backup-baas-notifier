package main

import (
	"context"
	"sync"
	"time"

	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type controller struct {
	client              kubernetes.Interface
	dynclient           dynamic.Interface
	nsinformer          cache.SharedIndexInformer
	backupinformer      cache.SharedIndexInformer
	mongoinformer       cache.SharedIndexInformer
	dynInformer         dynamicinformer.DynamicSharedInformerFactory
	backupLister        cache.GenericLister
	mongoLister         cache.GenericLister
	stopChannel         <-chan struct{}
	fullCacheSyncedOnce bool
	stateHistory        *StateHistory
	notifyClient        notification.Client
	queue               workqueue.RateLimitingInterface
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
const AVAILABLE string = "Available"

// Create Informers and add eventhandlers
func newController(client kubernetes.Interface, dynclient dynamic.Interface, dynInformer dynamicinformer.DynamicSharedInformerFactory,
	stopch <-chan struct{}, notifyClient notification.Client) *controller {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	Backupinf := dynInformer.ForResource(backupGVR).Informer()
	Mongoinf := dynInformer.ForResource(mongoGVR).Informer()

	ctx := context.Background()
	// labelstring := strings.Split(nsLabel, ":")
	// labelSelector := labels.Set(map[string]string{labelstring[0]: labelstring[1]}).AsSelector()

	nsinformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				// options.LabelSelector = labelSelector.String()
				return client.CoreV1().Namespaces().List(ctx, v1.ListOptions{})
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				// options.LabelSelector = labelSelector.String()
				return client.CoreV1().Namespaces().Watch(ctx, v1.ListOptions{})
			},
		},
		&corev1.Namespace{},
		0, //Skip resync
		cache.Indexers{},
	)

	c := &controller{
		dynclient:      dynclient,
		client:         client,
		nsinformer:     nsinformer,
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
		queue:        queue,
	}

	nsinformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			Logger.Info("Delete namespace event", "NameSpace", obj.(*corev1.Namespace).Name)
			c.queue.Add(obj)
		},
	})

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
				c.handleCRDeletion(obj)
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
				c.handleCRDeletion(obj)
			},
		},
	)
	return c
}

func (c *controller) SyncInformerCache() {
	if !c.fullCacheSyncedOnce {
		if !cache.WaitForNamedCacheSync("px-backup-notifier", c.stopChannel, c.backupinformer.HasSynced,
			c.nsinformer.HasSynced, c.mongoinformer.HasSynced) {
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
	defer c.queue.ShutDown()

	c.dynInformer.Start(stopch)
	go c.nsinformer.Run(stopch)

	go wait.Until(c.worker, 1*time.Second, stopch)

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
		// Mongo --> Pending/Available/NotReachable   AND   Backup --> NotFound
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

	Logger.Info("Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "Notification", state, "Event", "CR Update")

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

func (c *controller) worker() {
	for c.handleNamespaceDeletion() {

	}
}

func (c *controller) handleNamespaceDeletion() bool {
	item, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(item)
	key, err := cache.MetaNamespaceKeyFunc(item)
	if err != nil {
		Logger.Error(err, "getting key from cahce")
		return false
	}

	_, namespace, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		Logger.Error(err, "splitting key into namespace and name")
		return false
	}

	if _, exists, _ := c.nsinformer.GetIndexer().GetByKey(key); exists {
		Logger.Info("requeuing namespace deletion...", "Namespace", namespace)
		c.queue.AddAfter(item, 5*time.Second)
	} else {
		Logger.Info("Namespace deletion completed", "Namespace", namespace)
		note := notification.Note{
			State:          "Deleted", //TODO: handle unknown state transition error
			Namespace:      namespace,
			FailureMessage: "",
		}
		if err := c.notifyClient.Send(note); err != nil {
			Logger.Error(err, "Failed to send notification", "namespace", namespace)
		}

		c.stateHistory.Lock()
		c.stateHistory.perNamespaceHistory[namespace] = &NamespaceStateHistory{
			backupStatus: "NotFound",
			mongoStatus:  "NotFound",
			notification: "Deleted",
			lastUpdate:   time.Now(),
		}
		c.stateHistory.Unlock()

		c.queue.Forget(key)
	}

	return true
}

func (c *controller) handleCRDeletion(obj interface{}) {
	var mongoStatus, backupStatus, notificationstate string
	u := obj.(*unstructured.Unstructured)
	ns := u.Object["metadata"].(map[string]interface{})["namespace"].(string)

	backupStatus = getCRStatus(c.backupLister, ns)
	mongoStatus = getCRStatus(c.mongoLister, ns)

	if (backupStatus == NOTFOUND && mongoStatus == AVAILABLE) ||
		(backupStatus == AVAILABLE && mongoStatus == NOTFOUND) {
		notificationstate = "NotReachable"
		note := notification.Note{
			State:          notificationstate,
			Namespace:      ns,
			FailureMessage: "",
		}
		if err := c.notifyClient.Send(note); err != nil {
			Logger.Error(err, "Failed to send notification", "namespace", ns)
		}
		c.stateHistory.Lock()
		c.stateHistory.perNamespaceHistory[ns] = &NamespaceStateHistory{
			mongoStatus:  mongoStatus,
			backupStatus: backupStatus,
			notification: notificationstate,
			lastUpdate:   time.Now(),
		}
		c.stateHistory.Unlock()
	}
	Logger.Info("Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "Notification", notificationstate, "Event", "CR Deletion")

}
