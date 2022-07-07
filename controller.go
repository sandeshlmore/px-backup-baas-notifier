package main

import (
	"context"
	"encoding/json"
	"net/http"
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
	nsqueue             workqueue.RateLimitingInterface
	backupqueue         workqueue.RateLimitingInterface
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
	backupStatus    Status
	mongoStatus     Status
	notification    string
	schedulerStatus Status
	lastUpdate      time.Time
}

type StateHistory struct {
	sync.RWMutex
	perNamespaceHistory map[string]*NamespaceStateHistory
}

type SchedulerStatus struct {
	SUCCESS bool
}

type Status string

const (
	NOTFOUND  Status = "NotFound"
	AVAILABLE Status = "Available"
	PENDING   Status = "Pending"
	FAILED    Status = "Failed"
)

// Create Informers and add eventhandlers
func newController(client kubernetes.Interface, dynclient dynamic.Interface,
	dynInformer dynamicinformer.DynamicSharedInformerFactory,
	stopch <-chan struct{}, notifyClient notification.Client) *controller {
	nsqueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	backupqueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

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
		nsqueue:      nsqueue,
		backupqueue:  backupqueue,
	}

	nsinformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			Logger.Info("Delete namespace event", "NameSpace", obj.(*corev1.Namespace).Name)
			c.nsqueue.Add(obj)
		},
	})

	Backupinf.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				Logger.Info("Backup CREATE event")
				c.SyncInformerCache()
				c.backupqueue.Add(obj)
			},
			UpdateFunc: func(old, new interface{}) {
				Logger.Info("Backup UPDATE event")
				c.backupqueue.Add(new)
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
				c.backupqueue.Add(obj)
			},
			UpdateFunc: func(old, new interface{}) {
				Logger.Info("Mongo UPDATE event")
				c.backupqueue.Add(new)
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
	defer c.nsqueue.ShutDown()
	defer c.backupqueue.ShutDown()

	c.dynInformer.Start(stopch)
	go c.nsinformer.Run(stopch)

	go wait.Until(c.worker1, 1*time.Second, stopch)

	go wait.Until(c.worker2, 1*time.Second, stopch)

	<-stopch

	Logger.Info("Shutting down notification controller")

}

func getCRStatus(lister cache.GenericLister, ns string) Status {
	cr, _ := lister.ByNamespace(ns).List(labels.NewSelector())
	if len(cr) != 0 {
		u := cr[0].(*unstructured.Unstructured)
		status := extractStateFromCRStatus(u.Object)
		return Status(status)
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

func (c *controller) worker1() {
	for c.handleNamespaceDeletion() {
	}
}

func (c *controller) worker2() {
	for c.handleBackupAndMongoCreateUpdateEvents() {
	}
}

func (c *controller) handleNamespaceDeletion() bool {
	item, quit := c.nsqueue.Get()
	if quit {
		return false
	}
	defer c.nsqueue.Done(item)
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
		c.nsqueue.AddAfter(item, 10*time.Second)
	} else {
		Logger.Info("Namespace deletion completed", "Namespace", namespace)
		state := "Deleted"
		if c.skipNotification(namespace, state, NOTFOUND, NOTFOUND) {
			return true
		}
		note := notification.Note{
			State:          state, //TODO: handle unknown state transition error
			Namespace:      namespace,
			FailureMessage: "",
		}
		if err := c.notifyClient.Send(note); err != nil {
			Logger.Error(err, "Failed to send notification", "namespace", namespace)
		}

		c.nsqueue.Forget(key)
	}

	return true
}

func (c *controller) handleCRDeletion(obj interface{}) {
	var mongoStatus, backupStatus Status
	var notificationstate string
	u := obj.(*unstructured.Unstructured)
	ns := u.Object["metadata"].(map[string]interface{})["namespace"].(string)

	backupStatus = getCRStatus(c.backupLister, ns)
	mongoStatus = getCRStatus(c.mongoLister, ns)

	if (backupStatus == NOTFOUND && mongoStatus == AVAILABLE) ||
		(backupStatus == AVAILABLE && mongoStatus == NOTFOUND) &&
			(c.stateHistory.perNamespaceHistory[ns].notification == "Success") {
		notificationstate = "NotReachable"
		if c.skipNotification(ns, notificationstate, backupStatus, mongoStatus) {
			return
		}
		note := notification.Note{
			State:          notificationstate,
			Namespace:      ns,
			FailureMessage: "",
		}
		if err := c.notifyClient.Send(note); err != nil {
			Logger.Error(err, "Failed to send notification", "namespace", ns)
		}
	}
	Logger.Info("Skipping Notification. Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "Notification", notificationstate, "Event", "CR Deletion")
}

func (c *controller) handleBackupAndMongoCreateUpdateEvents() bool {
	var mongoStatus, backupStatus, schedulerStatus Status
	item, quit := c.backupqueue.Get()
	if quit {
		return false
	}
	defer c.backupqueue.Done(item)

	u := item.(*unstructured.Unstructured)
	ns := u.GetNamespace()
	creationTime := u.GetCreationTimestamp()

	backupStatus = getCRStatus(c.backupLister, ns)
	mongoStatus = getCRStatus(c.mongoLister, ns)

	state := notification.StatesAndNotificationsMapping[string(backupStatus)][string(mongoStatus)]

	if ((backupStatus == NOTFOUND && mongoStatus != NOTFOUND) ||
		(backupStatus != NOTFOUND && mongoStatus == NOTFOUND)) &&
		(time.Since(creationTime.Time) < time.Duration(1*time.Minute)) {
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

	if state == "Success" {
		schedulerStatus = getSchedulerStatus(creationTime, ns)
		Logger.Info("", "SchedulerStatus", schedulerStatus, "NameSpace", ns)
		state = notification.BackupAndSchedulerStatusMapping[string(AVAILABLE)][string(schedulerStatus)]
		if state == "Provisioning" {
			defer c.backupqueue.AddAfter(item, time.Duration(RetryDelaySeconds)*time.Second)
		} else {
			defer c.backupqueue.Forget(item)
		}
	}

	if c.skipNotification(ns, state, backupStatus, mongoStatus) {
		return true
	}

	note := notification.Note{
		State:          state, //TODO: handle unknown state transition error
		Namespace:      ns,
		FailureMessage: "",
	}

	err := c.notifyClient.Send(note) //TODO: check if notification send failed and retry in case of non 200
	if err != nil {
		Logger.Error(err, "Failed to send notification")
	}

	return true
}

func getSchedulerStatus(creationTime v1.Time, ns string) Status {
	isReady, err := IsBackupSchedulerReady(ns)
	if err != nil {
		Logger.Error(err, "Failed to get scheduler status")
	}
	if !isReady {
		if time.Since(creationTime.Time) > time.Duration(BackupTimeout)*time.Minute { //TODO: make configurable backup timeout
			return FAILED
		}
		return PENDING
	}
	return AVAILABLE
}

func IsBackupSchedulerReady(ns string) (bool, error) {
	status := SchedulerStatus{}
	client := &http.Client{}

	request, err := http.NewRequest("GET", schedulerUrl, nil)
	q := request.URL.Query()
	q.Add("name", ns)
	request.URL.RawQuery = q.Encode()
	if err != nil {
		return false, err
	}
	resp, err := client.Do(request)
	if err != nil {
		return false, err
	}
	err = json.NewDecoder(resp.Body).Decode(&status)
	return status.SUCCESS, err
}

func (c *controller) skipNotification(ns, state string, backupStatus, mongoStatus Status) bool {
	msg, skip := "", false
	c.stateHistory.Lock()

	if previousState, ok := c.stateHistory.perNamespaceHistory[ns]; ok && previousState.notification == state {
		msg, skip = "Skipping notification. ", true
	}
	c.stateHistory.perNamespaceHistory[ns] = &NamespaceStateHistory{
		backupStatus: backupStatus,
		mongoStatus:  mongoStatus,
		notification: state,
		lastUpdate:   time.Now(),
	}
	Logger.Info(msg+"Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "Notification", state)
	c.stateHistory.Unlock()
	return skip
}
