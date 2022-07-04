package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/portworx/px-backup-baas-notifier/pkg/notification"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	jobInformer         cache.SharedIndexInformer
	dynInformer         dynamicinformer.DynamicSharedInformerFactory
	backupLister        cache.GenericLister
	mongoLister         cache.GenericLister
	jobLister           cache.GenericLister
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
var jobGVR = schema.GroupVersionResource{Group: "batch",
	Version: "v1", Resource: "jobs"}

type NamespaceStateHistory struct {
	backupStatus Status
	mongoStatus  Status
	notification string
	lastUpdate   time.Time
}

type StateHistory struct {
	sync.RWMutex
	perNamespaceHistory map[string]*NamespaceStateHistory
}

type Status string

const (
	FAILED       Status = "Failed"
	JOBSUCCEEDED Status = "Succeeded"
	PENDING      Status = "Pending"
	AVAILABLE    Status = "Available"
	NOTFOUND     Status = "NotFound"
)

// Create Informers and add eventhandlers
func newController(client kubernetes.Interface, dynclient dynamic.Interface, dynInformer dynamicinformer.DynamicSharedInformerFactory,
	stopch <-chan struct{}, notifyClient notification.Client) *controller {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	Backupinf := dynInformer.ForResource(backupGVR).Informer()
	Mongoinf := dynInformer.ForResource(mongoGVR).Informer()

	ctx := context.Background()
	labelstring := strings.Split(nsLabel, ":")
	labelSelector := labels.Set(map[string]string{labelstring[0]: labelstring[1]}).AsSelector()

	nsinformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = labelSelector.String()
				return client.CoreV1().Namespaces().List(ctx, options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labelSelector.String()
				return client.CoreV1().Namespaces().Watch(ctx, options)
			},
		},
		&corev1.Namespace{},
		0, //Skip resync
		cache.Indexers{},
	)
	Jobinf := dynInformer.ForResource(jobGVR).Informer()

	c := &controller{
		dynclient:      dynclient,
		client:         client,
		nsinformer:     nsinformer,
		backupinformer: Backupinf,
		mongoinformer:  Mongoinf,
		jobInformer:    Jobinf,
		dynInformer:    dynInformer,
		backupLister:   dynInformer.ForResource(backupGVR).Lister(),
		mongoLister:    dynInformer.ForResource(mongoGVR).Lister(),
		jobLister:      dynInformer.ForResource(jobGVR).Lister(),
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

	// register evnet handlers for job updates
	Jobinf.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(old, new interface{}) {
				Logger.Info("Job UPDATE event")
				job, _ := convertUnstructuredToConcreteTypeJob(new.(runtime.Object))
				if job.Name == JobName {
					_, ok := c.stateHistory.perNamespaceHistory[job.Namespace]
					if ok {
						// call handler only if mongo and backup cr present
						//so JOB status is only checked if backup is in Available state
						c.handleBackupAndMongoStatus(new)
					}
				}
			},
		},
	)

	return c
}

func (c *controller) SyncInformerCache() {
	if !c.fullCacheSyncedOnce {
		if !cache.WaitForNamedCacheSync("px-backup-notifier", c.stopChannel,
			c.backupinformer.HasSynced, c.mongoinformer.HasSynced, c.jobInformer.HasSynced, c.nsinformer.HasSynced) {
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

func (c *controller) handleBackupAndMongoStatus(obj interface{}) {
	var mongoStatus, backupStatus, jobStatus Status
	var err error

	u := obj.(*unstructured.Unstructured)
	ns := u.Object["metadata"].(map[string]interface{})["namespace"].(string)
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

	// if backup is in available state then also check for JOB status
	if state == "Success" {
		jobStatus, err = c.getJobtatus(c.jobLister, ns)
		if err != nil {
			Logger.Error(err, "Failed to get Job status", "Namespace", ns)
			return
		}
		state = notification.BackupStatesJobStatesAndNotificationsMapping[string(AVAILABLE)][string(jobStatus)]
		Logger.Info("JObstatus", "Namespace", ns, "State", jobStatus, "state", state)
	}

	c.stateHistory.Lock()

	previousState, ok := c.stateHistory.perNamespaceHistory[ns]
	if ok {
		if previousState.notification == state {
			// skip sending notification because current state matches previous state
			Logger.Info("Skipping notification", "Namespace", ns)
			c.stateHistory.Unlock()
			return
		}
	}

	// update entry in stateHistory
	c.stateHistory.perNamespaceHistory[ns] = &NamespaceStateHistory{
		backupStatus: backupStatus,
		mongoStatus:  mongoStatus,
		notification: state,
		lastUpdate:   time.Now(),
	}

	c.stateHistory.Unlock()

	Logger.Info("Curent Status: ", "NameSpace", ns, "Backup", backupStatus, "Mongo", mongoStatus, "JOB", jobStatus, "Notification", state, "Event", "CR Update")

	note := notification.Note{
		State:          state, //TODO: handle unknown state transition error
		Namespace:      ns,
		FailureMessage: "",
	}

	if err = c.notifyClient.Send(note); err != nil {
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
	var mongoStatus, backupStatus Status
	var notificationstate string
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

func (c *controller) getJobtatus(lister cache.GenericLister, ns string) (Status, error) {
	if Job, err := c.jobLister.ByNamespace(ns).Get(JobName); err != nil {
		if apierrors.IsNotFound(err) {
			return NOTFOUND, nil
		}
		return "", err
	} else {
		if job, err := convertUnstructuredToConcreteTypeJob(Job); err != nil {
			return "", err
		} else {
			succeeded := job.Status.Succeeded
			failed := job.Status.Failed
			Logger.Info("Current Job Status", "Namespace", job.Namespace, "Active", job.Status.Active, "Success", job.Status.Succeeded, "Failed", job.Status.Failed)
			if succeeded == 1 {
				return JOBSUCCEEDED, nil
			} else if failed == int32(*job.Spec.BackoffLimit+1) || time.Since(job.CreationTimestamp.Time) > 15*time.Minute {
				return FAILED, nil
			} else {
				return PENDING, nil
			}
		}
	}
}

func convertUnstructuredToConcreteTypeJob(Obj runtime.Object) (batchv1.Job, error) {
	Job := &batchv1.Job{}
	var err error
	if j, err := runtime.DefaultUnstructuredConverter.ToUnstructured(Obj); err != nil {
		Logger.Error(err, "conversion to unstructured failed")
	} else {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(j, Job); err != nil {
			Logger.Error(err, "conversion from unstructured failed")
		}
	}
	return *Job, err
}
