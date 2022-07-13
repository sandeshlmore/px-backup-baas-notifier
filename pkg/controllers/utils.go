package controllers

import (
	"github.com/portworx/px-backup-baas-notifier/pkg/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

func getCRStatus(lister cache.GenericLister, ns string) types.Status {
	cr, _ := lister.ByNamespace(ns).List(labels.NewSelector())
	if len(cr) != 0 {
		u := cr[0].(*unstructured.Unstructured)
		status := extractStateFromCRStatus(u.Object)
		return types.Status(status)
	}
	return types.NOTFOUND
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
