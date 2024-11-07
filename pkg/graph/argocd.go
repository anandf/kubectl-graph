// Copyright 2024 Anand Francis Joseph
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	set "github.com/hashicorp/go-set/v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ApplicationV1alpha1Graph is used to graph all routing resources.
type ApplicationV1alpha1Graph struct {
	graph *Graph
}

// NewApplicationV1alpha1Graph creates a new ApplicationV1alpha1Graph.
func NewApplicationV1alpha1Graph(g *Graph) *ApplicationV1alpha1Graph {
	return &ApplicationV1alpha1Graph{
		graph: g,
	}
}

// ApplicationV1alpha1 retrieves the ApplicationV1alpha1Graph.
func (g *Graph) ApplicationV1alpha1() *ApplicationV1alpha1Graph {
	return g.applicationV1alpha1
}

// Unstructured adds an unstructured node to the Graph.
func (g *ApplicationV1alpha1Graph) Unstructured(unstr *unstructured.Unstructured) (*Node, error) {
	switch unstr.GetKind() {
	case "ApplicationSet":
		return g.ApplicationSet(unstr)
	case "Application":
		return g.Application(unstr)
	case "AppProject":
		return g.AppProject(unstr)
	default:
		return g.graph.Node(unstr.GroupVersionKind(), unstr), nil
	}
}

// Application adds a v1alpha1.Application resource to the Graph.
func (g *ApplicationV1alpha1Graph) Application(app *unstructured.Unstructured) (*Node, error) {
	n := g.graph.Node(app.GroupVersionKind(), app)

	fields := app.Object
	projName := fields["spec"].(map[string]interface{})["project"].(string)

	objs, err := g.getAllObjects()
	children := set.New[*unstructured.Unstructured](len(objs))
	if err != nil {
		return nil, err
	}
	namespaces := set.New[string](10)
	// Track the immediate children, and AppProject of the Application
	for _, obj := range objs {
		if obj.GetKind() == "AppProject" && obj.GetAPIVersion() == "argoproj.io/v1alpha1" {
			if obj.GetName() == projName {
				childNode, err := g.graph.Unstructured(obj)
				if err != nil {
					return n, err
				}
				g.graph.Relationship(n, obj.GetKind(), childNode)
			}
		}
		annotations := obj.GetAnnotations()
		if trackingID, ok := annotations["argocd.argoproj.io/tracking-id"]; ok {
			if strings.HasPrefix(trackingID, fmt.Sprintf("%s:", app.GetName())) {
				children.Insert(obj)
				if len(obj.GetNamespace()) > 0 {
					namespaces.Insert(obj.GetNamespace())
				}
			}
		}
		labels := obj.GetLabels()
		if trackingLabel, ok := labels["app.kubernetes.io/instance"]; ok {
			if trackingLabel == app.GetName() {
				children.Insert(obj)
				if len(obj.GetNamespace()) > 0 {
					namespaces.Insert(obj.GetNamespace())
				}
			}
		}
	}
	// Add objects that are created in the same namespace of the immediate children
	for _, obj := range objs {
		if namespaces.Contains(obj.GetNamespace()) {
			children.Insert(obj)
		}
	}
	for _, child := range children.Slice() {
		childNode, err := g.graph.Unstructured(child)
		if err != nil {
			return n, err
		}
		g.graph.Relationship(n, child.GetKind(), childNode)
	}
	return n, nil
}

// ApplicationSet adds a v1alpha1.ApplicationSet resource to the Graph.
func (g *ApplicationV1alpha1Graph) ApplicationSet(appset *unstructured.Unstructured) (*Node, error) {
	objs, err := g.getChildApplications()
	if err != nil {
		return nil, err
	}
	n := g.graph.Node(appset.GroupVersionKind(), appset)
	for _, obj := range objs {
		ownerReferences := obj.GetOwnerReferences()
		for _, ownerRef := range ownerReferences {
			if ownerRef.UID == appset.GetUID() {
				childNode, err := g.graph.Unstructured(obj)
				if err != nil {
					return nil, err
				}
				g.graph.Relationship(n, obj.GetKind(), childNode)
			}
		}
	}
	return n, nil
}

// AppProject adds a v1alpha1.AppProject resource to the Graph.
func (g *ApplicationV1alpha1Graph) AppProject(obj *unstructured.Unstructured) (*Node, error) {
	n := g.graph.Node(obj.GroupVersionKind(), obj)
	return n, nil
}

func (g *ApplicationV1alpha1Graph) getChildApplications() ([]*unstructured.Unstructured, error) {
	results := make(map[string][]*unstructured.Unstructured)
	objs := make([]*unstructured.Unstructured, 0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	lock := sync.Mutex{}
	err := g.getObjectsForAResource(schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}, results, &wg, &lock)
	if err != nil {
		return objs, err
	}
	wg.Wait()
	for _, resourceObjs := range results {
		objs = append(objs, resourceObjs...)
	}
	return objs, nil
}

func (g *ApplicationV1alpha1Graph) getAllObjects() ([]*unstructured.Unstructured, error) {
	apiResources, err := g.graph.clientset.Discovery().ServerPreferredResources()
	if err != nil {
		return nil, err
	}
	objs := make([]*unstructured.Unstructured, 0, len(apiResources))
	var wg sync.WaitGroup
	for _, apiResource := range apiResources {
		results := make(map[string][]*unstructured.Unstructured, len(apiResource.APIResources))
		lock := &sync.Mutex{}
		for _, api := range apiResource.APIResources {
			if api.Kind == "Event" {
				continue
			}
			wg.Add(1)
			gvk := schema.FromAPIVersionAndKind(apiResource.GroupVersion, apiResource.Kind)
			gv := gvk.GroupVersion()
			gvr := gv.WithResource(api.Name)
			go g.getObjectsForAResource(gvr, results, &wg, lock)
		}
		wg.Wait()
		for _, resourceObjs := range results {
			objs = append(objs, resourceObjs...)
		}
	}

	return objs, nil
}

func (g *ApplicationV1alpha1Graph) getObjectsForAResource(gvr schema.GroupVersionResource, results map[string][]*unstructured.Unstructured, wg *sync.WaitGroup, lock *sync.Mutex) error {
	defer wg.Done()
	defer lock.Unlock()
	objList, err := dynamic.New(g.graph.clientset.RESTClient()).Resource(gvr).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		lock.Lock()
		results[gvr.String()] = make([]*unstructured.Unstructured, 0)
		return err
	}
	result := make([]*unstructured.Unstructured, 0, len(objList.Items))
	for _, obj := range objList.Items {
		result = append(result, &obj)
	}
	lock.Lock()
	results[gvr.String()] = result
	return nil
}
