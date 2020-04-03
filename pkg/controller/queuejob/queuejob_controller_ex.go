/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queuejob

import (
	"fmt"
	"github.com/IBM/multi-cluster-app-dispatcher/cmd/kar-controllers/app/options"
	"github.com/golang/glog"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/metrics/adapter"
	"math"
	"math/rand"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"strconv"
	"time"

	"k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"

	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources"
	resconfigmap "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/configmap" // ConfigMap
	resdeployment "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/deployment"
	resnamespace "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/namespace"                         // NP
	resnetworkpolicy "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/networkpolicy"                 // NetworkPolicy
	respersistentvolume "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/persistentvolume"           // PV
	respersistentvolumeclaim "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/persistentvolumeclaim" // PVC
	respod "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/pod"
	ressecret "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/secret" // Secret
	resservice "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/service"
	resstatefulset "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobresources/statefulset"
	"k8s.io/apimachinery/pkg/labels"

	arbv1 "github.com/IBM/multi-cluster-app-dispatcher/pkg/apis/controller/v1alpha1"
	clientset "github.com/IBM/multi-cluster-app-dispatcher/pkg/client/clientset/controller-versioned"
	"github.com/IBM/multi-cluster-app-dispatcher/pkg/client/clientset/controller-versioned/clients"
	arbinformers "github.com/IBM/multi-cluster-app-dispatcher/pkg/client/informers/controller-externalversion"
	informersv1 "github.com/IBM/multi-cluster-app-dispatcher/pkg/client/informers/controller-externalversion/v1"
	listersv1 "github.com/IBM/multi-cluster-app-dispatcher/pkg/client/listers/controller/v1"

	"github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/queuejobdispatch"

	clusterstateapi "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/clusterstate/api"
	clusterstatecache "github.com/IBM/multi-cluster-app-dispatcher/pkg/controller/clusterstate/cache"
)

const (
	// QueueJobNameLabel label string for queuejob name
	QueueJobNameLabel string = "appwrapper-name"

	// ControllerUIDLabel label string for queuejob controller uid
	ControllerUIDLabel string = "controller-uid"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = arbv1.SchemeGroupVersion.WithKind("AppWrapper")

//XController the AppWrapper Controller type
type XController struct {
	config           *rest.Config
	serverOption     *options.ServerOption

	queueJobInformer informersv1.AppWrapperInformer
	// resources registered for the AppWrapper
	qjobRegisteredResources queuejobresources.RegisteredResources
	// controllers for these resources
	qjobResControls map[arbv1.ResourceType]queuejobresources.Interface

	clients    *kubernetes.Clientset
	arbclients *clientset.Clientset

	// A store of jobs
	queueJobLister listersv1.AppWrapperLister
	queueJobSynced func() bool

	// QueueJobs that need to be initialized
	// Add labels and selectors to AppWrapper
	initQueue *cache.FIFO

	// QueueJobs that need to sync up after initialization
	updateQueue *cache.FIFO

	// eventQueue that need to sync up
	eventQueue *cache.FIFO

	//QJ queue that needs to be allocated
	qjqueue SchedulingQueue

	// our own local cache, used for computing total amount of resources
	cache clusterstatecache.Cache

	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager

	// is dispatcher or deployer?
	isDispatcher bool

	// Agent map: agentID -> JobClusterAgent
	agentMap map[string]*queuejobdispatch.JobClusterAgent
	agentList []string

	// Map for AppWrapper -> JobClusterAgent
	dispatchMap map[string]string

	// Metrics API Server
	metricsAdapter *adapter.MetricsAdpater

	// EventQueueforAgent
	agentEventQueue *cache.FIFO
}

type JobAndClusterAgent struct{
	queueJobKey string
	queueJobAgentKey string
}

func NewJobAndClusterAgent(qjKey string, qaKey string) *JobAndClusterAgent {
	return &JobAndClusterAgent{
		queueJobKey: qjKey,
		queueJobAgentKey: qaKey,
	}
}


//RegisterAllQueueJobResourceTypes - gegisters all resources
func RegisterAllQueueJobResourceTypes(regs *queuejobresources.RegisteredResources) {
	respod.Register(regs)
	resservice.Register(regs)
	resdeployment.Register(regs)
	resstatefulset.Register(regs)
	respersistentvolume.Register(regs)
	respersistentvolumeclaim.Register(regs)
	resnamespace.Register(regs)
	resconfigmap.Register(regs)
	ressecret.Register(regs)
	resnetworkpolicy.Register(regs)
}

func GetQueueJobAgentKey(obj interface{}) (string, error) {
	qa, ok := obj.(*queuejobdispatch.JobClusterAgent)
	if !ok {
		return "", fmt.Errorf("not a AppWrapperAgent")
	}
	return fmt.Sprintf("%s;%s", qa.AgentId, qa.DeploymentName), nil
}


func GetQueueJobKey(obj interface{}) (string, error) {
	qj, ok := obj.(*arbv1.AppWrapper)
	if !ok {
		return "", fmt.Errorf("not a AppWrapper")
	}

	return fmt.Sprintf("%s/%s", qj.Namespace, qj.Name), nil
}

//NewJobController create new AppWrapper Controller
func NewJobController(config *rest.Config, serverOption *options.ServerOption) *XController {
	cc := &XController{
		config:			config,
		serverOption:		serverOption,
		clients:		kubernetes.NewForConfigOrDie(config),
		arbclients:  		clientset.NewForConfigOrDie(config),
		eventQueue:  		cache.NewFIFO(GetQueueJobKey),
		agentEventQueue:	cache.NewFIFO(GetQueueJobKey),
		initQueue: 		cache.NewFIFO(GetQueueJobKey),
		updateQueue:		cache.NewFIFO(GetQueueJobKey),
		qjqueue:		NewSchedulingQueue(),
		cache: 			clusterstatecache.New(config),
	}
	glog.V(10).Infof("[NewJobController] &serverOption=%p serverOption=%+v", serverOption, serverOption)

	cc.metricsAdapter =  adapter.New(config, cc.cache)

	cc.qjobResControls = map[arbv1.ResourceType]queuejobresources.Interface{}
	RegisterAllQueueJobResourceTypes(&cc.qjobRegisteredResources)

	//initialize pod sub-resource control
	resControlPod, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypePod, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type Pod not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypePod] = resControlPod

	// initialize service sub-resource control
	resControlService, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeService, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type Service not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeService] = resControlService

	// initialize PV sub-resource control
	resControlPersistentVolume, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypePersistentVolume, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type PersistentVolume not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypePersistentVolume] = resControlPersistentVolume

	resControlPersistentVolumeClaim, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypePersistentVolumeClaim, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type PersistentVolumeClaim not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypePersistentVolumeClaim] = resControlPersistentVolumeClaim

	resControlNamespace, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeNamespace, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type Namespace not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeNamespace] = resControlNamespace

	resControlConfigMap, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeConfigMap, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type ConfigMap not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeConfigMap] = resControlConfigMap

	resControlSecret, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeSecret, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type Secret not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeSecret] = resControlSecret

	resControlNetworkPolicy, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeNetworkPolicy, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type NetworkPolicy not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeNetworkPolicy] = resControlNetworkPolicy



	// initialize deployment sub-resource control
	resControlDeployment, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeDeployment, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type Service not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeDeployment] = resControlDeployment

	// initialize SS sub-resource
	resControlSS, found, err := cc.qjobRegisteredResources.InitQueueJobResource(arbv1.ResourceTypeStatefulSet, config)
	if err != nil {
		glog.Errorf("fail to create queuejob resource control")
		return nil
	}
	if !found {
		glog.Errorf("queuejob resource type StatefulSet not found")
		return nil
	}
	cc.qjobResControls[arbv1.ResourceTypeStatefulSet] = resControlSS

	queueJobClient, _, err := clients.NewClient(cc.config)
	if err != nil {
		panic(err)
	}
	cc.queueJobInformer = arbinformers.NewSharedInformerFactory(queueJobClient, 0).AppWrapper().AppWrappers()
	cc.queueJobInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *arbv1.AppWrapper:
					glog.V(10).Infof("Filter Name=%s Version=%s Local=%t FilterIgnore=%t Sender=%s &qj=%p qj=%v", t.Name, t.ResourceVersion, t.Status.Local, t.Status.FilterIgnore, t.Status.Sender, t, t)
					if t.Status.Local == true { // ignore duplicate message from cache
						return false
					}
					t.Status.Local = true
					return !t.Status.FilterIgnore  // ignore update messages
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    cc.addQueueJob,
				UpdateFunc: cc.updateQueueJob,
				DeleteFunc: cc.deleteQueueJob,
			},
		})
	cc.queueJobLister = cc.queueJobInformer.Lister()

	cc.queueJobSynced = cc.queueJobInformer.Informer().HasSynced

	//create sub-resource reference manager
	cc.refManager = queuejobresources.NewLabelRefManager()

	// Set dispatcher mode or agent mode
	cc.isDispatcher=serverOption.Dispatcher
	if cc.isDispatcher {
		glog.Infof("[Controller] Dispatcher mode")
 	}	else {
		glog.Infof("[Controller] Agent mode")
	}

	//create agents and agentMap
	cc.agentMap=map[string]*queuejobdispatch.JobClusterAgent{}
	cc.agentList=[]string{}
	for _, agentconfig := range strings.Split(serverOption.AgentConfigs,",") {
		agentData := strings.Split(agentconfig,":")
		cc.agentMap["/root/kubernetes/" + agentData[0]]=queuejobdispatch.NewJobClusterAgent(agentconfig, cc.agentEventQueue)
		cc.agentList=append(cc.agentList, "/root/kubernetes/" + agentData[0])
	}

	if cc.isDispatcher && len(cc.agentMap)==0 {
		glog.Errorf("Dispatcher mode: no agent information")
		return nil
	}

	//create (empty) dispatchMap
	cc.dispatchMap=map[string]string{}

	return cc
}

func (qjm *XController) PreemptQueueJobs() {
	qjobs := qjm.GetQueueJobsEligibleForPreemption()
	for _, q := range qjobs {
		newjob, e := qjm.queueJobLister.AppWrappers(q.Namespace).Get(q.Name)
		if e != nil {
			continue
		}
		newjob.Status.CanRun = false
		if _, err := qjm.arbclients.ArbV1().AppWrappers(q.Namespace).Update(newjob); err != nil {
			glog.Errorf("Failed to update status of AppWrapper %v/%v: %v",
				q.Namespace, q.Name, err)
		}
	}
}

func (qjm *XController) GetQueueJobsEligibleForPreemption() []*arbv1.AppWrapper {
	qjobs := make([]*arbv1.AppWrapper, 0)

	queueJobs, err := qjm.queueJobLister.AppWrappers("").List(labels.Everything())
	if err != nil {
		glog.Errorf("List of queueJobs %+v", qjobs)
		return qjobs
	}

	if !qjm.isDispatcher {		// Agent Mode
		for _, value := range queueJobs {
			replicas := value.Spec.SchedSpec.MinAvailable

			if int(value.Status.Succeeded) == replicas {
				if (replicas>0) {
					qjm.arbclients.ArbV1().AppWrappers(value.Namespace).Delete(value.Name, &metav1.DeleteOptions{})
					continue
				}
			}
			if value.Status.State == arbv1.AppWrapperStateEnqueued {
				continue
			}

			if int(value.Status.Running) < replicas {
				if (replicas>0) {
					glog.V(4).Infof("XQJ %s is eligible for preemption %v - %v , %v !!! \n", value.Name, value.Status.Running, replicas, value.Status.Succeeded)
					qjobs = append(qjobs, value)
				}
			}
		}
	}
	return qjobs
}

func GetPodTemplate(qjobRes *arbv1.AppWrapperResource) (*v1.PodTemplateSpec, error) {
	rtScheme := runtime.NewScheme()
	v1.AddToScheme(rtScheme)

	jsonSerializer := json.NewYAMLSerializer(json.DefaultMetaFactory, rtScheme, rtScheme)

	podGVK := schema.GroupVersion{Group: v1.GroupName, Version: "v1"}.WithKind("PodTemplate")

	obj, _, err := jsonSerializer.Decode(qjobRes.Template.Raw, &podGVK, nil)
	if err != nil {
		return nil, err
	}

	template, ok := obj.(*v1.PodTemplate)
	if !ok {
		return nil, fmt.Errorf("Resource template not define as a PodTemplate")
	}

	return &template.Template, nil

}

func (qjm *XController) GetAggregatedResources(cqj *arbv1.AppWrapper) *clusterstateapi.Resource {
	allocated := clusterstateapi.EmptyResource()
        for _, resctrl := range qjm.qjobResControls {
                qjv     := resctrl.GetAggregatedResources(cqj)
                allocated = allocated.Add(qjv)
        }

        return allocated
}


func (qjm *XController) getAggregatedAvailableResourcesPriority(targetpr float64, cqj string) *clusterstateapi.Resource {
	r := qjm.cache.GetUnallocatedResources()
	preemptable := clusterstateapi.EmptyResource()
	// Resources that can fit but have not dispatched.
	pending := clusterstateapi.EmptyResource()

	glog.V(4).Infof("[getAggAvaiResPri] Idle cluster resources %+v", r)

	queueJobs, err := qjm.queueJobLister.AppWrappers("").List(labels.Everything())
	if err != nil {
		glog.Errorf("[getAggAvaiResPri] Unable to obtain the list of queueJobs %+v", err)
		return r
	}

	for _, value := range queueJobs {
		if value.Name == cqj {
			glog.V(11).Infof("[getAggAvaiResPri] %s: Skipping adjustments for %s since it is the job being processed.", time.Now().String(), value.Name)
			continue
		} else if !value.Status.CanRun {
			glog.V(11).Infof("[getAggAvaiResPri] %s: Skipping adjustments for %s since it can not run.", time.Now().String(), value.Name)
			continue
		} else if value.Status.SystemPriority < targetpr {
			for _, resctrl := range qjm.qjobResControls {
				qjv := resctrl.GetAggregatedResources(value)
				preemptable = preemptable.Add(qjv)
			}
			continue
		} else if value.Status.State == arbv1.AppWrapperStateEnqueued {
			// Don't count the resources that can run but not yet realized (job orchestration pending or partially running).
			for _, resctrl := range qjm.qjobResControls {
				qjv := resctrl.GetAggregatedResources(value)
				pending = pending.Add(qjv)
				glog.V(10).Infof("[getAggAvaiResPri] Subtract all resources %+v for job %s which can-run is set to: %v but state is still pending.", qjv, value.Name, value.Status.CanRun)
			}
			continue
		} else if value.Status.State == arbv1.AppWrapperStateActive {
			if value.Status.Pending > 0 {
				//Don't count partially running jobs with pods still pending.
				for _, resctrl := range qjm.qjobResControls {
					qjv := resctrl.GetAggregatedResources(value)
					pending = pending.Add(qjv)
					glog.V(10).Infof("[getAggAvaiResPri] Subtract all resources %+v for job %s which can-run is set to: %v and status set to: %s but %v pod(s) are pending.", qjv, value.Name, value.Status.CanRun, value.Status.State, value.Status.Pending)
				}
			} else {
				// TODO: Hack to handle race condition when Running jobs have not yet updated the pod counts
				// This hack uses the golang struct implied behavior of defining the object without a value.  In this case
				// of using 'int32' novalue and value of 0 are the same.
				if value.Status.Pending == 0 && value.Status.Running == 0 && value.Status.Succeeded == 0 && value.Status.Failed == 0 {
					for _, resctrl := range qjm.qjobResControls {
						qjv := resctrl.GetAggregatedResources(value)
						pending = pending.Add(qjv)
						glog.V(10).Infof("[getAggAvaiResPri] Subtract all resources %+v for job %s which can-run is set to: %v and status set to: %s but no pod counts in the state have been defined.", qjv, value.Name, value.Status.CanRun, value.Status.State)
					}
				}
			}
			continue
		} else {
			//Do nothing
		}
	}

	glog.V(6).Infof("[getAggAvaiResPri] Schedulable idle cluster resources: %+v, subtracting dispatched resources: %+v and adding preemptable cluster resources: %+v", r, pending, preemptable)

	r = r.Add(preemptable)
	r = r.NonNegSub(pending)

	glog.V(4).Infof("[getAggAvaiResPri] %+v available resources to schedule", r)
	return r
}

func (qjm *XController) chooseAgent(qj *arbv1.AppWrapper) string{

	qjAggrResources := qjm.GetAggregatedResources(qj)

	glog.V(2).Infof("[Controller: Dispatcher Mode] Aggr Resources of XQJ %s: %v\n", qj.Name, qjAggrResources)

	agentId := qjm.agentList[rand.Int() % len(qjm.agentList)]
	glog.V(2).Infof("[Controller: Dispatcher Mode] Agent %s is chosen randomly\n", agentId)
	resources := qjm.agentMap[agentId].AggrResources
	glog.V(2).Infof("[Controller: Dispatcher Mode] Aggr Resources of Agent %s: %v\n", agentId, resources)
	if qjAggrResources.LessEqual(resources) {
		glog.V(2).Infof("[Controller: Dispatcher Mode] Agent %s has enough resources\n", agentId)
	return agentId
	}
	glog.V(2).Infof("[Controller: Dispatcher Mode] Agent %s does not have enough resources\n", agentId)

	return ""
}


// Thread to find queue-job(QJ) for next schedule
func (qjm *XController) ScheduleNext() {
	// get next QJ from the queue
	// check if we have enough compute resources for it
	// if we have enough compute resources then we set the AllocatedReplicas to the total
	// amount of resources asked by the job
	glog.V(10).Infof("[ScheduleNext] isDispatcher=%t &serverOption=%p serverOption=%+v", qjm.isDispatcher, qjm.serverOption, qjm.serverOption)
	SendUpdate := qjm.serverOption.SendUpdate
	Preemption := qjm.serverOption.Preemption
	DynamicPriority := qjm.serverOption.DynamicPriority
	Demo := qjm.serverOption.Demo

	glog.V(10).Infof("[ScheduleNext] BeforePop qjqLength=%d",qjm.qjqueue.Length())
	qjtemp, _ := qjm.qjqueue.Pop()
	glog.V(10).Infof("[ScheduleNext] qjqueuePoped qj=%s oldSystemPriority=%.1f Remaining qjqLength=%d", qjtemp.Name, qjtemp.Status.SystemPriority, qjm.qjqueue.Length())
	if Demo {
		time.Sleep(time.Millisecond * 100)
	}
	//  Create newHeap to temporarily store qjqueue jobs for updating SystemPriority
	tempQ := newHeap(cache.MetaNamespaceKeyFunc, HigherSystemPriorityQJ)
	elapsedtime := (time.Now().Sub(qjtemp.CreationTimestamp.Time)).Seconds()
	qjtemp.Status.SystemPriority = qjtemp.Spec.Priority + qjtemp.Spec.PrioritySlope * elapsedtime
	tempQ.Add(qjtemp)
	for qjm.qjqueue.Length() > 0 {
		qjtemp, _ = qjm.qjqueue.Pop()
		glog.V(10).Infof("[ScheduleNext] qjqueuePoped qj=%s oldSystemPriority=%.1f Remaining qjqLength=%d", qjtemp.Name, qjtemp.Status.SystemPriority, qjm.qjqueue.Length())
		elapsedtime := (time.Now().Sub(qjtemp.CreationTimestamp.Time)).Seconds()
		qjtemp.Status.SystemPriority = qjtemp.Spec.Priority + qjtemp.Spec.PrioritySlope * elapsedtime
		qjtemp.Status.FilterIgnore = true   // update SystemPriority only
		qjm.updateEtcd(qjtemp, "[ScheduleNext]updateSystemPriority", SendUpdate)
		tempQ.Add(qjtemp)
	}
	for tempQ.data.Len() > 0 {
		qjtemp, _ := tempQ.Pop()
		qjm.qjqueue.AddIfNotPresent(qjtemp.(*arbv1.AppWrapper))
	}
	// Print qjqueue.ativeQ for debugging
	pq := qjm.qjqueue.(*PriorityQueue)
	if qjm.qjqueue.Length() > 0 {
		items := pq.activeQ.data.items
		for key, element := range items {
			qjtemp := element.obj.(*arbv1.AppWrapper)
			glog.V(10).Infof("[ScheduleNext] AfterCalc: qjqLength=%d Key=%s index=%d Priority=%.1f SystemPriority=%.1f CreationTime=%s QueueJobState=%s",
				qjm.qjqueue.Length(), key, element.index, qjtemp.Spec.Priority, qjtemp.Status.SystemPriority, qjtemp.CreationTimestamp, qjtemp.Status.QueueJobState)
		}
	}
	// Retrive HeadOfLine
	qj, err := qjm.qjqueue.Pop()
	if err != nil {
		glog.V(4).Infof("[ScheduleNext] Cannot pop QueueJob from qjqueue!")
	}
	qj.Status.QueueJobState = arbv1.QueueJobStateHeadOfLine
	qj.Status.FilterIgnore = true   // update QueueJobState only
	qjm.updateEtcd(qj, "[ScheduleNext]setHOL", SendUpdate)
	qjm.qjqueue.AddUnschedulableIfNotPresent(qj)  // working on qj, avoid other threads putting it back to activeQ
	glog.V(10).Infof("[ScheduleNext] after Pop qjqLength=%d qj %s version=%s activeQ=%t Unsched=%t Status=%+v", qjm.qjqueue.Length(), qj.Name, qj.ResourceVersion, qjm.qjqueue.IfExistActiveQ(qj), qjm.qjqueue.IfExistUnschedulableQ(qj), qj.Status)
	// glog.Infof("I have queuejob %+v", qj)
	if(qjm.isDispatcher) {
		glog.V(2).Infof("[ScheduleNext] [Dispatcher Mode] Dispatch Next QueueJob: %s\n", qj.Name)
	}else{
		glog.V(2).Infof("[ScheduleNext] [Agent Mode] Deploy Next QueueJob: %s Status=%+v\n", qj.Name, qj.Status)
	}

	if qj.Status.CanRun {
		return
	}

	if qjm.isDispatcher {			// Dispatcher Mode
		agentId:=qjm.chooseAgent(qj)
		if agentId != "" {			// A proper agent is found.
														// Update states (CanRun=True) of XQJ in API Server
														// Add XQJ -> Agent Map
			apiQueueJob, err := qjm.queueJobLister.AppWrappers(qj.Namespace).Get(qj.Name)
			if err != nil {
				return
			}
			apiQueueJob.Status.CanRun = true
			// qj.Status.CanRun = true
			glog.V(10).Infof("[TTime] %s, %s: ScheduleNextBeforeEtcd", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
			if _, err := qjm.arbclients.ArbV1().AppWrappers(qj.Namespace).Update(apiQueueJob); err != nil {
				glog.Errorf("Failed to update status of AppWrapper %v/%v: %v",
																	qj.Namespace, qj.Name, err)
			}
			queueJobKey,_:=GetQueueJobKey(qj)
			qjm.dispatchMap[queueJobKey]=agentId
			glog.V(10).Infof("[TTime] %s, %s: ScheduleNextAfterEtcd", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
			return
		} else {
			glog.V(2).Infof("[Controller: Dispatcher Mode] Cannot find an Agent with enough Resources\n")
			go qjm.backoff(qj)
		}
	} else {						// Agent Mode
		aggqj := qjm.GetAggregatedResources(qj)

		// HeadOfLine logic
		HOLStartTime := time.Now()
		dispatched := false
		// Try to dispatch for at most HeadOfLineHoldingTime
		for (!dispatched && time.Now().Before(HOLStartTime.Add(time.Duration(qjm.serverOption.HeadOfLineHoldingTime)*time.Second))) {
			priorityindex := qj.Status.SystemPriority
			// Preemption not allowed under DynamicPriority
			if (DynamicPriority || !Preemption) { priorityindex = -math.MaxFloat64 }
			resources := qjm.getAggregatedAvailableResourcesPriority(priorityindex, qj.Name)
	//		resources := qjm.getAggregatedAvailableResourcesPriority(qj.Status.SystemPriority, qj.Name)
			glog.V(2).Infof("[ScheduleNext] XQJ %s with resources %v to be scheduled on aggregated idle resources %v", qj.Name, aggqj, resources)

			if aggqj.LessEqual(resources) {
				// qj is ready to go!
				// Why get apiQueueJob from cache, instead of operating on qj directly?
				apiQueueJob, e := qjm.queueJobLister.AppWrappers(qj.Namespace).Get(qj.Name)
				if e != nil {
					return
				}
				if larger(apiQueueJob.ResourceVersion, qj.ResourceVersion) {
					glog.V(10).Infof("[ScheduleNext] %s found more recent copy from cache          &qj=%p          qj=%+v", qj.Name, qj, qj)
					glog.V(10).Infof("[ScheduleNext] %s found more recent copy from cache &apiQueueJob=%p apiQueueJob=%+v", apiQueueJob.Name, apiQueueJob, apiQueueJob)
					apiQueueJob.DeepCopyInto(qj)
				}
				desired := int32(0)
				for i, ar := range qj.Spec.AggrResources.Items {
					desired += ar.Replicas
					qj.Spec.AggrResources.Items[i].AllocatedReplicas = ar.Replicas
//					apiQueueJob.Spec.AggrResources.Items[i].AllocatedReplicas = ar.Replicas
				}
				// Important Update() of CanRun for Informer-Filter to pickup. Try 3 times
				qj.Status.CanRun = true
				qj.Status.FilterIgnore = true   // update CanRun & Spec
				qjm.updateEtcd(qj, "[ScheduleNext]setCanRun", SendUpdate)
				glog.V(10).Infof("[ScheduleNext] before eventQueue.Add %s: &qj=%p version=%s Status=%+v", qj.Name, qj, qj.ResourceVersion, qj.Status)
				qjm.eventQueue.Add(qj)  // add to eventQueue for dispatching to Etcd
				qjm.qjqueue.Delete(qj)
				dispatched = true
				glog.V(10).Infof("[ScheduleNext] after qjqueue delete %s activeQ=%t, Unsched=%t Status=%+v", qj.Name, qjm.qjqueue.IfExistActiveQ(qj), qjm.qjqueue.IfExistUnschedulableQ(qj), qj.Status)
				if Demo {  // Delay 9.9 sec for demo only
					time.Sleep(time.Millisecond * 9900)
				}
			} else {  // Not enough free resources to dispatch HOL
				glog.V(10).Infof("[ScheduleNext] HOL Blocking before AddUn by %s for %s activeQ=%t Unsched=%t", qj.Name, time.Now().Sub(HOLStartTime), qjm.qjqueue.IfExistActiveQ(qj), qjm.qjqueue.IfExistUnschedulableQ(qj))
				qjm.qjqueue.Delete(qj)  // Delete from activeQ in case other threads add it back
				qjm.qjqueue.AddUnschedulableIfNotPresent(qj)
				glog.V(10).Infof("[ScheduleNext] HOL Blocking after  AddUn by %s for %s activeQ=%t Unsched=%t", qj.Name, time.Now().Sub(HOLStartTime), qjm.qjqueue.IfExistActiveQ(qj), qjm.qjqueue.IfExistUnschedulableQ(qj))
				time.Sleep(time.Second * 1)  // Try to dispatch once per second
			}
		}
		if !dispatched {
//			} else {
			// start thread to backoff
			glog.V(10).Infof("[ScheduleNext] HOL backoff %s after waiting for %s activeQ=%t Unsched=%t", qj.Name, time.Now().Sub(HOLStartTime), qjm.qjqueue.IfExistActiveQ(qj), qjm.qjqueue.IfExistUnschedulableQ(qj))
			go qjm.backoff(qj)
		}
	}
}

// func (cc *XController)updateEtcd(qj, "setHeadOfLine", SendUpdate)
func (cc *XController) updateEtcd(qj *arbv1.AppWrapper, at string, send bool) error {
	if send {
		qj.Status.Sender = "before "+ at
		qj.Status.Local  = false
		if qjj, err := cc.arbclients.ArbV1().AppWrappers(qj.Namespace).Update(qj); err != nil {
			glog.Errorf("[updateEtcd] Failed to update status of AppWrapper %v at %s %v qj=%+v", qj.Name, at, err, qj)
			return err
		} else {
			qj.ResourceVersion = qjj.ResourceVersion
		}
		glog.V(10).Infof("[updateEtcd] AppWrapperUpdate success %s at %s &qj=%p qj=%+v", qj.Name, at, qj, qj)
		qj.Status.Local  = true
		qj.Status.Sender = "after  "+ at
	}
	return nil
}

func (qjm *XController) backoff(q *arbv1.AppWrapper) {
	SendUpdate := qjm.serverOption.SendUpdate

	q.Status.QueueJobState = arbv1.QueueJobStateRejoining
	q.Status.FilterIgnore = true  // update QueueJobState only, no work needed
	qjm.updateEtcd(q, "[backoff]Rejoining", SendUpdate)
	glog.V(10).Infof("[backoff] %s before AddUnsched for %d seconds, activeQ=%t Unsched=%t QueueJobState=%s", q.Name, qjm.serverOption.BackoffTime, qjm.qjqueue.IfExistActiveQ((q)), qjm.qjqueue.IfExistUnschedulableQ((q)), q.Status.QueueJobState)
	qjm.qjqueue.AddUnschedulableIfNotPresent(q)
	glog.V(10).Infof("[backoff] %s before sleep for %d seconds, activeQ=%t Unsched=%t QueueJobState=%s", q.Name, qjm.serverOption.BackoffTime, qjm.qjqueue.IfExistActiveQ((q)), qjm.qjqueue.IfExistUnschedulableQ((q)), q.Status.QueueJobState)
	time.Sleep(time.Duration(qjm.serverOption.BackoffTime) * time.Second)
	qjm.qjqueue.MoveToActiveQueueIfExists(q)
	q.Status.QueueJobState = arbv1.QueueJobStateQueueing
	q.Status.FilterIgnore = true  // update QueueJobState only, no work needed
	qjm.updateEtcd(q, "[backoff] Queueing", SendUpdate)
	glog.V(10).Infof("[backoff] %s after  sleep for %d seconds, activeQ=%t Unsched=%t QueueJobState=%s", q.Name, qjm.serverOption.BackoffTime, qjm.qjqueue.IfExistActiveQ((q)), qjm.qjqueue.IfExistUnschedulableQ((q)), q.Status.QueueJobState)
}

// Run start AppWrapper Controller
func (cc *XController) Run(stopCh chan struct{}) {
	// initialized
	createAppWrapperKind(cc.config)

	go cc.queueJobInformer.Informer().Run(stopCh)

	go cc.qjobResControls[arbv1.ResourceTypePod].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeService].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeDeployment].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeStatefulSet].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypePersistentVolume].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypePersistentVolumeClaim].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeNamespace].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeConfigMap].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeSecret].Run(stopCh)
	go cc.qjobResControls[arbv1.ResourceTypeNetworkPolicy].Run(stopCh)

	cache.WaitForCacheSync(stopCh, cc.queueJobSynced)

	cc.cache.Run(stopCh)

	// go wait.Until(cc.ScheduleNext, 2*time.Second, stopCh)
	go wait.Until(cc.ScheduleNext, 0, stopCh)
	// start preempt thread based on preemption of pods

	// TODO - scheduleNext...Job....
	// start preempt thread based on preemption of pods
	go wait.Until(cc.PreemptQueueJobs, 60*time.Second, stopCh)

	// This thread is used as a heartbeat to calculate runtime spec in the status
	go wait.Until(cc.UpdateQueueJobs, 5*time.Second, stopCh)

	if cc.isDispatcher {
		go wait.Until(cc.UpdateAgent, 2*time.Second, stopCh)			// In the Agent?
		for _, jobClusterAgent := range cc.agentMap {
			go jobClusterAgent.Run(stopCh)
		}
		go wait.Until(cc.agentEventQueueWorker, time.Second, stopCh)		// Update Agent Worker
	}

	// go wait.Until(cc.worker, time.Second, stopCh)
	go wait.Until(cc.worker, 0, stopCh)
}

func (qjm *XController) UpdateAgent() {
	glog.V(4).Infof("[Controller] Update AggrResources for All Agents\n")
	for _, jobClusterAgent := range qjm.agentMap {
		jobClusterAgent.UpdateAggrResources()
	}
}

func (qjm *XController) UpdateQueueJobs() {
	queueJobs, err := qjm.queueJobLister.AppWrappers("").List(labels.Everything())
	if err != nil {
		glog.Errorf("List of queueJobs %+v", err)
		return
	}
	for _, newjob := range queueJobs {
		glog.V(10).Infof("[UpdateQueueJobs] %s: qjqueue=%t &qj=%p version=%s Status=%+v", newjob.Name, qjm.qjqueue.IfExist(newjob), newjob, newjob.ResourceVersion, newjob.Status)
		if !qjm.qjqueue.IfExist(newjob) {
			glog.V(10).Infof("[TTime] %s, %s: UpdateQueueJobs delay: %s", time.Now().String(), newjob.Name, time.Now().Sub(newjob.CreationTimestamp.Time))
			qjm.enqueue(newjob)
		}
  	}
}

func (cc *XController) addQueueJob(obj interface{}) {
	firstTime := metav1.Now()
	qj, ok := obj.(*arbv1.AppWrapper)
	if !ok {
		glog.Errorf("obj is not AppWrapper")
		return
	}
	glog.V(10).Infof("[Informer-AddQJ] %s &qj=%p  qj=%+v", qj.Name, qj, qj)
	if qj.Status.QueueJobState == "" {
		qj.Status.ControllerFirstTimestamp = firstTime
		qj.Status.SystemPriority = qj.Spec.Priority
		qj.Status.QueueJobState  = arbv1.QueueJobStateInit
		glog.V(10).Infof("[Informer-AddQJ] now=%s %s CreationTimestamp=%s ControllerFirstTimestamp=%s 0Delay=%s",
			time.Now(), qj.Name, qj.CreationTimestamp, qj.Status.ControllerFirstTimestamp,
			time.Now().Sub(qj.Status.ControllerFirstTimestamp.Time))
	} else {
		glog.V(10).Infof("[Informer-AddQJ] now=%s %s CreationTimestamp=%s ControllerFirstTimestamp=%s *Delay=%s",
			time.Now(), qj.Name, qj.CreationTimestamp, qj.Status.ControllerFirstTimestamp,
			time.Now().Sub(qj.Status.ControllerFirstTimestamp.Time))
	}
	glog.V(10).Infof("[Informer-AddQJ] %s &qj=%p  qj=%+v", qj.Name, qj, qj)
	glog.V(4).Infof("[Informer-AddQJ] enqueue QueueJob %s Status=%+v", qj.Name, qj.Status)
	cc.enqueue(qj)
}

func (cc *XController) updateQueueJob(oldObj, newObj interface{}) {
	newQJ, ok := newObj.(*arbv1.AppWrapper)
	if !ok {
		glog.Errorf("[Informer-updateQJ] newObj is not AppWrapper")
		return
	}
	glog.V(10).Infof("[TTime] %s, %s: updateQueueJob delay: %s", time.Now().String(), newQJ.Name, time.Now().Sub(newQJ.CreationTimestamp.Time))
	oldQJ, ok := oldObj.(*arbv1.AppWrapper)
	if !ok {
		glog.Errorf("[Informer-updateQJ] oldObj is not AppWrapper.  enqueue(newQJ)")
		glog.V(10).Infof("[Informer-updateQJ] %s &newQJ=%p newQJ=%+v", newQJ.Name, newQJ, newQJ)
		cc.enqueue(newQJ)
		return
	}
	// AppWrappers may come out of order.  Ignore old ones.
	if (oldQJ.Name == newQJ.Name) && (larger(oldQJ.ResourceVersion, newQJ.ResourceVersion)) {
		glog.V(10).Infof("[Informer-updateQJ] ignore OutOfOrder arrival &oldQJ=%p oldQJ=%+v", oldQJ, oldQJ)
		glog.V(10).Infof("[Informer-updateQJ] ignore OutOfOrder arrival &newQJ=%p newQJ=%+v", newQJ, newQJ)
		return
	}
	glog.V(10).Infof("[Informer-updateQJ] %s &newQJ=%p newQJ=%+v", newQJ.Name, newQJ, newQJ)
	cc.enqueue(newQJ)
}

// a, b arbitrary length numerical string.  returns true if a larger than b
func larger (a, b string) bool {
	if len(a) > len(b) { return true  } // Longer string is larger
	if len(a) < len(b) { return false } // Longer string is larger
	return a > b // Equal length, lexicographic order
}

func (cc *XController) deleteQueueJob(obj interface{}) {
	qj, ok := obj.(*arbv1.AppWrapper)
	if !ok {
		glog.Errorf("obj is not AppWrapper")
		return
	}
	glog.V(10).Infof("[TTime] %s, %s: deleteQueueJob delay: %s", time.Now().String(), qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
	cc.enqueue(qj)
}

func (cc *XController) enqueue(obj interface{}) {
	qj, ok := obj.(*arbv1.AppWrapper)
	if !ok {
		glog.Errorf("obj is not AppWrapper")
		return
	}

	glog.V(10).Infof("[enqueue] before eventQueue.Add %s Version=%s Local=%t Sender=%s &qj=%p qj=%+v", qj.Name, qj.ResourceVersion, qj.Status.Local, qj.Status.Sender, qj, qj)

	err := cc.eventQueue.Add(obj)

	if err != nil {
		glog.Errorf("Fail to enqueue AppWrapper to updateQueue, err %#v", err)
	}
}

func (cc *XController) agentEventQueueWorker() {
	if _, err := cc.agentEventQueue.Pop(func(obj interface{}) error {
		var queuejob *arbv1.AppWrapper
		switch v := obj.(type) {
		case *arbv1.AppWrapper:
			queuejob = v
		default:
			glog.Errorf("Un-supported type of %v", obj)
			return nil
		}

		if queuejob == nil {
			if acc, err := meta.Accessor(obj); err != nil {
				glog.Warningf("Failed to get AppWrapper for %v/%v", acc.GetNamespace(), acc.GetName())
			}

			return nil
		}
		glog.V(4).Infof("[Controller: Dispatcher Mode] XQJ Status Update from AGENT: Name:%s, Status: %+v\n", queuejob.Name, queuejob.Status)


		// sync AppWrapper
		if err := cc.updateQueueJobStatus(queuejob); err != nil {
			glog.Errorf("Failed to sync AppWrapper %s, err %#v", queuejob.Name, err)
			// If any error, requeue it.
			return err
		}

		return nil
	}); err != nil {
		glog.Errorf("Fail to pop item from updateQueue, err %#v", err)
		return
	}
}

func (cc *XController) updateQueueJobStatus(queueJobFromAgent *arbv1.AppWrapper) error {
	queueJobInEtcd, err := cc.queueJobLister.AppWrappers(queueJobFromAgent.Namespace).Get(queueJobFromAgent.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if (cc.isDispatcher) {
				cc.Cleanup(queueJobFromAgent)
				cc.qjqueue.Delete(queueJobFromAgent)
			}
			return nil
		}
		return err
	}
	if(len(queueJobFromAgent.Status.State)==0 || queueJobInEtcd.Status.State == queueJobFromAgent.Status.State) {
		return nil
	}
	new_flag := queueJobFromAgent.Status.State
	queueJobInEtcd.Status.State = new_flag
	_, err = cc.arbclients.ArbV1().AppWrappers(queueJobInEtcd.Namespace).Update(queueJobInEtcd)
	if err != nil {
		return err
	}
	return nil
}


func (cc *XController) worker() {
	if _, err := cc.eventQueue.Pop(func(obj interface{}) error {
		var queuejob *arbv1.AppWrapper
		switch v := obj.(type) {
		case *arbv1.AppWrapper:
			queuejob = v
		default:
			glog.Errorf("Un-supported type of %v", obj)
			return nil
		}
//		glog.V(10).Infof("[TTime] %s: %s, WorkerFromEventQueue delay: %s - Pop Begin", time.Now().String(), queuejob.Name, time.Now().Sub(queuejob.CreationTimestamp.Time))
//		glog.V(10).Infof("[worker] eventQueue len=%d Pop begin %s: &qj=%p version=%s Status=%+v", len(cc.eventQueue.ListKeys()), queuejob.Name, queuejob, queuejob.ResourceVersion, queuejob.Status)
		glog.V(10).Infof("[worker] after eventQueue.Pop %s: &qj=%p version=%s Status=%+v", queuejob.Name, queuejob, queuejob.ResourceVersion, queuejob.Status)

		if queuejob == nil {
			if acc, err := meta.Accessor(obj); err != nil {
				glog.Warningf("Failed to get AppWrapper for %v/%v", acc.GetNamespace(), acc.GetName())
			}

			return nil
		}
		// sync AppWrapper
		glog.V(10).Infof("[worker] before syncQueueJob %s &qj=%p qj=%+v", queuejob.Name, queuejob, queuejob)
		if err := cc.syncQueueJob(queuejob); err != nil {
			glog.Errorf("Failed to sync AppWrapper %s, err %#v", queuejob.Name, err)
			// If any error, requeue it.
			return err
		}
		glog.V(10).Infof("[worker] after syncQueueJob %s &qj=%p qj=%+v", queuejob.Name, queuejob, queuejob)

//		glog.V(10).Infof("[TTime] %s: %s, WorkerFromEventQueue delay: %s - Pop End", time.Now().String(), queuejob.Name, time.Now().Sub(queuejob.CreationTimestamp.Time))
		return nil
	}); err != nil {
		glog.Errorf("[worker] Fail to pop item from eventQueue, err %#v", err)
		return
	}
}

func (cc *XController) syncQueueJob(qj *arbv1.AppWrapper) error {
	SendUpdate := cc.serverOption.SendUpdate
//	queueJob, err := cc.queueJobLister.AppWrappers(qj.Namespace).Get(qj.Name)
	_, err := cc.queueJobLister.AppWrappers(qj.Namespace).Get(qj.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if (cc.isDispatcher) {
				cc.Cleanup(qj)
				cc.qjqueue.Delete(qj)
			}
			return nil
		}
		return err
	}

	// If it is Agent (not a dispatcher), update pod information
	if(!cc.isDispatcher){
		// we call sync for each controller
	  // update pods running, pending,...
		glog.V(10).Infof("[worker-syncQJ] %s before UpdateQueueJobStatus version=%s State=%v", qj.Name, qj.ResourceVersion, qj.Status)
		cc.qjobResControls[arbv1.ResourceTypePod].UpdateQueueJobStatus(qj)
		glog.V(10).Infof("[worker-syncQJ] %s after  UpdateQueueJobStatus version=%s State=%v", qj.Name, qj.ResourceVersion, qj.Status)
		if (qj.Status.Running > 0) {  // set QueueJobStateRunning if at least one resource running
			qj.Status.QueueJobState = arbv1.QueueJobStateRunning
			qj.Status.FilterIgnore = true  // Update QueueJobStateRunning
			cc.updateEtcd(qj, "[syncQueueJob]setRunning", SendUpdate)
		}
	}
	glog.V(10).Infof("[worker-syncQJ] %s activeQ=%t Unsched=%t", qj.Name, cc.qjqueue.IfExistActiveQ(qj), cc.qjqueue.IfExistUnschedulableQ(qj))
	glog.V(10).Infof("[worker-SyncQJ] before manageQueueJob %s &qj=%p qj=%+v", qj.Name, qj, qj)
	err = cc.manageQueueJob(qj)
	glog.V(10).Infof("[worker-SyncQJ] after  manageQueueJob %s &qj=%p qj=%+v err=%v", qj.Name, qj, qj, err)
	return err
//	return cc.manageQueueJob(queueJob)
}

// manageQueueJob is the core method responsible for managing the number of running
// pods according to what is specified in the job.Spec.
// Does NOT modify <activePods>.
func (cc *XController) manageQueueJob(qj *arbv1.AppWrapper) error {
	var err error
	SendUpdate := cc.serverOption.SendUpdate
//	startTime := time.Now()
	defer func() {
//		glog.V(4).Infof("[Controller] Finished syncing queue job %q (%v)", qj.Name, time.Now().Sub(startTime))
		glog.V(10).Infof("[worker-manageQJ] ends %s version=%s Status=%+v &qj=%p qj=%+v", qj.Name, qj.ResourceVersion, qj.Status, qj, qj)
	}()
	glog.V(10).Infof("[worker-manageQJ] starts %s version=%s Status=%+v &qj=%p qj=%+v", qj.Name, qj.ResourceVersion, qj.Status, qj, qj)

	if(!cc.isDispatcher) { // Agent Mode

		if qj.DeletionTimestamp != nil {

			// cleanup resources for running job
			err = cc.Cleanup(qj)
			if err != nil {
				return err
			}
			//empty finalizers and delete the queuejob again
			accessor, err := meta.Accessor(qj)
			if err != nil {
				return err
			}
			accessor.SetFinalizers(nil)

			// we delete the job from the queue if it is there
			cc.qjqueue.Delete(qj)

			return nil
			//var result arbv1.AppWrapper
			//return cc.arbclients.Put().
			//	Namespace(qj.Namespace).Resource(arbv1.QueueJobPlural).
			//	Name(qj.Name).Body(qj).Do().Into(&result)
		}

		// glog.Infof("I have job with name %s status %+v ", qj.Name, qj.Status)
		// First execution of qj to set Status.State = Enqueued
		if !qj.Status.CanRun && (qj.Status.State != arbv1.AppWrapperStateEnqueued && qj.Status.State != arbv1.AppWrapperStateDeleted) {
			// if there are running resources for this job then delete them because the job was put in
			// pending state...
			glog.V(2).Infof("[Agent Mode] Before Cleanup &qj=%p, qj=%+v", qj, qj)
			glog.V(2).Infof("[Agent Mode] Deleting resources for XQJ %s because it will be preempted (newjob)\n", qj.Name)
			err = cc.Cleanup(qj)
			if err != nil {
				return err
			}

			qj.Status.State = arbv1.AppWrapperStateEnqueued
			glog.V(10).Infof("[TTime] %s, %s: WorkerBeforeEtcd", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
/*			qjj, err := cc.arbclients.ArbV1().AppWrappers(qj.Namespace).Update(qj)
			if err != nil {
				return err
			}
			return nil
		}  // End of first execution of qj, result set Status.State = Enqueued
	//  Combine first and second execution of qj
		// Second execution of qj to add to qjqueue for ScheduleNext
		if !qj.Status.CanRun && qj.Status.State == arbv1.AppWrapperStateEnqueued {
			cc.qjqueue.AddIfNotPresent(qj)
*/
			//  add qj to qjqueue only when it is not in UnschedulableQ
			if cc.qjqueue.IfExistUnschedulableQ(qj) {
				glog.V(10).Infof("[worker-manageQJ] leaving %s to qjqueue.UnschedulableQ, QueueJobState=%s", qj.Name, qj.Status.QueueJobState)
			} else {
				glog.V(10).Infof("[worker-manageQJ] before add to activeQ %s version=%s activeQ=%t Unsched=%t QueueJobState=%s", qj.Name, qj.ResourceVersion, cc.qjqueue.IfExistActiveQ(qj), cc.qjqueue.IfExistUnschedulableQ(qj), qj.Status.QueueJobState)
				qj.Status.QueueJobState  = arbv1.QueueJobStateQueueing
				qj.Status.FilterIgnore = true  // Update Queueing status, add to qjqueue for ScheduleNext
				cc.updateEtcd(qj, "[manageQueueJob]setQueueing", SendUpdate)
				cc.qjqueue.AddIfNotPresent(qj)
				glog.V(10).Infof("[worker-manageQJ] after  add to activeQ %s 1Delay=%s version=%s activeQ=%t Unsched=%t QueueJobState=%s",
					qj.Name, metav1.Now().Sub(qj.Status.ControllerFirstTimestamp.Time), qj.ResourceVersion, cc.qjqueue.IfExistActiveQ(qj), cc.qjqueue.IfExistUnschedulableQ(qj), qj.Status.QueueJobState)
			}
			return nil
		} // End of second execution of qj to add to qjqueue for ScheduleNext

		// Third execution of qj to add qj to Etcd for dispatch
		is_first_deployment := false
		glog.V(10).Infof("[worker-manageQJ] before deployment Version=%s Local=%t Sender=%s &qj=%p qj=%+v", qj.ResourceVersion, qj.Status.Local, qj.Status.Sender, qj, qj)
		if qj.Status.CanRun && qj.Status.State != arbv1.AppWrapperStateActive {
			is_first_deployment = true
			qj.Status.State = arbv1.AppWrapperStateActive
		// Bugfix to eliminate performance problem of overloading the event queue.}

			if qj.Spec.AggrResources.Items != nil {
				for i := range qj.Spec.AggrResources.Items {
					err := cc.refManager.AddTag(&qj.Spec.AggrResources.Items[i], func() string {
						return strconv.Itoa(i)
					})
					if err != nil {
						return err
					}
				}
			}
			if glog.V(10) {
				if (is_first_deployment) {
					current_time := time.Now()
//					glog.V(10).Infof("[TTime] %s, %s: WorkerBeforeCreatingResouces", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
					glog.V(10).Infof("[Agent Controller] XQJ %s has Overhead Before Creating Resouces: %s", qj.Name, current_time.Sub(qj.CreationTimestamp.Time))
				}
			}
			glog.V(10).Infof("[worker-manageQJ] before [ar.Type].SyncQueueJob Version=%s Local=%t Sender=%s &qj=%p qj=%+v", qj.ResourceVersion, qj.Status.Local, qj.Status.Sender, qj, qj)
			glog.V(4).Infof("[worker-manageQJ] before dispatching to Etcd %s 2Delay=%s version=%s Status=%+v",
				qj.Name, metav1.Now().Sub(qj.Status.ControllerFirstTimestamp.Time), qj.ResourceVersion, qj.Status)
			success := false
			for _, ar := range qj.Spec.AggrResources.Items {
				glog.V(10).Infof("[worker-manageQJ] before dispatch [%v].SyncQueueJob %s version=%s Status=%+v",
					ar.Type, qj.Name, qj.ResourceVersion, qj.Status)
//				Call Resource Controller of ar.Type to issue REST call to Etcd for resource creation
				err00 := cc.qjobResControls[ar.Type].SyncQueueJob(qj, &ar)
				if err00 != nil {
					glog.V(4).Infof("Error from sync job: %v", err00)
				} else {
					success = true
				}
			}
			if success { // set QueueJobStateRunning if at least one resource is successfully dispatched
				qj.Status.QueueJobState = arbv1.QueueJobStateDispatched
			}
			if glog.V(10) {
				if (is_first_deployment) {
					glog.V(4).Infof("[worker-manageQJ] %s, 3Delay=%s: WorkerAfterCreatingResouces", qj.Name, metav1.Now().Sub(qj.Status.ControllerFirstTimestamp.Time))
					glog.V(10).Infof("[worker-manageQJ] After  [ar.Type].SyncQueueJob %s version=%s Status=%+v", qj.Name, qj.ResourceVersion, qj.Status)
				}
			}

			// TODO(k82cn): replaced it with `UpdateStatus`
//			cc.qjobResControls[ar.Type].SyncQueueJob(qj, &ar) could have updated Etcd
			qj.Status.FilterIgnore = false  // keep original behavior
			if err := cc.updateEtcd(qj, "[manageQueueJob]after[ar.Type].SyncQueueJob", SendUpdate); err != nil {
				return err
			}
		} // Bugfix to eliminate performance problem of overloading the event queue.
		// Third execution of qj to add qj to Etcd for dispatch

	}	else { 				// Dispatcher Mode

		if qj.DeletionTimestamp != nil {
			// cleanup resources for running job
			err = cc.Cleanup(qj)
			if err != nil {
				return err
			}
			//empty finalizers and delete the queuejob again
			accessor, err := meta.Accessor(qj)
			if err != nil {
				return err
			}
			accessor.SetFinalizers(nil)

			cc.qjqueue.Delete(qj)

			return nil
		}

		if !qj.Status.CanRun && (qj.Status.State != arbv1.AppWrapperStateEnqueued && qj.Status.State != arbv1.AppWrapperStateDeleted) {
			// if there are running resources for this job then delete them because the job was put in
			// pending state...
			glog.V(4).Infof("Deleting queuejob resources because it will be preempted! %s", qj.Name)
			err = cc.Cleanup(qj)
			if err != nil {
				return err
			}

			qj.Status.State = arbv1.AppWrapperStateEnqueued
			qj.Status.FilterIgnore = false   // keep original behavior
			err = cc.updateEtcd(qj, "[manageQueueJob]setStateEnqueued", SendUpdate)
//			_, err = cc.arbclients.ArbV1().AppWrappers(qj.Namespace).Update(qj)
			if err != nil {
				return err
			}
			return nil
		}

		// if !qj.Status.CanRun && qj.Status.State == arbv1.QueueJobStateEnqueued {
		if !qj.Status.CanRun && qj.Status.State == arbv1.AppWrapperStateEnqueued {
			cc.qjqueue.AddIfNotPresent(qj)
			return nil
		}

		if qj.Status.CanRun && !qj.Status.IsDispatched{

			if glog.V(10) {
			current_time:=time.Now()
			glog.V(10).Infof("[Dispatcher Controller] XQJ %s has Overhead Before Dispatching: %s", qj.Name,current_time.Sub(qj.CreationTimestamp.Time))
			glog.V(10).Infof("[TTime] %s, %s: WorkerBeforeDispatch", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
			}

			qj.Status.IsDispatched = true
			queuejobKey, _:=GetQueueJobKey(qj)
			// obj:=cc.dispatchMap[queuejobKey]
			// if obj!=nil {
			if obj, ok:=cc.dispatchMap[queuejobKey]; ok {
				cc.agentMap[obj].CreateJob(qj)
			}
			if glog.V(10) {
			current_time:=time.Now()
			glog.V(10).Infof("[Dispatcher Controller] XQJ %s has Overhead After Dispatching: %s", qj.Name,current_time.Sub(qj.CreationTimestamp.Time))
			glog.V(10).Infof("[TTime] %s, %s: WorkerAfterDispatch", qj.Name, time.Now().Sub(qj.CreationTimestamp.Time))
			}
			qj.Status.FilterIgnore = true   // update QueueJobState only
			cc.updateEtcd(qj, "[manageQueueJob]agentMap[obj].CreateJob", SendUpdate)
		}

	}
	return err
}

//Cleanup function
func (cc *XController) Cleanup(queuejob *arbv1.AppWrapper) error {
	glog.V(4).Infof("[Cleanup] begin AppWrapper %s version=%s Status=%+v\n", queuejob.Name, queuejob.ResourceVersion, queuejob.Status)
	if !cc.isDispatcher {
		if queuejob.Spec.AggrResources.Items != nil {
			// we call clean-up for each controller
			for _, ar := range queuejob.Spec.AggrResources.Items {
				cc.qjobResControls[ar.Type].Cleanup(queuejob, &ar)
			}
		}
	} else {
		// glog.Infof("[Dispatcher] Cleanup: State=%s\n", queuejob.Status.State)
		if queuejob.Status.CanRun && queuejob.Status.IsDispatched {
			queuejobKey, _:=GetQueueJobKey(queuejob)
			if obj, ok:=cc.dispatchMap[queuejobKey]; ok {
				cc.agentMap[obj].DeleteJob(queuejob)
			}
		}
	}

	queuejob.Status.Pending      = 0
	queuejob.Status.Running      = 0
	queuejob.Status.Succeeded    = 0
	queuejob.Status.Failed       = 0
	glog.V(10).Infof("[Cleanup] end   AppWrapper %s version=%s Status=%+v\n", queuejob.Name, queuejob.ResourceVersion, queuejob.Status)

	return nil
}
