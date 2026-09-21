/*
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

package instancestatus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	instancestatusprovider "github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

const reconcileInterval = time.Minute

type unhealthyKey struct {
	instanceID string
	category   instancestatusprovider.Category
}

type assessmentResult struct {
	category instancestatusprovider.Category
	statuses map[string]instancestatusprovider.HealthStatus
	err      error
}

type scanResult struct {
	assessment assessmentResult
	err        error
}

type registeredNode struct {
	instanceID string
	node       *corev1.Node
}

// Controller publishes EC2 instance and system reachability assessments as Node health.
type Controller struct {
	kubeClient client.Client
	provider   instancestatusprovider.Provider
	clock      clock.Clock
	seen       map[unhealthyKey]struct{}
	mu         sync.Mutex
}

func NewController(kubeClient client.Client, clk clock.Clock, provider instancestatusprovider.Provider) *Controller {
	return &Controller{
		kubeClient: kubeClient,
		provider:   provider,
		clock:      clk,
		seen:       make(map[unhealthyKey]struct{}),
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, "instancestatus")

	instanceAssessment, instanceErr, systemAssessment, systemErr := c.scanHealth(ctx)
	observationTime := c.clock.Now()
	currentKeys := make(map[unhealthyKey]struct{})

	canPrune, err := c.publishConditions(ctx, instanceAssessment, systemAssessment, observationTime, currentKeys)
	if canPrune {
		processed := make(map[instancestatusprovider.Category]struct{}, 2)
		for _, assessment := range []assessmentResult{instanceAssessment, systemAssessment} {
			if assessment.err == nil {
				processed[assessment.category] = struct{}{}
			}
		}
		c.pruneSeen(currentKeys, processed)
	}

	if errs := multierr.Combine(err, instanceErr, systemErr); errs != nil {
		return reconciler.Result{}, errs
	}
	return reconciler.Result{RequeueAfter: reconcileInterval}, nil
}

func (c *Controller) scanHealth(ctx context.Context) (assessmentResult, error, assessmentResult, error) {
	scan := func(category instancestatusprovider.Category) <-chan scanResult {
		results := make(chan scanResult, 1)
		go func() {
			assessment, err := c.scan(ctx, category)
			results <- scanResult{assessment: assessment, err: err}
		}()
		return results
	}

	instanceResult := scan(instancestatusprovider.InstanceStatus)
	systemResult := scan(instancestatusprovider.SystemStatus)
	instanceAssessment := <-instanceResult
	systemAssessment := <-systemResult
	return instanceAssessment.assessment, instanceAssessment.err, systemAssessment.assessment, systemAssessment.err
}

func (c *Controller) scan(ctx context.Context, category instancestatusprovider.Category) (assessmentResult, error) {
	statuses, err := c.provider.List(ctx, category)
	result := assessmentResult{
		category: category,
		statuses: map[string]instancestatusprovider.HealthStatus{},
		err:      err,
	}
	if err != nil {
		if awserrors.IsUnauthorizedOperationError(err) {
			return result, fmt.Errorf("ec2:DescribeInstanceStatus permission is not allowed for %s checks; grant the permission and Karpenter will retry automatically, %w", category, err)
		}
		return result, fmt.Errorf("getting EC2 %s checks, %w", category, err)
	}
	for _, status := range statuses {
		result.statuses[status.InstanceID] = status
	}
	return result, nil
}

func (c *Controller) publishConditions(
	ctx context.Context,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
	currentKeys map[unhealthyKey]struct{},
) (bool, error) {
	instanceComplete := instanceAssessment.err == nil
	systemComplete := systemAssessment.err == nil
	if !instanceComplete && !systemComplete {
		return true, nil
	}
	if (!instanceComplete && len(systemAssessment.statuses) == 0) ||
		(!systemComplete && len(instanceAssessment.statuses) == 0) {
		return true, nil
	}

	nodes, err := c.registeredNodes(ctx)
	if err != nil {
		return false, fmt.Errorf("listing registered managed Nodes, %w", err)
	}

	errs := make([]error, len(nodes))
	workqueue.ParallelizeUntil(ctx, 10, len(nodes), func(i int) {
		errs[i] = c.publishCondition(ctx, nodes[i].node, nodes[i].instanceID, instanceAssessment, systemAssessment, observationTime, currentKeys)
	})
	return true, multierr.Combine(errs...)
}

func (c *Controller) publishCondition(
	ctx context.Context,
	node *corev1.Node,
	instanceID string,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
	currentKeys map[unhealthyKey]struct{},
) error {
	instanceHealth, instanceImpaired := instanceAssessment.statuses[instanceID]
	systemHealth, systemImpaired := systemAssessment.statuses[instanceID]
	if instanceImpaired {
		c.recordUnhealthyInstance(ctx, instanceID, instancestatusprovider.InstanceStatus, currentKeys)
	}
	if systemImpaired {
		c.recordUnhealthyInstance(ctx, instanceID, instancestatusprovider.SystemStatus, currentKeys)
	}

	instanceComplete := instanceAssessment.err == nil
	systemComplete := systemAssessment.err == nil
	if !instanceImpaired && !systemImpaired && (!instanceComplete || !systemComplete) {
		return nil
	}
	condition := conditionForAssessments(
		node,
		instanceHealth,
		instanceImpaired,
		instanceComplete,
		systemHealth,
		systemImpaired,
		systemComplete,
		observationTime,
	)
	if err := c.patchCondition(ctx, node, condition); err != nil {
		return fmt.Errorf("patching Node %q EC2 status condition, %w", node.Name, err)
	}
	return nil
}

func (c *Controller) registeredNodes(ctx context.Context) ([]registeredNode, error) {
	type nodeClaimListResult struct {
		list *karpv1.NodeClaimList
		err  error
	}
	type nodeListResult struct {
		list *corev1.NodeList
		err  error
	}

	nodeClaimsResult := make(chan nodeClaimListResult, 1)
	go func() {
		nodeClaims := &karpv1.NodeClaimList{}
		err := c.kubeClient.List(ctx, nodeClaims, client.UnsafeDisableDeepCopy)
		nodeClaimsResult <- nodeClaimListResult{list: nodeClaims, err: err}
	}()
	nodesResult := make(chan nodeListResult, 1)
	go func() {
		nodes := &corev1.NodeList{}
		err := c.kubeClient.List(ctx, nodes, client.UnsafeDisableDeepCopy)
		nodesResult <- nodeListResult{list: nodes, err: err}
	}()

	nodeClaims := <-nodeClaimsResult
	nodes := <-nodesResult
	if nodeClaims.err != nil {
		return nil, nodeClaims.err
	}
	if nodes.err != nil {
		return nil, nodes.err
	}
	return registeredNodesFor(nodeClaims.list.Items, nodes.list.Items), nil
}

func registeredNodesFor(nodeClaims []karpv1.NodeClaim, nodes []corev1.Node) []registeredNode {
	registeredInstanceIDs := make(map[string]struct{}, len(nodeClaims))
	registeredProviderIDs := make(map[string]string, len(nodeClaims))
	for i := range nodeClaims {
		nodeClaim := &nodeClaims[i]
		if nodeClaim.Spec.NodeClassRef == nil ||
			nodeClaim.Spec.NodeClassRef.Group != apis.Group ||
			nodeClaim.Spec.NodeClassRef.Kind != "EC2NodeClass" ||
			!nodeClaimIsRegistered(nodeClaim) {
			continue
		}
		instanceID, err := utils.ParseInstanceID(nodeClaim.Status.ProviderID)
		if err != nil {
			continue
		}
		registeredInstanceIDs[instanceID] = struct{}{}
		registeredProviderIDs[nodeClaim.Status.ProviderID] = instanceID
	}

	registeredNodes := make([]registeredNode, 0, min(len(nodes), len(registeredInstanceIDs)))
	for i := range nodes {
		node := &nodes[i]
		if instanceID, ok := registeredProviderIDs[node.Spec.ProviderID]; ok {
			registeredNodes = append(registeredNodes, registeredNode{instanceID: instanceID, node: node})
			continue
		}
		instanceID, err := utils.ParseInstanceID(node.Spec.ProviderID)
		if err != nil {
			continue
		}
		if _, ok := registeredInstanceIDs[instanceID]; ok {
			registeredNodes = append(registeredNodes, registeredNode{instanceID: instanceID, node: node})
		}
	}
	return registeredNodes
}

func nodeClaimIsRegistered(nodeClaim *karpv1.NodeClaim) bool {
	for i := range nodeClaim.Status.Conditions {
		condition := &nodeClaim.Status.Conditions[i]
		if condition.Type == karpv1.ConditionTypeRegistered {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}

func conditionForAssessments(
	node *corev1.Node,
	instanceHealth instancestatusprovider.HealthStatus,
	instanceImpaired bool,
	instanceComplete bool,
	systemHealth instancestatusprovider.HealthStatus,
	systemImpaired bool,
	systemComplete bool,
	observationTime time.Time,
) corev1.NodeCondition {
	status := corev1.ConditionFalse
	reason := instancestatusprovider.ReasonNoImpairmentReported
	message := "EC2 reports no recognized instance or system reachability impairment."
	transitionTime := observationTime
	if instanceImpaired || systemImpaired {
		status = corev1.ConditionTrue
		reason = instancestatusprovider.ReasonReachabilityFailed
		message = impairmentMessage(instanceImpaired, instanceComplete, systemImpaired, systemComplete)
		transitionTime = earliestImpairedSince(observationTime, instanceHealth, instanceImpaired, systemHealth, systemImpaired)
	}

	current := conditionForType(node, instancestatusprovider.ConditionTypeEC2StatusImpaired)
	if current != nil && current.Status == status {
		transitionTime = current.LastTransitionTime.Time
		reason = current.Reason
	}
	return corev1.NodeCondition{
		Type:               instancestatusprovider.ConditionTypeEC2StatusImpaired,
		Status:             status,
		LastTransitionTime: metav1.Time{Time: transitionTime},
		Reason:             reason,
		Message:            message,
	}
}

func impairmentMessage(instanceImpaired, instanceComplete, systemImpaired, systemComplete bool) string {
	var message string
	switch {
	case instanceImpaired && systemImpaired:
		message = "EC2 instance and system reachability checks are failing."
	case instanceImpaired:
		message = "EC2 instance reachability check is failing."
	case systemImpaired:
		message = "EC2 system reachability check is failing."
	}
	if !instanceComplete {
		message += " The EC2 instance status assessment did not complete."
	}
	if !systemComplete {
		message += " The EC2 system status assessment did not complete."
	}
	return message
}

func earliestImpairedSince(
	fallback time.Time,
	instanceHealth instancestatusprovider.HealthStatus,
	instanceImpaired bool,
	systemHealth instancestatusprovider.HealthStatus,
	systemImpaired bool,
) time.Time {
	earliest := fallback
	found := false
	for _, health := range []struct {
		status   instancestatusprovider.HealthStatus
		impaired bool
	}{
		{status: instanceHealth, impaired: instanceImpaired},
		{status: systemHealth, impaired: systemImpaired},
	} {
		if !health.impaired || health.status.ImpairedSince.IsZero() {
			continue
		}
		if !found || health.status.ImpairedSince.Before(earliest) {
			earliest = health.status.ImpairedSince
			found = true
		}
	}
	return earliest
}

func conditionForType(node *corev1.Node, conditionType corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == conditionType {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

func (c *Controller) patchCondition(ctx context.Context, node *corev1.Node, condition corev1.NodeCondition) error {
	current := conditionForType(node, condition.Type)
	if current != nil && equality.Semantic.DeepEqual(*current, condition) {
		return nil
	}

	updated := node.DeepCopy()
	replaced := false
	for i := range updated.Status.Conditions {
		if updated.Status.Conditions[i].Type == condition.Type {
			updated.Status.Conditions[i] = condition
			replaced = true
			break
		}
	}
	if !replaced {
		updated.Status.Conditions = append(updated.Status.Conditions, condition)
	}
	return client.IgnoreNotFound(c.kubeClient.Status().Patch(ctx, updated, client.StrategicMergeFrom(node)))
}

func (c *Controller) recordUnhealthyInstance(
	ctx context.Context,
	instanceID string,
	category instancestatusprovider.Category,
	currentKeys map[unhealthyKey]struct{},
) {
	key := unhealthyKey{instanceID: instanceID, category: category}
	c.mu.Lock()
	currentKeys[key] = struct{}{}
	_, already := c.seen[key]
	if !already {
		c.seen[key] = struct{}{}
	}
	c.mu.Unlock()
	if !already {
		log.FromContext(ctx).Info("detected unhealthy instance owned by cluster",
			"instanceID", instanceID,
			"category", string(category))
		instancestatusprovider.UnhealthyTotal.Inc(map[string]string{instancestatusprovider.CategoryLabel.Name: string(category)})
	}
}

func (c *Controller) pruneSeen(currentKeys map[unhealthyKey]struct{}, processed map[instancestatusprovider.Category]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.seen {
		if _, ok := processed[key.category]; !ok {
			continue
		}
		if _, ok := currentKeys[key]; !ok {
			delete(c.seen, key)
		}
	}
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("instancestatus").
		WatchesRawSource(singleton.Source()).
		WithOptions(controller.Options{
			RateLimiter: rateLimiter(),
		}).
		Complete(singleton.AsReconciler(c))
}

func rateLimiter() workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](reconcileInterval, reconcileInterval)
}
