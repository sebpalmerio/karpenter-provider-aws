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
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
	instancestatusmsg "github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages/instancestatus"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	instancestatusprovider "github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

const reconcileInterval = time.Minute

const (
	impairedConditionMessage = "EC2 reports a reachability impairment."
	healthyConditionMessage  = "EC2 reports no reachability impairment."
)

type assessmentResult struct {
	statusesByInstanceID map[string]instancestatusprovider.HealthStatus
	err                  error
}

type registeredNode struct {
	instanceID string
	node       *corev1.Node
}

type interruptionObservation struct {
	instanceID    string
	kind          messages.Kind
	impairedSince time.Time
}

// Controller publishes EC2 instance and system reachability assessments as Node health.
// When configured, it also preserves direct interruption remediation for clusters with NodeRepair disabled.
type Controller struct {
	kubeClient                client.Client
	provider                  instancestatusprovider.Provider
	clock                     clock.Clock
	legacyInterruptionHandler func(context.Context, messages.Message) error
}

func NewController(
	kubeClient client.Client,
	clk clock.Clock,
	provider instancestatusprovider.Provider,
	legacyInterruptionHandler func(context.Context, messages.Message) error,
) *Controller {
	return &Controller{
		kubeClient:                kubeClient,
		provider:                  provider,
		clock:                     clk,
		legacyInterruptionHandler: legacyInterruptionHandler,
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, "instancestatus")

	instanceAssessment, systemAssessment, scanErr := c.scanHealth(ctx)
	observationTime := c.clock.Now()

	publishErr := c.publishConditions(ctx, instanceAssessment, systemAssessment, observationTime)
	interruptionErr := c.handleLegacyInterruption(ctx, instanceAssessment, systemAssessment, observationTime)
	if errs := multierr.Combine(publishErr, interruptionErr, scanErr); errs != nil {
		return reconciler.Result{}, errs
	}
	return reconciler.Result{RequeueAfter: reconcileInterval}, nil
}

func (c *Controller) scanHealth(ctx context.Context) (assessmentResult, assessmentResult, error) {
	scan := func(category instancestatusprovider.Category) <-chan assessmentResult {
		results := make(chan assessmentResult, 1)
		go func() {
			results <- c.scan(ctx, category)
		}()
		return results
	}

	instanceResult := scan(instancestatusprovider.InstanceStatus)
	systemResult := scan(instancestatusprovider.SystemStatus)
	instanceAssessment := <-instanceResult
	systemAssessment := <-systemResult
	return instanceAssessment, systemAssessment, multierr.Combine(instanceAssessment.err, systemAssessment.err)
}

func (c *Controller) scan(ctx context.Context, category instancestatusprovider.Category) assessmentResult {
	statuses, err := c.provider.List(ctx, category)
	result := assessmentResult{
		statusesByInstanceID: map[string]instancestatusprovider.HealthStatus{},
	}
	if err != nil {
		if awserrors.IsUnauthorizedOperationError(err) {
			result.err = fmt.Errorf("ec2:DescribeInstanceStatus permission is not allowed for %s checks; grant the permission and Karpenter will retry automatically, %w", category, err)
			return result
		}
		result.err = fmt.Errorf("getting EC2 %s checks, %w", category, err)
		return result
	}
	for _, status := range statuses {
		result.statusesByInstanceID[status.InstanceID] = status
	}
	return result
}

func (c *Controller) handleLegacyInterruption(
	ctx context.Context,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
) error {
	if c.legacyInterruptionHandler == nil {
		return nil
	}

	observations := legacyInterruptionObservations(instanceAssessment, systemAssessment, observationTime)
	errs := make([]error, len(observations))
	workqueue.ParallelizeUntil(ctx, 10, len(observations), func(i int) {
		if err := c.legacyInterruptionHandler(ctx, instancestatusmsg.New(
			observations[i].instanceID,
			observations[i].kind,
			observations[i].impairedSince,
		)); err != nil {
			errs[i] = fmt.Errorf("handling EC2 %s interruption, %w", observations[i].kind, err)
		}
	})
	return multierr.Combine(errs...)
}

func legacyInterruptionObservations(
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
) []interruptionObservation {
	observationsByInstanceID := map[string]interruptionObservation{}
	// Emit one action per instance. Instance status is the stable reason when both categories
	// qualify, while the earliest impairment time is retained for the aggregate observation.
	addLegacyInterruptionObservations(observationsByInstanceID, instanceAssessment, messages.InstanceStatusKind, observationTime)
	addLegacyInterruptionObservations(observationsByInstanceID, systemAssessment, messages.SystemStatusKind, observationTime)

	observations := make([]interruptionObservation, 0, len(observationsByInstanceID))
	for _, observation := range observationsByInstanceID {
		observations = append(observations, observation)
	}
	return observations
}

func addLegacyInterruptionObservations(
	observationsByInstanceID map[string]interruptionObservation,
	assessment assessmentResult,
	kind messages.Kind,
	observationTime time.Time,
) {
	if assessment.err != nil {
		return
	}
	for _, status := range assessment.statusesByInstanceID {
		if observationTime.Sub(status.ImpairedSince) < instancestatusprovider.ImpairmentTolerationDuration {
			continue
		}
		current, ok := observationsByInstanceID[status.InstanceID]
		if !ok {
			observationsByInstanceID[status.InstanceID] = interruptionObservation{
				instanceID:    status.InstanceID,
				kind:          kind,
				impairedSince: status.ImpairedSince,
			}
			continue
		}
		if status.ImpairedSince.IsZero() ||
			(!current.impairedSince.IsZero() && status.ImpairedSince.Before(current.impairedSince)) {
			current.impairedSince = status.ImpairedSince
			observationsByInstanceID[status.InstanceID] = current
		}
	}
}

func (c *Controller) publishConditions(
	ctx context.Context,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
) error {
	instanceComplete := instanceAssessment.err == nil
	systemComplete := systemAssessment.err == nil
	if !instanceComplete && !systemComplete {
		return nil
	}
	if (!instanceComplete && len(systemAssessment.statusesByInstanceID) == 0) ||
		(!systemComplete && len(instanceAssessment.statusesByInstanceID) == 0) {
		return nil
	}

	nodes, err := c.registeredNodes(ctx)
	if err != nil {
		return fmt.Errorf("listing registered managed Nodes, %w", err)
	}

	errs := make([]error, len(nodes))
	workqueue.ParallelizeUntil(ctx, 10, len(nodes), func(i int) {
		errs[i] = c.publishCondition(ctx, nodes[i].node, nodes[i].instanceID, instanceAssessment, systemAssessment, observationTime)
	})
	return multierr.Combine(errs...)
}

func (c *Controller) publishCondition(
	ctx context.Context,
	node *corev1.Node,
	instanceID string,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
) error {
	instanceHealth, instanceImpaired := instanceAssessment.statusesByInstanceID[instanceID]
	systemHealth, systemImpaired := systemAssessment.statusesByInstanceID[instanceID]

	instanceComplete := instanceAssessment.err == nil
	systemComplete := systemAssessment.err == nil
	if !instanceImpaired && !systemImpaired && (!instanceComplete || !systemComplete) {
		return nil
	}
	condition := conditionForAssessments(
		node,
		instanceHealth,
		instanceImpaired,
		systemHealth,
		systemImpaired,
		observationTime,
	)
	if err := c.patchCondition(ctx, node, condition); err != nil {
		return fmt.Errorf("patching Node %q EC2 status condition, %w", node.Name, err)
	}
	return nil
}

func (c *Controller) registeredNodes(ctx context.Context) ([]registeredNode, error) {
	// Node labels alone are not authoritative here: registration labels can be written before
	// the NodeClaim registration condition and can outlive the NodeClaim. Joining both informer
	// caches ensures health is published only for a registered managed Node and NodeClaim pair.
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
	// Avoid StatusConditions here. Even observed-only condition sets allocate per NodeClaim,
	// and the default condition set mutates objects returned with UnsafeDisableDeepCopy.
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
	systemHealth instancestatusprovider.HealthStatus,
	systemImpaired bool,
	observationTime time.Time,
) corev1.NodeCondition {
	status := corev1.ConditionFalse
	reason := instancestatusprovider.ReasonNoImpairmentReported
	message := healthyConditionMessage
	transitionTime := observationTime
	if instanceImpaired || systemImpaired {
		status = corev1.ConditionTrue
		reason = instancestatusprovider.ReasonReachabilityFailed
		message = impairedConditionMessage
		transitionTime = earliestImpairedSince(observationTime, instanceHealth, instanceImpaired, systemHealth, systemImpaired)
	}

	current := nodeutils.GetCondition(node, instancestatusprovider.ConditionTypeEC2StatusImpaired)
	if current.Type == instancestatusprovider.ConditionTypeEC2StatusImpaired && current.Status == status {
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

func (c *Controller) patchCondition(ctx context.Context, node *corev1.Node, condition corev1.NodeCondition) error {
	current := nodeutils.GetCondition(node, condition.Type)
	if current.Type == condition.Type && equality.Semantic.DeepEqual(current, condition) {
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
