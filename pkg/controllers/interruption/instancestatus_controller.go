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

package interruption

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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	instancestatusmsg "github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages/instancestatus"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
	"github.com/aws/karpenter-provider-aws/pkg/utils"
)

// unhealthyKey uniquely identifies an unhealthy status check for deduplication.
// The metric is only incremented the first time a given instance+category is observed.
type unhealthyKey struct {
	instanceID string
	category   string
}

type assessmentResult struct {
	category instancestatus.Category
	statuses map[string]instancestatus.HealthStatus
	err      error
}

var (
	// InstanceStatusInterval is the polling interval for the EC2 DescribeInstanceStatus API.
	InstanceStatusInterval = 1 * time.Minute
)

// InstanceStatusController polls EC2 DescribeInstanceStatus. Instance-status and
// system-status diagnoses are published as Node health, while scheduled events
// continue through interruption handling.
type InstanceStatusController struct {
	InterruptionHandler
	instanceStatusProvider instancestatus.Provider
	clk                    clock.Clock
	seen                   map[unhealthyKey]struct{}
	mu                     sync.Mutex
}

func NewInstanceStatusController(
	kubeClient client.Client,
	clk clock.Clock,
	recorder events.Recorder,
	instanceStatusProvider instancestatus.Provider,
) *InstanceStatusController {
	return &InstanceStatusController{
		InterruptionHandler: InterruptionHandler{
			kubeClient: kubeClient,
			recorder:   recorder,
		},
		instanceStatusProvider: instanceStatusProvider,
		clk:                    clk,
		seen:                   map[unhealthyKey]struct{}{},
	}
}

func (c *InstanceStatusController) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, "interruption.instancestatus")

	instanceAssessment, instanceErr := c.scan(ctx, instancestatus.InstanceStatus)
	systemAssessment, systemErr := c.scan(ctx, instancestatus.SystemStatus)
	eventAssessment, eventErr := c.scan(ctx, instancestatus.EventStatus)
	observationTime := c.clk.Now()

	currentKeys := map[unhealthyKey]struct{}{}
	var errs error
	if err := c.publishConditions(ctx, instanceAssessment, systemAssessment, observationTime, currentKeys); err != nil {
		errs = multierr.Append(errs, err)
	}
	if eventAssessment.err == nil {
		if err := c.handleEvents(ctx, eventAssessment, currentKeys); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	c.pruneSeen(currentKeys, instanceAssessment, systemAssessment, eventAssessment)

	errs = multierr.Append(errs, multierr.Combine(instanceErr, systemErr, eventErr))
	if errs != nil {
		return reconciler.Result{}, errs
	}
	return reconciler.Result{RequeueAfter: InstanceStatusInterval}, nil
}

func (c *InstanceStatusController) scan(ctx context.Context, category instancestatus.Category) (assessmentResult, error) {
	statuses, err := c.instanceStatusProvider.List(ctx, category)
	result := assessmentResult{
		category: category,
		statuses: map[string]instancestatus.HealthStatus{},
		err:      err,
	}
	if err != nil {
		if awserrors.IsUnauthorizedOperationError(err) {
			log.FromContext(ctx).Error(err, "ec2:DescribeInstanceStatus permission is not allowed, update the IAM policy and restart the Karpenter deployment to enable instance status health checks",
				"category", category)
			return result, nil
		}
		return result, fmt.Errorf("getting EC2 %s checks, %w", category, err)
	}
	for _, status := range statuses {
		result.statuses[status.InstanceID] = status
	}
	return result, nil
}

func (c *InstanceStatusController) publishConditions(
	ctx context.Context,
	instanceAssessment assessmentResult,
	systemAssessment assessmentResult,
	observationTime time.Time,
	currentKeys map[unhealthyKey]struct{},
) error {
	nodesByInstanceID, err := c.registeredNodesByInstanceID(ctx)
	if err != nil {
		return fmt.Errorf("listing registered managed Nodes, %w", err)
	}

	instanceComplete := instanceAssessment.err == nil
	systemComplete := systemAssessment.err == nil
	if !instanceComplete && !systemComplete {
		return nil
	}

	instanceIDs := make([]string, 0, len(nodesByInstanceID))
	for instanceID := range nodesByInstanceID {
		instanceIDs = append(instanceIDs, instanceID)
	}
	errs := make([]error, len(instanceIDs))
	workqueue.ParallelizeUntil(ctx, 10, len(instanceIDs), func(i int) {
		instanceID := instanceIDs[i]
		node := nodesByInstanceID[instanceID]
		errs[i] = c.publishCondition(ctx, node, instanceID, instanceAssessment, systemAssessment, observationTime, currentKeys)
	})
	return multierr.Combine(errs...)
}

func (c *InstanceStatusController) publishCondition(
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
		c.recordUnhealthyInstance(ctx, instanceID, instancestatus.InstanceStatus, currentKeys)
	}
	if systemImpaired {
		c.recordUnhealthyInstance(ctx, instanceID, instancestatus.SystemStatus, currentKeys)
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

func (c *InstanceStatusController) registeredNodesByInstanceID(ctx context.Context) (map[string]*corev1.Node, error) {
	nodeClaims := &karpv1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaims); err != nil {
		return nil, err
	}
	registeredInstanceIDs := map[string]struct{}{}
	for i := range nodeClaims.Items {
		nodeClaim := &nodeClaims.Items[i]
		if !nodeClaim.StatusConditions().IsTrue(karpv1.ConditionTypeRegistered) {
			continue
		}
		instanceID, err := utils.ParseInstanceID(nodeClaim.Status.ProviderID)
		if err != nil {
			continue
		}
		registeredInstanceIDs[instanceID] = struct{}{}
	}

	nodes := &corev1.NodeList{}
	if err := c.kubeClient.List(ctx, nodes); err != nil {
		return nil, err
	}
	nodesByInstanceID := map[string]*corev1.Node{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		instanceID, err := utils.ParseInstanceID(node.Spec.ProviderID)
		if err != nil {
			continue
		}
		if _, ok := registeredInstanceIDs[instanceID]; ok {
			nodesByInstanceID[instanceID] = node
		}
	}
	return nodesByInstanceID, nil
}

func conditionForAssessments(
	node *corev1.Node,
	instanceHealth instancestatus.HealthStatus,
	instanceImpaired bool,
	instanceComplete bool,
	systemHealth instancestatus.HealthStatus,
	systemImpaired bool,
	systemComplete bool,
	observationTime time.Time,
) corev1.NodeCondition {
	status := corev1.ConditionFalse
	reason := instancestatus.ReasonNoImpairmentReported
	message := "EC2 reports no recognized instance or system reachability impairment."
	transitionTime := observationTime
	if instanceImpaired || systemImpaired {
		status = corev1.ConditionTrue
		reason = instancestatus.ReasonReachabilityFailed
		message = impairmentMessage(instanceImpaired, instanceComplete, systemImpaired, systemComplete)
		transitionTime = earliestImpairedSince(observationTime, instanceHealth, instanceImpaired, systemHealth, systemImpaired)
	}

	current := conditionForType(node, instancestatus.ConditionTypeEC2StatusImpaired)
	if current != nil && current.Status == status {
		transitionTime = current.LastTransitionTime.Time
		reason = current.Reason
	}
	return corev1.NodeCondition{
		Type:               instancestatus.ConditionTypeEC2StatusImpaired,
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
	instanceHealth instancestatus.HealthStatus,
	instanceImpaired bool,
	systemHealth instancestatus.HealthStatus,
	systemImpaired bool,
) time.Time {
	earliest := fallback
	found := false
	for _, health := range []struct {
		status   instancestatus.HealthStatus
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

func (c *InstanceStatusController) patchCondition(ctx context.Context, node *corev1.Node, condition corev1.NodeCondition) error {
	stored := node.DeepCopy()
	replaced := false
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == condition.Type {
			node.Status.Conditions[i] = condition
			replaced = true
			break
		}
	}
	if !replaced {
		node.Status.Conditions = append(node.Status.Conditions, condition)
	}
	if equality.Semantic.DeepEqual(stored.Status.Conditions, node.Status.Conditions) {
		return nil
	}
	return client.IgnoreNotFound(c.kubeClient.Status().Patch(ctx, node, client.StrategicMergeFrom(stored)))
}

func (c *InstanceStatusController) handleEvents(ctx context.Context, assessment assessmentResult, currentKeys map[unhealthyKey]struct{}) error {
	statuses := make([]instancestatus.HealthStatus, 0, len(assessment.statuses))
	for _, status := range assessment.statuses {
		statuses = append(statuses, status)
	}
	errs := make([]error, len(statuses))
	workqueue.ParallelizeUntil(ctx, 10, len(statuses), func(i int) {
		healthStatus := statuses[i]
		found, err := c.handleMessage(ctx, instancestatusmsg.New(healthStatus.InstanceID, healthStatus.ImpairedSince), false)
		if err != nil {
			errs[i] = fmt.Errorf("handling scheduled event message, %w", err)
			return
		}
		if found {
			c.recordUnhealthyInstance(ctx, healthStatus.InstanceID, instancestatus.EventStatus, currentKeys)
		}
	})
	return multierr.Combine(errs...)
}

func (c *InstanceStatusController) recordUnhealthyInstance(ctx context.Context, instanceID string, category instancestatus.Category, currentKeys map[unhealthyKey]struct{}) {
	key := unhealthyKey{instanceID: instanceID, category: string(category)}
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
		InstanceStatusUnhealthy.Inc(map[string]string{categoryLabel: string(category)})
	}
}

func (c *InstanceStatusController) pruneSeen(currentKeys map[unhealthyKey]struct{}, assessments ...assessmentResult) {
	completed := map[string]struct{}{}
	for _, assessment := range assessments {
		if assessment.err == nil {
			completed[string(assessment.category)] = struct{}{}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.seen {
		if _, ok := completed[key.category]; !ok {
			continue
		}
		if _, ok := currentKeys[key]; !ok {
			delete(c.seen, key)
		}
	}
}

func (c *InstanceStatusController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("interruption.instancestatus").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}
