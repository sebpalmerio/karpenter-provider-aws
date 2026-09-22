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

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/cache"
	interruptionevents "github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/events"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
)

type Action string

const (
	CordonAndDrain Action = "CordonAndDrain"
	NoAction       Action = "NoAction"
)

// InterruptionHandler contains shared logic for handling interruption messages
// from both the SQS queue and the DescribeInstanceStatus API.
type InterruptionHandler struct {
	kubeClient                  client.Client
	recorder                    events.Recorder
	unavailableOfferingsCache   *cache.UnavailableOfferings
	capacityReservationProvider capacityreservation.Provider
}

// handleMessage takes an action against every node involved in the message that is owned by a NodePool.
func (h *InterruptionHandler) handleMessage(ctx context.Context, msg messages.Message) (err error) {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("messageKind", msg.Kind()))

	if msg.Kind() == messages.NoOpKind {
		return nil
	}
	for _, instanceID := range msg.EC2InstanceIDs() {
		nodeClaimList := &karpv1.NodeClaimList{}
		if e := h.kubeClient.List(ctx, nodeClaimList, client.MatchingFields{"status.instanceID": instanceID}); e != nil {
			err = multierr.Append(err, e)
			continue
		}
		if len(nodeClaimList.Items) == 0 {
			continue
		}
		for _, nodeClaim := range nodeClaimList.Items {
			nodeList := &corev1.NodeList{}
			if e := h.kubeClient.List(ctx, nodeList, client.MatchingFields{"spec.instanceID": instanceID}); e != nil {
				err = multierr.Append(err, e)
				continue
			}
			var node *corev1.Node
			if len(nodeList.Items) > 0 {
				node = &nodeList.Items[0]
			}
			if e := h.handleNodeClaim(ctx, msg, &nodeClaim, node); e != nil {
				err = multierr.Append(err, e)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("acting on NodeClaims, %w", err)
	}
	return nil
}

// handleNodeClaim retrieves the action for the message and then performs the appropriate action against the node
func (h *InterruptionHandler) handleNodeClaim(ctx context.Context, msg messages.Message, nodeClaim *karpv1.NodeClaim, node *corev1.Node) error {
	action := actionForMessage(msg)
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(nodeClaim), "action", string(action)))
	if node != nil {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KObj(node)))
	}

	// Record metric and event for this action
	h.notifyForMessage(msg, nodeClaim, node)
	h.markUnavailableOfferings(ctx, msg, nodeClaim)

	switch action {
	case CordonAndDrain:
		return h.deleteNodeClaim(ctx, msg, nodeClaim, node)
	default:
		return nil
	}
}

// markUnavailableOfferings updates caches when an interruption signals that an offering is no longer available.
func (h *InterruptionHandler) markUnavailableOfferings(ctx context.Context, msg messages.Message, nodeClaim *karpv1.NodeClaim) {
	if msg.Kind() == messages.SpotInterruptionKind && h.unavailableOfferingsCache != nil {
		zone := nodeClaim.Labels[corev1.LabelTopologyZone]
		instanceType := nodeClaim.Labels[corev1.LabelInstanceTypeStable]
		if zone != "" && instanceType != "" {
			unavailableReason := map[string]string{
				"reason": string(msg.Kind()),
			}
			h.unavailableOfferingsCache.MarkUnavailable(ctx, ec2types.InstanceType(instanceType), zone, karpv1.CapacityTypeSpot, unavailableReason)
		}
	}
	if msg.Kind() == messages.CapacityReservationInterruptionKind && h.capacityReservationProvider != nil {
		reservationID := nodeClaim.Labels[v1.LabelCapacityReservationID]
		if reservationID != "" {
			h.capacityReservationProvider.MarkUnavailable(reservationID)
		}
	}
}

// deleteNodeClaim removes the NodeClaim from the api-server
func (h *InterruptionHandler) deleteNodeClaim(ctx context.Context, msg messages.Message, nodeClaim *karpv1.NodeClaim, node *corev1.Node) error {
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := h.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return client.IgnoreNotFound(fmt.Errorf("deleting the node on interruption message, %w", err))
	}
	log.FromContext(ctx).Info("initiating delete from interruption message")
	h.recorder.Publish(interruptionevents.TerminatingOnInterruption(node, nodeClaim)...)
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       string(msg.Kind()),
		metrics.NodePoolLabel:     nodeClaim.Labels[karpv1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: nodeClaim.Labels[karpv1.CapacityTypeLabelKey],
		// Interruption isn't consolidation, so there's no consolidation policy to report - the same as the
		// core health controller reports when it deletes an unhealthy node.
		metrics.ConsolidationPolicyLabel: "",
		metrics.TerminationModeLabel:     nodeclaimutils.DisruptionTerminationMode(nodeClaim),
	})
	return nil
}

// notifyForMessage publishes the relevant alert based on the message kind
func (h *InterruptionHandler) notifyForMessage(msg messages.Message, nodeClaim *karpv1.NodeClaim, n *corev1.Node) {
	switch msg.Kind() {
	case messages.RebalanceRecommendationKind:
		h.recorder.Publish(interruptionevents.RebalanceRecommendation(n, nodeClaim)...)
	case messages.ScheduledChangeKind, messages.EventStatusKind:
		h.recorder.Publish(interruptionevents.Unhealthy(n, nodeClaim)...)
	case messages.SpotInterruptionKind:
		h.recorder.Publish(interruptionevents.SpotInterrupted(n, nodeClaim)...)
	case messages.CapacityReservationInterruptionKind:
		h.recorder.Publish(interruptionevents.CapacityReservationInstanceInterrupted(n, nodeClaim)...)
	case messages.InstanceStoppedKind:
		h.recorder.Publish(interruptionevents.Stopping(n, nodeClaim)...)
	case messages.InstanceTerminatedKind:
		h.recorder.Publish(interruptionevents.Terminating(n, nodeClaim)...)
	default:
	}
}

func actionForMessage(msg messages.Message) Action {
	switch msg.Kind() {
	case messages.ScheduledChangeKind, messages.EventStatusKind, messages.SpotInterruptionKind, messages.InstanceStoppedKind, messages.InstanceTerminatedKind, messages.CapacityReservationInterruptionKind:
		return CordonAndDrain
	default:
		return NoAction
	}
}
