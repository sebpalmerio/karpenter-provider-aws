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

package interruption_test

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	statuscontroller "github.com/aws/karpenter-provider-aws/pkg/controllers/instancestatus"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type instanceStatusProvider struct {
	statuses map[instancestatus.Category][]instancestatus.HealthStatus
	errors   map[instancestatus.Category]error
}

func (p *instanceStatusProvider) List(_ context.Context, category instancestatus.Category) ([]instancestatus.HealthStatus, error) {
	if err := p.errors[category]; err != nil {
		return nil, err
	}
	return append([]instancestatus.HealthStatus{}, p.statuses[category]...), nil
}

var _ = Describe("EC2 Status Conditions", func() {
	var nodeClaim *karpv1.NodeClaim
	var node *corev1.Node
	var provider *instanceStatusProvider
	var statusController *statuscontroller.Controller
	var instanceID string

	BeforeEach(func() {
		fakeClock.SetTime(time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC))
		provider = &instanceStatusProvider{
			statuses: map[instancestatus.Category][]instancestatus.HealthStatus{},
			errors:   map[instancestatus.Category]error{},
		}
		statusController = statuscontroller.NewController(
			env.Client,
			fakeClock,
			provider,
			nil,
		)
		nodeClaim, node = coretest.NodeClaimAndNode(karpv1.NodeClaim{
			Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
				Group: apis.Group, Kind: "EC2NodeClass", Name: "default",
			}},
			Status: karpv1.NodeClaimStatus{
				ProviderID: "aws:///test-zone/i-0123456789",
			},
		})
		nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
		instanceID = "i-0123456789"
	})

	It("aggregates instance and system impairment into one condition", func() {
		instanceImpairedSince := fakeClock.Now().Add(-3 * time.Minute)
		systemImpairedSince := fakeClock.Now().Add(-2 * time.Minute)
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: instanceImpairedSince,
		}}
		provider.statuses[instancestatus.SystemStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: systemImpairedSince,
		}}
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		})
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
		Expect(condition.LastTransitionTime.Time.Equal(instanceImpairedSince)).To(BeTrue())
		Expect(condition.Message).To(Equal("EC2 reports a reachability impairment."))
		Expect(node.Status.Conditions).To(ContainElement(HaveField("Type", corev1.NodeReady)))
		ExpectExists(ctx, env.Client, nodeClaim)
	})

	It("preserves the transition clock when the contributing assessment changes", func() {
		impairedSince := fakeClock.Now().Add(-3 * time.Minute)
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: impairedSince,
		}}
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ExpectSingletonReconciled(ctx, statusController)

		provider.statuses[instancestatus.InstanceStatus] = nil
		provider.statuses[instancestatus.SystemStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: fakeClock.Now(),
		}}
		fakeClock.Step(time.Minute)
		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
		Expect(condition.LastTransitionTime.Time.Equal(impairedSince)).To(BeTrue())
	})

	It("uses observation time when both complete scans report no impairment", func() {
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:               instancestatus.ConditionTypeEC2StatusImpaired,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(fakeClock.Now().Add(-time.Hour)),
			Reason:             instancestatus.ReasonReachabilityFailed,
		})
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionFalse, instancestatus.ReasonNoImpairmentReported)
		Expect(condition.LastTransitionTime.Time.Equal(fakeClock.Now())).To(BeTrue())
	})

	It("publishes false when the first complete assessment reports no impairment", func() {
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionFalse, instancestatus.ReasonNoImpairmentReported)
		Expect(condition.LastTransitionTime.Time.Equal(fakeClock.Now())).To(BeTrue())
	})

	It("uses observation time when EC2 does not report impairment onset", func() {
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID: instanceID,
		}}
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
		Expect(condition.LastTransitionTime.Time.Equal(fakeClock.Now())).To(BeTrue())
	})

	It("does not change the condition when no positive scan completes and another scan fails", func() {
		existing := corev1.NodeCondition{
			Type:               instancestatus.ConditionTypeEC2StatusImpaired,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(fakeClock.Now().Add(-time.Hour)),
			Reason:             instancestatus.ReasonReachabilityFailed,
			Message:            "existing observation",
		}
		node.Status.Conditions = append(node.Status.Conditions, existing)
		provider.errors[instancestatus.SystemStatus] = errors.New("system assessment failed")
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		_ = ExpectSingletonReconcileFailed(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, existing.Status, existing.Reason)
		Expect(condition.LastTransitionTime.Time.Equal(existing.LastTransitionTime.Time)).To(BeTrue())
		Expect(condition.Message).To(Equal(existing.Message))
	})

	It("publishes positive evidence when the other assessment fails", func() {
		impairedSince := fakeClock.Now().Add(-3 * time.Minute)
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: impairedSince,
		}}
		provider.errors[instancestatus.SystemStatus] = errors.New("system assessment failed")
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		_ = ExpectSingletonReconcileFailed(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		condition := ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
		Expect(condition.LastTransitionTime.Time.Equal(impairedSince)).To(BeTrue())
		Expect(condition.Message).To(Equal("EC2 reports a reachability impairment."))
	})

	It("does not publish before the NodeClaim is registered", func() {
		nodeClaim.Status.Conditions = nil
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: fakeClock.Now(),
		}}
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		Expect(hasEC2StatusCondition(node)).To(BeFalse())
	})

	It("does not publish for a NodeClaim managed by another provider", func() {
		nodeClaim.Spec.NodeClassRef.Group = "example.com"
		nodeClaim.Spec.NodeClassRef.Kind = "OtherNodeClass"
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: fakeClock.Now(),
		}}
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		ExpectSingletonReconciled(ctx, statusController)

		node = ExpectExists(ctx, env.Client, node)
		Expect(hasEC2StatusCondition(node)).To(BeFalse())
	})

	It("forcefully terminates after the impairment threshold when legacy remediation is configured", func() {
		provider.statuses[instancestatus.InstanceStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: fakeClock.Now().Add(-instancestatus.ImpairmentTolerationDuration),
		}}
		nodeClaim.Finalizers = []string{"testing/finalizer"}
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		legacyController := statuscontroller.NewController(
			env.Client,
			fakeClock,
			provider,
			interruption.NewLegacyStatusInterruptionHandler(env.Client, fakeClock, events.NewRecorder(&record.FakeRecorder{})),
		)
		ExpectSingletonReconciled(ctx, legacyController)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(nodeClaim.Annotations).To(HaveKeyWithValue(
			karpv1.NodeClaimTerminationTimestampAnnotationKey,
			fakeClock.Now().Format(time.RFC3339),
		))
		node = ExpectExists(ctx, env.Client, node)
		ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
	})

	It("does not use legacy interruption before the impairment threshold", func() {
		provider.statuses[instancestatus.SystemStatus] = []instancestatus.HealthStatus{{
			InstanceID:    instanceID,
			ImpairedSince: fakeClock.Now().Add(-instancestatus.ImpairmentTolerationDuration + time.Second),
		}}
		ExpectApplied(ctx, env.Client, nodeClaim, node)

		legacyController := statuscontroller.NewController(
			env.Client,
			fakeClock,
			provider,
			interruption.NewLegacyStatusInterruptionHandler(env.Client, fakeClock, events.NewRecorder(&record.FakeRecorder{})),
		)
		ExpectSingletonReconciled(ctx, legacyController)

		ExpectExists(ctx, env.Client, nodeClaim)
		node = ExpectExists(ctx, env.Client, node)
		ExpectEC2StatusCondition(node, corev1.ConditionTrue, instancestatus.ReasonReachabilityFailed)
	})

})

func hasEC2StatusCondition(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == instancestatus.ConditionTypeEC2StatusImpaired {
			return true
		}
	}
	return false
}
