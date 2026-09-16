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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

func TestInstanceStatusRateLimiter(t *testing.T) {
	rateLimiter := instanceStatusRateLimiter()
	request := reconcile.Request{}

	if delay := rateLimiter.When(request); delay != InstanceStatusInterval {
		t.Fatalf("expected first instance status reconciliation retry after %s, got %s", InstanceStatusInterval, delay)
	}
	for range 20 {
		if delay := rateLimiter.When(request); delay != InstanceStatusInterval {
			t.Fatalf("expected instance status reconciliation retry after %s, got %s", InstanceStatusInterval, delay)
		}
	}

	rateLimiter.Forget(request)
	if delay := rateLimiter.When(request); delay != InstanceStatusInterval {
		t.Fatalf("expected successful reconciliation to reset the retry delay to %s, got %s", InstanceStatusInterval, delay)
	}
}

type internalInstanceStatusProvider struct {
	instanceID string
	errors     map[instancestatus.Category]error
	impaired   *atomic.Bool
	statuses   map[instancestatus.Category][]instancestatus.HealthStatus
}

type blockingHealthScanProvider struct {
	started chan instancestatus.Category
	release chan struct{}
}

func (p blockingHealthScanProvider) List(_ context.Context, category instancestatus.Category) ([]instancestatus.HealthStatus, error) {
	if category == instancestatus.InstanceStatus || category == instancestatus.SystemStatus {
		p.started <- category
		<-p.release
	}
	return nil, nil
}

func (p internalInstanceStatusProvider) List(_ context.Context, category instancestatus.Category) ([]instancestatus.HealthStatus, error) {
	if err := p.errors[category]; err != nil {
		return nil, err
	}
	if p.statuses != nil {
		return append([]instancestatus.HealthStatus{}, p.statuses[category]...), nil
	}
	if category == instancestatus.InstanceStatus && p.instanceID != "" && (p.impaired == nil || p.impaired.Load()) {
		return []instancestatus.HealthStatus{{
			InstanceID:    p.instanceID,
			ImpairedSince: time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC),
		}}, nil
	}
	return nil, nil
}

func TestInstanceStatusScansHealthAssessmentsConcurrently(t *testing.T) {
	provider := blockingHealthScanProvider{
		started: make(chan instancestatus.Category, 2),
		release: make(chan struct{}),
	}
	controller := NewInstanceStatusController(
		fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		provider,
	)
	done := make(chan error, 1)
	go func() {
		_, instanceErr, _, systemErr := controller.scanHealth(context.Background())
		done <- errors.Join(instanceErr, systemErr)
	}()

	started := map[instancestatus.Category]struct{}{}
	for range 2 {
		select {
		case category := <-provider.started:
			started[category] = struct{}{}
		case <-time.After(5 * time.Second):
			close(provider.release)
			t.Fatal("expected both health assessments to start without waiting for the other")
		}
	}
	close(provider.release)
	if _, ok := started[instancestatus.InstanceStatus]; !ok {
		t.Fatal("expected the instance-status assessment to start")
	}
	if _, ok := started[instancestatus.SystemStatus]; !ok {
		t.Fatal("expected the system-status assessment to start")
	}
	if err := <-done; err != nil {
		t.Fatalf("scanning health assessments, %v", err)
	}
}

func TestInstanceStatusSkipsClusterListsForInconclusiveHealth(t *testing.T) {
	for name, provider := range map[string]internalInstanceStatusProvider{
		"both scans fail": {
			errors: map[instancestatus.Category]error{
				instancestatus.InstanceStatus: errors.New("instance scan failed"),
				instancestatus.SystemStatus:   errors.New("system scan failed"),
			},
		},
		"one scan fails and the other is empty": {
			errors: map[instancestatus.Category]error{
				instancestatus.SystemStatus: errors.New("system scan failed"),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var nodeClaimLists atomic.Int64
			var nodeLists atomic.Int64
			kubeClient := fake.NewClientBuilder().
				WithScheme(scheme.Scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						switch list.(type) {
						case *karpv1.NodeClaimList:
							nodeClaimLists.Add(1)
						case *corev1.NodeList:
							nodeLists.Add(1)
						}
						return c.List(ctx, list, opts...)
					},
				}).
				Build()
			controller := NewInstanceStatusController(
				kubeClient,
				clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
				events.NewRecorder(record.NewFakeRecorder(10)),
				provider,
			)

			_, _ = controller.Reconcile(context.Background())

			if got := nodeClaimLists.Load(); got != 0 {
				t.Fatalf("expected no NodeClaim lists, got %d", got)
			}
			if got := nodeLists.Load(); got != 0 {
				t.Fatalf("expected no Node lists, got %d", got)
			}
		})
	}
}

func BenchmarkRegisteredNodesFor(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			nodeClaims := make([]karpv1.NodeClaim, size)
			nodes := make([]corev1.Node, size)
			for i := range size {
				instanceID := fmt.Sprintf("i-%017d", i)
				nodeClaims[i] = karpv1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{Generation: 1},
					Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
						Group: apis.Group,
						Kind:  "EC2NodeClass",
						Name:  "default",
					}},
					Status: karpv1.NodeClaimStatus{ProviderID: "aws:///test-zone/" + instanceID},
				}
				nodeClaims[i].StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
				nodes[i].Spec.ProviderID = "aws:///test-zone/" + instanceID
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if got := registeredNodesFor(nodeClaims, nodes); len(got) != size {
					b.Fatalf("expected %d registered Nodes, got %d", size, len(got))
				}
			}
		})
	}
}

func TestInstanceStatusReturnsUnauthorizedScanErrors(t *testing.T) {
	unauthorized := &smithy.GenericAPIError{
		Code:    "UnauthorizedOperation",
		Message: "not authorized to call ec2:DescribeInstanceStatus",
	}
	controller := NewInstanceStatusController(
		fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			errors: map[instancestatus.Category]error{
				instancestatus.InstanceStatus: unauthorized,
			},
		},
	)

	_, err := controller.Reconcile(context.Background())
	if !errors.Is(err, unauthorized) {
		t.Fatalf("expected unauthorized scan error, got %v", err)
	}
}

func TestInstanceStatusHandlesScheduledEventsBeforePublishingHealth(t *testing.T) {
	eventInstanceID := "i-event"
	var eventHandled atomic.Bool
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*karpv1.NodeClaimList); ok {
					listOptions := &client.ListOptions{}
					listOptions.ApplyOptions(opts)
					if listOptions.FieldSelector != nil {
						eventHandled.Store(true)
						return nil
					}
					if !eventHandled.Load() {
						return errors.New("health publication started before scheduled-event handling")
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			errors: map[instancestatus.Category]error{},
			statuses: map[instancestatus.Category][]instancestatus.HealthStatus{
				instancestatus.EventStatus: {{InstanceID: eventInstanceID}},
			},
		},
	)

	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconciling instance status, %v", err)
	}
	if !eventHandled.Load() {
		t.Fatal("expected scheduled event handling before health publication")
	}
}

func TestInstanceStatusPrunesUnrelatedEventDedupeAfterOneHandlerFails(t *testing.T) {
	failedInstanceID := "i-failed"
	recoveredInstanceID := "i-recovered"
	expectedErr := errors.New("injected scheduled-event lookup failure")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*karpv1.NodeClaimList); ok {
					listOptions := &client.ListOptions{}
					listOptions.ApplyOptions(opts)
					if listOptions.FieldSelector != nil &&
						listOptions.FieldSelector.String() == "status.instanceID="+failedInstanceID {
						return expectedErr
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			errors: map[instancestatus.Category]error{},
			statuses: map[instancestatus.Category][]instancestatus.HealthStatus{
				instancestatus.EventStatus: {{InstanceID: failedInstanceID}},
			},
		},
	)
	failedKey := unhealthyKey{instanceID: failedInstanceID, category: instancestatus.EventStatus}
	recoveredKey := unhealthyKey{instanceID: recoveredInstanceID, category: instancestatus.EventStatus}
	controller.seen[failedKey] = struct{}{}
	controller.seen[recoveredKey] = struct{}{}

	if _, err := controller.Reconcile(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("expected scheduled-event handling failure, got %v", err)
	}
	if _, ok := controller.seen[failedKey]; !ok {
		t.Fatal("expected failed event key to retain dedupe state")
	}
	if _, ok := controller.seen[recoveredKey]; ok {
		t.Fatal("expected unrelated recovered event key to be pruned")
	}
}

func TestInstanceStatusRetainsDedupeStateAfterKubernetesFailure(t *testing.T) {
	instanceID := "i-0123456789"
	nodeClaim, node := coretest.NodeClaimAndNode(karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: apis.Group, Kind: "EC2NodeClass", Name: "default",
		}},
		Status: karpv1.NodeClaimStatus{ProviderID: "aws:///test-zone/" + instanceID},
	})
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)

	var failNextNodeClaimList atomic.Bool
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&corev1.Node{}).
		WithObjects(nodeClaim, node).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*karpv1.NodeClaimList); ok && failNextNodeClaimList.CompareAndSwap(true, false) {
					return errors.New("injected NodeClaim list failure")
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			instanceID: instanceID,
			errors:     map[instancestatus.Category]error{},
		},
	)

	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial reconciliation failed, %v", err)
	}
	key := unhealthyKey{instanceID: instanceID, category: instancestatus.InstanceStatus}
	if _, ok := controller.seen[key]; !ok {
		t.Fatal("expected initial impairment to be retained in dedupe state")
	}

	failNextNodeClaimList.Store(true)
	if _, err := controller.Reconcile(context.Background()); err == nil {
		t.Fatal("expected injected NodeClaim list failure")
	}
	if _, ok := controller.seen[key]; !ok {
		t.Fatal("expected transient Kubernetes failure to preserve dedupe state")
	}
}

func TestInstanceStatusPrunesDedupeStateAfterNodePatchFailure(t *testing.T) {
	instanceID := "i-0123456789"
	nodeClaim, node := coretest.NodeClaimAndNode(karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: apis.Group, Kind: "EC2NodeClass", Name: "default",
		}},
		Status: karpv1.NodeClaimStatus{ProviderID: "aws:///test-zone/" + instanceID},
	})
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)

	var impaired atomic.Bool
	impaired.Store(true)
	var failPatch atomic.Bool
	expectedErr := errors.New("injected Node status patch failure")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&corev1.Node{}).
		WithObjects(nodeClaim, node).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context,
				c client.Client,
				subResourceName string,
				obj client.Object,
				patch client.Patch,
				opts ...client.SubResourcePatchOption,
			) error {
				if failPatch.Load() {
					return expectedErr
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			instanceID: instanceID,
			errors:     map[instancestatus.Category]error{},
			impaired:   &impaired,
		},
	)

	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial reconciliation failed, %v", err)
	}
	key := unhealthyKey{instanceID: instanceID, category: instancestatus.InstanceStatus}
	if _, ok := controller.seen[key]; !ok {
		t.Fatal("expected initial impairment to be retained in dedupe state")
	}

	impaired.Store(false)
	failPatch.Store(true)
	if _, err := controller.Reconcile(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("expected injected Node patch failure, got %v", err)
	}
	if _, ok := controller.seen[key]; ok {
		t.Fatal("expected a complete recovery observation to prune dedupe state despite a Node patch failure")
	}
}

func TestInstanceStatusDoesNotPatchUnchangedNodeCondition(t *testing.T) {
	instanceID := "i-0123456789"
	nodeClaim, node := coretest.NodeClaimAndNode(karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: apis.Group, Kind: "EC2NodeClass", Name: "default",
		}},
		Status: karpv1.NodeClaimStatus{ProviderID: "aws:///test-zone/" + instanceID},
	})
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)

	var nodeStatusPatches atomic.Int64
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&corev1.Node{}).
		WithObjects(nodeClaim, node).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context,
				c client.Client,
				subResourceName string,
				obj client.Object,
				patch client.Patch,
				opts ...client.SubResourcePatchOption,
			) error {
				if subResourceName == "status" {
					if _, ok := obj.(*corev1.Node); ok {
						nodeStatusPatches.Add(1)
					}
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{
			instanceID: instanceID,
			errors:     map[instancestatus.Category]error{},
		},
	)

	for range 2 {
		if _, err := controller.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconciling instance status, %v", err)
		}
	}
	if got := nodeStatusPatches.Load(); got != 1 {
		t.Fatalf("expected one Node status patch across identical observations, got %d", got)
	}
}

func TestInstanceStatusConditionPatchUsesCopyOnWrite(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
		}}},
	}
	expectedErr := errors.New("injected status patch failure")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&corev1.Node{}).
		WithObjects(node).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				context.Context,
				client.Client,
				string,
				client.Object,
				client.Patch,
				...client.SubResourcePatchOption,
			) error {
				return expectedErr
			},
		}).
		Build()
	original := node.DeepCopy()
	controller := NewInstanceStatusController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		events.NewRecorder(record.NewFakeRecorder(10)),
		internalInstanceStatusProvider{errors: map[instancestatus.Category]error{}},
	)

	err := controller.patchCondition(context.Background(), node, corev1.NodeCondition{
		Type:               instancestatus.ConditionTypeEC2StatusImpaired,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		Reason:             instancestatus.ReasonReachabilityFailed,
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected injected patch failure, got %v", err)
	}
	if !equality.Semantic.DeepEqual(node, original) {
		t.Fatal("expected a failed status patch to leave the listed Node object unchanged")
	}
}
