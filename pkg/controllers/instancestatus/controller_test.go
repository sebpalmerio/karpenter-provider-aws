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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	"github.com/aws/karpenter-provider-aws/pkg/apis"
	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
	instancestatusprovider "github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

type fakeProvider struct {
	instanceID string
	errors     map[instancestatusprovider.Category]error
	statuses   map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus
}

type recordingMessageHandler struct {
	mu       sync.Mutex
	received []messages.Message
	err      error
}

type legacyInterruptionTestCase struct {
	name          string
	statuses      map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus
	errors        map[instancestatusprovider.Category]error
	expectedKind  messages.Kind
	expectedStart time.Time
	expectedCalls int
	expectedError error
	handlerError  error
}

func (h *recordingMessageHandler) HandleMessage(_ context.Context, msg messages.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.received = append(h.received, msg)
	return h.err
}

func (h *recordingMessageHandler) Messages() []messages.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]messages.Message{}, h.received...)
}

func (p fakeProvider) List(_ context.Context, category instancestatusprovider.Category) ([]instancestatusprovider.HealthStatus, error) {
	if err := p.errors[category]; err != nil {
		return nil, err
	}
	if p.statuses != nil {
		return append([]instancestatusprovider.HealthStatus{}, p.statuses[category]...), nil
	}
	if category == instancestatusprovider.InstanceStatus && p.instanceID != "" {
		return []instancestatusprovider.HealthStatus{{
			InstanceID:    p.instanceID,
			ImpairedSince: time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC),
		}}, nil
	}
	return nil, nil
}

func TestLegacyInterruptionRouting(t *testing.T) {
	now := time.Date(2026, time.September, 22, 12, 0, 0, 0, time.UTC)
	instanceID := "i-0123456789"
	scanErr := errors.New("system assessment failed")
	handlerErr := errors.New("interruption failed")

	for _, test := range []legacyInterruptionTestCase{
		{
			name: "combined impairment",
			statuses: map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus{
				instancestatusprovider.InstanceStatus: {{
					InstanceID:    instanceID,
					ImpairedSince: now.Add(-3 * time.Minute),
				}},
				instancestatusprovider.SystemStatus: {{
					InstanceID:    instanceID,
					ImpairedSince: now.Add(-4 * time.Minute),
				}},
			},
			expectedKind:  messages.InstanceStatusKind,
			expectedStart: now.Add(-4 * time.Minute),
			expectedCalls: 1,
		},
		{
			name: "system impairment",
			statuses: map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus{
				instancestatusprovider.SystemStatus: {{
					InstanceID:    instanceID,
					ImpairedSince: now.Add(-3 * time.Minute),
				}},
			},
			expectedKind:  messages.SystemStatusKind,
			expectedStart: now.Add(-3 * time.Minute),
			expectedCalls: 1,
		},
		{
			name: "partial scan failure",
			statuses: map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus{
				instancestatusprovider.InstanceStatus: {{
					InstanceID:    instanceID,
					ImpairedSince: now.Add(-instancestatusprovider.ImpairmentTolerationDuration),
				}},
			},
			errors: map[instancestatusprovider.Category]error{
				instancestatusprovider.SystemStatus: scanErr,
			},
			expectedKind:  messages.InstanceStatusKind,
			expectedStart: now.Add(-instancestatusprovider.ImpairmentTolerationDuration),
			expectedCalls: 1,
			expectedError: scanErr,
		},
		{
			name: "handler failure",
			statuses: map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus{
				instancestatusprovider.InstanceStatus: {{
					InstanceID:    instanceID,
					ImpairedSince: now.Add(-3 * time.Minute),
				}},
			},
			expectedKind:  messages.InstanceStatusKind,
			expectedStart: now.Add(-3 * time.Minute),
			expectedCalls: 1,
			expectedError: handlerErr,
			handlerError:  handlerErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runLegacyInterruptionTest(t, now, test)
		})
	}
}

func runLegacyInterruptionTest(t *testing.T, now time.Time, test legacyInterruptionTestCase) {
	t.Helper()
	handler := &recordingMessageHandler{err: test.handlerError}
	controller := NewController(
		fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		clocktesting.NewFakeClock(now),
		fakeProvider{
			statuses: test.statuses,
			errors:   test.errors,
		},
		handler.HandleMessage,
	)

	_, err := controller.Reconcile(context.Background())
	if !errors.Is(err, test.expectedError) {
		t.Fatalf("expected error %v, got %v", test.expectedError, err)
	}
	received := handler.Messages()
	if len(received) != test.expectedCalls {
		t.Fatalf("expected %d interruption calls, got %d", test.expectedCalls, len(received))
	}
	if test.expectedCalls == 0 {
		return
	}
	if received[0].Kind() != test.expectedKind {
		t.Fatalf("expected interruption kind %q, got %q", test.expectedKind, received[0].Kind())
	}
	if !received[0].StartTime().Equal(test.expectedStart) {
		t.Fatalf("expected interruption start %s, got %s", test.expectedStart, received[0].StartTime())
	}
}

func TestRateLimiter(t *testing.T) {
	limiter := rateLimiter()
	request := reconcile.Request{}

	for range 20 {
		if delay := limiter.When(request); delay != reconcileInterval {
			t.Fatalf("expected retry after %s, got %s", reconcileInterval, delay)
		}
	}
	limiter.Forget(request)
	if delay := limiter.When(request); delay != reconcileInterval {
		t.Fatalf("expected successful reconciliation to reset retry delay to %s, got %s", reconcileInterval, delay)
	}
}

func TestUnauthorizedScanErrorIsReturned(t *testing.T) {
	unauthorized := &smithy.GenericAPIError{
		Code:    "UnauthorizedOperation",
		Message: "not authorized to call ec2:DescribeInstanceStatus",
	}
	controller := NewController(
		fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)),
		fakeProvider{
			errors: map[instancestatusprovider.Category]error{
				instancestatusprovider.InstanceStatus: unauthorized,
			},
		},
		nil,
	)

	_, err := controller.Reconcile(context.Background())
	if !errors.Is(err, unauthorized) {
		t.Fatalf("expected unauthorized scan error, got %v", err)
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

func TestRegisteredNodesForDoesNotMutateNodeClaims(t *testing.T) {
	nodeClaims := []karpv1.NodeClaim{{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: apis.Group,
			Kind:  "EC2NodeClass",
			Name:  "default",
		}},
	}}
	original := nodeClaims[0].DeepCopy()

	registeredNodesFor(nodeClaims, nil)

	if !equality.Semantic.DeepEqual(&nodeClaims[0], original) {
		t.Fatal("expected registered Node filtering to leave informer-cached NodeClaims unchanged")
	}
}

func TestKubernetesReadErrorIsReturned(t *testing.T) {
	instanceID := "i-0123456789"
	expectedErr := errors.New("injected NodeClaim list failure")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*karpv1.NodeClaimList); ok {
					return expectedErr
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	controller := NewController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		fakeProvider{
			instanceID: instanceID,
			errors:     map[instancestatusprovider.Category]error{},
		},
		nil,
	)

	if _, err := controller.Reconcile(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("expected injected NodeClaim list failure, got %v", err)
	}
}

func TestUnchangedAggregateConditionIsNotPatchedWhenSourceChanges(t *testing.T) {
	instanceID := "i-0123456789"
	nodeClaim, node := managedNode(instanceID)
	impairedSince := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
	statuses := map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus{
		instancestatusprovider.InstanceStatus: {{
			InstanceID:    instanceID,
			ImpairedSince: impairedSince,
		}},
	}
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
	controller := NewController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		fakeProvider{
			errors:   map[instancestatusprovider.Category]error{},
			statuses: statuses,
		},
		nil,
	)

	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconciling instance status, %v", err)
	}
	statuses[instancestatusprovider.InstanceStatus] = nil
	statuses[instancestatusprovider.SystemStatus] = []instancestatusprovider.HealthStatus{{
		InstanceID:    instanceID,
		ImpairedSince: impairedSince.Add(time.Minute),
	}}
	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconciling changed instance status source, %v", err)
	}
	if got := nodeStatusPatches.Load(); got != 1 {
		t.Fatalf("expected one Node status patch across an aggregate source change, got %d", got)
	}
}

func TestConditionPatchUsesCopyOnWrite(t *testing.T) {
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
	controller := NewController(
		kubeClient,
		clocktesting.NewFakeClock(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		fakeProvider{errors: map[instancestatusprovider.Category]error{}},
		nil,
	)

	err := controller.patchCondition(context.Background(), node, corev1.NodeCondition{
		Type:               instancestatusprovider.ConditionTypeEC2StatusImpaired,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Date(2026, time.September, 16, 12, 1, 0, 0, time.UTC)),
		Reason:             instancestatusprovider.ReasonReachabilityFailed,
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected injected patch failure, got %v", err)
	}
	if !equality.Semantic.DeepEqual(node, original) {
		t.Fatal("expected failed status patch to leave listed Node unchanged")
	}
}

func managedNode(instanceID string) (*karpv1.NodeClaim, *corev1.Node) {
	nodeClaim, node := coretest.NodeClaimAndNode(karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{
			Group: apis.Group,
			Kind:  "EC2NodeClass",
			Name:  "default",
		}},
		Status: karpv1.NodeClaimStatus{ProviderID: "aws:///test-zone/" + instanceID},
	})
	nodeClaim.StatusConditions().SetTrue(karpv1.ConditionTypeRegistered)
	return nodeClaim, node
}
