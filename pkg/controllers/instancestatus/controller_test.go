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
	instancestatusprovider "github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

type fakeProvider struct {
	instanceID string
	errors     map[instancestatusprovider.Category]error
	statuses   map[instancestatusprovider.Category][]instancestatusprovider.HealthStatus
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
	)

	if _, err := controller.Reconcile(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("expected injected NodeClaim list failure, got %v", err)
	}
}

func TestUnchangedConditionIsNotPatched(t *testing.T) {
	instanceID := "i-0123456789"
	nodeClaim, node := managedNode(instanceID)
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
			instanceID: instanceID,
			errors:     map[instancestatusprovider.Category]error{},
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
