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
	"sync/atomic"
	"testing"

	"github.com/aws/smithy-go"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"

	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

type eventStatusProvider struct {
	errors   map[instancestatus.Category]error
	statuses map[instancestatus.Category][]instancestatus.HealthStatus
}

func (p eventStatusProvider) List(_ context.Context, category instancestatus.Category) ([]instancestatus.HealthStatus, error) {
	if err := p.errors[category]; err != nil {
		return nil, err
	}
	return append([]instancestatus.HealthStatus{}, p.statuses[category]...), nil
}

func TestScheduledEventRateLimiter(t *testing.T) {
	limiter := scheduledEventRateLimiter()
	request := reconcile.Request{}

	for range 20 {
		if delay := limiter.When(request); delay != scheduledEventInterval {
			t.Fatalf("expected retry after %s, got %s", scheduledEventInterval, delay)
		}
	}
	limiter.Forget(request)
	if delay := limiter.When(request); delay != scheduledEventInterval {
		t.Fatalf("expected successful reconciliation to reset retry delay to %s, got %s", scheduledEventInterval, delay)
	}
}

func TestScheduledEventReturnsUnauthorizedScanError(t *testing.T) {
	unauthorized := &smithy.GenericAPIError{
		Code:    "UnauthorizedOperation",
		Message: "not authorized to call ec2:DescribeInstanceStatus",
	}
	controller := NewScheduledEventController(
		fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
		events.NewRecorder(record.NewFakeRecorder(10)),
		eventStatusProvider{
			errors: map[instancestatus.Category]error{
				instancestatus.EventStatus: unauthorized,
			},
		},
	)

	_, err := controller.Reconcile(context.Background())
	if !errors.Is(err, unauthorized) {
		t.Fatalf("expected unauthorized event scan error, got %v", err)
	}
}

func TestScheduledEventPrunesUnrelatedDedupeAfterOneHandlerFails(t *testing.T) {
	failedInstanceID := "i-failed"
	recoveredInstanceID := "i-recovered"
	expectedErr := errors.New("injected scheduled-event lookup failure")
	var eventLookupAttempted atomic.Bool
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*karpv1.NodeClaimList); ok {
					listOptions := &client.ListOptions{}
					listOptions.ApplyOptions(opts)
					if listOptions.FieldSelector != nil &&
						listOptions.FieldSelector.String() == "status.instanceID="+failedInstanceID {
						eventLookupAttempted.Store(true)
						return expectedErr
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	controller := NewScheduledEventController(
		kubeClient,
		events.NewRecorder(record.NewFakeRecorder(10)),
		eventStatusProvider{
			errors: map[instancestatus.Category]error{},
			statuses: map[instancestatus.Category][]instancestatus.HealthStatus{
				instancestatus.EventStatus: {{InstanceID: failedInstanceID}},
			},
		},
	)
	failedKey := unhealthyKey{instanceID: failedInstanceID}
	recoveredKey := unhealthyKey{instanceID: recoveredInstanceID}
	controller.seen[failedKey] = struct{}{}
	controller.seen[recoveredKey] = struct{}{}

	if _, err := controller.Reconcile(context.Background()); !errors.Is(err, expectedErr) {
		t.Fatalf("expected scheduled-event handling failure, got %v", err)
	}
	if !eventLookupAttempted.Load() {
		t.Fatal("expected scheduled-event NodeClaim lookup")
	}
	if _, ok := controller.seen[failedKey]; !ok {
		t.Fatal("expected failed event key to retain dedupe state")
	}
	if _, ok := controller.seen[recoveredKey]; ok {
		t.Fatal("expected unrelated recovered event key to be pruned")
	}
}
