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
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"go.uber.org/multierr"
	"k8s.io/client-go/util/workqueue"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
	instancestatusmsg "github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages/instancestatus"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

const scheduledEventInterval = time.Minute

// ScheduledEventController polls EC2 scheduled events and routes them through interruption handling.
type ScheduledEventController struct {
	InterruptionHandler
	provider instancestatus.Provider
}

func NewScheduledEventController(
	kubeClient client.Client,
	recorder events.Recorder,
	provider instancestatus.Provider,
) *ScheduledEventController {
	return &ScheduledEventController{
		InterruptionHandler: InterruptionHandler{
			kubeClient: kubeClient,
			recorder:   recorder,
		},
		provider: provider,
	}
}

func (c *ScheduledEventController) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, "interruption.instancestatus")

	statuses, err := c.provider.List(ctx, instancestatus.EventStatus)
	if err != nil {
		if awserrors.IsUnauthorizedOperationError(err) {
			return reconciler.Result{}, fmt.Errorf("ec2:DescribeInstanceStatus permission is not allowed for %s checks; grant the permission and Karpenter will retry automatically, %w", instancestatus.EventStatus, err)
		}
		return reconciler.Result{}, fmt.Errorf("getting EC2 %s checks, %w", instancestatus.EventStatus, err)
	}

	errs := make([]error, len(statuses))
	workqueue.ParallelizeUntil(ctx, 10, len(statuses), func(i int) {
		status := statuses[i]
		handleErr := c.handleMessage(ctx, instancestatusmsg.New(status.InstanceID, messages.EventStatusKind, status.ImpairedSince))
		if handleErr != nil {
			errs[i] = fmt.Errorf("handling scheduled event message, %w", handleErr)
		}
	})

	if err := multierr.Combine(errs...); err != nil {
		return reconciler.Result{}, err
	}
	return reconciler.Result{RequeueAfter: scheduledEventInterval}, nil
}

func (c *ScheduledEventController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("interruption.instancestatus").
		WatchesRawSource(singleton.Source()).
		WithOptions(controller.Options{
			RateLimiter: scheduledEventRateLimiter(),
		}).
		Complete(singleton.AsReconciler(c))
}

func scheduledEventRateLimiter() workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](scheduledEventInterval, scheduledEventInterval)
}
