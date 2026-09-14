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
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	sdk "github.com/aws/karpenter-provider-aws/pkg/aws"
)

type Category string

const (
	InstanceStatus = Category("InstanceStatus")
	SystemStatus   = Category("SystemStatus")
	// EventStatus surfaces scheduled maintenance events. These are also consumed via EventBridge
	// in the Interruption controller when an SQS queue is configured. The handling of maintenance events is
	// currently primitive where we treat all events as instance degradation with an involuntary replacement.
	EventStatus = Category("EventStatus")
	// EBSStatus check failures are currently ignored until we can differentiate which volumes affect the node vs pods w/ PVCs
	EBSStatus = Category("EBSStatus")
)

const (
	ConditionTypeEC2StatusImpaired corev1.NodeConditionType = "EC2StatusImpaired"
	ReasonReachabilityFailed       string                   = "ReachabilityFailed"
	ReasonNoImpairmentReported     string                   = "NoImpairmentReported"
)

var instanceStatusFilters = map[Category][]ec2types.Filter{
	InstanceStatus: {{Name: lo.ToPtr("instance-status.status"), Values: []string{string(ec2types.SummaryStatusImpaired)}}},
	SystemStatus:   {{Name: lo.ToPtr("system-status.status"), Values: []string{string(ec2types.SummaryStatusImpaired)}}},
	EventStatus: {
		{Name: lo.ToPtr("event.code"), Values: []string{
			string(ec2types.EventCodeInstanceReboot),
			string(ec2types.EventCodeSystemReboot),
			string(ec2types.EventCodeSystemMaintenance),
			string(ec2types.EventCodeInstanceRetirement),
			string(ec2types.EventCodeInstanceStop),
		}},
	},
}

type Provider interface {
	List(context.Context, Category) ([]HealthStatus, error)
}

type DefaultProvider struct {
	ec2api sdk.EC2API
	clk    clock.Clock
}

type HealthStatus struct {
	InstanceID    string
	Overall       ec2types.SummaryStatus
	ImpairedSince time.Time
	Details       []Details
}

type Details struct {
	Category      Category
	Name          string
	ImpairedSince time.Time
	Status        ec2types.StatusType
}

func NewDefaultProvider(ec2API sdk.EC2API, clk clock.Clock) *DefaultProvider {
	return &DefaultProvider{
		ec2api: ec2API,
		clk:    clk,
	}
}

func (p DefaultProvider) List(ctx context.Context, category Category) ([]HealthStatus, error) {
	filters, ok := instanceStatusFilters[category]
	if !ok {
		return nil, fmt.Errorf("unsupported EC2 instance status category %q", category)
	}
	var statuses []ec2types.InstanceStatus
	pager := ec2.NewDescribeInstanceStatusPaginator(p.ec2api, &ec2.DescribeInstanceStatusInput{
		Filters: filters,
	})
	for pager.HasMorePages() {
		out, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("describing EC2 %s checks, %w", category, err)
		}
		statuses = append(statuses, out.InstanceStatuses...)
	}

	var healthStatuses []HealthStatus
	for _, statusChecks := range statuses {
		healthStatus := p.newHealthStatus(statusChecks, category)
		healthStatus.Details = lo.Filter(healthStatus.Details, func(details Details, _ int) bool {
			return details.Status == ec2types.StatusTypeFailed &&
				(details.Category == EventStatus || details.Name == string(ec2types.StatusNameReachability))
		})
		if len(healthStatus.Details) == 0 {
			continue
		}
		healthStatus.ImpairedSince = slices.MinFunc(healthStatus.Details, func(a, b Details) int {
			return a.ImpairedSince.Compare(b.ImpairedSince)
		}).ImpairedSince
		healthStatuses = append(healthStatuses, healthStatus)
	}
	return healthStatuses, nil
}

// newHealthStatus constructs a more consumable version of Health Status Details from the different status checks
func (p DefaultProvider) newHealthStatus(statusChecks ec2types.InstanceStatus, category Category) HealthStatus {
	healthStatus := HealthStatus{
		InstanceID: *statusChecks.InstanceId,
		Overall:    ec2types.SummaryStatusImpaired,
	}
	if category == InstanceStatus && statusChecks.InstanceStatus != nil {
		healthStatus.Details = append(healthStatus.Details, lo.Map(statusChecks.InstanceStatus.Details, func(details ec2types.InstanceStatusDetails, _ int) Details {
			return newStatusDetails(details, InstanceStatus)
		})...)
	}
	if category == SystemStatus && statusChecks.SystemStatus != nil {
		healthStatus.Details = append(healthStatus.Details, lo.Map(statusChecks.SystemStatus.Details, func(details ec2types.InstanceStatusDetails, _ int) Details {
			return newStatusDetails(details, SystemStatus)
		})...)
	}
	if category == EventStatus {
		healthStatus.Details = append(healthStatus.Details, lo.Map(statusChecks.Events, func(details ec2types.InstanceStatusEvent, _ int) Details {
			return p.newEventDetails(details)
		})...)
	}
	return healthStatus
}

func newStatusDetails(details ec2types.InstanceStatusDetails, category Category) Details {
	return Details{
		Category:      category,
		Name:          string(details.Name),
		Status:        details.Status,
		ImpairedSince: lo.FromPtr(details.ImpairedSince),
	}
}

func (p DefaultProvider) newEventDetails(event ec2types.InstanceStatusEvent) Details {
	return Details{
		Category: EventStatus,
		Name:     string(event.Code),
		// All scheduled maintenance events remain actionable interruption signals.
		Status:        ec2types.StatusTypeFailed,
		ImpairedSince: p.clk.Now(),
	}
}
