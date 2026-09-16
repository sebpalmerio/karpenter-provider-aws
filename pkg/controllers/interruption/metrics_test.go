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
	"testing"

	"github.com/awslabs/operatorpkg/metrics"

	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
)

func TestInstanceStatusMetricCategoryValues(t *testing.T) {
	for _, category := range []instancestatus.Category{
		instancestatus.InstanceStatus,
		instancestatus.SystemStatus,
		instancestatus.EventStatus,
	} {
		value := instanceStatusMetricCategoryName(category)
		if !hasMetricValue(Category.Values, value) {
			t.Fatalf("expected instance status category %q to be documented", value)
		}
	}
}

func TestInterruptionMetricValueSets(t *testing.T) {
	eventStatus := string(messages.EventStatusKind)
	if hasMetricValue(interruptionMessageKindValues, eventStatus) {
		t.Fatalf("expected %q to be excluded from SQS message types", eventStatus)
	}
	if !hasMetricValue(interruptionDisruptionReasonValues, eventStatus) {
		t.Fatalf("expected %q to remain a NodeClaim disruption reason", eventStatus)
	}
}

func hasMetricValue(values []metrics.Value, name string) bool {
	for _, value := range values {
		if value.Name == name {
			return true
		}
	}
	return false
}
