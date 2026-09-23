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
)

func TestInterruptionMetricValueSets(t *testing.T) {
	for _, kind := range []messages.Kind{
		messages.InstanceStatusKind,
		messages.SystemStatusKind,
		messages.EventStatusKind,
	} {
		name := string(kind)
		if hasMetricValue(interruptionMessageKindValues, name) {
			t.Fatalf("expected %q to be excluded from SQS message types", name)
		}
		if !hasMetricValue(interruptionDisruptionReasonValues, name) {
			t.Fatalf("expected %q to remain a NodeClaim disruption reason", name)
		}
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
