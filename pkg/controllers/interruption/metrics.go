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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"

	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
)

const (
	interruptionSubsystem = "interruption"
	messageTypeLabel      = "message_type"
)

var (
	MessageType = opmetrics.Label{
		Name:   messageTypeLabel,
		Help:   "The type of interruption message received from the SQS queue. See https://karpenter.sh/docs/concepts/disruption/#interruption.",
		Values: interruptionMessageKindValues,
	}
)

var (
	spotInterruptionKindValue = opmetrics.Value{
		Name: string(messages.SpotInterruptionKind),
		Help: "EC2 issued a two-minute Spot interruption notice for the instance.",
	}
	rebalanceRecommendationKindValue = opmetrics.Value{
		Name: string(messages.RebalanceRecommendationKind),
		Help: "EC2 issued a Spot rebalance recommendation for the instance.",
	}
	scheduledChangeKindValue = opmetrics.Value{
		Name: string(messages.ScheduledChangeKind),
		Help: "AWS Health scheduled a change (e.g. maintenance or retirement) affecting the instance.",
	}
	instanceStoppedKindValue = opmetrics.Value{
		Name: string(messages.InstanceStoppedKind),
		Help: "The EC2 instance was stopped.",
	}
	instanceTerminatedKindValue = opmetrics.Value{
		Name: string(messages.InstanceTerminatedKind),
		Help: "The EC2 instance was terminated.",
	}
	capacityReservationInterruptionKindValue = opmetrics.Value{
		Name: string(messages.CapacityReservationInterruptionKind),
		Help: "The instance's capacity reservation was interrupted.",
	}
	eventStatusKindValue = opmetrics.Value{
		Name: string(messages.EventStatusKind),
		Help: "An EC2 scheduled-event status check fired for the instance.",
	}
	instanceStatusKindValue = opmetrics.Value{
		Name: string(messages.InstanceStatusKind),
		Help: "An EC2 instance reachability status check remained impaired while Node Repair was disabled.",
	}
	systemStatusKindValue = opmetrics.Value{
		Name: string(messages.SystemStatusKind),
		Help: "An EC2 system reachability status check remained impaired while Node Repair was disabled.",
	}
)

// interruptionMessageKindValues contains only msg.Kind() values emitted by the SQS controller.
var interruptionMessageKindValues = []opmetrics.Value{
	spotInterruptionKindValue,
	rebalanceRecommendationKindValue,
	scheduledChangeKindValue,
	instanceStoppedKindValue,
	instanceTerminatedKindValue,
	capacityReservationInterruptionKindValue,
}

// interruptionDisruptionReasonValues contains every provider-owned reason emitted by NodeClaimsDisruptedTotal.
var interruptionDisruptionReasonValues = []opmetrics.Value{
	spotInterruptionKindValue,
	rebalanceRecommendationKindValue,
	scheduledChangeKindValue,
	instanceStoppedKindValue,
	instanceTerminatedKindValue,
	capacityReservationInterruptionKindValue,
	instanceStatusKindValue,
	systemStatusKindValue,
	eventStatusKindValue,
}

var (
	ReceivedMessages = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: interruptionSubsystem,
			Name:      "received_messages_total",
			Help:      "Count of messages received from the SQS queue. Broken down by message type and whether the message was actionable.",
		},
		[]opmetrics.Label{MessageType},
		opmetrics.GA,
	)
	DeletedMessages = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: interruptionSubsystem,
			Name:      "deleted_messages_total",
			Help:      "Count of messages deleted from the SQS queue.",
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
	MessageLatency = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: interruptionSubsystem,
			Name:      "message_queue_duration_seconds",
			Help:      "Amount of time an interruption message is on the queue before it is processed by karpenter.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]opmetrics.Label{},
		opmetrics.GA,
	)
)
