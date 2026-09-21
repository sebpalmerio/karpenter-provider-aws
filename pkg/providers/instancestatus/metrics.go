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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

// CategoryLabel describes the bounded DescribeInstanceStatus assessment categories.
var CategoryLabel = opmetrics.Label{
	Name: "category",
	Help: "The EC2 instance status check category that was detected as unhealthy. " +
		"See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-system-instance-status-check.html.",
	Values: []opmetrics.Value{
		{
			Name: string(InstanceStatus),
			Help: "The EC2 instance reachability status check reported an impairment.",
		},
		{
			Name: string(SystemStatus),
			Help: "The EC2 system reachability status check reported an impairment.",
		},
		{
			Name: string(EventStatus),
			Help: "EC2 reported a scheduled maintenance event for the instance.",
		},
	},
}

// UnhealthyTotal counts uninterrupted unhealthy assessment occurrences observed by the controllers.
var UnhealthyTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: "interruption",
		Name:      "instance_status_unhealthy_total",
		Help:      "Count of unhealthy EC2 instance status occurrences detected during this controller process. Broken down by status check category.",
	},
	[]opmetrics.Label{CategoryLabel},
	opmetrics.GA,
)
