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
	"time"

	"github.com/aws/karpenter-provider-aws/pkg/controllers/interruption/messages"
)

// Message represents a scheduled event from an EC2 DescribeInstanceStatus response.
type Message struct {
	instanceID string
	startTime  time.Time
}

func New(instanceID string, startTime time.Time) Message {
	return Message{
		instanceID: instanceID,
		startTime:  startTime,
	}
}

func (m Message) EC2InstanceIDs() []string { return []string{m.instanceID} }
func (m Message) Kind() messages.Kind      { return messages.EventStatusKind }
func (m Message) StartTime() time.Time     { return m.startTime }
