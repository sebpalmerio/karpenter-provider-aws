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

package cloudprovider_test

import (
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Repair Policies", func() {
	It("registers EC2 status impairment with the replacement policy", func() {
		policy, found := lo.Find(cloudProvider.RepairPolicies(), func(policy corecloudprovider.RepairPolicy) bool {
			return policy.ConditionType == instancestatus.ConditionTypeEC2StatusImpaired
		})
		Expect(found).To(BeTrue())
		Expect(policy.ConditionStatus).To(Equal(corev1.ConditionTrue))
		Expect(policy.ReasonRegex).To(BeEmpty())
		Expect(policy.TolerationDuration).To(Equal(2 * time.Minute))
		Expect(policy.TerminationGracePeriod).NotTo(BeNil())
		Expect(*policy.TerminationGracePeriod).To(Equal(5 * time.Minute))
		Expect(policy.Action).To(Equal(corecloudprovider.ReplaceNode))
	})

	It("registers every supported condition as an explicit replacement fallback", func() {
		for _, policy := range cloudProvider.RepairPolicies() {
			Expect(policy.ReasonRegex).To(BeEmpty())
			Expect(policy.Action).To(Equal(corecloudprovider.ReplaceNode))
		}
	})

	It("preserves forceful replacement for the pre-existing health policies", func() {
		for _, policy := range cloudProvider.RepairPolicies() {
			if policy.ConditionType == instancestatus.ConditionTypeEC2StatusImpaired {
				continue
			}
			Expect(policy.TerminationGracePeriod).NotTo(BeNil(), "condition %q", policy.ConditionType)
			Expect(*policy.TerminationGracePeriod).To(BeZero(), "condition %q", policy.ConditionType)
		}
	})
})
