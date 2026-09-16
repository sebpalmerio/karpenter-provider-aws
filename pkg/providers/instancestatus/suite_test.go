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

package instancestatus_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/samber/lo"
	coretest "sigs.k8s.io/karpenter/pkg/test"

	sdk "github.com/aws/karpenter-provider-aws/pkg/aws"
	"github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/instancestatus"
	"github.com/aws/karpenter-provider-aws/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *coretest.Environment
var awsEnv *test.Environment

type failSecondDescribeInstanceStatusEC2API struct {
	sdk.EC2API
	calls atomic.Int64
}

func (f *failSecondDescribeInstanceStatusEC2API) DescribeInstanceStatus(
	ctx context.Context,
	input *ec2.DescribeInstanceStatusInput,
	optFns ...func(*ec2.Options),
) (*ec2.DescribeInstanceStatusOutput, error) {
	if f.calls.Add(1) == 2 {
		return nil, errors.New("injected second-page failure")
	}
	return f.EC2API.DescribeInstanceStatus(ctx, input, optFns...)
}

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "InstanceStatusProvider")
}

var _ = BeforeSuite(func() {
	env = coretest.NewEnvironment(coretest.WithCRDs(v1alpha1.CRDs...))
	ctx = options.ToContext(ctx, test.Options())
	awsEnv = test.NewEnvironment(ctx, env)
})

var _ = Describe("Instance Status Provider", func() {

	BeforeEach(func() {
		awsEnv.Clock.SetTime(time.Time{})
		awsEnv.EC2API.DescribeInstanceStatusBehavior.Reset()
		statuses := []ec2types.InstanceStatus{
			{
				InstanceId: lo.ToPtr("i-0123456789"),
				InstanceStatus: &ec2types.InstanceStatusSummary{
					Status: ec2types.SummaryStatusImpaired,
					Details: []ec2types.InstanceStatusDetails{
						{
							Status:        ec2types.StatusTypeFailed,
							Name:          ec2types.StatusNameReachability,
							ImpairedSince: lo.ToPtr(awsEnv.Clock.Now()),
						},
					},
				},
				SystemStatus: &ec2types.InstanceStatusSummary{
					Status: ec2types.SummaryStatusImpaired,
					Details: []ec2types.InstanceStatusDetails{
						{
							Status:        ec2types.StatusTypeFailed,
							Name:          ec2types.StatusNameReachability,
							ImpairedSince: lo.ToPtr(awsEnv.Clock.Now()),
						},
					},
				},
				AttachedEbsStatus: &ec2types.EbsStatusSummary{
					Status: ec2types.SummaryStatusImpaired,
					Details: []ec2types.EbsStatusDetails{
						{
							Status:        ec2types.StatusTypeFailed,
							Name:          ec2types.StatusNameReachability,
							ImpairedSince: lo.ToPtr(awsEnv.Clock.Now()),
						},
					},
				},
				Events: []ec2types.InstanceStatusEvent{
					{
						Code: ec2types.EventCodeInstanceRetirement,
					},
				},
			},
		}
		awsEnv.EC2API.DescribeInstanceStatusOutput.Set(&ec2.DescribeInstanceStatusOutput{
			InstanceStatuses: statuses,
		})
	})
	Context("List", func() {
		DescribeTable("should scan each assessment independently",
			func(category instancestatus.Category, filterName string, detail instancestatus.Details) {
				statuses, err := awsEnv.InstanceStatusProvider.List(ctx, category)
				Expect(err).ToNot(HaveOccurred())
				Expect(statuses).To(HaveLen(1))
				Expect(statuses[0].InstanceID).To(Equal("i-0123456789"))
				Expect(statuses[0].Overall).To(Equal(ec2types.SummaryStatusImpaired))
				Expect(statuses[0].Details).To(ConsistOf(detail))
				Expect(statuses[0].ImpairedSince).To(Equal(detail.ImpairedSince))

				Expect(awsEnv.EC2API.DescribeInstanceStatusBehavior.CalledWithInput.Len()).To(Equal(1))
				input := awsEnv.EC2API.DescribeInstanceStatusBehavior.CalledWithInput.At(0)
				Expect(input.Filters).To(HaveLen(1))
				Expect(lo.FromPtr(input.Filters[0].Name)).To(Equal(filterName))
			},
			Entry("instance status",
				instancestatus.InstanceStatus,
				"instance-status.status",
				instancestatus.Details{
					Category:      instancestatus.InstanceStatus,
					Name:          string(ec2types.StatusNameReachability),
					Status:        ec2types.StatusTypeFailed,
					ImpairedSince: time.Time{},
				},
			),
			Entry("system status",
				instancestatus.SystemStatus,
				"system-status.status",
				instancestatus.Details{
					Category:      instancestatus.SystemStatus,
					Name:          string(ec2types.StatusNameReachability),
					Status:        ec2types.StatusTypeFailed,
					ImpairedSince: time.Time{},
				},
			),
			Entry("scheduled events",
				instancestatus.EventStatus,
				"event.code",
				instancestatus.Details{
					Category:      instancestatus.EventStatus,
					Name:          string(ec2types.EventCodeInstanceRetirement),
					Status:        ec2types.StatusTypeFailed,
					ImpairedSince: time.Time{},
				},
			),
		)
		It("should surface reachability impairment without a provider-side delay", func() {
			statuses, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.InstanceStatus)
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).To(HaveLen(1))
			Expect(statuses[0].ImpairedSince).To(Equal(awsEnv.Clock.Now()))
		})
		It("should select the earliest reported non-zero impairment time", func() {
			validOnset := awsEnv.Clock.Now().Add(-5 * time.Minute)
			awsEnv.EC2API.DescribeInstanceStatusOutput.Set(&ec2.DescribeInstanceStatusOutput{
				InstanceStatuses: []ec2types.InstanceStatus{{
					InstanceId: lo.ToPtr("i-0123456789"),
					InstanceStatus: &ec2types.InstanceStatusSummary{
						Status: ec2types.SummaryStatusImpaired,
						Details: []ec2types.InstanceStatusDetails{
							{
								Name:   ec2types.StatusNameReachability,
								Status: ec2types.StatusTypeFailed,
							},
							{
								Name:          ec2types.StatusNameReachability,
								Status:        ec2types.StatusTypeFailed,
								ImpairedSince: lo.ToPtr(validOnset),
							},
						},
					},
				}},
			})

			statuses, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.InstanceStatus)
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).To(HaveLen(1))
			Expect(statuses[0].ImpairedSince).To(Equal(validOnset))
		})
		It("should not return healthy statuses", func() {
			awsEnv.EC2API.DescribeInstanceStatusOutput.Set(&ec2.DescribeInstanceStatusOutput{
				InstanceStatuses: []ec2types.InstanceStatus{
					{
						InstanceId: lo.ToPtr("i-0123456789"),
						SystemStatus: &ec2types.InstanceStatusSummary{
							Status: ec2types.SummaryStatusInitializing,
						},
						InstanceStatus: &ec2types.InstanceStatusSummary{
							Status: ec2types.SummaryStatusInsufficientData,
						},
						AttachedEbsStatus: &ec2types.EbsStatusSummary{
							Status: ec2types.SummaryStatusInitializing,
						},
					},
				},
			})
			for _, category := range []instancestatus.Category{
				instancestatus.InstanceStatus,
				instancestatus.SystemStatus,
				instancestatus.EventStatus,
			} {
				statuses, err := awsEnv.InstanceStatusProvider.List(ctx, category)
				Expect(err).ToNot(HaveOccurred())
				Expect(statuses).To(BeEmpty())
			}
		})
		It("should ignore failed details that are not reachability checks", func() {
			awsEnv.EC2API.DescribeInstanceStatusOutput.Set(&ec2.DescribeInstanceStatusOutput{
				InstanceStatuses: []ec2types.InstanceStatus{{
					InstanceId: lo.ToPtr("i-0123456789"),
					InstanceStatus: &ec2types.InstanceStatusSummary{
						Status: ec2types.SummaryStatusImpaired,
						Details: []ec2types.InstanceStatusDetails{{
							Status: ec2types.StatusTypeFailed,
							Name:   ec2types.StatusName("other"),
						}},
					},
				}},
			})

			statuses, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.InstanceStatus)
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).To(BeEmpty())
		})
		It("should return scheduled maintenance events even when other statuses are healthy", func() {
			awsEnv.EC2API.DescribeInstanceStatusOutput.Set(&ec2.DescribeInstanceStatusOutput{
				InstanceStatuses: []ec2types.InstanceStatus{
					{
						InstanceId: lo.ToPtr("i-0123456789"),
						SystemStatus: &ec2types.InstanceStatusSummary{
							Status: ec2types.SummaryStatusInitializing,
						},
						InstanceStatus: &ec2types.InstanceStatusSummary{
							Status: ec2types.SummaryStatusInsufficientData,
						},
						Events: []ec2types.InstanceStatusEvent{
							{
								Code: ec2types.EventCodeInstanceRetirement,
							},
						},
					},
				},
			})
			statuses, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.EventStatus)
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).To(HaveLen(1))
			Expect(statuses[0].Details).To(HaveLen(1))
			Expect(statuses[0].Details[0].Category).To(Equal(instancestatus.EventStatus))
		})
		It("should retrieve every response page before completing an assessment", func() {
			awsEnv.EC2API.DescribeInstanceStatusOutput.Reset()
			for _, instanceID := range []string{"i-1", "i-2"} {
				awsEnv.EC2API.DescribeInstanceStatusBehavior.OutputPages.Add(&ec2.DescribeInstanceStatusOutput{
					InstanceStatuses: []ec2types.InstanceStatus{{
						InstanceId: lo.ToPtr(instanceID),
						InstanceStatus: &ec2types.InstanceStatusSummary{
							Details: []ec2types.InstanceStatusDetails{{
								Name:          ec2types.StatusNameReachability,
								Status:        ec2types.StatusTypeFailed,
								ImpairedSince: lo.ToPtr(awsEnv.Clock.Now()),
							}},
						},
					}},
				})
			}

			statuses, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.InstanceStatus)
			Expect(err).ToNot(HaveOccurred())
			Expect(statuses).To(HaveLen(2))
			Expect(awsEnv.EC2API.DescribeInstanceStatusBehavior.SuccessfulCalls()).To(Equal(2))
		})
		It("should discard partial results when a later response page fails", func() {
			awsEnv.EC2API.DescribeInstanceStatusOutput.Reset()
			for _, instanceID := range []string{"i-1", "i-2"} {
				awsEnv.EC2API.DescribeInstanceStatusBehavior.OutputPages.Add(&ec2.DescribeInstanceStatusOutput{
					InstanceStatuses: []ec2types.InstanceStatus{{
						InstanceId: lo.ToPtr(instanceID),
						InstanceStatus: &ec2types.InstanceStatusSummary{
							Details: []ec2types.InstanceStatusDetails{{
								Name:          ec2types.StatusNameReachability,
								Status:        ec2types.StatusTypeFailed,
								ImpairedSince: lo.ToPtr(awsEnv.Clock.Now()),
							}},
						},
					}},
				})
			}
			ec2API := &failSecondDescribeInstanceStatusEC2API{EC2API: awsEnv.EC2API}
			provider := instancestatus.NewDefaultProvider(ec2API, awsEnv.Clock)

			statuses, err := provider.List(ctx, instancestatus.InstanceStatus)
			Expect(err).To(MatchError(ContainSubstring("injected second-page failure")))
			Expect(statuses).To(BeNil())
			Expect(ec2API.calls.Load()).To(Equal(int64(2)))
		})
		It("should reject assessment categories without a routing contract", func() {
			_, err := awsEnv.InstanceStatusProvider.List(ctx, instancestatus.EBSStatus)
			Expect(err).To(MatchError(ContainSubstring("unsupported EC2 instance status category")))
			Expect(awsEnv.EC2API.DescribeInstanceStatusBehavior.CalledWithInput.Len()).To(BeZero())
		})
	})
})
