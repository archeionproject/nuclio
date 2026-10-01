//go:build test_unit

/*
Copyright 2023 The Nuclio Authors.

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

package swarm

import (
	"testing"

	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"
	"github.com/nuclio/nuclio/pkg/platform/abstract"
	"github.com/nuclio/nuclio/pkg/platformconfig"

	"github.com/stretchr/testify/suite"
)

type swarmPlatformTestSuite struct {
	suite.Suite
}

func (suite *swarmPlatformTestSuite) TestResolveFunctionNetworks() {
	for _, testCase := range []struct {
		name             string
		defaultNetwork   string
		attributes       map[string]interface{}
		expectedNetworks []string
		expectedError    bool
	}{
		{
			name:             "NoAttributesUsesDefault",
			defaultNetwork:   "default-net",
			expectedNetworks: []string{"default-net"},
		},
		{
			name:             "NoAttributesNoDefault",
			expectedNetworks: nil,
		},
		{
			name:             "SingleNetworkReplacesDefault",
			defaultNetwork:   "default-net",
			attributes:       map[string]interface{}{"network": "net-a"},
			expectedNetworks: []string{"net-a"},
		},
		{

			// attributes are decoded from JSON, so lists arrive as []interface{}
			name:             "NetworksListReplacesDefault",
			defaultNetwork:   "default-net",
			attributes:       map[string]interface{}{"networks": []interface{}{"net-a", "net-b"}},
			expectedNetworks: []string{"net-a", "net-b"},
		},
		{
			name:           "NetworkAndNetworksCombinedWithoutDuplicates",
			defaultNetwork: "default-net",
			attributes: map[string]interface{}{
				"network":  "net-a",
				"networks": []interface{}{"net-b", "net-a", "", "net-c"},
			},
			expectedNetworks: []string{"net-a", "net-b", "net-c"},
		},
		{
			name:             "EmptyNetworksListUsesDefault",
			defaultNetwork:   "default-net",
			attributes:       map[string]interface{}{"networks": []interface{}{}},
			expectedNetworks: []string{"default-net"},
		},
		{
			name:          "NetworksNotAListFails",
			attributes:    map[string]interface{}{"networks": "net-a"},
			expectedError: true,
		},
	} {
		suite.Run(testCase.name, func() {
			swarmPlatform := &Platform{
				Platform: &abstract.Platform{
					Config: &platformconfig.Config{
						Local: platformconfig.PlatformLocalConfig{
							DefaultFunctionContainerNetworkName: testCase.defaultNetwork,
						},
					},
				},
			}

			createFunctionOptions := &platform.CreateFunctionOptions{
				FunctionConfig: functionconfig.Config{
					Spec: functionconfig.Spec{
						Platform: functionconfig.Platform{
							Attributes: testCase.attributes,
						},
					},
				},
			}

			networks, err := swarmPlatform.resolveFunctionNetworks(createFunctionOptions)
			if testCase.expectedError {
				suite.Require().Error(err)
				return
			}
			suite.Require().NoError(err)
			suite.Require().Equal(testCase.expectedNetworks, networks)
		})
	}
}

func TestSwarmPlatformTestSuite(t *testing.T) {
	suite.Run(t, new(swarmPlatformTestSuite))
}
