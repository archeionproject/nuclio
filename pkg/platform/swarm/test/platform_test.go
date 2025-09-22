//go:build test_integration && test_local

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
package test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nuclio/nuclio/pkg/dockerclient"
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"
	"github.com/nuclio/nuclio/pkg/platform/swarm"
	processorsuite "github.com/nuclio/nuclio/pkg/processor/test/suite"

	"github.com/stretchr/testify/suite"
)

type TestSuite struct {
	processorsuite.TestSuite
	namespace string
	ctx       context.Context
}

func (suite *TestSuite) SetupSuite() {
	suite.PlatformType = "swarm"
	suite.TestSuite.SetupSuite()
	suite.ctx = context.Background()
	suite.Runtime = "python"

	namespaces, err := suite.Platform.GetNamespaces(suite.ctx)
	suite.Require().NoError(err, "Failed to get namespaces")

	// we will work on the first one
	suite.namespace = namespaces[0]

	getProjectsOptions := &platform.CreateProjectOptions{
		ProjectConfig: &platform.ProjectConfig{Meta: platform.ProjectMeta{Name: platform.DefaultProjectName, Namespace: suite.namespace}, Spec: platform.ProjectSpec{
			Description: "just a description",
		}},
	}
	err = suite.Platform.CreateProject(suite.ctx, getProjectsOptions)
	suite.Require().NoError(err, "Failed to create project")
}

// Test function containers healthiness validation
func (suite *TestSuite) TestValidateFunctionContainersHealthiness() {
	createFunctionOptions := suite.getDeployOptions("health-validation")
	createFunctionOptions.FunctionConfig.Meta.Namespace = suite.namespace
	suite.DeployFunction(createFunctionOptions,
		func(deployResult *platform.CreateFunctionResult) bool {
			suite.NotEmpty(deployResult, "Function hasn't been deployed")
			functionName := deployResult.UpdatedFunctionConfig.Meta.Name

			// Ensure function state is ready
			suite.WaitForFunctionState(&platform.GetFunctionsOptions{
				Name:      functionName,
				Namespace: suite.namespace,
			}, functionconfig.FunctionStateReady, time.Second)

			// Stop the sevice
			err := suite.DockerClient.StopService(deployResult.ContainerID)
			suite.Require().NoError(err, "Could not stop service")

			// Trigger function containers healthiness validation
			go suite.Platform.(*swarm.Platform).ValidateFunctionServiceHealthiness(suite.ctx)

			// Wait for function to become unhealthy
			suite.WaitForFunctionState(&platform.GetFunctionsOptions{
				Name:      functionName,
				Namespace: suite.namespace,
			}, functionconfig.FunctionStateUnhealthy, time.Minute)

			// Start the service
			err = suite.DockerClient.StartService(deployResult.ContainerID)
			suite.Require().NoError(err, "Failed to start service")

			// Trigger function service healthiness validation
			go suite.Platform.(*swarm.Platform).ValidateFunctionServiceHealthiness(suite.ctx)

			suite.WaitForFunctionState(&platform.GetFunctionsOptions{
				Name:      functionName,
				Namespace: suite.namespace,
			}, functionconfig.FunctionStateReady, time.Minute)

			return true
		})
}

func (suite *TestSuite) TestImportFunctionFlow() {

	createFunctionOptions := suite.getDeployOptions("importable")
	createFunctionOptions.FunctionConfig.Meta.Namespace = suite.namespace
	createFunctionOptions.FunctionConfig.Meta.Annotations = map[string]string{
		functionconfig.FunctionAnnotationSkipBuild:  "true",
		functionconfig.FunctionAnnotationSkipDeploy: "true",
	}
	suite.DeployFunctionAndRedeploy(createFunctionOptions,
		func(deployResult *platform.CreateFunctionResult) bool {
			functionName := deployResult.UpdatedFunctionConfig.Meta.Name

			suite.WaitForFunctionState(&platform.GetFunctionsOptions{
				Name:      deployResult.UpdatedFunctionConfig.Meta.Name,
				Namespace: suite.namespace,
			}, functionconfig.FunctionStateImported, time.Second)

			// Check its state is ready
			function := suite.GetFunction(&platform.GetFunctionsOptions{
				Name:      functionName,
				Namespace: suite.namespace,
			})
			functionConfig := function.GetConfig()

			// Check that the annotations have been removed
			_, skipBuildExists := functionConfig.Meta.Annotations[functionconfig.FunctionAnnotationSkipBuild]
			_, skipDeployExists := functionConfig.Meta.Annotations[functionconfig.FunctionAnnotationSkipDeploy]
			suite.Assert().False(skipBuildExists)
			suite.Assert().False(skipDeployExists)

			createFunctionOptions.FunctionConfig.Meta = functionConfig.Meta
			createFunctionOptions.FunctionConfig.Spec = functionConfig.Spec
			return true
		},
		func(deployResult *platform.CreateFunctionResult) bool {
			suite.WaitForFunctionState(&platform.GetFunctionsOptions{
				Name:      deployResult.UpdatedFunctionConfig.Meta.Name,
				Namespace: suite.namespace,
			}, functionconfig.FunctionStateReady, time.Second)
			return true
		})
}

func (suite *TestSuite) TestDeployFunctionDisablePublishingPorts() {
	createFunctionOptions := suite.getDeployOptions("no-publish-ports")
	name := createFunctionOptions.FunctionConfig.Meta.Name
	createFunctionOptions.FunctionConfig.Meta.Namespace = suite.namespace
	swarmPlatform := suite.Platform.(*swarm.Platform)

	for _, testCase := range []struct {
		name          string
		useAttributes bool
	}{
		{
			name:          "DisableWithTriggerAttributes",
			useAttributes: true,
		},
		{
			name: "DisableWithAnnotation",
		},
	} {
		suite.Run(testCase.name, func() {

			if testCase.useAttributes {
				// use trigger attributes to disable publishing ports
				createFunctionOptions.FunctionConfig.Spec.Triggers = map[string]functionconfig.Trigger{
					"http": {
						Kind: "http",
						Attributes: map[string]interface{}{
							"disablePortPublishing": true,
						},
					},
				}
			} else {
				// restoring the name as it get modified by DeployFunction appending the testId (which it causes to get longer than 63 chars which is the maxium for a service name)
				createFunctionOptions.FunctionConfig.Meta.Name = name
				// use trigger annotation to disable publishing ports
				createFunctionOptions.FunctionConfig.Spec.Triggers = map[string]functionconfig.Trigger{
					"http": {
						Kind: "http",
						Annotations: map[string]string{
							"nuclio.io/disable-port-publishing": "true",
						},
					},
				}
			}

			suite.DeployFunction(createFunctionOptions,
				func(deployResult *platform.CreateFunctionResult) bool {
					services, err := suite.DockerClient.GetServices(&dockerclient.GetServiceOptions{
						Name: swarmPlatform.GetFunctionServiceName(&createFunctionOptions.FunctionConfig),
					})
					suite.Require().NoError(err, "Failed to get services")
					suite.Require().Len(services, 1, "Expected to get one service")

					// check that the service ports are not published
					suite.Require().Empty(services[0].Endpoint.Ports)

					return true
				})
		})
	}
}

func (suite *TestSuite) TestDeployFunctionDisabledDefaultHttpTrigger() {
	createFunctionOptions := suite.getDeployOptions("disable-default-http")
	createFunctionOptions.FunctionConfig.Meta.Namespace = suite.namespace
	trueValue := true
	createFunctionOptions.FunctionConfig.Spec.DisableDefaultHTTPTrigger = &trueValue
	swarmPlatform := suite.Platform.(*swarm.Platform)
	suite.DeployFunction(createFunctionOptions,

		// sanity
		func(deployResult *platform.CreateFunctionResult) bool {
			containerId := suite.getFunctionServiceId(swarmPlatform, &createFunctionOptions.FunctionConfig)
			suite.Require().NotEqual("", containerId)
			return true
		})
}

func (suite *TestSuite) getDeployOptions(functionName string) *platform.CreateFunctionOptions {
	functionPath := []string{suite.GetTestFunctionsDir(), "common", "reverser", "python", "reverser.py"}
	createFunctionOptions := suite.TestSuite.GetDeployOptions(functionName, filepath.Join(functionPath...))
	createFunctionOptions.FunctionConfig.Spec.Build.NoBaseImagesPull = true
	return createFunctionOptions
}

func (suite *TestSuite) getFunctionServiceId(swarmPlatform *swarm.Platform, config *functionconfig.Config) string {
	services, err := suite.DockerClient.GetServices(&dockerclient.GetServiceOptions{
		Name: swarmPlatform.GetFunctionServiceName(config),
	})
	suite.Require().NoError(err, "Failed to get services")
	suite.Require().Len(services, 1, "Expected to get one service")
	return services[0].ID
}

func TestProjectTestSuite(t *testing.T) {
	if testing.Short() {
		return
	}

	suite.Run(t, new(TestSuite))
}
