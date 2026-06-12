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

package local

import (
	"context"

	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"
)

// Storage is the storage abstraction used by both the local and swarm platforms.
// The concrete *Store (Docker-volume backed) and LocalFileStore (direct OS I/O)
// both implement this interface.
type Storage interface {
	Initialize() (string, error)

	CreateOrUpdateProject(projectConfig *platform.ProjectConfig) error
	GetProjects(projectMeta *platform.ProjectMeta) ([]platform.Project, error)
	DeleteProject(ctx context.Context, projectMeta *platform.ProjectMeta) error

	CreateOrUpdateFunctionEvent(functionEventConfig *platform.FunctionEventConfig) error
	GetFunctionEvents(getFunctionEventsOptions *platform.GetFunctionEventsOptions) ([]platform.FunctionEvent, error)
	DeleteFunctionEvent(functionEventMeta *platform.FunctionEventMeta) error

	CreateOrUpdateFunction(functionConfig *functionconfig.ConfigWithStatus) error
	GetProjectFunctions(getFunctionsOptions *platform.GetFunctionsOptions) ([]platform.Function, error)
	GetFunctions(functionMeta *functionconfig.Meta) ([]platform.Function, error)
	DeleteFunction(ctx context.Context, functionMeta *functionconfig.Meta) error
}
