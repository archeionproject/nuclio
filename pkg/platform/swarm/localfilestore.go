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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/nuclio/nuclio/pkg/common"
	"github.com/nuclio/nuclio/pkg/errgroup"
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	nuclio "github.com/nuclio/nuclio-sdk-go"
)

const (
	defaultStoreDir = "/var/nuclio/store"
	storeDirEnvVar  = "NUCLIO_STORE_DIR"

	funcSubDir    = "functions"
	projectSubDir = "projects"
	eventSubDir   = "function-events"
)

// LocalFileStore is a Backend implementation that reads and writes JSON files
// directly on the local filesystem. It is intended for use with the Swarm
// platform when the nuclio service is pinned to a single Swarm node.
//
// Directory layout (mirrors the Docker-volume store):
//
//	<baseDir>/
//	  functions/<namespace>/<name>.json
//	  projects/<namespace>/<name>.json
//	  function-events/<namespace>/<name>.json
//
// The base directory is read from the NUCLIO_STORE_DIR environment variable,
// defaulting to /var/nuclio/store.
type LocalFileStore struct {
	logger   logger.Logger
	platform platform.Platform
	baseDir  string
}

func newLocalFileStore(parentLogger logger.Logger, pl platform.Platform) (*LocalFileStore, error) {
	baseDir := os.Getenv(storeDirEnvVar)
	if baseDir == "" {
		baseDir = defaultStoreDir
	}
	return &LocalFileStore{
		logger:   parentLogger.GetChild("local-file-store"),
		platform: pl,
		baseDir:  baseDir,
	}, nil
}

func (s *LocalFileStore) Initialize() (string, error) {
	for _, sub := range []string{funcSubDir, projectSubDir, eventSubDir} {
		if err := os.MkdirAll(filepath.Join(s.baseDir, sub), 0o755); err != nil {
			return "", errors.Wrapf(err, "Failed to create store subdirectory %s", sub)
		}
	}
	return "", nil
}

// --- Project ---

func (s *LocalFileStore) CreateOrUpdateProject(projectConfig *platform.ProjectConfig) error {
	now := time.Now()
	projectConfig.Status.UpdatedAt = &now
	return s.writeResource(projectSubDir, projectConfig.Meta.Namespace, projectConfig.Meta.Name, projectConfig)
}

func (s *LocalFileStore) GetProjects(projectMeta *platform.ProjectMeta) ([]platform.Project, error) {
	rows, err := s.readResources(projectSubDir, projectMeta.Namespace, projectMeta.Name)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get projects")
	}

	var projects []platform.Project
	for _, row := range rows {
		p := platform.AbstractProject{}
		if err := json.Unmarshal(row, &p.ProjectConfig); err != nil {
			return nil, errors.Wrap(err, "Failed to unmarshal project")
		}
		projects = append(projects, &p)
	}
	return projects, nil
}

func (s *LocalFileStore) DeleteProject(ctx context.Context, projectMeta *platform.ProjectMeta) error {
	functions, err := s.GetProjectFunctions(&platform.GetFunctionsOptions{
		Namespace: projectMeta.Namespace,
		Labels:    fmt.Sprintf("%s=%s", common.NuclioResourceLabelKeyProjectName, projectMeta.Name),
	})
	if err != nil {
		return errors.Wrap(err, "Failed to get project functions")
	}

	eg, egCtx := errgroup.WithContext(ctx, s.logger)
	for _, fn := range functions {
		fn := fn
		eg.Go("Delete function", func() error {
			return s.DeleteFunction(egCtx, &fn.GetConfig().Meta)
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "Failed to delete functions")
	}

	return s.deleteResource(projectSubDir, projectMeta.Namespace, projectMeta.Name)
}

// --- FunctionEvent ---

func (s *LocalFileStore) CreateOrUpdateFunctionEvent(functionEventConfig *platform.FunctionEventConfig) error {
	return s.writeResource(eventSubDir,
		functionEventConfig.Meta.Namespace,
		functionEventConfig.Meta.Name,
		functionEventConfig)
}

func (s *LocalFileStore) GetFunctionEvents(opts *platform.GetFunctionEventsOptions) ([]platform.FunctionEvent, error) {
	functionName := opts.Meta.Labels[common.NuclioResourceLabelKeyFunctionName]
	functionNames := opts.FunctionNames
	sort.Strings(functionNames)

	rows, err := s.readResources(eventSubDir, opts.Meta.Namespace, opts.Meta.Name)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get function events")
	}

	var events []platform.FunctionEvent
	for _, row := range rows {
		fe := platform.AbstractFunctionEvent{}
		if err := json.Unmarshal(row, &fe.FunctionEventConfig); err != nil {
			return nil, errors.Wrap(err, "Failed to unmarshal function event")
		}
		if functionName != "" &&
			fe.GetConfig().Meta.Labels != nil &&
			functionName != fe.GetConfig().Meta.Labels[common.NuclioResourceLabelKeyFunctionName] {
			continue
		}
		if len(functionNames) > 0 {
			idx := sort.SearchStrings(functionNames, fe.GetConfig().Meta.Name)
			if idx == len(functionNames) || functionNames[idx] != fe.GetConfig().Meta.Name {
				continue
			}
		}
		events = append(events, &fe)
	}
	return events, nil
}

func (s *LocalFileStore) DeleteFunctionEvent(meta *platform.FunctionEventMeta) error {
	return s.deleteResource(eventSubDir, meta.Namespace, meta.Name)
}

// --- Function ---

func (s *LocalFileStore) CreateOrUpdateFunction(functionConfig *functionconfig.ConfigWithStatus) error {
	return s.writeResource(funcSubDir,
		functionConfig.Meta.Namespace,
		functionConfig.Meta.Name,
		functionConfig)
}

func (s *LocalFileStore) GetProjectFunctions(opts *platform.GetFunctionsOptions) ([]platform.Function, error) {
	projectName := common.StringToStringMap(opts.Labels, "=")[common.NuclioResourceLabelKeyProjectName]

	all, err := s.GetFunctions(&functionconfig.Meta{
		Name:      opts.Name,
		Namespace: opts.Namespace,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read functions from local store")
	}

	var functions []platform.Function
	for _, fn := range all {
		if projectName != "" && fn.GetConfig().Meta.Labels[common.NuclioResourceLabelKeyProjectName] != projectName {
			continue
		}
		functions = append(functions, fn)
	}
	return functions, nil
}

func (s *LocalFileStore) GetFunctions(meta *functionconfig.Meta) ([]platform.Function, error) {
	rows, err := s.readResources(funcSubDir, meta.Namespace, meta.Name)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get functions")
	}

	var functions []platform.Function
	for _, row := range rows {
		cws := functionconfig.ConfigWithStatus{}
		if err := json.Unmarshal(row, &cws); err != nil {
			return nil, errors.Wrap(err, "Failed to unmarshal function")
		}
		fn, err := newLocalStoreFunction(s.logger, s.platform, &cws.Config, &cws.Status)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create function")
		}
		functions = append(functions, fn)
	}
	return functions, nil
}

func (s *LocalFileStore) DeleteFunction(ctx context.Context, meta *functionconfig.Meta) error {
	events, err := s.GetFunctionEvents(&platform.GetFunctionEventsOptions{
		Meta: platform.FunctionEventMeta{
			Namespace: meta.Namespace,
			Labels:    map[string]string{common.NuclioResourceLabelKeyFunctionName: meta.Name},
		},
	})
	if err != nil {
		return errors.Wrap(err, "Failed to get function events")
	}

	eg, _ := errgroup.WithContext(ctx, s.logger)
	for _, event := range events {
		event := event
		eg.Go("Delete function event", func() error {
			return s.DeleteFunctionEvent(&event.GetConfig().Meta)
		})
	}
	if err := eg.Wait(); err != nil {
		s.logger.WarnWithCtx(ctx, "Failed to delete function events, deleting function anyway", "err", err)
		return errors.Wrap(err, "Failed to delete function events")
	}

	return s.deleteResource(funcSubDir, meta.Namespace, meta.Name)
}

// --- helpers ---

func (s *LocalFileStore) writeResource(subDir, namespace, name string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal resource")
	}
	dir := filepath.Join(s.baseDir, subDir, namespace)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrap(err, "Failed to create namespace directory")
	}
	return os.WriteFile(filepath.Join(dir, name+".json"), data, 0o644)
}

func (s *LocalFileStore) readResources(subDir, namespace, name string) ([][]byte, error) {
	if name != "" {
		data, err := os.ReadFile(filepath.Join(s.baseDir, subDir, namespace, name+".json"))
		if os.IsNotExist(err) {
			return nil, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "Failed to read resource file")
		}
		return [][]byte{data}, nil
	}

	paths, err := filepath.Glob(filepath.Join(s.baseDir, subDir, namespace, "*.json"))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to list resource files")
	}

	var results [][]byte
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to read resource file %s", p)
		}
		results = append(results, data)
	}
	return results, nil
}

func (s *LocalFileStore) deleteResource(subDir, namespace, name string) error {
	p := filepath.Join(s.baseDir, subDir, namespace, name+".json")
	if err := os.Remove(p); os.IsNotExist(err) {
		return nuclio.ErrNotFound
	} else if err != nil {
		return errors.Wrapf(err, "Failed to delete resource file %s", p)
	}
	return nil
}

// --- localStoreFunction ---
// Mirrors client.function, defined here to avoid exporting client internals.

type localStoreFunction struct {
	platform.AbstractFunction
}

func newLocalStoreFunction(parentLogger logger.Logger,
	parentPlatform platform.Platform,
	config *functionconfig.Config,
	status *functionconfig.Status) (*localStoreFunction, error) {

	f := &localStoreFunction{}
	abstract, err := platform.NewAbstractFunction(parentLogger, parentPlatform, config, status, f)
	if err != nil {
		return nil, err
	}
	f.AbstractFunction = *abstract
	return f, nil
}

func (f *localStoreFunction) Initialize(context.Context, []string) error { return nil }
func (f *localStoreFunction) GetReplicas() (int, int)                     { return 1, 1 }
