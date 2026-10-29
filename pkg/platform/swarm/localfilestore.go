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
	"sync"
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

	// resources are written to ".<name>.json.tmp-<random>" first, which readers never list (they list "*.json")
	tempFileInfix = ".json.tmp-"

	// temporary files older than this are leftovers of interrupted writes, as a write takes milliseconds
	staleTempFileAge = time.Minute
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

	// serializes function writes and deletes, so that UpdateFunctionStatus can't race with them
	functionsLock sync.Mutex
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

	s.removeStaleTempFiles()
	return "", nil
}

// removeStaleTempFiles deletes the temporary files left behind by writes interrupted by a crash. fresh ones are
// kept, as they may belong to a write in progress by another process sharing the store
func (s *LocalFileStore) removeStaleTempFiles() {
	paths, err := filepath.Glob(filepath.Join(s.baseDir, "*", "*", ".*"+tempFileInfix+"*"))
	if err != nil {
		s.logger.WarnWith("Failed to list temporary store files", "err", err.Error())
		return
	}

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil || time.Since(info.ModTime()) < staleTempFileAge {
			continue
		}
		if err := os.Remove(path); err != nil {
			s.logger.WarnWith("Failed to remove stale temporary store file", "path", path, "err", err.Error())
			continue
		}
		s.logger.InfoWith("Removed stale temporary store file", "path", path)
	}
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
	s.functionsLock.Lock()
	defer s.functionsLock.Unlock()

	return s.writeResource(funcSubDir,
		functionConfig.Meta.Namespace,
		functionConfig.Meta.Name,
		functionConfig)
}

// UpdateFunctionStatus reads a function and passes it to update, writing it back if update returns true.
// unlike CreateOrUpdateFunction, it returns nuclio.ErrNotFound for a missing function instead of creating it,
// and it is atomic with respect to the other function writes and deletes
func (s *LocalFileStore) UpdateFunctionStatus(meta *functionconfig.Meta,
	update func(function *functionconfig.ConfigWithStatus) bool) error {
	if meta.Name == "" {
		return errors.New("Function name is required to update its status")
	}

	s.functionsLock.Lock()
	defer s.functionsLock.Unlock()

	rows, err := s.readResources(funcSubDir, meta.Namespace, meta.Name)
	if err != nil {
		return errors.Wrap(err, "Failed to read function")
	}
	if len(rows) == 0 {
		return nuclio.ErrNotFound
	}

	function := functionconfig.ConfigWithStatus{}
	if err := json.Unmarshal(rows[0], &function); err != nil {
		return errors.Wrap(err, "Failed to unmarshal function")
	}

	if !update(&function) {
		return nil
	}

	return s.writeResource(funcSubDir, meta.Namespace, meta.Name, &function)
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

	s.functionsLock.Lock()
	defer s.functionsLock.Unlock()

	return s.deleteResource(funcSubDir, meta.Namespace, meta.Name)
}

// --- helpers ---

// writeResource replaces the resource file atomically: the data is written and synced to a temporary file in the
// same directory, which is then renamed over the resource file. readers see either the previous or the new
// content, never a partial one, and a crash midway leaves the previous content intact
func (s *LocalFileStore) writeResource(subDir, namespace, name string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal resource")
	}
	dir := filepath.Join(s.baseDir, subDir, namespace)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrap(err, "Failed to create namespace directory")
	}

	tempFile, err := os.CreateTemp(dir, "."+name+tempFileInfix+"*")
	if err != nil {
		return errors.Wrap(err, "Failed to create temporary resource file")
	}
	tempPath := tempFile.Name()

	if err := writeAndSyncFile(tempFile, data); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "Failed to write temporary resource file")
	}

	if err := os.Rename(tempPath, filepath.Join(dir, name+".json")); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "Failed to replace resource file")
	}

	// persist the rename itself across a host crash. best effort, as not every platform can sync a directory
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	return nil
}

// writeAndSyncFile writes data to the file, flushes it to disk and closes the file
func writeAndSyncFile(file *os.File, data []byte) error {
	_, err := file.Write(data)
	if err == nil {

		// temporary files are created as 0600, resource files were always 0644
		err = file.Chmod(0o644)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
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
func (f *localStoreFunction) GetReplicas() (int, int)                    { return 1, 1 }
