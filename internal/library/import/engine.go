package libraryimport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jamie75/librarr/internal/config"
	"github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/models"
)

const (
	EngineModeLegacy = "legacy"
	EngineModeV2     = "v2"
)

type ImportRequest struct {
	Source           library.ImportSource
	RootPath         string
	OriginalPath     string
	TitleHint        string
	AuthorHint       string
	MetadataOverride CandidateMetadata
}

type EngineResult struct {
	Planning       *ImportResult     `json:"planning,omitempty"`
	Execution      *ExecutionSummary `json:"execution,omitempty"`
	LegacyID       int64             `json:"legacy_id,omitempty"`
	InsertedCount  int               `json:"inserted_count,omitempty"`
	DuplicateCount int               `json:"duplicate_count,omitempty"`
	SkippedCount   int               `json:"skipped_count,omitempty"`
	ConflictCount  int               `json:"conflict_count,omitempty"`
}

type ImportEngine interface {
	Import(context.Context, ImportRequest) (*EngineResult, error)
}

type Planner interface {
	Plan(context.Context, PlanningContext) (ImportResult, error)
}

type Executor interface {
	Execute(context.Context, ExecutionContext, []ImportPlan) (ExecutionSummary, error)
}

type EngineSelection struct {
	Mode   string
	Engine ImportEngine
}

func NewConfiguredImportEngine(cfg *config.Config, database *db.DB, librarySvc *library.LibraryService, repositoryMode string) (EngineSelection, error) {
	mode, err := cfg.ImportEngineMode()
	if err != nil {
		return EngineSelection{}, err
	}
	switch mode {
	case EngineModeLegacy:
		if repositoryMode != "" && repositoryMode != library.RepositoryModeLegacy {
			return EngineSelection{}, fmt.Errorf("LIBRARR_IMPORT_ENGINE=legacy requires LIBRARR_LIBRARY_REPOSITORY_MODE=legacy")
		}
		return EngineSelection{
			Mode:   mode,
			Engine: NewLegacyImportEngine(database),
		}, nil
	case EngineModeV2:
		if repositoryMode != library.RepositoryModeNormalized {
			return EngineSelection{}, fmt.Errorf("LIBRARR_IMPORT_ENGINE=v2 requires LIBRARR_LIBRARY_REPOSITORY_MODE=normalized")
		}
		if librarySvc == nil {
			return EngineSelection{}, fmt.Errorf("normalized import engine requires LibraryService")
		}
		return EngineSelection{
			Mode:   mode,
			Engine: NewNormalizedImportEngine(NewImportPlanner(librarySvc), NewImportExecutor(librarySvc)),
		}, nil
	default:
		return EngineSelection{}, fmt.Errorf("unsupported import engine mode %q", mode)
	}
}

type LegacyImportEngine struct {
	db *db.DB
}

func NewLegacyImportEngine(database *db.DB) *LegacyImportEngine {
	return &LegacyImportEngine{db: database}
}

func (e *LegacyImportEngine) Import(_ context.Context, request ImportRequest) (*EngineResult, error) {
	if e == nil || e.db == nil {
		return nil, fmt.Errorf("database is required")
	}
	info, _ := os.Stat(request.RootPath)
	fileSize := int64(0)
	if info != nil {
		fileSize = info.Size()
	}
	title := firstValue(strings.TrimSpace(request.TitleHint), defaultTitleFromPath(request.RootPath))
	author := strings.TrimSpace(request.AuthorHint)
	item := &models.LibraryItem{
		Title:        title,
		Author:       author,
		FilePath:     request.RootPath,
		OriginalPath: firstValue(strings.TrimSpace(request.OriginalPath), request.RootPath),
		FileSize:     fileSize,
		FileFormat:   fileFormatForPath(request.RootPath),
		MediaType:    string(request.Source.MediaType),
		Source:       request.Source.Name,
		SourceID:     request.Source.SourceID,
	}
	outcome, err := e.db.AddItemWithOutcome(item)
	if err != nil {
		return nil, err
	}
	result := &EngineResult{LegacyID: outcome.ID}
	if outcome.Inserted {
		result.InsertedCount = 1
	} else {
		result.DuplicateCount = 1
	}
	return result, nil
}

type NormalizedImportEngine struct {
	planner  Planner
	executor Executor
}

func NewNormalizedImportEngine(planner Planner, executor Executor) *NormalizedImportEngine {
	return &NormalizedImportEngine{planner: planner, executor: executor}
}

func (e *NormalizedImportEngine) Import(ctx context.Context, request ImportRequest) (*EngineResult, error) {
	if e == nil || e.planner == nil || e.executor == nil {
		return nil, fmt.Errorf("planner and executor are required")
	}
	planned, err := e.planner.Plan(ctx, PlanningContext{
		Source:           request.Source,
		RootPath:         request.RootPath,
		OriginalPath:     request.OriginalPath,
		TitleHint:        request.TitleHint,
		AuthorHint:       request.AuthorHint,
		MetadataOverride: request.MetadataOverride,
	})
	if err != nil {
		return nil, err
	}
	executed, err := e.executor.Execute(ctx, ExecutionContext{}, planned.Plans)
	if err != nil {
		return nil, err
	}
	result := &EngineResult{
		Planning:  &planned,
		Execution: &executed,
	}
	var firstErr error
	for _, item := range executed.Results {
		switch item.Status {
		case ExecutionStatusSuccess:
			result.InsertedCount++
		case ExecutionStatusDuplicate:
			result.DuplicateCount++
		case ExecutionStatusSkipped:
			result.SkippedCount++
			if firstErr == nil {
				firstErr = errors.New(item.Reason)
			}
		case ExecutionStatusConflict:
			result.ConflictCount++
			if firstErr == nil {
				firstErr = errors.New(firstValue(item.Reason, "import conflict"))
			}
		case ExecutionStatusRolledBack:
			if firstErr == nil {
				firstErr = item.Error
				if firstErr == nil {
					firstErr = fmt.Errorf("import transaction rolled back")
				}
			}
		}
	}
	return result, firstErr
}

func defaultTitleFromPath(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext != "" {
		return strings.TrimSuffix(base, ext)
	}
	return base
}

func fileFormatForPath(path string) string {
	return strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
}

func firstValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
