package libraryimport

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jamie75/librarr/internal/config"
	librarrdb "github.com/jamie75/librarr/internal/db"
	"github.com/jamie75/librarr/internal/library"
)

func TestConfiguredImportEngineDefaultSelectsLegacy(t *testing.T) {
	database := testImportDB(t)
	defer database.Close()

	selection, err := NewConfiguredImportEngine(&config.Config{}, database, nil, library.RepositoryModeLegacy)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Mode != EngineModeLegacy {
		t.Fatalf("mode = %s", selection.Mode)
	}
	if _, ok := selection.Engine.(*LegacyImportEngine); !ok {
		t.Fatalf("engine = %T", selection.Engine)
	}
}

func TestConfiguredImportEngineExplicitLegacy(t *testing.T) {
	database := testImportDB(t)
	defer database.Close()

	selection, err := NewConfiguredImportEngine(&config.Config{ImportEngine: EngineModeLegacy}, database, nil, library.RepositoryModeLegacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selection.Engine.(*LegacyImportEngine); !ok {
		t.Fatalf("engine = %T", selection.Engine)
	}
}

func TestConfiguredImportEngineExplicitV2(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()
	database := testImportDB(t)
	defer database.Close()

	selection, err := NewConfiguredImportEngine(&config.Config{ImportEngine: EngineModeV2}, database, service, library.RepositoryModeNormalized)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selection.Engine.(*NormalizedImportEngine); !ok {
		t.Fatalf("engine = %T", selection.Engine)
	}
}

func TestConfiguredImportEngineRejectsInvalidValue(t *testing.T) {
	database := testImportDB(t)
	defer database.Close()

	if _, err := NewConfiguredImportEngine(&config.Config{ImportEngine: "nope"}, database, nil, library.RepositoryModeLegacy); err == nil {
		t.Fatal("expected invalid engine error")
	}
}

func TestConfiguredImportEngineRejectsRepositoryMismatch(t *testing.T) {
	service, cleanup := newNormalizedLibraryService(t)
	defer cleanup()
	database := testImportDB(t)
	defer database.Close()

	if _, err := NewConfiguredImportEngine(&config.Config{ImportEngine: EngineModeV2}, database, service, library.RepositoryModeLegacy); err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestNormalizedImportEngineCallsPlannerThenExecutor(t *testing.T) {
	planner := &callOrderPlanner{}
	executor := &callOrderExecutor{t: t, planner: planner}
	engine := NewNormalizedImportEngine(planner, executor)

	_, err := engine.Import(context.Background(), ImportRequest{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: "/tmp/book.epub",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !planner.called {
		t.Fatal("planner was not called")
	}
	if !executor.called {
		t.Fatal("executor was not called")
	}
}

func TestLegacyImportEnginePathUnchanged(t *testing.T) {
	database := testImportDB(t)
	defer database.Close()

	path := filepath.Join(t.TempDir(), "book.epub")
	writeEPUB(t, path, "The Guardian's Path", "Jane Doe")
	engine := NewLegacyImportEngine(database)
	result, err := engine.Import(context.Background(), ImportRequest{
		Source:       library.ImportSource{Name: "manual_import", SourceID: "legacy-1", MediaType: library.MediaTypeEbook},
		RootPath:     path,
		OriginalPath: path,
		TitleHint:    "The Guardian's Path",
		AuthorHint:   "Jane Doe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.InsertedCount != 1 {
		t.Fatalf("result = %+v", result)
	}
	if !database.HasSourceID("legacy-1") {
		t.Fatal("expected legacy source id to be stored")
	}
}

type callOrderPlanner struct {
	called bool
}

func (p *callOrderPlanner) Plan(context.Context, PlanningContext) (ImportResult, error) {
	p.called = true
	return ImportResult{Plans: []ImportPlan{{Disposition: DispositionCreateNewBook}}}, nil
}

type callOrderExecutor struct {
	t       *testing.T
	planner *callOrderPlanner
	called  bool
}

func (e *callOrderExecutor) Execute(context.Context, ExecutionContext, []ImportPlan) (ExecutionSummary, error) {
	if !e.planner.called {
		e.t.Fatal("executor called before planner")
	}
	e.called = true
	return ExecutionSummary{Results: []ExecutionResult{{Status: ExecutionStatusSuccess}}}, nil
}

func testImportDB(t *testing.T) *librarrdb.DB {
	t.Helper()
	database, err := librarrdb.New(filepath.Join(t.TempDir(), "import-engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func TestNormalizedImportEngineReportsRollbackError(t *testing.T) {
	engine := NewNormalizedImportEngine(&callOrderPlanner{}, rollbackExecutor{})
	_, err := engine.Import(context.Background(), ImportRequest{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: "/tmp/book.epub",
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
}

type rollbackExecutor struct{}

func (rollbackExecutor) Execute(context.Context, ExecutionContext, []ImportPlan) (ExecutionSummary, error) {
	return ExecutionSummary{Results: []ExecutionResult{{
		Status: ExecutionStatusRolledBack,
		Error:  &ExecutionError{Stage: "file_attach", Message: "boom"},
	}}}, nil
}

func TestNormalizedImportEngineSkipsManualReviewAsError(t *testing.T) {
	planner := plannerFunc(func(context.Context, PlanningContext) (ImportResult, error) {
		return ImportResult{Plans: []ImportPlan{{Disposition: DispositionNeedsManualReview}}}, nil
	})
	executor := executorFunc(func(context.Context, ExecutionContext, []ImportPlan) (ExecutionSummary, error) {
		return ExecutionSummary{Results: []ExecutionResult{{Status: ExecutionStatusSkipped, Reason: "plan requires manual review"}}}, nil
	})
	engine := NewNormalizedImportEngine(planner, executor)
	_, err := engine.Import(context.Background(), ImportRequest{
		Source:   library.ImportSource{Name: "manual", MediaType: library.MediaTypeEbook},
		RootPath: "/tmp/book.epub",
	})
	if err == nil {
		t.Fatal("expected manual review error")
	}
	if !strings.Contains(err.Error(), "manual review") {
		t.Fatalf("error = %v", err)
	}
}

type plannerFunc func(context.Context, PlanningContext) (ImportResult, error)

func (f plannerFunc) Plan(ctx context.Context, pc PlanningContext) (ImportResult, error) {
	return f(ctx, pc)
}

type executorFunc func(context.Context, ExecutionContext, []ImportPlan) (ExecutionSummary, error)

func (f executorFunc) Execute(ctx context.Context, ec ExecutionContext, plans []ImportPlan) (ExecutionSummary, error) {
	return f(ctx, ec, plans)
}
