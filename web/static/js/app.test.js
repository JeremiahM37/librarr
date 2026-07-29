const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const jsPath = path.join(__dirname, 'app.js');
const htmlPath = path.join(__dirname, '..', '..', 'index.html');
const cssPath = path.join(__dirname, '..', 'css', 'app.css');
const appSource = fs.readFileSync(jsPath, 'utf8');
const indexHTML = fs.readFileSync(htmlPath, 'utf8');
const appCSS = fs.readFileSync(cssPath, 'utf8');

function extractFunctionSource(name) {
  const asyncStart = appSource.indexOf(`async function ${name}`);
  const plainStart = appSource.indexOf(`function ${name}`);
  const start = asyncStart !== -1 ? asyncStart : plainStart;
  if (start === -1) {
    throw new Error(`function ${name} not found`);
  }
  let bodyStart = -1;
  let parenDepth = 0;
  let sawParams = false;
  for (let i = start; i < appSource.length; i++) {
    const ch = appSource[i];
    if (ch === '(') {
      parenDepth++;
      sawParams = true;
      continue;
    }
    if (ch === ')') {
      parenDepth--;
      continue;
    }
    if (ch === '{' && sawParams && parenDepth === 0) {
      bodyStart = i;
      break;
    }
  }
  if (bodyStart === -1) {
    throw new Error(`function ${name} has no body`);
  }
  let braceDepth = 0;
  for (let i = bodyStart; i < appSource.length; i++) {
    const ch = appSource[i];
    if (ch === '{') {
      braceDepth++;
    } else if (ch === '}') {
      braceDepth--;
      if (braceDepth === 0) {
        return appSource.slice(start, i + 1);
      }
    }
  }
  throw new Error(`unterminated function ${name}`);
}

const functionBundle = [
  extractFunctionSource('currentLibraryCount'),
  extractFunctionSource('normalizeDownloadsResponse'),
  extractFunctionSource('isAdminUser'),
  extractFunctionSource('homeDisplayName'),
  extractFunctionSource('buildDashboardActivitySummary'),
  extractFunctionSource('buildDashboardAttention'),
  extractFunctionSource('buildHomeDashboardMarkup'),
  extractFunctionSource('renderWantedDashboardPanel'),
  extractFunctionSource('renderRecentlyAddedShelf'),
  extractFunctionSource('renderHomeBookCard'),
  extractFunctionSource('renderNeedsAttention'),
  extractFunctionSource('hasDashboardActivity'),
  extractFunctionSource('renderDashboardActivity'),
  extractFunctionSource('renderActivityChip'),
  extractFunctionSource('renderOnboardingChecklist'),
  extractFunctionSource('mapV1BookToUIBook'),
  extractFunctionSource('normalizeFormatLabels'),
  extractFunctionSource('renderBookCover'),
  extractFunctionSource('makePlaceholderHtml'),
  extractFunctionSource('normalizeWantedKeyPart'),
  extractFunctionSource('wantedIdentityKey'),
  extractFunctionSource('searchResultMediaType'),
  extractFunctionSource('wantedStateForResult'),
  extractFunctionSource('refreshWantedData'),
  extractFunctionSource('renderBookCard'),
  extractFunctionSource('renderLibraryBookCard'),
  extractFunctionSource('renderWantedGroups'),
  extractFunctionSource('renderWantedCard'),
  extractFunctionSource('removeWantedBook'),
  extractFunctionSource('renderBookDeletionPanel'),
  extractFunctionSource('renderBookDeleteConfirmation'),
  extractFunctionSource('openBookDeleteDialog'),
  extractFunctionSource('cancelBookDeleteDialog'),
  extractFunctionSource('mergeMatchingBookDuplicates'),
  extractFunctionSource('confirmBookDelete'),
  extractFunctionSource('formatBookDeleteError'),
  extractFunctionSource('renderMetricCard'),
  extractFunctionSource('loadHomeDashboard'),
  extractFunctionSource('renderCompactDownload'),
  extractFunctionSource('renderActivityRow'),
  extractFunctionSource('renderDashboardEmpty'),
  extractFunctionSource('buildFormatCounts'),
  extractFunctionSource('getLibraryImportFormValues'),
  extractFunctionSource('sanitizeLibraryImportValues'),
  extractFunctionSource('validateLibraryImportSettings'),
  extractFunctionSource('libraryImportSummaryLines'),
  extractFunctionSource('applyLibraryImportLoadedState'),
  extractFunctionSource('setLibraryImportStep2State'),
  extractFunctionSource('routeTabFromLocation'),
  extractFunctionSource('syncTabRoute'),
  extractFunctionSource('switchTab'),
  extractFunctionSource('setupLibrarr2Shell'),
  extractFunctionSource('loadWanted'),
  extractFunctionSource('updateLibraryRepairCardVisibility'),
  extractFunctionSource('renderNestedEbookPathRepair'),
  extractFunctionSource('renderRepairMetric'),
  extractFunctionSource('renderRepairEntry'),
  extractFunctionSource('repairStatusClass'),
  extractFunctionSource('previewNestedEbookPathRepair'),
  extractFunctionSource('runNestedEbookPathRepair'),
  extractFunctionSource('renderLibraryScanWorkspace'),
  extractFunctionSource('renderLibraryScanReady'),
  extractFunctionSource('renderLibraryScanProgress'),
  extractFunctionSource('renderLibraryScanProgressMetric'),
  extractFunctionSource('renderLibraryImportProgress'),
  extractFunctionSource('renderLibraryImportSummary'),
  extractFunctionSource('libraryImportElapsedSeconds'),
  extractFunctionSource('formatDurationSeconds'),
  extractFunctionSource('renderLibraryScanError'),
  extractFunctionSource('renderLibraryScanReview'),
  extractFunctionSource('renderLibraryScanImportActions'),
  extractFunctionSource('renderLibraryScanTotals'),
  extractFunctionSource('renderLibraryScanToolbar'),
  extractFunctionSource('renderLibraryScanSection'),
  extractFunctionSource('renderLibraryScanCandidate'),
  extractFunctionSource('renderLibraryScanCover'),
  extractFunctionSource('renderLibraryScanDestination'),
  extractFunctionSource('renderLibraryScanDuplicate'),
  extractFunctionSource('renderLibraryScanManualReview'),
  extractFunctionSource('renderLibraryScanMetadataEditor'),
  extractFunctionSource('findLibraryScanCandidate'),
  extractFunctionSource('metadataEditorDraftFromCandidate'),
  extractFunctionSource('openScanMetadataEditor'),
  extractFunctionSource('closeMetadataEditor'),
  extractFunctionSource('resetMetadataEditor'),
  extractFunctionSource('updateMetadataEditorDraft'),
  extractFunctionSource('updateMetadataEditorPreview'),
  extractFunctionSource('metadataEditorPreview'),
  extractFunctionSource('validateMetadataEditorDraft'),
  extractFunctionSource('metadataEditorPayload'),
  extractFunctionSource('saveMetadataEditor'),
  extractFunctionSource('pathExtension'),
  extractFunctionSource('pathDirname'),
  extractFunctionSource('safeFilenamePart'),
  extractFunctionSource('formatMetadataSource'),
  extractFunctionSource('formatMetadataConfidence'),
  extractFunctionSource('filterLibraryScanCandidates'),
  extractFunctionSource('groupLibraryScanCandidates'),
  extractFunctionSource('readyLibraryScanCandidates'),
  extractFunctionSource('selectedReadyLibraryScanCandidates'),
  extractFunctionSource('formatLibraryScanPhase'),
  extractFunctionSource('formatLibraryScanElapsed'),
  extractFunctionSource('startLibraryScan'),
  extractFunctionSource('pollLibraryScanJob'),
  extractFunctionSource('loadLibraryScanResults'),
  extractFunctionSource('startLibraryImport'),
  extractFunctionSource('pollLibraryImportJob'),
  extractFunctionSource('loadLibraryImportResults'),
  extractFunctionSource('resolveLibraryScanCandidate'),
  extractFunctionSource('retryFailedLibraryImport'),
  extractFunctionSource('refreshLibraryAfterScanImport'),
  extractFunctionSource('diagnosticPayload'),
  extractFunctionSource('renderDiagnosticResult'),
  extractFunctionSource('renderDiagnosticStep'),
  extractFunctionSource('diagnosticStatusClass'),
  extractFunctionSource('diagnosticStatusIcon'),
  extractFunctionSource('diagnosticStatusLabel'),
  extractFunctionSource('testConnection'),
  extractFunctionSource('configItem'),
  extractFunctionSource('loadConfig'),
  extractFunctionSource('loadStats'),
  extractFunctionSource('updateLibraryImportSaveState'),
  extractFunctionSource('saveLibraryImportSettings'),
  extractFunctionSource('libraryMetadataDraftFromBook'),
  extractFunctionSource('renderLibraryMetadataEditor'),
  extractFunctionSource('openLibraryMetadataEditor'),
  extractFunctionSource('closeLibraryMetadataEditor'),
  extractFunctionSource('resetLibraryMetadataEditor'),
  extractFunctionSource('updateLibraryMetadataEditorDraft'),
  extractFunctionSource('updateLibraryMetadataEditorValidation'),
  extractFunctionSource('validateLibraryMetadataDraft'),
  extractFunctionSource('libraryMetadataPatchFromDraft'),
  extractFunctionSource('saveLibraryMetadataEditor'),
  extractFunctionSource('libraryMetadataCandidate'),
  extractFunctionSource('publicationYearFromMetadata'),
  extractFunctionSource('loadUsers'),
  extractFunctionSource('editUser'),
  extractFunctionSource('changeUserRole'),
  extractFunctionSource('resetUserPassword'),
  extractFunctionSource('toggleUserEnabled'),
  extractFunctionSource('updateUser'),
  extractFunctionSource('cancelCreateUser'),
  extractFunctionSource('addUser'),
  extractFunctionSource('loadInviteCodes'),
].join('\n\n');

function libraryImportState(overrides = {}) {
  return {
    completed: false,
    dirty: false,
    lastSaved: null,
    flashTimer: null,
    scan: {
      running: false,
      jobId: '',
      pollTimer: null,
      startedAt: null,
      progress: null,
      result: null,
      filter: 'all',
      search: '',
      selected: new Set(),
      skipped: new Set(),
      editor: {
        candidateId: '',
        draft: null,
        errors: [],
      },
      import: {
        running: false,
        jobId: '',
        pollTimer: null,
        startedAt: null,
        progress: null,
        result: null,
        error: '',
      },
      sections: {
        new: true,
        manual_review: true,
        already_imported: false,
        duplicate: true,
        unsupported: false,
        unreadable: false,
      },
      error: '',
    },
    ...overrides,
  };
}

function createContext(overrides = {}) {
  const base = {
    t: key => key,
    formatIdentifierList: identifiers => (identifiers || []).map(item => item.value || item).join(', '),
    formatSize: size => `${size} B`,
    renderBookCover: () => '<cover />',
    renderOnboardingChecklist: () => '<section id="onboarding">onboarding</section>',
    renderCompactBookCard: (book, index) => `<article data-book="${index}">${book.title}</article>`,
    renderMetricCard: (label, value) => `<metric data-label="${label}">${value}</metric>`,
    renderCompactDownload: item => `<download>${item.title}</download>`,
    renderActivityRow: item => `<activity>${item.title}</activity>`,
    renderDashboardEmpty: () => '<empty />',
    escapeHtml: value => String(value),
    updateHomeHero: () => {},
    COVER_GRADIENTS: ['from-amber-700 to-stone-900', 'from-slate-700 to-stone-900'],
    LIBRARY_IMPORT_FIELDS: ['incoming_dir', 'ebook_dir', 'audiobook_dir', 'manga_dir'],
    state: {
      currentTab: 'home',
      libraryImport: libraryImportState(),
      libraryMetadataEditor: { open: false, draft: null, errors: [] },
      bookDeleteDialog: { open: false, deleteFiles: false, loading: false, error: '' },
      libraryBooks: [],
      wantedBooks: [],
      wantedIndex: new Set(),
      libraryMatchIndex: new Set(),
      searchTab: 'ebooks',
      searchResults: [],
      renderedResults: [],
      pendingDownloads: new Set(),
      trackedDownloadJobs: new Map(),
      downloadOutcomes: new Map(),
      activeDetailBook: null,
      activeDetailContext: null,
      libraryRepair: { nestedEbookPaths: { loading: false, running: false, plan: null, result: null, error: '' } },
      config: { library_repository_mode: 'normalized' },
    },
    document: { getElementById: () => null, querySelector: () => null },
    api: async () => ({ ok: true, json: async () => ({ success: true }) }),
    apiJson: async () => ({ success: true }),
    showToast: () => {},
    closeMobileNav: () => {},
    stopDownloadPolling: () => {},
    startDownloadPolling: () => {},
    loadHomeDashboard: async () => {},
    loadLibrary: async () => {},
    loadWanted: async () => {},
    loadSettings: async () => {},
    refreshDownloads: async () => {},
    getDownloadKey: result => result.title,
    hasTrackedAnnaDownload: () => false,
    hasTrackedDirectDownload: () => false,
    getTrackedDownloadJob: () => null,
    SOURCE_COLORS: { prowlarr: { bg: '#000', text: '#fff', label: 'Prowlarr' } },
    scrollToSettingsSection: () => {},
    window: { setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {}, prompt: () => null, location: { hash: '' }, addEventListener: () => {} },
    normalizedLibraryMode: () => false,
    groupLibraryItems: items => items,
  };
  const context = vm.createContext({ ...base, ...overrides });
  vm.runInContext(functionBundle, context);
  return context;
}

test('currentLibraryCount uses normalized total_books', () => {
  const context = createContext();
  assert.equal(context.currentLibraryCount(true, { total_books: 0, total_items: 9 }), 0);
  assert.equal(context.currentLibraryCount(true, { total_books: 4, total_items: 0 }), 4);
});

test('currentLibraryCount uses legacy total_items', () => {
  const context = createContext();
  assert.equal(context.currentLibraryCount(false, { total_books: 7, total_items: 0 }), 0);
  assert.equal(context.currentLibraryCount(false, { total_books: 0, total_items: 3 }), 3);
});

test('buildHomeDashboardMarkup returns onboarding-only markup for empty libraries', () => {
  const context = createContext();
  const markup = context.buildHomeDashboardMarkup({
    showOnboarding: true,
    recentBooks: [],
    formatCounts: {},
    downloads: [],
    wanted: { counts: { total: 1, monitored: 1, ignored: 0 } },
    activity: [],
    stats: { total_books: 0 },
    bookCount: 0,
  });

  assert.match(markup, /onboarding/);
  assert.match(markup, /dashboard_wanted/);
  assert.doesNotMatch(markup, /dashboard_recent/);
  assert.doesNotMatch(markup, /dashboard_totals/);
});

test('buildHomeDashboardMarkup returns dashboard panels for non-empty libraries', () => {
  const context = createContext();
  const markup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [{ title: 'The Martian', author: 'Andy Weir', formats: ['EPUB'], coverUrl: '/api/v1/books/1/cover' }],
    formatCounts: { EPUB: 1 },
    downloads: [],
    activity: [],
    activitySummary: {},
    attention: [],
    stats: { ebooks: 1, audiobooks: 0, manga: 0 },
    wanted: { counts: { wanted: 2, monitored: 1, ignored: 1 } },
    bookCount: 1,
    isAdmin: true,
  });

  assert.match(markup, /dashboard_recent/);
  assert.match(markup, /dashboard_totals/);
  assert.match(markup, /dashboard_downloading/);
  assert.match(markup, /dashboard_quick_actions/);
  assert.match(markup, /dashboard_wanted/);
  assert.doesNotMatch(markup, /id="onboarding"/);
});

test('discover card shows add to wanted when result is neither wanted nor in library', () => {
  const context = createContext({
    state: {
      wantedIndex: new Set(),
      libraryMatchIndex: new Set(),
      pendingDownloads: new Set(),
      trackedDownloadJobs: new Map(),
      downloadOutcomes: new Map(),
      searchTab: 'ebooks',
    },
  });

  const html = context.renderBookCard({ title: 'The Martian', author: 'Andy Weir', source: 'prowlarr' }, 0);

  assert.match(html, /data-action="addWantedFromSearch"/);
  assert.match(html, /wanted_add/);
});

test('discover card shows wanted badge for existing wanted result', () => {
  const key = 'the martian|andy weir|ebook';
  const context = createContext({
    state: {
      wantedIndex: new Set([key]),
      libraryMatchIndex: new Set(),
      pendingDownloads: new Set(),
      trackedDownloadJobs: new Map(),
      downloadOutcomes: new Map(),
      searchTab: 'ebooks',
    },
  });

  const html = context.renderBookCard({ title: 'The Martian', author: 'Andy Weir', source: 'prowlarr' }, 0);

  assert.doesNotMatch(html, /data-action="addWantedFromSearch"/);
  assert.match(html, /wanted_added/);
});

test('discover card shows in-library badge when result already exists in library', () => {
  const key = 'the martian|andy weir|ebook';
  const context = createContext({
    state: {
      wantedIndex: new Set(),
      libraryMatchIndex: new Set([key]),
      pendingDownloads: new Set(),
      trackedDownloadJobs: new Map(),
      downloadOutcomes: new Map(),
      searchTab: 'ebooks',
    },
  });

  const html = context.renderBookCard({ title: 'The Martian', author: 'Andy Weir', source: 'prowlarr' }, 0);

  assert.match(html, /wanted_in_library/);
});

test('home dashboard renders recent shelf with whole-card details action and covers', () => {
  const context = createContext();
  const markup = context.renderRecentlyAddedShelf([
    { title: 'Project Hail Mary', author: 'Andy Weir', formats: ['EPUB'], coverUrl: '/api/v1/books/42/cover' },
    { title: 'No Cover', author: 'Writer', formats: [] },
  ]);

  assert.match(markup, /data-action="openHomeBookDetails"/);
  assert.match(markup, /w-44 sm:w-48/);
  assert.match(markup, /src="\/api\/v1\/books\/42\/cover"/);
  assert.match(markup, /cover-placeholder/);
  assert.match(markup, /focus-visible:ring-2/);
});

test('compact home hero still renders welcome text and primary actions', () => {
  assert.match(appSource, /home-hero rounded-\[2rem\] p-4 sm:p-5 mb-4/);
  assert.match(appSource, /home-hero-title/);
  assert.match(appSource, /home-hero-subtitle/);
  assert.match(appSource, /home_open_library/);
  assert.match(appSource, /home_discover/);
});

test('home dashboard hides needs attention when empty and renders actionable items when present', () => {
  const context = createContext();
  let markup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [],
    formatCounts: {},
    downloads: [],
    activity: [],
    activitySummary: {},
    attention: [],
    stats: {},
    bookCount: 3,
    isAdmin: true,
  });
  assert.doesNotMatch(markup, /dashboard_attention/);

  markup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [],
    formatCounts: {},
    downloads: [],
    activity: [],
    activitySummary: { failed: 1 },
    attention: [{ title: '1 import failed', reason: 'Check details', action: 'switchTab', arg: 'downloads', label: 'View Details' }],
    stats: {},
    bookCount: 3,
    isAdmin: true,
  });
  assert.match(markup, /dashboard_attention/);
  assert.match(markup, /1 import failed/);
  assert.match(markup, /data-action="switchTab"/);
});

test('home dashboard activity summary normalizes downloads response and counts failures', () => {
  const context = createContext();
  assert.deepEqual(context.normalizeDownloadsResponse({ downloads: [{ status: 'downloading' }] }), [{ status: 'downloading' }]);
  const summary = context.buildDashboardActivitySummary(
    [{ status: 'downloading' }, { status: 'retry_wait' }, { status: 'error' }],
    [{ detail: 'Manual review required' }, { detail: 'Ready to import' }]
  );
  assert.equal(summary.downloading, 1);
  assert.equal(summary.waiting, 1);
  assert.equal(summary.failed, 1);
  assert.equal(summary.manualReview, 1);
  assert.equal(summary.ready, 1);
});

test('all-zero dashboard activity renders compact empty state instead of zero boxes', () => {
  const context = createContext();
  const markup = context.renderDashboardActivity({
    downloading: 0,
    waiting: 0,
    ready: 0,
    manualReview: 0,
    importing: 0,
    failed: 0,
  }, true);

  assert.match(markup, /dashboard_all_clear/);
  assert.doesNotMatch(markup, /dashboard_downloading_count/);
  assert.doesNotMatch(markup, />0</);
});

test('nonzero dashboard activity renders only meaningful states', () => {
  const context = createContext();
  const markup = context.renderDashboardActivity({
    downloading: 2,
    waiting: 0,
    ready: 1,
    manualReview: 0,
    importing: 0,
    failed: 1,
  }, true);

  assert.match(markup, /dashboard_downloading_count/);
  assert.match(markup, /dashboard_ready_to_import/);
  assert.match(markup, /dashboard_failed/);
  assert.doesNotMatch(markup, /dashboard_waiting/);
  assert.doesNotMatch(markup, /dashboard_manual_review/);
  assert.match(markup, /data-action="switchTab"/);
  assert.match(markup, /data-action="openImportSettings"/);
});

test('home quick actions hide admin-only scan action for normal users', () => {
  const context = createContext();
  const userMarkup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [],
    formatCounts: {},
    downloads: [],
    activity: [],
    activitySummary: {},
    attention: [],
    stats: {},
    bookCount: 2,
    isAdmin: false,
  });
  assert.doesNotMatch(userMarkup, /home_scan_library/);
  assert.match(userMarkup, /dashboard_open_opds/);

  const adminMarkup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [],
    formatCounts: {},
    downloads: [],
    activity: [],
    activitySummary: {},
    attention: [],
    stats: {},
    bookCount: 2,
    isAdmin: true,
  });
  assert.match(adminMarkup, /home_scan_library/);
});

test('empty-library onboarding respects admin actions and OPDS step', () => {
  const context = createContext();
  const adminMarkup = context.renderOnboardingChecklist(true);
  assert.match(adminMarkup, /home_import_library/);
  assert.match(adminMarkup, /home_scan_library/);
  assert.match(adminMarkup, /home_step_opds/);

  const userMarkup = context.renderOnboardingChecklist(false);
  assert.doesNotMatch(userMarkup, /home_import_library/);
  assert.doesNotMatch(userMarkup, /home_scan_library/);
  assert.match(userMarkup, /dashboard_open_opds/);
});

test('index.html uses renamed import folder label and helper text', () => {
  assert.match(indexHTML, /Import Folder \(Downloads\)/);
  assert.match(indexHTML, /Folder where Librarr watches for newly downloaded books\./);
  assert.doesNotMatch(indexHTML, /Incoming Directory/);
});

test('index.html uses requested field ordering', () => {
  const labels = [
    'Import Folder (Downloads)',
    'Ebook Library',
    'Audiobook Library',
    'Manga Library',
  ];
  const positions = labels.map(label => indexHTML.indexOf(label));
  positions.forEach((pos, idx) => {
    assert.notEqual(pos, -1, `missing label ${labels[idx]}`);
  });
  assert.ok(positions[0] < positions[1] && positions[1] < positions[2] && positions[2] < positions[3], 'field order does not match requested progression');
});

test('index.html uses improved Step 2 copy and Save & Continue action', () => {
  assert.match(indexHTML, /Step 2 – Scan Your Existing Library/);
  assert.match(indexHTML, /Available after saving your library folders\./);
  assert.match(indexHTML, /Save & Continue/);
  assert.match(indexHTML, /Step 1 Complete/);
  assert.match(indexHTML, /settings-library-scan-workspace/);
  assert.match(appSource, /Scan Library/);
  assert.doesNotMatch(indexHTML, /Next: Scan Existing Collection/);
});

test('index.html places Save & Continue before Step 2', () => {
  const buttonPos = indexHTML.indexOf('Save & Continue');
  const step2Pos = indexHTML.indexOf('Step 2 – Scan Your Existing Library');
  assert.notEqual(buttonPos, -1);
  assert.notEqual(step2Pos, -1);
  assert.ok(buttonPos < step2Pos, 'Save & Continue should appear before Step 2');
});

test('index.html shows Step 2 as disabled before save', () => {
  assert.match(indexHTML, /data-state="locked"/);
  assert.match(indexHTML, /🔒/);
  assert.match(indexHTML, /opacity-75/);
  assert.match(indexHTML, /Available after saving your library folders\./);
});

test('index.html uses Step 1 heading', () => {
  assert.match(indexHTML, /Step 1 – Configure Library Folders/);
  assert.doesNotMatch(indexHTML, /Import starts here\./);
});

test('validateLibraryImportSettings rejects empty, duplicate, double-slash, and trailing-space paths', () => {
  const context = createContext();
  const result = context.validateLibraryImportSettings({
    incoming_dir: '/downloads  ',
    ebook_dir: '',
    audiobook_dir: '/books//audio',
    manga_dir: '/downloads',
    file_org_enabled: false,
  });

  assert.match(result.errors.join('\n'), /Ebook Library is required\./);
  assert.match(result.errors.join('\n'), /Import Folder contains leading or trailing whitespace\./);
  assert.match(result.errors.join('\n'), /Audiobook Library contains a double slash\./);
});

test('saveLibraryImportSettings uses existing settings save path', async () => {
  const elements = {
    'setting-incoming_dir': { value: '/downloads' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: true },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: {}, classList: fakeClassList(), focus() {} },
    'settings-library-import-step2-icon': { textContent: '', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList() },
    'settings-library-import-complete': { classList: fakeClassList(['hidden']) },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  let request = null;
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async (url, options) => {
      request = { url, options };
      return { success: true };
    },
  });

  await context.saveLibraryImportSettings(true);

  assert.equal(request.url, '/api/settings');
  assert.equal(request.options.method, 'POST');
  const body = JSON.parse(request.options.body);
  assert.deepEqual(body, {
    incoming_dir: '/downloads',
    ebook_dir: '/books',
    audiobook_dir: '/audiobooks',
    manga_dir: '/manga',
    file_org_enabled: true,
  });
});

test('successful Save & Continue advances focus to Step 2 panel', async () => {
  const events = [];
  const step2 = { focus: () => events.push('focus') };
  const elements = {
    'setting-incoming_dir': { value: '/downloads' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: false },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': Object.assign(step2, { dataset: {}, classList: fakeClassList() }),
    'settings-library-import-step2-icon': { textContent: '', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList() },
    'settings-library-import-complete': { classList: fakeClassList(['hidden']) },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async () => ({ success: true }),
    showToast: (msg, kind) => events.push(`toast:${kind}:${msg}`),
    scrollToSettingsSection: id => events.push(`scroll:${id}`),
  });

  await context.saveLibraryImportSettings(true);

  assert.deepEqual(events, [
    'toast:success:Library and import settings saved',
    'scroll:settings-library-import-step2',
    'focus',
  ]);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'ready');
  assert.equal(elements['settings-library-import-step2-copy'].textContent, 'Your folders are saved. Librarr is ready to scan your existing collection.');
  assert.match(elements['settings-library-import-summary'].innerHTML, /Import Folder/);
  assert.match(elements['settings-library-import-summary'].innerHTML, /\/downloads/);
  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), true);
  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-complete-title'].textContent, 'Step 1 Complete');
});

test('failed Save & Continue shows an error and does not advance', async () => {
  const events = [];
  const elements = {
    'setting-incoming_dir': { value: '/downloads' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: false },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: {}, classList: fakeClassList(), focus: () => events.push('focus') },
    'settings-library-import-step2-icon': { textContent: '', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: 'old', classList: fakeClassList() },
    'settings-library-import-save-continue': { classList: fakeClassList() },
    'settings-library-import-complete': { classList: fakeClassList(['hidden']) },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async () => ({ success: false, error: 'Nope' }),
    showToast: (msg, kind) => events.push(`toast:${kind}:${msg}`),
    scrollToSettingsSection: id => events.push(`scroll:${id}`),
  });

  await context.saveLibraryImportSettings(true);

  assert.deepEqual(events, ['toast:error:Nope']);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'locked');
  assert.equal(elements['settings-library-import-summary'].innerHTML, '');
  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), true);
});

test('fresh configuration shows Save & Continue and keeps Step 2 locked', () => {
  const elements = {
    'setting-incoming_dir': { value: '' },
    'setting-ebook_dir': { value: '' },
    'setting-audiobook_dir': { value: '' },
    'setting-manga_dir': { value: '' },
    'setting-file_org_enabled': { checked: false },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: { state: 'locked' }, classList: fakeClassList(['opacity-75']) },
    'settings-library-import-step2-icon': { textContent: '🔒', className: '' },
    'settings-library-import-step2-copy': { textContent: 'Available after saving your library folders.' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList(['hidden']), textContent: '' },
    'settings-library-import-complete': { classList: fakeClassList(['hidden']) },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
  });

  context.updateLibraryImportSaveState();

  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), true);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'locked');
});

test('completed state restores from loaded settings and keeps Step 2 active', () => {
  const elements = {
    'setting-incoming_dir': { value: '/downloads' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: true },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: { state: 'ready' }, classList: fakeClassList() },
    'settings-library-import-step2-icon': { textContent: '✅', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList(['hidden']), textContent: '' },
    'settings-library-import-complete': { classList: fakeClassList() },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
  });

  context.applyLibraryImportLoadedState({
    incoming_dir: '/downloads',
    ebook_dir: '/books',
    audiobook_dir: '/audiobooks',
    manga_dir: '/manga',
    file_org_enabled: true,
  });

  assert.equal(context.state.libraryImport.completed, true);
  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), true);
  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'ready');
  assert.match(elements['settings-library-import-summary'].innerHTML, /\/downloads/);
});

test('Save & Continue does not return after later edits', () => {
  const elements = {
    'setting-incoming_dir': { value: '/downloads-2' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: true },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: { state: 'ready' }, classList: fakeClassList() },
    'settings-library-import-step2-icon': { textContent: '✅', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList(['hidden']), textContent: '' },
    'settings-library-import-complete': { classList: fakeClassList() },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, dirty: false, lastSaved: {
      incoming_dir: '/downloads',
      ebook_dir: '/books',
      audiobook_dir: '/audiobooks',
      manga_dir: '/manga',
      file_org_enabled: true,
    } }) },
    document: { getElementById: id => elements[id] || null },
  });

  context.updateLibraryImportSaveState();

  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), true);
  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-unsaved'].classList.contains('hidden'), false);
  assert.equal(context.state.libraryImport.dirty, true);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'ready');
});

test('standard purple Save persists Library & Import changes after onboarding and updates summary', async () => {
  const events = [];
  const elements = {
    'setting-incoming_dir': { value: '/downloads-2' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: true },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: {}, classList: fakeClassList(), focus: () => events.push('focus') },
    'settings-library-import-step2-icon': { textContent: '', className: '' },
    'settings-library-import-step2-copy': { textContent: '' },
    'settings-library-import-summary': { innerHTML: '', classList: fakeClassList(['hidden']) },
    'settings-library-import-save-continue': { classList: fakeClassList(), textContent: '' },
    'settings-library-import-complete': { classList: fakeClassList(['hidden']) },
    'settings-library-import-complete-title': { textContent: '' },
    'settings-library-import-complete-copy': { textContent: '' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, dirty: true, lastSaved: {
      incoming_dir: '/downloads',
      ebook_dir: '/books',
      audiobook_dir: '/audiobooks',
      manga_dir: '/manga',
      file_org_enabled: true,
    } }) },
    document: { getElementById: id => elements[id] || null },
    apiJson: async () => ({ success: true }),
    showToast: (msg, kind) => events.push(`toast:${kind}:${msg}`),
    scrollToSettingsSection: id => events.push(`scroll:${id}`),
    window: { setTimeout: fn => { events.push('timer'); fn(); return 1; }, clearTimeout: () => {} },
  });

  await context.saveLibraryImportSettings(false);

  assert.equal(elements['settings-library-import-complete'].classList.contains('hidden'), false);
  assert.equal(elements['settings-library-import-complete-title'].textContent, 'Step 1 Complete');
  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), true);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'ready');
  assert.match(elements['settings-library-import-summary'].innerHTML, /\/downloads-2/);
  assert.equal(context.state.libraryImport.dirty, false);
  assert.match(events.join('\n'), /toast:success:Library and import settings saved/);
  assert.ok(!events.includes('scroll:settings-library-import-step2'));
});

test('failed standard save leaves Step 2 active and does not update summary', async () => {
  const events = [];
  const elements = {
    'setting-incoming_dir': { value: '/downloads-2' },
    'setting-ebook_dir': { value: '/books' },
    'setting-audiobook_dir': { value: '/audiobooks' },
    'setting-manga_dir': { value: '/manga' },
    'setting-file_org_enabled': { checked: true },
    'settings-library-import-save-standard': { disabled: false, classList: fakeClassList() },
    'settings-library-import-step2': { dataset: { state: 'ready' }, classList: fakeClassList() },
    'settings-library-import-step2-icon': { textContent: '✅', className: '' },
    'settings-library-import-step2-copy': { textContent: 'Your folders are saved. Librarr is ready to scan your existing collection.' },
    'settings-library-import-summary': { innerHTML: '<div>/downloads</div>', classList: fakeClassList() },
    'settings-library-import-save-continue': { classList: fakeClassList(['hidden']), textContent: '' },
    'settings-library-import-complete': { classList: fakeClassList() },
    'settings-library-import-complete-title': { textContent: 'Step 1 Complete' },
    'settings-library-import-complete-copy': { textContent: 'Library folders configured successfully.' },
    'settings-library-import-unsaved': { classList: fakeClassList(['hidden']) },
    'settings-library-import-validation': { classList: fakeClassList(['hidden']), innerHTML: '' },
  };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, dirty: true, lastSaved: {
      incoming_dir: '/downloads',
      ebook_dir: '/books',
      audiobook_dir: '/audiobooks',
      manga_dir: '/manga',
      file_org_enabled: true,
    } }) },
    document: { getElementById: id => elements[id] || null },
    apiJson: async () => ({ success: false, error: 'Nope' }),
    showToast: (msg, kind) => events.push(`toast:${kind}:${msg}`),
  });

  await context.saveLibraryImportSettings(false);

  assert.deepEqual(events, ['toast:error:Nope']);
  assert.equal(elements['settings-library-import-step2'].dataset.state, 'ready');
  assert.match(elements['settings-library-import-summary'].innerHTML, /\/downloads/);
  assert.equal(elements['settings-library-import-save-continue'].classList.contains('hidden'), true);
});

test('index.html keeps the standard Settings save control', () => {
  assert.match(indexHTML, /id="settings-library-import-save-standard"/);
  assert.match(indexHTML, /data-action="saveLibraryImportStandard"/);
  assert.match(indexHTML, />Save<\/button>/);
});

test('admin nested ebook path repair card renders and normal users do not see it', () => {
  const card = { classList: fakeClassList(['hidden']) };
  const output = { innerHTML: '' };
  const context = createContext({
    state: {
      currentRole: 'user',
      libraryRepair: { nestedEbookPaths: { loading: false, running: false, plan: null, result: null, error: '' } },
    },
    document: { getElementById: id => id === 'settings-library-repairs' ? card : (id === 'settings-library-repairs-output' ? output : null) },
  });

  context.updateLibraryRepairCardVisibility();
  assert.equal(card.classList.contains('hidden'), true);

  context.state.currentRole = 'admin';
  context.updateLibraryRepairCardVisibility();
  assert.equal(card.classList.contains('hidden'), false);
  assert.match(indexHTML, /Repair Nested Ebook Paths/);
  assert.match(indexHTML, /data-action="previewNestedEbookPathRepair"/);
  assert.match(indexHTML, /data-action="runNestedEbookPathRepair"/);
});

test('nested ebook path repair preview renders summary and statuses', async () => {
  const output = { innerHTML: '' };
  const plan = {
    success: true,
    legacy_root: '/books/ebooks/ebooks',
    total_affected_files: 3,
    files_found_on_disk: 4,
    correct_root_files: 1,
    reconciliation: { cataloged_normalized: 1, cataloged_legacy_only: 1, cataloged_unmanaged: 1, uncataloged: 1 },
    summary: { ready: 1, collision: 1, missing: 1 },
    entries: [
      { book_title: 'Ready', file_id: 1, format: 'epub', source_path: '/books/ebooks/ebooks/A/Ready.epub', destination_path: '/books/ebooks/A/Ready.epub', status: 'ready', class: 'cataloged_normalized', message: 'ready to move' },
      { book_title: 'Collision', file_id: 2, format: 'mobi', source_path: '/books/ebooks/ebooks/A/Collision.mobi', destination_path: '/books/ebooks/A/Collision.mobi', status: 'collision', message: 'destination already exists' },
      { book_title: 'Missing', file_id: 3, format: 'pdf', source_path: '/books/ebooks/ebooks/A/Missing.pdf', destination_path: '/books/ebooks/A/Missing.pdf', status: 'missing', message: 'source file is missing' },
    ],
  };
  const context = createContext({
    state: { libraryRepair: { nestedEbookPaths: { loading: false, running: false, plan: null, result: null, error: '' } } },
    document: { getElementById: id => id === 'settings-library-repairs-output' ? output : null },
    apiJson: async url => {
      assert.equal(url, '/api/v1/library/repairs/nested-ebook-paths');
      return plan;
    },
  });

  await context.previewNestedEbookPathRepair();

  assert.match(output.innerHTML, /Files on disk/);
  assert.match(output.innerHTML, /Normalized/);
  assert.match(output.innerHTML, /Legacy-only/);
  assert.match(output.innerHTML, /Unmanaged/);
  assert.match(output.innerHTML, /Ready to move/);
  assert.match(output.innerHTML, /cataloged normalized/);
  assert.match(output.innerHTML, /Collision/);
  assert.match(output.innerHTML, /source file is missing/);
});

test('nested ebook path repair no-op explains why nothing can move', async () => {
  const output = { innerHTML: '' };
  const context = createContext({
    state: {
      libraryRepair: {
        nestedEbookPaths: {
          loading: false,
          running: false,
          error: '',
          plan: {
            success: true,
            legacy_root: '/books/ebooks/ebooks',
            files_found_on_disk: 27,
            reconciliation: { uncataloged: 27 },
            summary: { ready: 0 },
            entries: [{ source_path: '/books/ebooks/ebooks/Orphan.epub', status: 'already_repaired', class: 'uncataloged', message: 'left unchanged' }],
          },
          result: null,
        },
      },
    },
    document: { getElementById: id => id === 'settings-library-repairs-output' ? output : null },
  });

  context.renderNestedEbookPathRepair();

  assert.match(output.innerHTML, /No files are currently eligible/);
  assert.match(output.innerHTML, /Uncataloged/);
  assert.match(output.innerHTML, /left unchanged/);
});

test('nested ebook path repair execution requires confirmation and renders success', async () => {
  const calls = [];
  const output = { innerHTML: '' };
  const context = createContext({
    state: {
      libraryRepair: {
        nestedEbookPaths: {
          loading: false,
          running: false,
          error: '',
          plan: { total_affected_files: 2, summary: { ready: 2 }, entries: [] },
          result: null,
        },
      },
    },
    document: { getElementById: id => id === 'settings-library-repairs-output' ? output : null },
    window: { confirm: () => false, setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {} },
    apiJson: async () => {
      calls.push('api');
      return {};
    },
  });

  await context.runNestedEbookPathRepair();
  assert.deepEqual(calls, []);

  context.window.confirm = () => true;
  context.apiJson = async (url, options = {}) => {
    calls.push(`${options.method}:${url}`);
    return { executed: true, legacy_root_removed: true, total_affected_files: 2, summary: { moved: 2 }, entries: [] };
  };
  context.loadLibrary = async () => calls.push('loadLibrary');
  context.showToast = (message, kind) => calls.push(`${kind}:${message}`);

  await context.runNestedEbookPathRepair();

  assert.deepEqual(calls.slice(0, 2), ['POST:/api/v1/library/repairs/nested-ebook-paths', 'loadLibrary']);
  assert.match(output.innerHTML, /Repair complete/);
  assert.match(output.innerHTML, /legacy nested directory was removed/);
});

test('nested ebook path repair API failure remains actionable', async () => {
  const output = { innerHTML: '' };
  const toasts = [];
  const context = createContext({
    state: {
      libraryRepair: { nestedEbookPaths: { loading: false, running: false, plan: { summary: { ready: 1 }, entries: [] }, result: null, error: '' } },
    },
    document: { getElementById: id => id === 'settings-library-repairs-output' ? output : null },
    window: { confirm: () => true, setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {} },
    apiJson: async () => { throw new Error('permission denied'); },
    showToast: (message, kind) => toasts.push({ message, kind }),
  });

  await context.runNestedEbookPathRepair();

  assert.match(output.innerHTML, /permission denied/);
  assert.deepEqual(toasts, [{ message: 'permission denied', kind: 'error' }]);
});

test('main navigation is focused on Librarr 2.0 primary destinations', () => {
  assert.match(indexHTML, /data-arg="home"/);
  assert.match(indexHTML, /data-arg="library"/);
  assert.match(indexHTML, /data-arg="search"[\s\S]*nav_discover/);
  assert.match(indexHTML, /data-arg="wanted"[\s\S]*nav_wanted/);
  assert.match(indexHTML, /data-arg="settings"/);
  assert.doesNotMatch(indexHTML, /id="lang-toggle"/);
  assert.doesNotMatch(appSource, /lang-toggle|toggleLanguage/);
  assert.doesNotMatch(appCSS, /Russian locale/);
  assert.doesNotMatch(indexHTML, /data-arg="downloads" class="nav-tab/);
  assert.doesNotMatch(indexHTML, /data-arg="wishlist" class="nav-tab/);
});

test('setupLibrarr2Shell preserves Wanted nav in runtime shell', () => {
  const nav = { innerHTML: '' };
  const main = { insertBefore: () => {} };
  const context = createContext({
    document: {
      body: { classList: { remove: () => {}, add: () => {} } },
      querySelector: selector => {
        if (selector === 'header') return { classList: { add: () => {} } };
        if (selector === 'main') return main;
        return null;
      },
      getElementById: id => {
        if (id === 'app') return { classList: { add: () => {} } };
        if (id === 'main-nav') return nav;
        if (id === 'tab-search') return {};
        if (id === 'tab-home') return {};
        if (id === 'book-detail-modal') return {};
        if (id === 'library-results') return null;
        return null;
      },
      createElement: () => ({ className: '', id: '', innerHTML: '' }),
    },
  });

  context.setupLibrarr2Shell();

  assert.match(nav.innerHTML, /data-arg="wanted"/);
});

test('routeTabFromLocation resolves wanted and settings section hashes', () => {
  const context = createContext({ window: { location: { hash: '#wanted' }, addEventListener: () => {}, setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {} } });
  assert.equal(context.routeTabFromLocation(), 'wanted');
  context.window.location.hash = '#settings-library-import';
  assert.equal(context.routeTabFromLocation(), 'settings');
  context.window.location.hash = '';
  assert.equal(context.routeTabFromLocation(), 'home');
});

test('switchTab activates wanted, renders tab, and loads wanted data', () => {
  const calls = [];
  const navWanted = { dataset: { tab: 'wanted' }, classList: { toggle: (name, active) => calls.push(`nav:${name}:${active}`) } };
  const navHome = { dataset: { tab: 'home' }, classList: { toggle: () => {} } };
  const tabWanted = { id: 'tab-wanted', classList: { toggle: (name, active) => calls.push(`tab-wanted:${active}`) } };
  const tabHome = { id: 'tab-home', classList: { toggle: () => {} } };
  const context = createContext({
    document: {
      querySelectorAll: selector => {
        if (selector === '.nav-tab') return [navHome, navWanted];
        if (selector === '.tab-content') return [tabHome, tabWanted];
        return [];
      },
    },
    closeMobileNav: () => calls.push('closeMobileNav'),
    stopDownloadPolling: () => calls.push('stopDownloadPolling'),
    window: { location: { hash: '' }, addEventListener: () => {}, setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {} },
  });
  context.loadWanted = () => calls.push('loadWanted');

  context.switchTab('wanted');

  assert.equal(context.state.currentTab, 'wanted');
  assert.equal(context.window.location.hash, 'wanted');
  assert.match(calls.join('|'), /loadWanted/);
  assert.match(calls.join('|'), /tab-wanted:true/);
});

test('loadWanted calls wanted API and renders summary counts', async () => {
  const wantedList = { innerHTML: '' };
  const wantedEmpty = { classList: { remove: () => {}, add: () => {} } };
  const wantedSummary = { innerHTML: '' };
  const context = createContext({
    document: {
      getElementById: id => {
        if (id === 'wanted-list') return wantedList;
        if (id === 'wanted-empty') return wantedEmpty;
        if (id === 'wanted-summary') return wantedSummary;
        return null;
      },
    },
    apiJson: async url => {
      assert.equal(url, '/api/v1/wanted');
      return {
        items: [{ id: 1, title: 'The Martian', author: 'Andy Weir', status: 'wanted', monitored: true, media_type: 'ebook' }],
        counts: { total: 1, wanted: 1, monitored: 1, ignored: 0 },
      };
    },
  });

  await context.loadWanted();

  assert.match(wantedSummary.innerHTML, />1</);
  assert.match(wantedList.innerHTML, /The Martian/);
});

test('home wanted widget is clickable and survives wanted API failure', () => {
  const context = createContext();
  const markup = context.buildHomeDashboardMarkup({
    showOnboarding: false,
    recentBooks: [{ title: 'The Martian', author: 'Andy Weir', formats: ['EPUB'] }],
    formatCounts: { EPUB: 1 },
    downloads: [],
    activity: [],
    activitySummary: {},
    attention: [],
    stats: { ebooks: 1, audiobooks: 0, manga: 0 },
    wanted: { counts: {} },
    bookCount: 1,
    isAdmin: false,
  });

  assert.match(markup, /data-action="switchTab" data-arg="wanted"/);
  assert.match(markup, /dashboard_wanted/);
});

test('loadHomeDashboard keeps Home usable when wanted API fails', async () => {
  const container = { innerHTML: '' };
  const context = createContext({
    normalizedLibraryMode: () => true,
    document: { getElementById: id => id === 'home-dashboard' ? container : null },
    apiJson: async url => {
      if (url === '/api/v1/library/summary') return { total_books: 2, ebooks: 2, audiobooks: 0, manga: 0, format_distribution: { EPUB: 2 } };
      if (url === '/api/v1/books?limit=8&offset=0&sort=recently_added&order=desc') return { items: [{ id: 1, title: 'The Martian', primary_author: { name: 'Andy Weir' }, media_type: 'ebook', formats: ['EPUB'], cover_url: '/cover' }] };
      if (url === '/api/downloads') return [];
      if (url === '/api/activity?limit=6') return { events: [] };
      if (url === '/api/v1/wanted') throw new Error('API error: 500');
      throw new Error(`unexpected ${url}`);
    },
    mapV1BookToUIBook: book => ({ title: book.title, author: book.primary_author?.name || '', formats: book.formats || [], coverUrl: book.cover_url || '' }),
  });

  await context.loadHomeDashboard();

  assert.match(container.innerHTML, /dashboard_recent/);
  assert.match(container.innerHTML, /dashboard_wanted/);
});

test('wanted page groups wanted and ignored books', () => {
  const context = createContext();
  const html = context.renderWantedGroups([
    { id: 1, title: 'The Martian', author: 'Andy Weir', status: 'wanted', monitored: true, media_type: 'ebook' },
    { id: 2, title: 'Ignored Book', author: 'Writer', status: 'ignored', monitored: false, media_type: 'ebook' },
  ]);

  assert.match(html, /wanted_group_wanted/);
  assert.match(html, /wanted_group_ignored/);
  assert.match(html, /The Martian/);
  assert.match(html, /Ignored Book/);
});

test('remove wanted book confirms before deleting', async () => {
  const calls = [];
  const context = createContext({
    apiJson: async (url, options = {}) => {
      calls.push(`${options.method}:${url}`);
      return { success: true };
    },
    refreshDiscoverIndexes: async () => calls.push('refreshDiscoverIndexes'),
    renderSearchResults: () => calls.push('renderSearchResults'),
    showToast: message => calls.push(message),
    window: { confirm: () => true, setTimeout: fn => { fn(); return 1; }, clearTimeout: () => {} },
    state: { searchResults: [] },
  });
  context.loadWanted = async () => calls.push('loadWanted');
  context.loadHomeDashboard = async () => calls.push('loadHomeDashboard');

  await context.removeWantedBook(7, 'The Martian');

  assert.deepEqual(calls.slice(0, 3), ['DELETE:/api/v1/wanted/7', 'loadWanted', 'loadHomeDashboard']);
});

test('unfinished device actions are not exposed in book cards', () => {
  const context = createContext({
    renderBookCover: () => '<cover />',
    renderFormatBadge: format => `<format>${format}</format>`,
  });

  const html = context.renderLibraryBookCard({ title: 'Book', author: 'Author', formats: ['EPUB'] }, 0);

  assert.doesNotMatch(html, /sendBookToDevices|>Send</);
  assert.doesNotMatch(appSource, /function sendBookToDevices|tab-devices|devices-grid/);
});

test('library book card renders returned cover URL', () => {
	const context = createContext({
		renderFormatBadge: format => `<format>${format}</format>`,
	});

  const html = context.renderLibraryBookCard({
    title: 'Covered Book',
    author: 'Author',
    formats: ['EPUB'],
    coverUrl: '/api/v1/books/42/cover',
	}, 0);

	assert.match(html, /<img src="\/api\/v1\/books\/42\/cover"/);
});

test('normalized book cards render one card with sorted unique format chips', () => {
  const context = createContext({
    renderBookCover: () => '<cover />',
    renderFormatBadge: format => `<format>${format}</format>`,
  });
  const book = context.mapV1BookToUIBook({
    id: 42,
    title: 'Ameritopia',
    primary_author: { name: 'Mark R. Levin' },
    media_type: 'ebook',
    formats: ['mobi', 'EPUB', 'epub', 'azw3'],
  });
  const html = context.renderLibraryBookCard(book, 0);

  assert.deepEqual(book.formats, ['EPUB', 'AZW3', 'MOBI']);
  assert.equal((html.match(/<format>/g) || []).length, 3);
  assert.match(html, /Ameritopia/);
  assert.match(html, /Mark R\. Levin/);
});

test('book management panel exposes admin duplicate merge repair', () => {
  const context = createContext({
    state: {
      currentRole: 'admin',
      bookDeleteDialog: { open: false, deleteFiles: false, loading: false, error: '' },
    },
  });
  const html = context.renderBookDeletionPanel(
    { id: 42, title: 'Men in Black', author: 'Mark R. Levin', formats: ['EPUB', 'MOBI'] },
    [{ path: '/books/men-in-black.epub', format: 'EPUB', size: 10 }],
  );

  assert.match(html, /Merge Matching Duplicates/);
  assert.match(html, /data-action="mergeMatchingBookDuplicates"/);
});

test('merge matching duplicate books posts repair endpoint and reopens surviving book', async () => {
  const calls = [];
  const context = createContext({
    state: {
      currentTab: 'library',
      activeDetailBook: { id: 17, title: 'Men in Black- How the Supreme Court is Destroying America' },
      libraryBooks: [{ id: 16, title: 'Men in Black: How the Supreme Court is Destroying America' }],
    },
    apiJson: async (url, options = {}) => {
      calls.push({ type: 'api', url, method: options.method });
      return { success: true, target_book_id: 16, merged_count: 1 };
    },
    loadLibrary: async () => {
      calls.push({ type: 'loadLibrary' });
      context.state.libraryBooks = [{ id: 16, title: 'Men in Black: How the Supreme Court is Destroying America' }];
    },
    openBookDetails: async (index, collection) => calls.push({ type: 'openBookDetails', index, collection }),
    closeBookDetails: () => calls.push({ type: 'closeBookDetails' }),
    showToast: (message, kind) => calls.push({ type: 'toast', message, kind }),
  });
  context.loadStats = async () => calls.push({ type: 'loadStats' });

  await context.mergeMatchingBookDuplicates();

  assert.deepEqual(calls.map(call => call.type), ['api', 'loadLibrary', 'loadStats', 'openBookDetails', 'toast']);
  assert.equal(calls[0].url, '/api/v1/books/17/merge-matching');
  assert.equal(calls[0].method, 'POST');
  assert.deepEqual(calls[3], { type: 'openBookDetails', index: 0, collection: 'libraryBooks' });
  assert.match(calls[4].message, /Merged 1 duplicate book/);
});

test('book deletion panel explains remove-only and hides disk delete from non-admin users', () => {
  const context = createContext({
    state: {
      currentRole: 'user',
      bookDeleteDialog: { open: true, deleteFiles: false, loading: false, error: '' },
    },
  });
  const html = context.renderBookDeletionPanel(
    { id: 42, title: 'Ameritopia', author: 'Mark R. Levin', formats: ['EPUB', 'MOBI'] },
    [{ path: '/books/Ameritopia.epub', format: 'EPUB', size: 10 }, { path: '/books/Ameritopia.mobi', format: 'MOBI', size: 10 }],
  );

  assert.match(html, /Remove “Ameritopia” from Librarr/);
  assert.match(html, /files will remain on disk/);
  assert.doesNotMatch(html, /Delete Book and Files/);
});

test('admin destructive delete confirmation lists files and explicit count', () => {
  const context = createContext({
    state: {
      currentRole: 'admin',
      bookDeleteDialog: { open: true, deleteFiles: true, loading: false, error: '' },
    },
  });
  const html = context.renderBookDeletionPanel(
    { id: 42, title: 'Ameritopia', author: 'Mark R. Levin', formats: ['mobi', 'epub'] },
    [{ path: '/books/Ameritopia.epub', format: 'EPUB', size: 10 }, { path: '/books/Ameritopia.mobi', format: 'MOBI', size: 20 }],
  );

  assert.match(html, /Delete “Ameritopia” and 2 files/);
  assert.match(html, /Ameritopia\.epub/);
  assert.match(html, /Ameritopia\.mobi/);
  assert.match(html, /Delete Book and 2 Files/);
});

test('cancel book deletion makes no API request', async () => {
  const calls = [];
  const context = createContext({
    state: {
      bookDeleteDialog: { open: true, deleteFiles: true, loading: false, error: '' },
      activeDetailContext: { index: 0, collection: 'libraryBooks' },
    },
    apiJson: async () => {
      calls.push('api');
      return {};
    },
    openBookDetails: async () => calls.push('openBookDetails'),
  });

  context.cancelBookDeleteDialog();

  assert.equal(calls.includes('api'), false);
  assert.equal(context.state.bookDeleteDialog.open, false);
});

test('successful normalized book deletion refreshes library and home state', async () => {
  const calls = [];
  const context = createContext({
    state: {
      currentTab: 'home',
      activeDetailBook: { id: 42, title: 'Ameritopia' },
      bookDeleteDialog: { open: true, deleteFiles: true, loading: false, error: '' },
      activeDetailContext: { index: 0, collection: 'libraryBooks' },
    },
    api: async (url, options) => {
      calls.push({ type: 'api', url, method: options?.method });
      return { ok: true, json: async () => ({ success: true, title: 'Ameritopia', deleted_files: 2 }) };
    },
    openBookDetails: async () => calls.push({ type: 'openBookDetails' }),
    closeBookDetails: () => calls.push({ type: 'closeBookDetails' }),
    loadLibrary: async () => calls.push({ type: 'loadLibrary' }),
    loadStats: async () => calls.push({ type: 'loadStats' }),
    showToast: (message, kind) => calls.push({ type: 'toast', message, kind }),
    normalizedLibraryMode: () => true,
  });
  context.loadHomeDashboard = async () => calls.push({ type: 'loadHomeDashboard' });

  await context.confirmBookDelete();

  assert.deepEqual(calls.map(call => call.type), ['openBookDetails', 'api', 'closeBookDetails', 'loadLibrary', 'loadHomeDashboard', 'toast']);
  assert.equal(calls[1].url, '/api/v1/books/42?delete_files=true');
  assert.equal(calls[1].method, 'DELETE');
});

test('failed normalized book deletion keeps dialog open with structured error details', async () => {
  const context = createContext({
    state: {
      activeDetailBook: { id: 42, title: 'Ameritopia' },
      bookDeleteDialog: { open: true, deleteFiles: false, loading: false, error: '' },
      activeDetailContext: { index: 0, collection: 'libraryBooks' },
    },
    api: async () => ({
      ok: false,
      status: 409,
      json: async () => ({
        success: false,
        error: 'One or more files could not be deleted. The book remains in the catalog so deletion can be retried.',
        files: [{ filename: '9781260721485.pdf', error: 'delete failed' }],
      }),
    }),
    openBookDetails: async () => {},
  });

  await context.confirmBookDelete();

  assert.equal(context.state.bookDeleteDialog.open, true);
  assert.match(context.state.bookDeleteDialog.error, /One or more files could not be deleted/);
  assert.match(context.state.bookDeleteDialog.error, /9781260721485\.pdf: delete failed/);
  assert.doesNotMatch(context.state.bookDeleteDialog.error, /^API error: 409$/);
});

test('onboarding checklist does not duplicate the import action', () => {
  const context = createContext({
    state: { currentUser: 'admin', libraryImport: libraryImportState() },
  });
  const html = context.renderOnboardingChecklist();

  assert.match(html, /home_onboarding_title/);
  assert.doesNotMatch(html, /data-action="openImportSettings"/);
});

test('loadStats displays book-oriented header copy', async () => {
  const statsEl = { textContent: '', classList: fakeClassList(['hidden']) };
  const context = createContext({
    document: { getElementById: id => id === 'header-stats' ? statsEl : null },
    normalizedLibraryMode: () => true,
    t: (key, vars) => key === 'n_books_in_library' ? `${vars.n} books in library` : key,
    apiJson: async url => {
      assert.equal(url, '/api/v1/library/summary');
      return { total_books: 3 };
    },
  });

  await context.loadStats();

  assert.equal(statsEl.textContent, '3 books in library');
  assert.equal(statsEl.classList.contains('hidden'), false);
});

test('renderLibraryScanWorkspace shows active scan button after onboarding', () => {
  const workspace = { innerHTML: '' };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
  });

  context.renderLibraryScanWorkspace();

  assert.match(workspace.innerHTML, /data-action="startLibraryScan"/);
  assert.match(workspace.innerHTML, /Review results here before importing/);
});

test('successful scan flow posts, polls, loads results, and renders review', async () => {
  const workspace = { innerHTML: '' };
  const calls = [];
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET' });
      if (url === '/api/v1/library/scan') return { job_id: 'job-1', job: { started_at: '2026-01-01T00:00:00Z', progress: { status: 'scanning', current_phase: 'scanning', started_at: '2026-01-01T00:00:00Z' } } };
      if (url === '/api/v1/library/scan/job-1') return { id: 'job-1', status: 'completed', progress: { status: 'completed', files_processed: 2 } };
      if (url === '/api/v1/library/scan/job-1/results') return sampleScanResult();
      throw new Error(`unexpected ${url}`);
    },
  });

  await context.startLibraryScan();

  assert.deepEqual(calls.map(c => `${c.method} ${c.url}`), [
    'POST /api/v1/library/scan',
    'GET /api/v1/library/scan/job-1',
    'GET /api/v1/library/scan/job-1/results',
  ]);
  assert.match(workspace.innerHTML, /Files Found/);
  assert.match(workspace.innerHTML, /Ready to Import/);
  assert.match(workspace.innerHTML, /The Guardian/);
});

test('renderLibraryScanProgress shows progress fields', () => {
  const context = createContext();
  const html = context.renderLibraryScanProgress({
    running: true,
    startedAt: new Date().toISOString(),
    progress: {
      current_phase: 'processing_metadata',
      directories_scanned: 3,
      files_discovered: 29,
      files_processed: 17,
      candidates_ready: 12,
      current_path: '/books/example.epub',
    },
  });

  assert.match(html, /Processing Metadata/);
  assert.match(html, /Directories/);
  assert.match(html, /\/books\/example.epub/);
});

test('renderLibraryScanReview handles empty and duplicate-only scans', () => {
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true }) },
  });

  assert.match(context.renderLibraryScanReview({ totals: { found: 0 }, candidates: [] }), /No books found/);
  assert.match(context.renderLibraryScanReview({
    totals: { found: 2, ready_to_import: 0, duplicates: 2 },
    candidates: [
      { classification: 'duplicate', title: 'A', filename: 'a.epub', format: 'epub', metadata: {}, existing_path: '/library/a.epub' },
      { classification: 'duplicate', title: 'B', filename: 'b.epub', format: 'epub', metadata: {} },
    ],
  }), /Everything found in this scan already appears to be in the library/);
  assert.match(context.renderLibraryScanReview({
    totals: { found: 1, ready_to_import: 0, duplicates: 1 },
    candidates: [{ classification: 'duplicate', title: 'A', filename: 'a.epub', format: 'epub', metadata: {}, existing_path: '/library/a.epub' }],
  }), /Existing: \/library\/a\.epub/);
});

test('filterLibraryScanCandidates filters by bucket and search', () => {
  const context = createContext();
  const candidates = sampleScanResult().candidates;

  assert.equal(context.filterLibraryScanCandidates(candidates, 'duplicate', '').length, 1);
  assert.equal(context.filterLibraryScanCandidates(candidates, 'all', 'guardian').length, 1);
  assert.equal(context.filterLibraryScanCandidates(candidates, 'all', 'missing').length, 0);
});

test('scan failure renders retry state', async () => {
  const workspace = { innerHTML: '' };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
    apiJson: async url => {
      if (url === '/api/v1/library/scan') return { job_id: 'job-1' };
      if (url === '/api/v1/library/scan/job-1') return { id: 'job-1', status: 'failed', error: 'scan failed' };
      throw new Error(`unexpected ${url}`);
    },
  });

  await context.startLibraryScan();

  assert.match(workspace.innerHTML, /Library scan failed/);
  assert.match(workspace.innerHTML, /Retry/);
});

test('renderLibraryScanReview shows import actions and selectable ready rows', () => {
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: { ...libraryImportState().scan, result: sampleScanResult(), selected: new Set(['ready-1']) } }) },
  });

  const html = context.renderLibraryScanReview(sampleScanResult());

  assert.match(html, /Select All Ready/);
  assert.match(html, /Import Selected/);
  assert.match(html, /Import All Ready/);
  assert.match(html, /Skip Selected/);
  assert.match(html, /data-candidate-id="ready-1"/);
  assert.match(html, /Embedded metadata/);
  assert.match(html, /High/);
  assert.match(html, /Destination/);
  assert.match(html, /Carla Jablonski → The Guardian\.epub/);
});

test('renderLibraryScanReview shows manual review details and resolution controls', () => {
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: { ...libraryImportState().scan, result: manualReviewScanResult() } }) },
  });

  const html = context.renderLibraryScanReview(manualReviewScanResult());

  assert.match(html, /Manual Review Required/);
  assert.match(html, /Existing title match did not cleanly agree on author/);
  assert.match(html, /Filename parsing/);
  assert.match(html, /Medium/);
  assert.match(html, /Suggested Destination/);
  assert.match(html, /Use Suggested/);
  assert.match(html, /Edit Metadata/);
  assert.match(html, /Skip/);
});

test('ambiguous library scan manual review exposes merge matching books action', async () => {
  const ambiguous = manualReviewScanResult();
  ambiguous.candidates[0].classification_reason = 'Multiple existing books share the same title and author';
  ambiguous.candidates[0].manual_review.reason = 'Multiple existing books share the same title and author';
  const calls = [];
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: { ...libraryImportState().scan, result: ambiguous } }) },
    apiJson: async (url, options) => {
      calls.push({ url, method: options?.method, body: options?.body });
      return {
        ...ambiguous,
        candidates: [{ ...ambiguous.candidates[0], classification: 'already_imported', manual_review: null }],
      };
    },
    refreshLibraryAfterScanImport: async () => {},
  });

  const html = context.renderLibraryScanReview(ambiguous);
  assert.match(html, /Merge Matching Books/);

  await context.resolveLibraryScanCandidate('review-1', 'merge_matching_books');

  assert.equal(calls[0].url, '/api/v1/library/scan/job-1/resolve');
  assert.match(calls[0].body, /"action":"merge_matching_books"/);
});

test('renderLibraryScanReview shows duplicate details', () => {
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: { ...libraryImportState().scan, result: sampleScanResult() } }) },
  });

  const html = context.renderLibraryScanReview(sampleScanResult());

  assert.match(html, /Duplicate/);
  assert.match(html, /Identical hash/);
  assert.match(html, /Already There/);
  assert.match(html, /Jane Doe/);
  assert.match(html, /\/library\/dup\.mobi/);
});

test('selection helpers exclude skipped and non-ready candidates', () => {
  const scanState = libraryImportState().scan;
  scanState.result = sampleScanResult();
  scanState.selected = new Set(['ready-1', 'dup-1']);
  scanState.skipped = new Set(['ready-1']);
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
  });

  assert.equal(context.readyLibraryScanCandidates(sampleScanResult()).length, 0);
  assert.equal(context.selectedReadyLibraryScanCandidates(sampleScanResult()).length, 0);
  assert.equal(context.filterLibraryScanCandidates(sampleScanResult().candidates, 'all', '').length, 1);
});

test('successful import selected posts, polls, refreshes review, and clears imported selection', async () => {
  const workspace = { innerHTML: '' };
  const scanState = libraryImportState().scan;
  scanState.result = sampleScanResult();
  scanState.selected = new Set(['ready-1']);
  const calls = [];
  const context = createContext({
    state: { currentTab: 'home', libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET', body: options.body || '' });
      if (url === '/api/v1/library/import') return { job_id: 'imp-1', job: { started_at: '2026-01-01T00:00:00Z', progress: { status: 'importing', total: 1, imported: 0, started_at: '2026-01-01T00:00:00Z' } } };
      if (url === '/api/v1/library/import/imp-1') return { id: 'imp-1', status: 'completed', progress: { status: 'completed', total: 1, imported: 1 } };
      if (url === '/api/v1/library/import/imp-1/results') return { job_id: 'imp-1', scan_job_id: 'job-1', summary: { imported: 1, duplicates: 0, failed: 0 }, items: [{ candidate_id: 'ready-1', status: 'imported' }] };
      if (url === '/api/v1/library/scan/job-1/results') return importedScanResult();
      throw new Error(`unexpected ${url}`);
    },
  });
  context.loadHomeDashboard = async () => calls.push({ url: 'home-refresh', method: 'CALL' });

  await context.startLibraryImport(false);

  assert.deepEqual(calls.map(c => `${c.method} ${c.url}`), [
    'POST /api/v1/library/import',
    'GET /api/v1/library/import/imp-1',
    'GET /api/v1/library/import/imp-1/results',
    'GET /api/v1/library/scan/job-1/results',
    'CALL home-refresh',
  ]);
  assert.match(calls[0].body, /"candidate_ids":\["ready-1"\]/);
  assert.equal(context.state.libraryImport.scan.selected.has('ready-1'), false);
  assert.match(workspace.innerHTML, /Import Complete/);
  assert.match(workspace.innerHTML, /View Imported/);
  assert.match(workspace.innerHTML, /Retry Failed/);
  assert.match(workspace.innerHTML, /Close/);
  assert.match(workspace.innerHTML, /Already Imported/);
});

test('import all ready sends all_ready and import progress renders counts', async () => {
  const workspace = { innerHTML: '' };
  const scanState = libraryImportState().scan;
  scanState.result = sampleScanResult();
  const calls = [];
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET', body: options.body || '' });
      if (url === '/api/v1/library/import') return { job_id: 'imp-2', job: { progress: { status: 'importing', total: 1, imported: 0, current_title: 'The Guardian', started_at: new Date().toISOString() } } };
      if (url === '/api/v1/library/import/imp-2') return { id: 'imp-2', status: 'completed', progress: { status: 'completed', total: 1, imported: 1 } };
      if (url === '/api/v1/library/import/imp-2/results') return { job_id: 'imp-2', scan_job_id: 'job-1', summary: { imported: 1, duplicates: 0, failed: 0 }, items: [] };
      if (url === '/api/v1/library/scan/job-1/results') return importedScanResult();
      throw new Error(`unexpected ${url}`);
    },
  });

  assert.match(context.renderLibraryImportProgress({ startedAt: new Date().toISOString(), progress: { total: 17, imported: 8, failed: 0, duplicates: 0, current_title: 'The War of the Worlds' } }), /8 \/ 17 books/);
  await context.startLibraryImport(true);

  assert.match(calls[0].body, /"all_ready":true/);
});

test('import failure keeps review visible and reports error', async () => {
  const workspace = { innerHTML: '' };
  const scanState = libraryImportState().scan;
  scanState.result = sampleScanResult();
  scanState.selected = new Set(['ready-1']);
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    document: { getElementById: id => id === 'settings-library-scan-workspace' ? workspace : null },
    apiJson: async url => {
      if (url === '/api/v1/library/import') return { job_id: 'imp-err' };
      if (url === '/api/v1/library/import/imp-err') return { id: 'imp-err', status: 'failed', error: 'permission denied' };
      throw new Error(`unexpected ${url}`);
    },
  });

  await context.startLibraryImport(false);

  assert.match(workspace.innerHTML, /permission denied/);
  assert.match(workspace.innerHTML, /Ready to Import/);
});

test('import completion summary renders partial failures', () => {
  const context = createContext();
  const html = context.renderLibraryImportSummary({
    summary: { imported: 15, skipped: 0, duplicates: 0, failed: 2, total: 17 },
    started_at: '2026-01-01T00:00:00Z',
    completed_at: '2026-01-01T00:00:05Z',
    items: [
      { status: 'failed', title: 'Missing Book', error: 'file disappeared' },
      { status: 'failed', path: '/books/locked.epub', error: 'permission denied' },
    ],
  });

  assert.match(html, /15 imported/);
  assert.match(html, /2 failed/);
  assert.match(html, /3\.0\/s/);
  assert.match(html, /Show Details/);
  assert.match(html, /Retry Failed/);
  assert.match(html, /file disappeared/);
});

test('manual review use suggested resolves candidate through scan endpoint', async () => {
  const scanState = libraryImportState().scan;
  scanState.result = manualReviewScanResult();
  const calls = [];
  let refreshed = false;
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET', body: options.body || '' });
      return {
        ...manualReviewScanResult(),
        totals: { found: 1, ready_to_import: 1, manual_review: 0 },
        candidates: [{ ...manualReviewScanResult().candidates[0], classification: 'new', classification_reason: 'Ready to import after manual review', manual_review: null }],
      };
    },
  });
  context.refreshLibraryAfterScanImport = async () => {
    refreshed = true;
  };

  await context.resolveLibraryScanCandidate('review-1', 'use_suggested');

  assert.equal(calls[0].url, '/api/v1/library/scan/job-1/resolve');
  assert.equal(calls[0].method, 'POST');
  assert.match(calls[0].body, /"action":"use_suggested"/);
  assert.equal(context.state.libraryImport.scan.result.totals.ready_to_import, 1);
  assert.equal(context.state.libraryImport.scan.selected.has('review-1'), true);
  assert.equal(refreshed, true);
});

test('metadata editor renders live preview and validation for manual review items', () => {
  const scanState = libraryImportState().scan;
  scanState.result = manualReviewScanResult();
  scanState.editor = {
    candidateId: 'review-1',
    draft: {
      title: 'The Guardian’s Path',
      subtitle: '',
      author: 'Carla Jablonski',
      series: 'Prince of Persia',
      series_number: '1',
      publisher: 'Disney',
      publication_year: '2004',
      isbn: '978-1234567890',
      language: 'en',
      description: 'Adventure novel',
      tags: 'fantasy, tie-in',
      library: 'ebook',
    },
    errors: [],
  };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
  });

  const html = context.renderLibraryScanReview(manualReviewScanResult());

  assert.match(html, /Metadata Editor/);
  assert.match(html, /Destination Folder/);
  assert.match(html, /Filename/);
  assert.match(html, /Import Location/);
  assert.match(html, /Carla Jablonski - The Guardian’s Path\.mobi/);
  assert.match(html, /Save & Import/);
});

test('metadata editor destination preview collapses duplicate library segment', () => {
  const context = createContext();
  const preview = context.metadataEditorPreview({
    destination_path: '/books/ebooks/ebooks/Prince Of Persia.epub',
    path: '/books/ebooks/Prince Of Persia.epub',
    filename: 'Prince Of Persia.epub',
    format: 'epub',
  }, {
    title: "The Guardian's Path",
    author: 'Carla Jablonski',
  });

  assert.equal(preview.path, "/books/ebooks/Carla Jablonski - The Guardian's Path.epub");
  assert.doesNotMatch(preview.path, /\/ebooks\/ebooks\//);
});

test('metadata editor validation blocks missing title, bad year, bad ISBN, and duplicate filename', () => {
  const scanState = libraryImportState().scan;
  scanState.result = {
    ...manualReviewScanResult(),
    candidates: [
      manualReviewScanResult().candidates[0],
      {
        ...sampleScanResult().candidates[0],
        id: 'ready-2',
        classification: 'new',
        destination_path: '/books/ebooks/Disney Book Group/Carla Jablonski - Existing.mobi',
      },
    ],
  };
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
  });
  const candidate = scanState.result.candidates[0];

  const errors = context.validateMetadataEditorDraft(candidate, {
    title: '',
    author: '',
    publication_year: '20x4',
    isbn: 'abc',
  });

  assert(errors.some(error => /Title is required/.test(error)));
  assert(errors.some(error => /Author is required/.test(error)));
  assert(errors.some(error => /four-digit year/.test(error)));
  assert(errors.some(error => /ISBN/.test(error)));

  const duplicateErrors = context.validateMetadataEditorDraft(candidate, {
    title: 'Existing',
    author: 'Carla Jablonski',
  });
  assert(duplicateErrors.some(error => /destination filename/.test(error)));
});

test('metadata editor save posts edited fields and save import starts a single import', async () => {
  const scanState = libraryImportState().scan;
  scanState.result = manualReviewScanResult();
  scanState.editor = {
    candidateId: 'review-1',
    draft: {
      title: 'The Guardian’s Path',
      author: 'Carla Jablonski',
      publication_year: '2004',
      isbn: '9781234567890',
      tags: 'fantasy, tie-in',
      library: 'ebook',
    },
    errors: [],
  };
  const calls = [];
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    document: { getElementById: () => ({ innerHTML: '', scrollIntoView: () => {} }), querySelector: () => null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET', body: options.body || '' });
      if (url === '/api/v1/library/scan/job-1/resolve') {
        return {
          ...manualReviewScanResult(),
          totals: { found: 1, ready_to_import: 1, manual_review: 0 },
          candidates: [{
            ...manualReviewScanResult().candidates[0],
            classification: 'new',
            title: 'The Guardian’s Path',
            author: 'Carla Jablonski',
            metadata: { title: 'The Guardian’s Path', author: 'Carla Jablonski', source: 'manual_edit', confidence: 'high' },
            manual_review: null,
          }],
        };
      }
      if (url === '/api/v1/library/import') return { job_id: 'imp-1', job: { progress: { status: 'importing', total: 1, started_at: '2026-01-01T00:00:00Z' } } };
      if (url === '/api/v1/library/import/imp-1') return { id: 'imp-1', status: 'completed', progress: { status: 'completed', total: 1, imported: 1 } };
      if (url === '/api/v1/library/import/imp-1/results') return { job_id: 'imp-1', scan_job_id: 'job-1', summary: { imported: 1, duplicates: 0, failed: 0 }, items: [] };
      if (url === '/api/v1/library/scan/job-1/results') return importedScanResult();
      throw new Error(`unexpected ${url}`);
    },
  });

  await context.saveMetadataEditor(true);

  assert.match(calls[0].body, /"title":"The Guardian’s Path"/);
  assert.match(calls[0].body, /"publication_year":"2004"/);
  assert.match(calls[0].body, /"tags":\["fantasy","tie-in"\]/);
  assert.equal(calls[1].url, '/api/v1/library/import');
  assert.match(calls[1].body, /"candidate_ids":\["review-1"\]/);
});

function sampleDetailBook() {
  return {
    id: 42,
    title: 'Old Title',
    author: 'Andy Weir',
    series: 'Hail Mary',
    mediaType: 'ebook',
    formats: ['EPUB'],
    files: [{ path: '/books/Andy Weir - Old Title.epub', format: 'EPUB', size: 10 }],
    identifiers: [{ type: 'isbn', value: '9781234567890' }],
    editions: [{ title: 'Old Edition', subtitle: 'Old Subtitle', publisher: 'Ballantine', publication_date: '2021', language: 'en' }],
    description: 'Old description',
    metadata: {
      fields: {
        title: { value: 'Old Title' },
        edition_title: { value: 'Old Edition' },
        subtitle: { value: 'Old Subtitle' },
        publisher: { value: 'Ballantine' },
        publication_date: { value: '2021' },
        language: { value: 'en' },
        description: { value: 'Old description' },
        genres: { value: 'Science Fiction' },
      },
    },
  };
}

test('library book details metadata button uses delegated action instead of inline onclick', () => {
  assert.match(appSource, /data-action="openLibraryMetadataEditor"/);
  assert.match(appSource, /openLibraryMetadataEditor:\s*\(\)\s*=>\s*openLibraryMetadataEditor\(\)/);
  assert.match(appSource, /saveLibraryMetadataEditor:\s*\(\)\s*=>\s*saveLibraryMetadataEditor\(\)/);
  assert.match(appSource, /cancelLibraryMetadataEditor:\s*\(\)\s*=>\s*closeLibraryMetadataEditor\(\)/);
  assert.match(appSource, /resetLibraryMetadataEditor:\s*\(\)\s*=>\s*resetLibraryMetadataEditor\(\)/);
  assert.doesNotMatch(appSource, /onclick="openMetadataEditor\(\)"/);
});

test('library metadata editor populates current book metadata and documents unsupported fields', () => {
  const book = sampleDetailBook();
  const context = createContext({
    state: {
      config: { library_repository_mode: 'normalized' },
      libraryImport: libraryImportState(),
      libraryMetadataEditor: { open: true, draft: null, errors: [] },
      activeDetailBook: book,
      libraryBooks: [book],
    },
    normalizedLibraryMode: () => true,
  });

  const html = context.renderLibraryMetadataEditor(book);

  assert.match(html, /Metadata Editor/);
  assert.match(html, /value="Old Title"/);
  assert.match(html, /value="Old Edition"/);
  assert.match(html, /value="Science Fiction"/);
  assert.match(html, /Not editable here yet/);
  assert.match(html, /Author, series, ISBN, library, destination folder, and filename preview/);
});

test('library metadata editor save persists supported fields and refreshes library/details', async () => {
  const book = sampleDetailBook();
  const calls = [];
  const context = createContext({
    state: {
      config: { library_repository_mode: 'normalized' },
      libraryImport: libraryImportState(),
      libraryMetadataEditor: {
        open: true,
        draft: {
          title: 'New Title',
          edition_title: 'New Edition',
          subtitle: 'New Subtitle',
          publisher: 'New Publisher',
          publication_year: '2022',
          language: 'fr',
          description: 'New description',
          tags: 'sci-fi, space',
          author: 'Andy Weir',
        },
        errors: [],
      },
      activeDetailBook: book,
      activeDetailContext: { index: 0, collection: 'libraryBooks', bookId: 42 },
      libraryBooks: [book],
    },
    normalizedLibraryMode: () => true,
    loadLibrary: async () => {
      calls.push({ type: 'loadLibrary' });
      context.state.libraryBooks = [{ ...book, id: 42, title: 'New Title' }];
    },
    openBookDetails: async (index, collection) => calls.push({ type: 'openBookDetails', index, collection }),
    apiJson: async (url, options = {}) => {
      calls.push({ type: 'api', url, method: options.method, body: options.body });
      return { success: true };
    },
  });

  await context.saveLibraryMetadataEditor();

  assert.equal(calls[0].url, '/api/v1/books/42/metadata');
  assert.equal(calls[0].method, 'PATCH');
  assert.deepEqual(JSON.parse(calls[0].body), {
    fields: {
      title: 'New Title',
      edition_title: 'New Edition',
      subtitle: 'New Subtitle',
      publisher: 'New Publisher',
      publication_date: '2022',
      language: 'fr',
      description: 'New description',
      genres: ['sci-fi', 'space'],
    },
  });
  assert.deepEqual(calls.slice(1), [
    { type: 'loadLibrary' },
    { type: 'openBookDetails', index: 0, collection: 'libraryBooks' },
  ]);
  assert.equal(context.state.libraryMetadataEditor.open, false);
});

test('library metadata editor cancel and reset make no API changes', () => {
  const book = sampleDetailBook();
  const calls = [];
  const context = createContext({
    state: {
      config: { library_repository_mode: 'normalized' },
      libraryImport: libraryImportState(),
      libraryMetadataEditor: { open: true, draft: { title: 'Changed' }, errors: [] },
      activeDetailBook: book,
      activeDetailContext: { index: 0, collection: 'libraryBooks', bookId: 42 },
      libraryBooks: [book],
    },
    normalizedLibraryMode: () => true,
    openBookDetails: async () => calls.push('openBookDetails'),
    apiJson: async () => calls.push('api'),
  });

  context.resetLibraryMetadataEditor();
  assert.equal(context.state.libraryMetadataEditor.draft.title, 'Old Title');
  context.closeLibraryMetadataEditor();
  assert.equal(context.state.libraryMetadataEditor.open, false);
  assert.deepEqual(calls, ['openBookDetails', 'openBookDetails']);
});

test('library metadata editor API errors are displayed and editor remains open', async () => {
  const book = sampleDetailBook();
  const toasts = [];
  const context = createContext({
    state: {
      config: { library_repository_mode: 'normalized' },
      libraryImport: libraryImportState(),
      libraryMetadataEditor: { open: true, draft: { ...contextSafeDraft(), title: 'New Title', author: 'Andy Weir' }, errors: [] },
      activeDetailBook: book,
      activeDetailContext: { index: 0, collection: 'libraryBooks', bookId: 42 },
      libraryBooks: [book],
    },
    normalizedLibraryMode: () => true,
    showToast: (msg, type) => toasts.push({ msg, type }),
    apiJson: async () => { throw new Error('metadata service unavailable'); },
  });

  await context.saveLibraryMetadataEditor();

  assert.equal(context.state.libraryMetadataEditor.open, true);
  assert.deepEqual(toasts, [{ msg: 'metadata service unavailable', type: 'error' }]);
});

function contextSafeDraft() {
  return {
    title: 'Old Title',
    edition_title: 'Old Edition',
    subtitle: 'Old Subtitle',
    publisher: 'Ballantine',
    publication_year: '2021',
    language: 'en',
    description: 'Old description',
    tags: 'Science Fiction',
    author: 'Andy Weir',
  };
}

test('retry failed import posts only failed ready candidates', async () => {
  const scanState = libraryImportState().scan;
  scanState.result = {
    ...sampleScanResult(),
    candidates: [
      sampleScanResult().candidates[0],
      { ...sampleScanResult().candidates[0], id: 'ready-2', title: 'Other Book', path: '/books/other.epub' },
    ],
  };
  scanState.import.result = {
    job_id: 'imp-1',
    scan_job_id: 'job-1',
    summary: { imported: 1, failed: 1 },
    items: [
      { candidate_id: 'ready-1', status: 'failed', title: 'The Guardian', error: 'permission denied' },
      { candidate_id: 'ready-2', status: 'imported', title: 'Other Book' },
    ],
  };
  const calls = [];
  const context = createContext({
    state: { libraryImport: libraryImportState({ completed: true, scan: scanState }) },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method || 'GET', body: options.body || '' });
      if (url === '/api/v1/library/import') return { job_id: 'imp-retry' };
      if (url === '/api/v1/library/import/imp-retry') return { id: 'imp-retry', status: 'completed', progress: { total: 1, imported: 1 } };
      if (url === '/api/v1/library/import/imp-retry/results') return { job_id: 'imp-retry', scan_job_id: 'job-1', summary: { imported: 1, failed: 0 }, items: [] };
      if (url === '/api/v1/library/scan/job-1/results') return importedScanResult();
      throw new Error(`unexpected ${url}`);
    },
  });

  await context.retryFailedLibraryImport();

  assert.equal(calls[0].url, '/api/v1/library/import');
  assert.match(calls[0].body, /"candidate_ids":\["ready-1"\]/);
  assert.doesNotMatch(calls[0].body, /ready-2/);
});

test('renderDiagnosticResult shows staged actionable diagnostics', () => {
  const context = createContext();
  const html = context.renderDiagnosticResult({
    service: 'prowlarr',
    status: 'failed',
    success: false,
    duration_ms: 128,
    summary: 'HTTP 401 Unauthorized',
    steps: [
      { name: 'DNS Lookup', status: 'success', duration_ms: 3, message: 'Resolved 1 address.' },
      { name: 'Authentication', status: 'failed', message: 'HTTP 401 Unauthorized', suggestion: 'Verify API key.' },
    ],
  });

  assert.match(html, /HTTP 401 Unauthorized/);
  assert.match(html, /DNS Lookup/);
  assert.match(html, /Authentication/);
  assert.match(html, /Suggestion: Verify API key\./);
  assert.match(html, /128 ms/);
});

test('testConnection posts current qBittorrent settings and renders diagnostics', async () => {
  const elements = {
    'setting-qb_url': { value: 'https://qb.example' },
    'setting-qb_user': { value: 'admin' },
    'setting-qb_pass': { value: 'secret' },
    'test-qbittorrent-status': { textContent: '', className: '' },
    'diagnostic-qbittorrent-result': { innerHTML: '' },
    'diagnostic-qbittorrent': { querySelector: () => ({ disabled: false }) },
  };
  const calls = [];
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method, body: options.body });
      return {
        service: 'qbittorrent',
        status: 'connected',
        success: true,
        duration_ms: 84,
        summary: 'Connected',
        steps: [{ name: 'API Version', status: 'success', message: 'v5.0.0' }],
      };
    },
  });

  await context.testConnection('qbittorrent');

  assert.equal(calls[0].url, '/api/test/qbittorrent');
  assert.equal(calls[0].method, 'POST');
  assert.deepEqual(JSON.parse(calls[0].body), {
    url: 'https://qb.example',
    username: 'admin',
    password: 'secret',
  });
  assert.equal(elements['test-qbittorrent-status'].textContent, 'Connected');
  assert.match(elements['diagnostic-qbittorrent-result'].innerHTML, /API Version/);
});

test('qBittorrent integration save includes remote save paths and categories', () => {
  assert.match(appSource, /qb_save_path/);
  assert.match(appSource, /qb_audiobook_save_path/);
  assert.match(appSource, /qb_manga_save_path/);
  assert.match(indexHTML, /id="setting-qb_save_path"/);
  assert.match(indexHTML, /Remote path as seen by qBittorrent/);
});

test('settings exposes OPDS catalog connection details', () => {
  assert.match(indexHTML, /id="settings-opds"/);
  assert.match(indexHTML, /OPDS Catalog/);
  assert.match(indexHTML, /value="\/opds"/);
  assert.match(indexHTML, /HTTP Basic Auth/);
  assert.match(indexHTML, /EPUB and PDF are preferred/);
  assert.match(indexHTML, /use HTTPS/);
});

test('loadConfig renders build identity from health endpoint', async () => {
  const elements = {
    'config-info': { innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async url => {
      if (url === '/api/config') {
        return { qbittorrent: { url: 'https://qb.example.test' } };
      }
      if (url === '/api/health') {
        return {
          version: 'v2.0.0-beta.1',
          channel: 'v2.0.0-beta.1',
          commit: 'abc1234',
          build_time: '2026-07-26T15:42:00Z',
        };
      }
      throw new Error(`unexpected URL ${url}`);
    },
  });

  await context.loadConfig();

  assert.match(elements['config-info'].innerHTML, /qBittorrent/);
  assert.match(elements['config-info'].innerHTML, /v2\.0\.0-beta\.1/);
  assert.match(elements['config-info'].innerHTML, /abc1234/);
  assert.match(elements['config-info'].innerHTML, /2026-07-26T15:42:00Z/);
});

test('diagnosticPayload reads current unsaved Prowlarr form values', () => {
  const elements = {
    'setting-prowlarr_url': { value: 'http://unsaved-prowlarr:9697' },
    'setting-prowlarr_api_key': { value: 'unsaved-key' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
  });

  assert.deepEqual(JSON.parse(JSON.stringify(context.diagnosticPayload('prowlarr'))), {
    url: 'http://unsaved-prowlarr:9697',
    api_key: 'unsaved-key',
  });
});

test('loadUsers renders account table with status and management actions', async () => {
  const elements = {
    'user-management': { classList: fakeClassList(['hidden']) },
    'users-list': { innerHTML: '' },
  };
  const context = createContext({
    state: { currentUser: 'admin' },
    document: { getElementById: id => elements[id] || null },
    apiJson: async url => {
      assert.equal(url, '/api/users');
      return {
        success: true,
        users: [
          { id: 1, username: 'admin', role: 'admin', enabled: true, created_at: '2026-01-01T00:00:00Z' },
          { id: 2, username: 'reader', role: 'user', enabled: false, created_at: '2026-01-02T00:00:00Z', last_login: '2026-01-03T00:00:00Z' },
        ],
      };
    },
  });

  await context.loadUsers();

  assert.equal(elements['user-management'].classList.contains('hidden'), false);
  assert.match(elements['users-list'].innerHTML, /reader|Disabled|Reset Password|Enable|Delete/);
  assert.match(elements['users-list'].innerHTML, /disabled aria-disabled="true"|>You</);
});

test('addUser posts direct create payload with selected role', async () => {
  const calls = [];
  const elements = {
    'new-user-name': { value: 'reader' },
    'new-user-pass': { value: 'secret123' },
    'new-user-role': { value: 'admin' },
    'new-user-admin': { checked: false },
    'user-management': { classList: fakeClassList() },
    'users-list': { innerHTML: '' },
  };
  const events = [];
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async (url, options = {}) => {
      calls.push({ url, method: options.method, body: options.body ? JSON.parse(options.body) : null });
      if (url === '/api/users') return { success: true, users: [] };
      return { success: true };
    },
    showToast: (msg, kind) => events.push(`${kind}:${msg}`),
  });

  await context.addUser();

  assert.deepEqual(calls[0], {
    url: '/api/register',
    method: 'POST',
    body: { username: 'reader', password: 'secret123', role: 'admin' },
  });
  assert.equal(elements['new-user-name'].value, '');
  assert.equal(elements['new-user-pass'].value, '');
  assert.equal(elements['new-user-role'].value, 'user');
  assert.deepEqual(events, ['success:user_created']);
});

test('loadInviteCodes renders role, uses, expiration, created, used, and expired state', async () => {
  const elements = {
    'invite-codes-list': { innerHTML: '' },
  };
  const context = createContext({
    document: { getElementById: id => elements[id] || null },
    apiJson: async url => {
      assert.equal(url, '/api/invites');
      return {
        invites: [
          { id: 1, code: 'abc', role: 'user', uses: 1, max_uses: 2, expires_at: 1, created_at: 1 },
        ],
      };
    },
  });

  await context.loadInviteCodes();

  assert.match(elements['invite-codes-list'].innerHTML, /Role:/);
  assert.match(elements['invite-codes-list'].innerHTML, /Uses: 1 \/ 2/);
  assert.match(elements['invite-codes-list'].innerHTML, /Expiration:/);
  assert.match(elements['invite-codes-list'].innerHTML, /Created:/);
  assert.match(elements['invite-codes-list'].innerHTML, /Used: Yes/);
  assert.match(elements['invite-codes-list'].innerHTML, /Expired: Yes/);
});

function sampleScanResult() {
  return {
    job_id: 'job-1',
    status: 'completed',
    totals: {
      found: 2,
      ready_to_import: 1,
      manual_review: 0,
      duplicates: 1,
      already_imported: 0,
      unsupported: 0,
      unreadable: 0,
    },
    candidates: [
      {
        id: 'ready-1',
        classification: 'new',
        title: 'The Guardian',
        author: 'Carla Jablonski',
        filename: 'guardian.epub',
        format: 'epub',
        media_type: 'ebook',
        path: '/books/guardian.epub',
        destination_path: '/books/Carla Jablonski/The Guardian.epub',
        metadata: { source: 'embedded_metadata', confidence: 'high', title: 'The Guardian', author: 'Carla Jablonski' },
        classification_reason: 'Ready to import',
      },
      {
        id: 'dup-1',
        classification: 'duplicate',
        title: 'Already There',
        author: 'Jane Doe',
        filename: 'dup.mobi',
        format: 'mobi',
        media_type: 'ebook',
        path: '/books/dup.mobi',
        existing_path: '/library/dup.mobi',
        metadata: { source: 'filename_fallback', confidence: 'medium' },
        duplicate: { reason: 'Identical hash', existing_title: 'Already There', existing_author: 'Jane Doe', existing_format: 'mobi', existing_path: '/library/dup.mobi' },
        classification_reason: 'Identical hash',
      },
    ],
  };
}

function importedScanResult() {
  return {
    job_id: 'job-1',
    status: 'completed',
    totals: {
      found: 2,
      ready_to_import: 0,
      manual_review: 0,
      duplicates: 1,
      already_imported: 1,
      unsupported: 0,
      unreadable: 0,
    },
    candidates: [
      {
        id: 'ready-1',
        classification: 'already_imported',
        title: 'The Guardian',
        author: 'Carla Jablonski',
        filename: 'guardian.epub',
        format: 'epub',
        media_type: 'ebook',
        path: '/books/guardian.epub',
        existing_path: '/books/guardian.epub',
        metadata: { source: 'embedded_metadata', title: 'The Guardian', author: 'Carla Jablonski' },
        classification_reason: 'Imported into library',
      },
      sampleScanResult().candidates[1],
    ],
  };
}

function manualReviewScanResult() {
  return {
    job_id: 'job-1',
    status: 'completed',
    totals: {
      found: 1,
      ready_to_import: 0,
      manual_review: 1,
      duplicates: 0,
      already_imported: 0,
      unsupported: 0,
      unreadable: 0,
    },
    candidates: [
      {
        id: 'review-1',
        classification: 'manual_review',
        title: 'Prince Of Persia',
        author: 'Disney Book Group',
        filename: 'guardian.mobi',
        format: 'mobi',
        media_type: 'ebook',
        path: '/books/guardian.mobi',
        destination_path: '/books/ebooks/Disney Book Group/Prince Of Persia.mobi',
        metadata: { source: 'filename_fallback', confidence: 'medium', title: 'Prince Of Persia', author: 'Disney Book Group' },
        classification_reason: 'Existing title match did not cleanly agree on author',
        manual_review: {
          reason: 'Existing title match did not cleanly agree on author',
          suggested_destination: '/books/ebooks/Disney Book Group/Prince Of Persia.mobi',
          metadata_source: 'filename_fallback',
          confidence: 'medium',
        },
      },
    ],
  };
}

function fakeClassList(initial = []) {
  const set = new Set(initial);
  return {
    add: (...names) => names.forEach(name => set.add(name)),
    remove: (...names) => names.forEach(name => set.delete(name)),
    toggle: (name, force) => {
      if (force === undefined) {
        if (set.has(name)) set.delete(name);
        else set.add(name);
        return set.has(name);
      }
      if (force) set.add(name);
      else set.delete(name);
      return force;
    },
    contains: name => set.has(name),
  };
}
