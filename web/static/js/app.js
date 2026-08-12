// ============================================================
// I18N (INTERNATIONALIZATION)
// ============================================================
let currentLang = localStorage.getItem('librarr_lang') || 'en';

const I18N = {
  en: {
    // Navigation
    nav_search: 'Search',
    nav_library: 'Library',
    nav_downloads: 'Downloads',
    nav_wishlist: 'Wishlist',
    nav_settings: 'Settings',
    sign_out: 'Sign out',
    // Header
    header_subtitle: 'Self-hosted book manager',
    // Login modal
    login_subtitle: 'Sign in to continue',
    login_sign_in: 'Sign In',
    login_or: 'or',
    login_sso: 'Login with SSO',
    login_with: 'Login with {provider}',
    login_no_account: 'No account? <a href="#" data-action="showRegisterForm" class="text-indigo-400 hover:text-indigo-300">Register</a>',
    create_admin_account: 'Create your admin account',
    create_your_account: 'Create your account',
    create_account: 'Create Account',
    back_to_login: 'Back to login',
    // Labels
    label_username: 'Username',
    label_password: 'Password',
    // Placeholders
    ph_username: 'Username',
    ph_password: 'Password',
    ph_choose_username: 'Choose a username',
    ph_min_6_chars: 'Min 6 characters',
    // TOTP
    totp_enter_code: 'Enter the 6-digit code from your authenticator app, or a backup code.',
    totp_code_label: 'TOTP Code',
    totp_verify: 'Verify',
    s_totp_title: 'Two-Factor Authentication',
    s_totp_desc: 'Add an extra layer of security with a TOTP authenticator app.',
    enable_2fa: 'Enable 2FA',
    disable_2fa: 'Disable 2FA',
    totp_enabled_msg: 'Two-factor authentication is enabled.',
    totp_scan_qr: 'Scan this QR code with your authenticator app (Google Authenticator, Authy, etc.)',
    totp_manual_secret: 'Or enter this secret manually:',
    totp_backup_codes: 'Backup Codes (save these somewhere safe!):',
    verify_enable: 'Verify & Enable',
    confirm_disable: 'Confirm Disable',
    totp_disable_desc: 'Enter your current TOTP code to disable 2FA:',
    // Search
    tab_ebooks: 'Ebooks',
    tab_audiobooks: 'Audiobooks',
    tab_manga: 'Manga',
    search_placeholder: 'Search for books...',
    search_placeholder_ab: 'Search for audiobooks...',
    search_placeholder_manga: 'Search for manga...',
    sort_relevance: 'Relevance',
    sort_seeders: 'Seeders',
    sort_size: 'Size',
    n_results: '{n} results',
    search_empty_title: 'Search for your next read',
    search_empty_hint: 'Try searching by title, author, or ISBN',
    no_results: 'No results found',
    no_results_hint: 'Try different keywords or check your spelling',
    download: 'Download',
    download_added: 'Added',
    download_failed_state: 'Failed',
    search_failed: 'Search failed: {msg}',
    n_seeds: '{n} seed',
    n_leech: '{n} leech',
    n_copies: '{n} copies',
    n_copies_title: '{n} identical copies, differing only by file hash',
    in_library: 'In Library',
    in_library_title: 'Already in your library as "{title}"',
    download_anyway: 'Download anyway',
    already_in_library: 'Already in your library: {title}',
    // Library
    library_filter_placeholder: 'Filter library...',
    library_empty: 'Your library is empty',
    library_empty_hint: 'Download some books to get started',
    failed_load_library: 'Failed to load library',
    other: 'Other',
    n_items: '{n} items',
    n_files: '{n} files',
    n_pages: '{n} pages',
    open_in_abs: 'Open in Audiobookshelf',
    open_in_kavita: 'Open in Kavita',
    prev: 'Prev',
    next: 'Next',
    // Downloads
    downloads_title: 'Downloads',
    refresh: 'Refresh',
    clear_completed: 'Clear Completed',
    no_active_downloads: 'No active downloads',
    no_downloads_hint: 'Search for books and click download to get started',
    failed_load_downloads: 'Failed to load downloads',
    retry: 'Retry',
    // Status
    status_downloading: 'Downloading',
    status_completed: 'Completed',
    status_error: 'Error',
    status_organizing: 'Organizing',
    status_dead_letter: 'Dead Letter',
    status_queued: 'Queued',
    status_searching: 'Searching',
    status_importing: 'Importing',
    status_retry_wait: 'Retry Wait',
    // Download actions
    download_started: 'Download started: {title}',
    download_complete: 'Download completed: {title}',
    download_completed: 'Download completed: {title}',
    download_failed: 'Download failed: {msg}',
    download_failed_anna_no_match: "Anna's Archive could not find a matching LibGen MD5 for this book. Download it manually from Anna's Archive or choose another source.",
    download_failed_anna_no_match_action: "Open Anna's Archive",
    unknown_title: 'Unknown title',
    unknown_error: 'Unknown error',
    retrying_download: 'Retrying download',
    retry_failed: 'Retry failed',
    cleared_completed: 'Cleared completed downloads',
    failed_clear: 'Failed to clear',
    // Wishlist
    wishlist_title: 'Wishlist',
    wishlist_add_book: 'Add Book',
    ph_wishlist_title: 'Title',
    ph_wishlist_author: 'Author (optional)',
    opt_ebook: 'Ebook',
    opt_audiobook: 'Audiobook',
    opt_manga: 'Manga',
    add: 'Add',
    cancel: 'Cancel',
    wishlist_empty: 'Wishlist is empty',
    wishlist_empty_hint: 'Add books you want to find later',
    wishlist_search: 'Search',
    failed_load_wishlist: 'Failed to load wishlist',
    err_title_required: 'Title is required',
    added_to_wishlist: 'Added to wishlist',
    failed_add_wishlist: 'Failed to add to wishlist',
    removed_from_wishlist: 'Removed from wishlist',
    failed_delete: 'Failed to delete',
    // Settings
    s_user_mgmt_title: 'User Management',
    add_new_user: 'Add New User',
    add_user_btn: 'Add User',
    role_user: 'User',
    role_admin: 'Admin',
    last_login: 'Last login: {date}',
    never: 'Never',
    confirm_delete_user: 'Delete user "{username}"? This cannot be undone.',
    s_connection_tests: 'Connection Tests',
    conn_prowlarr: 'Prowlarr',
    conn_qbittorrent: 'qBittorrent',
    conn_transmission: 'Transmission',
    conn_sabnzbd: 'SABnzbd',
    conn_audiobookshelf: 'Audiobookshelf',
    conn_kavita: 'Kavita',
    not_tested: 'Not tested',
    btn_test: 'Test',
    s_search_sources: 'Search Sources',
    loading_sources: 'Loading sources...',
    s_configuration: 'Configuration',
    loading: 'Loading...',
    not_configured: 'Not configured',
    no_config_data: 'No configuration data available',
    failed_load_config: 'Failed to load configuration',
    no_sources: 'No sources configured',
    enabled: 'Enabled',
    disabled: 'Disabled',
    failed_load_sources: 'Failed to load sources',
    testing: 'Testing...',
    connected: 'Connected',
    conn_error: 'Error',
    // TOTP settings toasts
    failed_setup_totp: 'Failed to setup TOTP',
    enter_6digit_code: 'Enter the 6-digit code from your app',
    totp_enabled_success: 'Two-factor authentication enabled',
    verification_failed: 'Verification failed',
    enter_totp_code: 'Enter your current TOTP code',
    totp_disabled_success: 'Two-factor authentication disabled',
    failed_disable_totp: 'Failed to disable TOTP',
    // User management toasts
    user_role_updated: 'User role updated',
    failed_update_role: 'Failed to update role',
    user_deleted: 'User deleted',
    failed_delete_user: 'Failed to delete user',
    user_created: 'User created',
    failed_create_user: 'Failed to create user',
    // Auth
    signed_in: 'Signed in successfully',
    admin_created: 'Admin account created. Welcome!',
    account_created: 'Account created. Please sign in.',
    signed_out: 'Signed out',
    err_credentials_required: 'Username and password are required',
    err_invalid_credentials: 'Invalid credentials',
    err_connection: 'Connection error',
    err_code_required: 'Code is required',
    err_invalid_code: 'Invalid code',
    backup_code_used: 'Backup code used. Consider generating new ones.',
    registration_failed: 'Registration failed',
    // Stats
    n_items_in_library: '{n} items in library',
    // Search Preferences
    search_preferences: 'Search Preferences',
    filter_non_english: 'Filter non-English results',
    filter_non_english_desc: 'When enabled, books with non-English titles are hidden from English-focused sources. Multilingual sources (Flibusta, Z-Library) always show all results.',
    filter_enabled_toast: 'Foreign language filter enabled',
    filter_disabled_toast: 'Foreign language filter disabled — showing all languages',
    filter_update_failed: 'Failed to update filter setting',
    filter_save_failed: 'Failed to save filter setting',
    // Flibusta / Z-Library config display
    flibusta_enabled: 'Flibusta',
    zlibrary_enabled: 'Z-Library',
  },
  ru: {
    // Navigation
    nav_search: 'Поиск',
    nav_library: 'Библиотека',
    nav_downloads: 'Загрузки',
    nav_wishlist: 'Желаемое',
    nav_settings: 'Настройки',
    sign_out: 'Выйти',
    // Header
    header_subtitle: 'Менеджер книг для самохостинга',
    // Login modal
    login_subtitle: 'Войдите для продолжения',
    login_sign_in: 'Войти',
    login_or: 'или',
    login_sso: 'Войти через SSO',
    login_with: 'Войти через {provider}',
    login_no_account: 'Нет аккаунта? <a href="#" data-action="showRegisterForm" class="text-indigo-400 hover:text-indigo-300">Регистрация</a>',
    create_admin_account: 'Создайте аккаунт администратора',
    create_your_account: 'Создайте аккаунт',
    create_account: 'Создать аккаунт',
    back_to_login: 'Назад к входу',
    // Labels
    label_username: 'Логин',
    label_password: 'Пароль',
    // Placeholders
    ph_username: 'Логин',
    ph_password: 'Пароль',
    ph_choose_username: 'Выберите логин',
    ph_min_6_chars: 'Минимум 6 символов',
    // TOTP
    totp_enter_code: 'Введите 6-значный код из приложения-аутентификатора или резервный код.',
    totp_code_label: 'TOTP код',
    totp_verify: 'Проверить',
    s_totp_title: 'Двухфакторная аутентификация',
    s_totp_desc: 'Добавьте дополнительный уровень защиты с помощью TOTP-приложения.',
    enable_2fa: 'Включить 2FA',
    disable_2fa: 'Отключить 2FA',
    totp_enabled_msg: 'Двухфакторная аутентификация включена.',
    totp_scan_qr: 'Отсканируйте QR-код приложением-аутентификатором (Google Authenticator, Authy и т.д.)',
    totp_manual_secret: 'Или введите секрет вручную:',
    totp_backup_codes: 'Резервные коды (сохраните в безопасном месте!):',
    verify_enable: 'Проверить и включить',
    confirm_disable: 'Подтвердить отключение',
    totp_disable_desc: 'Введите текущий TOTP-код для отключения 2FA:',
    // Search
    tab_ebooks: 'Книги',
    tab_audiobooks: 'Аудиокниги',
    tab_manga: 'Манга',
    search_placeholder: 'Поиск книг...',
    search_placeholder_ab: 'Поиск аудиокниг...',
    search_placeholder_manga: 'Поиск манги...',
    sort_relevance: 'Релевантность',
    sort_seeders: 'Сидеры',
    sort_size: 'Размер',
    n_results: '{n} результатов',
    search_empty_title: 'Найдите следующую книгу для чтения',
    search_empty_hint: 'Попробуйте искать по названию, автору или ISBN',
    no_results: 'Ничего не найдено',
    no_results_hint: 'Попробуйте другие ключевые слова или проверьте написание',
    download: 'Скачать',
    download_added: 'Добавлено',
    download_failed_state: 'Ошибка',
    search_failed: 'Ошибка поиска: {msg}',
    n_seeds: '{n} сид.',
    n_leech: '{n} лич.',
    n_copies: '{n} копий',
    n_copies_title: '{n} одинаковых копий, различаются только хешем файла',
    in_library: 'В библиотеке',
    in_library_title: 'Уже в вашей библиотеке как «{title}»',
    download_anyway: 'Всё равно скачать',
    already_in_library: 'Уже в вашей библиотеке: {title}',
    // Library
    library_filter_placeholder: 'Фильтр библиотеки...',
    library_empty: 'Ваша библиотека пуста',
    library_empty_hint: 'Скачайте несколько книг для начала',
    failed_load_library: 'Не удалось загрузить библиотеку',
    other: 'Другое',
    n_items: '{n} элементов',
    n_files: '{n} файлов',
    n_pages: '{n} страниц',
    open_in_abs: 'Открыть в Audiobookshelf',
    open_in_kavita: 'Открыть в Kavita',
    prev: 'Назад',
    next: 'Далее',
    // Downloads
    downloads_title: 'Загрузки',
    refresh: 'Обновить',
    clear_completed: 'Очистить завершённые',
    no_active_downloads: 'Нет активных загрузок',
    no_downloads_hint: 'Найдите книги и нажмите «Скачать» для начала',
    failed_load_downloads: 'Не удалось загрузить список загрузок',
    retry: 'Повторить',
    // Status
    status_downloading: 'Загрузка',
    status_completed: 'Завершено',
    status_error: 'Ошибка',
    status_organizing: 'Организация',
    status_dead_letter: 'Dead Letter',
    status_queued: 'В очереди',
    status_searching: 'Поиск',
    status_importing: 'Импорт',
    status_retry_wait: 'Ожидание повтора',
    // Download actions
    download_started: 'Загрузка начата: {title}',
    download_complete: 'Загрузка завершена: {title}',
    download_completed: 'Загрузка завершена: {title}',
    download_failed: 'Ошибка загрузки: {msg}',
    download_failed_anna_no_match: 'Anna\'s Archive не смог найти совпадающий MD5 LibGen для этой книги. Скачайте её вручную из Anna\'s Archive или выберите другой источник.',
    download_failed_anna_no_match_action: 'Открыть Anna\'s Archive',
    unknown_title: 'Неизвестное название',
    unknown_error: 'Неизвестная ошибка',
    retrying_download: 'Повтор загрузки',
    retry_failed: 'Ошибка повтора',
    cleared_completed: 'Завершённые загрузки очищены',
    failed_clear: 'Не удалось очистить',
    // Wishlist
    wishlist_title: 'Список желаемого',
    wishlist_add_book: 'Добавить книгу',
    ph_wishlist_title: 'Название',
    ph_wishlist_author: 'Автор (необязательно)',
    opt_ebook: 'Книга',
    opt_audiobook: 'Аудиокнига',
    opt_manga: 'Манга',
    add: 'Добавить',
    cancel: 'Отмена',
    wishlist_empty: 'Список желаемого пуст',
    wishlist_empty_hint: 'Добавьте книги, которые хотите найти позже',
    wishlist_search: 'Найти',
    failed_load_wishlist: 'Не удалось загрузить список желаемого',
    err_title_required: 'Требуется название',
    added_to_wishlist: 'Добавлено в список желаемого',
    failed_add_wishlist: 'Не удалось добавить в список желаемого',
    removed_from_wishlist: 'Удалено из списка желаемого',
    failed_delete: 'Не удалось удалить',
    // Settings
    s_user_mgmt_title: 'Управление пользователями',
    add_new_user: 'Добавить пользователя',
    add_user_btn: 'Добавить',
    role_user: 'Пользователь',
    role_admin: 'Админ',
    last_login: 'Последний вход: {date}',
    never: 'Никогда',
    confirm_delete_user: 'Удалить пользователя «{username}»? Это нельзя отменить.',
    s_connection_tests: 'Проверка подключений',
    conn_prowlarr: 'Prowlarr',
    conn_qbittorrent: 'qBittorrent',
    conn_transmission: 'Transmission',
    conn_sabnzbd: 'SABnzbd',
    conn_audiobookshelf: 'Audiobookshelf',
    conn_kavita: 'Kavita',
    not_tested: 'Не проверено',
    btn_test: 'Проверить',
    s_search_sources: 'Источники поиска',
    loading_sources: 'Загрузка источников...',
    s_configuration: 'Конфигурация',
    loading: 'Загрузка...',
    not_configured: 'Не настроено',
    no_config_data: 'Нет данных конфигурации',
    failed_load_config: 'Не удалось загрузить конфигурацию',
    no_sources: 'Нет настроенных источников',
    enabled: 'Включено',
    disabled: 'Отключено',
    failed_load_sources: 'Не удалось загрузить источники',
    testing: 'Проверка...',
    connected: 'Подключено',
    conn_error: 'Ошибка',
    // TOTP settings toasts
    failed_setup_totp: 'Не удалось настроить TOTP',
    enter_6digit_code: 'Введите 6-значный код из приложения',
    totp_enabled_success: 'Двухфакторная аутентификация включена',
    verification_failed: 'Ошибка проверки',
    enter_totp_code: 'Введите текущий TOTP-код',
    totp_disabled_success: 'Двухфакторная аутентификация отключена',
    failed_disable_totp: 'Не удалось отключить TOTP',
    // User management toasts
    user_role_updated: 'Роль пользователя обновлена',
    failed_update_role: 'Не удалось обновить роль',
    user_deleted: 'Пользователь удалён',
    failed_delete_user: 'Не удалось удалить пользователя',
    user_created: 'Пользователь создан',
    failed_create_user: 'Не удалось создать пользователя',
    // Auth
    signed_in: 'Успешный вход',
    admin_created: 'Аккаунт администратора создан. Добро пожаловать!',
    account_created: 'Аккаунт создан. Пожалуйста, войдите.',
    signed_out: 'Вы вышли',
    err_credentials_required: 'Требуется логин и пароль',
    err_invalid_credentials: 'Неверные учётные данные',
    err_connection: 'Ошибка соединения',
    err_code_required: 'Требуется код',
    err_invalid_code: 'Неверный код',
    backup_code_used: 'Использован резервный код. Рекомендуется создать новые.',
    registration_failed: 'Ошибка регистрации',
    // Stats
    n_items_in_library: '{n} элементов в библиотеке',
    // Search Preferences
    search_preferences: 'Настройки поиска',
    filter_non_english: 'Фильтровать неанглоязычные результаты',
    filter_non_english_desc: 'Если включено, книги с неанглоязычными названиями скрываются из англоязычных источников. Многоязычные источники (Flibusta, Z-Library) всегда показывают все результаты.',
    filter_enabled_toast: 'Фильтр иностранных языков включён',
    filter_disabled_toast: 'Фильтр иностранных языков выключен — показаны все языки',
    filter_update_failed: 'Не удалось обновить настройку фильтра',
    filter_save_failed: 'Не удалось сохранить настройку фильтра',
    // Flibusta / Z-Library config display
    flibusta_enabled: 'Flibusta',
    zlibrary_enabled: 'Z-Library',
  }
};

// ============================================================
// I18N FUNCTIONS
// ============================================================
function t(key, vars) {
  const lang = I18N[currentLang] || I18N.en;
  let val = lang[key] || I18N.en[key] || key;
  if (vars) {
    for (const [k, v] of Object.entries(vars)) {
      val = val.replaceAll(`{${k}}`, v);
    }
  }
  return val;
}

function applyLanguage() {
  document.documentElement.lang = currentLang;
  document.getElementById('lang-toggle').textContent = currentLang === 'en' ? 'RU' : 'EN';

  document.querySelectorAll('[data-i18n]').forEach(el => {
    el.textContent = t(el.dataset.i18n);
  });
  document.querySelectorAll('[data-i18n-html]').forEach(el => {
    el.innerHTML = t(el.dataset.i18nHtml);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    el.placeholder = t(el.dataset.i18nPlaceholder);
  });
  document.querySelectorAll('[data-i18n-title]').forEach(el => {
    el.title = t(el.dataset.i18nTitle);
  });
}

function toggleLanguage() {
  currentLang = currentLang === 'en' ? 'ru' : 'en';
  localStorage.setItem('librarr_lang', currentLang);
  applyLanguage();
  refreshDynamicContent();
}

function refreshDynamicContent() {
  // Re-render current tab content with new language
  const tab = state.currentTab;
  if (tab === 'search' && state.searchResults.length > 0) {
    renderSearchResults();
  } else if (tab === 'downloads') {
    refreshDownloads();
  } else if (tab === 'library') {
    loadLibrary();
  } else if (tab === 'wishlist') {
    loadWishlist();
  } else if (tab === 'settings') {
    loadConfig();
    loadSources();
  }
}

// ============================================================
// STATE
// ============================================================
const state = {
  currentTab: 'search',
  searchTab: 'ebooks',
  libraryTab: 'ebooks',
  searchResults: [],
  pendingDownloads: new Set(),
  trackedDownloadJobs: new Map(),
  downloadOutcomes: new Map(),
  downloadOutcomeTimers: new Map(),
  downloadJobs: [],
  pendingRetryDownloads: new Set(),
  sortMode: 'relevance',
  libraryPage: 1,
  libraryPages: 1,
  config: null,
  downloadPollTimer: null,
  currentUser: null,
  currentRole: null,
};

const SOURCE_COLORS = {
  annas:           { bg: '#7c3aed', text: 'white', label: "Anna's Archive" },
  torrent:         { bg: '#2563eb', text: 'white', label: 'Prowlarr' },
  prowlarr_manga:  { bg: '#2563eb', text: 'white', label: 'Prowlarr' },
  audiobook:       { bg: '#2563eb', text: 'white', label: 'Prowlarr' },
  audiobookbay:    { bg: '#059669', text: 'white', label: 'AudioBookBay' },
  gutenberg:       { bg: '#d97706', text: 'white', label: 'Gutenberg' },
  openlibrary:     { bg: '#dc2626', text: 'white', label: 'Open Library' },
  standardebooks:  { bg: '#0891b2', text: 'white', label: 'Standard Ebooks' },
  librivox:        { bg: '#7c3aed', text: 'white', label: 'Librivox' },
  mangadex:        { bg: '#ff6740', text: 'white', label: 'MangaDex' },
  nyaa_manga:      { bg: '#16a34a', text: 'white', label: 'Nyaa' },
  annas_manga:     { bg: '#7c3aed', text: 'white', label: "Anna's Manga" },
  webnovel:        { bg: '#6366f1', text: 'white', label: 'Web Novel' },
  flibusta:        { bg: '#b91c1c', text: 'white', label: 'Flibusta' },
  zlibrary:        { bg: '#4338ca', text: 'white', label: 'Z-Library' },
  tpb:             { bg: '#1e40af', text: 'white', label: 'ThePirateBay' },
  tpb_audiobook:   { bg: '#1e40af', text: 'white', label: 'ThePirateBay' },
  booktracker:     { bg: '#0e7490', text: 'white', label: 'BookTracker' },
  booktracker_audiobook: { bg: '#0e7490', text: 'white', label: 'BookTracker' },
};

const COVER_GRADIENTS = [
  'from-indigo-600 to-purple-700',
  'from-blue-600 to-cyan-700',
  'from-emerald-600 to-teal-700',
  'from-rose-600 to-pink-700',
  'from-amber-600 to-orange-700',
  'from-violet-600 to-fuchsia-700',
  'from-sky-600 to-blue-700',
  'from-lime-600 to-green-700',
];

const STATUS_STYLES = {
  downloading: { bg: 'bg-blue-500/20', text: 'text-blue-400', border: 'border-blue-500/30', label: 'Downloading' },
  completed:   { bg: 'bg-emerald-500/20', text: 'text-emerald-400', border: 'border-emerald-500/30', label: 'Completed' },
  error:       { bg: 'bg-red-500/20', text: 'text-red-400', border: 'border-red-500/30', label: 'Error' },
  organizing:  { bg: 'bg-yellow-500/20', text: 'text-yellow-400', border: 'border-yellow-500/30', label: 'Organizing' },
  dead_letter: { bg: 'bg-slate-500/20', text: 'text-slate-400', border: 'border-slate-500/30', label: 'Dead Letter' },
  queued:      { bg: 'bg-slate-500/20', text: 'text-slate-400', border: 'border-slate-500/30', label: 'Queued' },
  searching:   { bg: 'bg-indigo-500/20', text: 'text-indigo-400', border: 'border-indigo-500/30', label: 'Searching' },
  importing:   { bg: 'bg-purple-500/20', text: 'text-purple-400', border: 'border-purple-500/30', label: 'Importing' },
  retry_wait:  { bg: 'bg-amber-500/20', text: 'text-amber-400', border: 'border-amber-500/30', label: 'Retry Wait' },
};

const TERMINAL_DOWNLOAD_STATUSES = new Set(['completed', 'error', 'dead_letter']);

// ============================================================
// API HELPERS
// ============================================================
function getApiKey() {
  return localStorage.getItem('librarr_apikey') || '';
}

async function api(path, options = {}) {
  const url = new URL(path, window.location.origin);
  const key = getApiKey();
  if (key) url.searchParams.set('apikey', key);

  const resp = await fetch(url.toString(), {
    credentials: 'include',
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });

  if (resp.status === 401) {
    showLoginModal();
    throw new Error('Unauthorized');
  }

  return resp;
}

async function apiJson(path, options = {}) {
  const resp = await api(path, options);
  if (!resp.ok) throw new Error(`API error: ${resp.status}`);
  return resp.json();
}

// ============================================================
// TOAST NOTIFICATIONS
// ============================================================
function showToast(message, type = 'info', options = {}) {
  const container = document.getElementById('toast-container');
  const {
    sticky = false,
    actionLabel = '',
    actionHref = '',
    actionTarget = '_blank',
  } = options;
  const colors = {
    info: 'bg-slate-800 border-slate-600',
    success: 'bg-emerald-900/80 border-emerald-600/50',
    error: 'bg-red-900/80 border-red-600/50',
    warning: 'bg-yellow-900/80 border-yellow-600/50',
  };
  const icons = {
    info: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>',
    success: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>',
    error: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"/>',
    warning: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"/>',
  };
  const iconColors = { info: 'text-slate-400', success: 'text-emerald-400', error: 'text-red-400', warning: 'text-yellow-400' };

  const el = document.createElement('div');
  el.className = `toast-enter ${colors[type]} border rounded-lg px-4 py-3 shadow-xl flex items-start gap-3`;
  const messageWrap = document.createElement('div');
  messageWrap.className = 'flex-1 min-w-0';

  const messageEl = document.createElement('p');
  messageEl.className = 'text-sm text-slate-200';
  messageEl.textContent = message;
  messageWrap.appendChild(messageEl);

  if (actionLabel && actionHref) {
    const action = document.createElement('a');
    action.href = actionHref;
    action.target = actionTarget;
    action.rel = 'noreferrer noopener';
    action.className = 'mt-2 inline-flex items-center rounded-lg bg-indigo-600 px-2.5 py-1 text-xs font-medium text-white transition-colors hover:bg-indigo-500';
    action.textContent = actionLabel;
    messageWrap.appendChild(action);
  }

  const close = document.createElement('button');
  close.className = 'text-slate-500 hover:text-slate-300 flex-shrink-0';
  close.addEventListener('click', () => el.remove());
  close.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>';

  const icon = document.createElement('svg');
  icon.className = `w-5 h-5 ${iconColors[type]} flex-shrink-0 mt-0.5`;
  icon.setAttribute('fill', 'none');
  icon.setAttribute('stroke', 'currentColor');
  icon.setAttribute('viewBox', '0 0 24 24');
  icon.innerHTML = icons[type];

  el.appendChild(icon);
  el.appendChild(messageWrap);
  el.appendChild(close);
  container.appendChild(el);

  if (!sticky) {
    setTimeout(() => {
      el.classList.remove('toast-enter');
      el.classList.add('toast-exit');
      setTimeout(() => el.remove(), 300);
    }, 4000);
  }
}

function isAnnaNoMatchError(msg) {
  return typeof msg === 'string' && (
    msg.includes('matching LibGen MD5') ||
    msg.includes('libgen no matching MD5') ||
    msg.includes('File not found in DB')
  );
}

function showAnnaNoMatchToast(title, annaUrl) {
  showToast(
    t('download_failed_anna_no_match'),
    'error',
    annaUrl ? {
      sticky: true,
      actionLabel: t('download_failed_anna_no_match_action'),
      actionHref: annaUrl,
    } : {
      sticky: true,
    }
  );
}

// ============================================================
// LOGIN / REGISTER / TOTP
// ============================================================
let pendingTOTPSession = '';

function showLoginModal() {
  document.getElementById('login-modal').classList.remove('hidden');
  document.getElementById('login-modal').classList.add('flex');
  showLoginForm();

  // Check auth status to decide showing register link or OIDC button.
  fetch('/api/auth/status', { credentials: 'include' }).then(r => r.json()).then(data => {
    if (data.authenticated) {
      hideLoginModal();
      return;
    }
    // Always show register link — invite codes make self-registration secure.
    // First user creates admin (no invite needed). After that, invite code required.
    const regLink = document.getElementById('login-register-link');
    regLink.classList.remove('hidden');
    if (!data.has_users) {
      // First-run: default to the register form with a welcome banner. The
      // login form has nothing to log into yet, so showing it first wastes a
      // click and hides the actual setup step behind a "Register" link.
      document.getElementById('login-subtitle').textContent = t('create_admin_account');
      document.getElementById('first-run-banner').classList.remove('hidden');
      const invField = document.getElementById('invite-code-field');
      if (invField) invField.classList.add('hidden');
      showRegisterForm();
      // No accounts exist — hide the "Back to login" link in register form.
      const backLinks = document.querySelectorAll('#register-form a[data-action="showLoginForm"]');
      backLinks.forEach(a => a.parentElement.classList.add('hidden'));
    } else {
      document.getElementById('login-subtitle').textContent = t('login_subtitle');
      document.getElementById('first-run-banner').classList.add('hidden');
    }
  }).catch(() => {});

  // Check OIDC config.
  fetch('/api/auth/status', { credentials: 'include' }).then(r => r.json()).then(data => {
    if (data.oidc_enabled) {
      document.getElementById('login-oidc-btn').classList.remove('hidden');
      document.getElementById('oidc-login-link').textContent = t('login_with', {provider: data.oidc_provider_name || 'SSO'});
    }
  }).catch(() => {});
}

function hideLoginModal() {
  document.getElementById('login-modal').classList.add('hidden');
  document.getElementById('login-modal').classList.remove('flex');
}

function showLoginForm() {
  document.getElementById('login-form').classList.remove('hidden');
  document.getElementById('totp-form').classList.add('hidden');
  document.getElementById('register-form').classList.add('hidden');
  document.getElementById('login-username').focus();
}

function showRegisterForm() {
  document.getElementById('login-form').classList.add('hidden');
  document.getElementById('totp-form').classList.add('hidden');
  document.getElementById('register-form').classList.remove('hidden');
  document.getElementById('login-subtitle').textContent = t('create_your_account');
  document.getElementById('register-username').focus();
}

function showTOTPForm() {
  document.getElementById('login-form').classList.add('hidden');
  document.getElementById('register-form').classList.add('hidden');
  document.getElementById('totp-form').classList.remove('hidden');
  document.getElementById('login-subtitle').textContent = t('s_totp_title');
  document.getElementById('totp-code').focus();
}

document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const errEl = document.getElementById('login-error');
  errEl.classList.add('hidden');

  const username = document.getElementById('login-username').value.trim();
  const password = document.getElementById('login-password').value;

  if (!username || !password) {
    errEl.textContent = t('err_credentials_required');
    errEl.classList.remove('hidden');
    return;
  }

  try {
    const resp = await fetch('/api/login', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });

    const data = await resp.json().catch(() => ({}));

    if (resp.ok && data.success) {
      if (data.needs_totp) {
        pendingTOTPSession = data.session_pending;
        showTOTPForm();
        return;
      }
      hideLoginModal();
      updateUserHeader(data.username, data.role);
      init();
      showToast(t('signed_in'), 'success');
    } else {
      errEl.textContent = data.error || t('err_invalid_credentials');
      errEl.classList.remove('hidden');
    }
  } catch (err) {
    errEl.textContent = t('err_connection');
    errEl.classList.remove('hidden');
  }
});

document.getElementById('totp-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const errEl = document.getElementById('totp-error');
  errEl.classList.add('hidden');

  const code = document.getElementById('totp-code').value.trim();
  if (!code) {
    errEl.textContent = t('err_code_required');
    errEl.classList.remove('hidden');
    return;
  }

  try {
    const resp = await fetch('/api/login/totp', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_pending: pendingTOTPSession, code }),
    });

    const data = await resp.json().catch(() => ({}));
    if (resp.ok && data.success) {
      hideLoginModal();
      updateUserHeader(data.username, data.role);
      init();
      showToast(t('signed_in'), 'success');
      if (data.backup_code_used) showToast(t('backup_code_used'), 'warning');
    } else {
      errEl.textContent = data.error || t('err_invalid_code');
      errEl.classList.remove('hidden');
    }
  } catch (err) {
    errEl.textContent = t('err_connection');
    errEl.classList.remove('hidden');
  }
});

document.getElementById('register-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const errEl = document.getElementById('register-error');
  errEl.classList.add('hidden');

  const username = document.getElementById('register-username').value.trim();
  const password = document.getElementById('register-password').value;
  const inviteCode = (document.getElementById('register-invite-code')?.value || '').trim();

  if (!username || !password) {
    errEl.textContent = t('err_credentials_required');
    errEl.classList.remove('hidden');
    return;
  }

  try {
    const body = { username, password };
    if (inviteCode) body.invite_code = inviteCode;

    const resp = await fetch('/api/register', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const data = await resp.json().catch(() => ({}));
    if (resp.ok && data.success) {
      if (data.token) {
        // First user — auto-logged in.
        hideLoginModal();
        updateUserHeader(data.username, data.role);
        init();
        showToast(t('admin_created'), 'success');
      } else {
        showLoginForm();
        showToast(t('account_created'), 'success');
      }
    } else {
      errEl.textContent = data.error || t('registration_failed');
      errEl.classList.remove('hidden');
    }
  } catch (err) {
    errEl.textContent = t('err_connection');
    errEl.classList.remove('hidden');
  }
});

function updateUserHeader(username, role) {
  if (username) {
    document.getElementById('header-user').classList.remove('hidden');
    document.getElementById('header-user').classList.add('flex');
    document.getElementById('header-username').textContent = username;
    document.getElementById('header-role').textContent = role || 'user';
    document.getElementById('logout-btn').classList.remove('hidden');
    state.currentUser = username;
    state.currentRole = role;
  } else {
    document.getElementById('header-user').classList.add('hidden');
    document.getElementById('logout-btn').classList.add('hidden');
    state.currentUser = null;
    state.currentRole = null;
  }
}

async function doLogout() {
  try {
    await fetch('/api/logout', { method: 'POST', credentials: 'include' });
  } catch (e) {}
  updateUserHeader(null, null);
  showLoginModal();
  showToast(t('signed_out'), 'info');
}

// ============================================================
// MOBILE NAV
// ============================================================
function toggleMobileNav() {
  const nav = document.getElementById('main-nav');
  nav.classList.toggle('mobile-open');
}

// Close mobile nav when a tab is selected
function closeMobileNav() {
  const nav = document.getElementById('main-nav');
  nav.classList.remove('mobile-open');
}

// TAB NAVIGATION
// ============================================================
function switchTab(tab) {
  closeMobileNav();
  state.currentTab = tab;

  // Update nav
  document.querySelectorAll('.nav-tab').forEach(el => {
    el.classList.toggle('active', el.dataset.tab === tab);
  });

  // Update content
  document.querySelectorAll('.tab-content').forEach(el => {
    el.classList.toggle('active', el.id === `tab-${tab}`);
  });

  // Load data for the tab
  if (tab === 'library') loadLibrary();
  if (tab === 'downloads') { refreshDownloads(); startDownloadPolling(); }
  else stopDownloadPolling();
  if (tab === 'wishlist') loadWishlist();
  if (tab === 'settings') loadSettings();
}

function switchSearchTab(tab) {
  state.searchTab = tab;
  document.querySelectorAll('.sub-tab').forEach(el => {
    el.classList.toggle('active', el.dataset.stab === tab);
  });
  // Clear results when switching
  document.getElementById('search-results').innerHTML = '';
  document.getElementById('search-sort-bar').classList.add('hidden');
  document.getElementById('search-no-results').classList.add('hidden');
  document.getElementById('search-empty').classList.remove('hidden');
  state.searchResults = [];

  // Update placeholder
  const placeholders = { ebooks: t('search_placeholder'), audiobooks: t('search_placeholder_ab'), manga: t('search_placeholder_manga') };
  document.getElementById('search-input').placeholder = placeholders[tab] || 'Search...';
  document.getElementById('search-input').value = '';
}

function switchLibraryTab(tab) {
  state.libraryTab = tab;
  state.libraryPage = 1;
  document.querySelectorAll('.lib-tab').forEach(el => {
    el.classList.toggle('active', el.dataset.ltab === tab);
  });
  loadLibrary();
}

// ============================================================
// SEARCH
// ============================================================
let searchTimeout = null;
let searchAbort = null;   // AbortController for in-flight request
let searchGeneration = 0; // monotonic counter — stale responses are discarded

document.getElementById('search-input').addEventListener('input', (e) => {
  clearTimeout(searchTimeout);
  const q = e.target.value.trim();
  if (q.length < 2) return;
  searchTimeout = setTimeout(() => doSearch(q), 300);
});

document.getElementById('search-input').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    if (q.length >= 1) doSearch(q);
  }
});

async function doSearch(query) {
  const endpoints = { ebooks: '/api/search', audiobooks: '/api/search/audiobooks', manga: '/api/search/manga' };
  const endpoint = endpoints[state.searchTab] || '/api/search';
  const streamEndpoint = `${endpoint}/stream`;

  // Abort any in-flight search — prevents stale results from overwriting
  // the new search when the old request finishes after the new one starts.
  if (searchAbort) searchAbort.abort();
  searchAbort = new AbortController();
  const gen = ++searchGeneration;

  // Show skeleton
  showSearchSkeleton();
  state.searchResults = [];
  document.getElementById('search-results').innerHTML = '';
  document.getElementById('search-empty').classList.add('hidden');
  document.getElementById('search-no-results').classList.add('hidden');
  document.getElementById('search-spinner').classList.remove('hidden');

  try {
    await doStreamingSearch(streamEndpoint, query, gen, searchAbort.signal);
  } catch (err) {
    if (err.name === 'AbortError') return; // expected — new search superseded this one
    if (gen !== searchGeneration) return;
    try {
      await doJsonSearch(endpoint, query, gen, searchAbort.signal);
    } catch (fallbackErr) {
      if (fallbackErr.name === 'AbortError') return;
      if (gen !== searchGeneration) return;
      document.getElementById('search-spinner').classList.add('hidden');
      hideSearchSkeleton();
      if (fallbackErr.message !== 'Unauthorized') {
        showToast(t('search_failed', {msg: fallbackErr.message}), 'error');
      }
    }
  }
}

async function doJsonSearch(endpoint, query, gen, signal) {
  const data = await apiJson(`${endpoint}?q=${encodeURIComponent(query)}`, { signal });
  if (gen !== searchGeneration) return;
  updateSearchResults(data.results || [], false);
}

async function doStreamingSearch(endpoint, query, gen, signal) {
  const resp = await api(`${endpoint}?q=${encodeURIComponent(query)}`, {
    signal,
    headers: { Accept: 'text/event-stream' },
  });
  if (!resp.ok || !resp.body) throw new Error(`API error: ${resp.status}`);

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let completed = false;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split('\n\n');
    buffer = frames.pop() || '';
    for (const frame of frames) {
      const evt = parseSSEFrame(frame);
      if (!evt || gen !== searchGeneration) continue;
      if (evt.event === 'results' || evt.event === 'complete') {
        updateSearchResults(evt.data.results || [], evt.event !== 'complete');
        completed = evt.event === 'complete';
      }
    }
  }

  if (gen === searchGeneration && !completed) {
    document.getElementById('search-spinner').classList.add('hidden');
    if (state.searchResults.length === 0) {
      hideSearchSkeleton();
      document.getElementById('search-no-results').classList.remove('hidden');
    }
  }
}

function parseSSEFrame(frame) {
  let event = 'message';
  const dataLines = [];
  frame.split('\n').forEach(line => {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  });
  if (dataLines.length === 0) return null;
  try {
    return { event, data: JSON.parse(dataLines.join('\n')) };
  } catch {
    return null;
  }
}

function updateSearchResults(results, searching) {
  state.searchResults = results || [];
  document.getElementById('search-spinner').classList.toggle('hidden', !searching);

  if (state.searchResults.length === 0) {
    document.getElementById('search-sort-bar').classList.add('hidden');
    if (searching) {
      document.getElementById('search-no-results').classList.add('hidden');
      return;
    }
    hideSearchSkeleton();
    document.getElementById('search-no-results').classList.toggle('hidden', searching);
    return;
  }

  hideSearchSkeleton();
  document.getElementById('search-no-results').classList.add('hidden');
  document.getElementById('search-sort-bar').classList.remove('hidden');
  document.getElementById('search-result-count').textContent = t('n_results', {n: state.searchResults.length});
  renderSearchResults();
}

function showSearchSkeleton() {
  const container = document.getElementById('search-skeleton');
  container.innerHTML = '';
  for (let i = 0; i < 12; i++) {
    container.innerHTML += `
      <div class="bg-slate-900 rounded-xl overflow-hidden border border-slate-800">
        <div class="skeleton h-48 w-full"></div>
        <div class="p-3 space-y-2">
          <div class="skeleton h-4 w-3/4 rounded"></div>
          <div class="skeleton h-3 w-1/2 rounded"></div>
          <div class="skeleton h-3 w-1/3 rounded"></div>
        </div>
      </div>
    `;
  }
  container.classList.remove('hidden');
}

function hideSearchSkeleton() {
  document.getElementById('search-skeleton').classList.add('hidden');
}

function setSortMode(mode) {
  state.sortMode = mode;
  document.querySelectorAll('.sort-btn').forEach(el => {
    const isActive = el.dataset.sort === mode;
    el.className = `sort-btn text-xs px-3 py-1 rounded-md transition-colors ${isActive ? 'bg-indigo-600 text-white' : 'bg-slate-800 text-slate-400 hover:text-white'}`;
  });
  renderSearchResults();
}

function sortResults(results) {
  const sorted = [...results];
  if (state.sortMode === 'seeders') {
    sorted.sort((a, b) => (b.seeders || 0) - (a.seeders || 0));
  } else if (state.sortMode === 'size') {
    sorted.sort((a, b) => parseSize(b.size || '') - parseSize(a.size || ''));
  }
  return sorted;
}

function parseSize(sizeStr) {
  if (!sizeStr) return 0;
  const s = sizeStr.toString().toUpperCase();
  const match = s.match(/([\d.]+)\s*(GB|MB|KB|B)?/);
  if (!match) return 0;
  const num = parseFloat(match[1]);
  const unit = match[2] || 'B';
  const multipliers = { B: 1, KB: 1024, MB: 1048576, GB: 1073741824 };
  return num * (multipliers[unit] || 1);
}

function renderSearchResults() {
  const container = document.getElementById('search-results');
  const sorted = sortResults(state.searchResults);
  state.renderedResults = sorted; // data-idx on cards indexes THIS (sorted) order
  container.innerHTML = sorted.map((r, i) => renderBookCard(r, i)).join('');
}

function renderBookCard(result, index) {
  const src = SOURCE_COLORS[result.source] || { bg: '#475569', text: 'white', label: result.source || 'Unknown' };
  const downloadKey = getDownloadKey(result);
  const isDownloading = state.pendingDownloads.has(downloadKey);
  const isTrackedAnnaDownload = hasTrackedAnnaDownload(downloadKey);
  const isTrackedDirectDownload = hasTrackedDirectDownload(downloadKey);
  const trackedJob = getTrackedDownloadJob(downloadKey);
  const downloadOutcome = state.downloadOutcomes.get(downloadKey);
  const coverHtml = result.cover_url
    ? `<img src="${escapeHtml(result.cover_url)}" alt="" class="w-full h-48 object-cover" loading="lazy" data-ph-title="${escapeHtml(result.title || '')}" data-ph-idx="${index}">`
    : makePlaceholderHtml(result.title || '?', index);

  // Ownership, resolved server-side against the library. Sits on the cover
  // rather than in the badge row so it is legible before reading the title —
  // the whole point is to stop a download that was about to happen.
  const ownedBadge = result.in_library
    ? `<span class="result-in-library absolute top-2 right-2 px-2 py-0.5 rounded text-xs font-medium bg-emerald-600 text-white" title="${escapeHtml(t('in_library_title', {title: result.library_title || result.title || ''}))}">${escapeHtml(t('in_library'))}</span>`
    : '';

  const seeders = result.seeders ? `<span class="text-emerald-400 text-xs font-medium">${t('n_seeds', {n: result.seeders})}</span>` : '';
  const leechers = result.leechers ? `<span class="text-amber-400 text-xs">${t('n_leech', {n: result.leechers})}</span>` : '';
  const sizeText = (result.size && result.size > 0) ? formatSize(result.size) : (result.size_human || result.sizeHuman || '');
  const size = sizeText ? `<span class="text-slate-400 text-xs font-medium">${escapeHtml(sizeText)}</span>` : '';
  const format = result.format ? `<span class="text-slate-500 text-xs uppercase">${escapeHtml(result.format)}</span>` : '';
  const indexer = result.indexer ? `<span class="text-slate-600 text-xs">${escapeHtml(result.indexer)}</span>` : '';
  // Edition metadata — what separates otherwise identical-looking hits.
  const language = result.language ? `<span class="result-language text-sky-400 text-xs uppercase font-medium">${escapeHtml(result.language)}</span>` : '';
  const year = result.year ? `<span class="result-year text-slate-500 text-xs">${escapeHtml(result.year)}</span>` : '';
  const publisher = result.publisher
    ? `<span class="result-publisher text-slate-500 text-xs truncate max-w-[10rem]" title="${escapeHtml(result.publisher)}">${escapeHtml(result.publisher)}</span>`
    : '';
  const copies = result.copies > 1
    ? `<span class="result-copies text-slate-500 text-xs" title="${escapeHtml(t('n_copies_title', {n: result.copies}))}">${escapeHtml(t('n_copies', {n: result.copies}))}</span>`
    : '';

  const buttonState = (isDownloading || isTrackedAnnaDownload || isTrackedDirectDownload) ? 'loading' : (downloadOutcome ? downloadOutcome.status : 'idle');
  const buttonStyles = {
    idle: 'bg-indigo-600 hover:bg-indigo-500 text-white',
    loading: 'bg-indigo-500/70 text-white cursor-not-allowed',
    success: 'bg-emerald-600 hover:bg-emerald-500 text-white cursor-default',
    error: 'bg-rose-600 hover:bg-rose-500 text-white cursor-pointer',
  };
  const buttonText = {
    idle: t('download'),
    loading: t('loading'),
    success: t('download_added'),
    error: t('download_failed_state'),
  };
  const buttonIcon = {
    idle: `<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>`,
    loading: `<svg class="w-4 h-4 spin" viewBox="0 0 24 24" fill="none" aria-hidden="true"><circle class="opacity-25" cx="12" cy="12" r="9" stroke="currentColor" stroke-width="2"></circle><path class="opacity-90" fill="currentColor" d="M12 3a9 9 0 0 1 9 9h-2.5A6.5 6.5 0 0 0 12 5.5V3z"></path></svg>`,
    success: `<svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/></svg>`,
    error: `<svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 8v4m0 4h.01M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z"/></svg>`,
  };
  // An owned book keeps a working button — a second edition is a legitimate
  // want — but the label has to say what the click will do, because the server
  // rejects the plain request and only honours an explicit override.
  if (result.in_library && buttonState === 'idle') {
    buttonStyles.idle = 'bg-amber-600 hover:bg-amber-500 text-white';
    buttonText.idle = t('download_anyway');
  }

  const displayButtonText = buttonState === 'loading' && trackedJob?.detail
    ? escapeHtml(trackedJob.detail)
    : (buttonText[buttonState] || buttonText.idle);

  return `
    <div class="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 hover:border-slate-600 flex flex-col">
      <div class="relative">
        ${coverHtml}
        <span class="absolute top-2 left-2 px-2 py-0.5 rounded text-xs font-medium" style="background:${src.bg};color:${src.text}">${escapeHtml(src.label)}</span>
        ${ownedBadge}
      </div>
      <div class="p-3 flex-1 flex flex-col">
        <h3 class="text-sm font-semibold text-white line-clamp-2 mb-1" title="${escapeHtml(result.title || '')}">${escapeHtml(result.title || 'Unknown')}</h3>
        <p class="text-xs text-slate-400 mb-2 line-clamp-1">${escapeHtml(result.author || '')}</p>
        <div class="flex items-center gap-2 flex-wrap mt-auto mb-2">
          ${seeders}${leechers}${size}${format}${language}${year}${publisher}${indexer}${copies}
        </div>
        <button
          data-action="startDownload" data-idx="${index}"
          ${buttonState === 'idle' || buttonState === 'error' ? '' : 'disabled aria-busy="true"'}
          class="w-full ${buttonStyles[buttonState] || buttonStyles.idle} text-white text-sm font-medium py-1.5 rounded-lg transition-colors flex items-center justify-center gap-1.5 disabled:opacity-100"
        >
          ${buttonIcon[buttonState] || buttonIcon.idle}
          <span class="truncate">${displayButtonText}</span>
        </button>
      </div>
    </div>
  `;
}

function makePlaceholderHtml(title, index) {
  const gradient = COVER_GRADIENTS[index % COVER_GRADIENTS.length];
  const letter = (title || '?').charAt(0).toUpperCase();
  return `<div class="w-full h-48 bg-gradient-to-br ${gradient} cover-placeholder">${escapeHtml(letter)}</div>`;
}

// Global function for img onerror fallback
window.makePlaceholder = function(title, index) {
  const gradient = COVER_GRADIENTS[index % COVER_GRADIENTS.length];
  const letter = (title || '?').charAt(0).toUpperCase();
  return `<div class="w-full h-48 bg-gradient-to-br ${gradient} cover-placeholder">${escapeHtml(letter)}</div>`;
};

function formatSize(size) {
  if (typeof size === 'string') return size;
  if (typeof size === 'number') {
    if (size > 1073741824) return (size / 1073741824).toFixed(1) + ' GB';
    if (size > 1048576) return (size / 1048576).toFixed(1) + ' MB';
    if (size > 1024) return (size / 1024).toFixed(1) + ' KB';
    return size + ' B';
  }
  return '';
}

function getDownloadKey(result) {
  return [
    result.source || '',
    result.download_url || '',
    result.url || '',
    result.abb_url || '',
    result.info_hash || '',
    result.magnet || '',
    result.md5 || '',
    result.source_id || '',
    result.title || '',
    result.author || '',
  ].join('|');
}

function hasTrackedAnnaDownload(downloadKey) {
  for (const tracked of state.trackedDownloadJobs.values()) {
    if (tracked.key === downloadKey && tracked.source === 'annas') return true;
  }
  return false;
}

function hasTrackedDirectDownload(downloadKey) {
  for (const tracked of state.trackedDownloadJobs.values()) {
    if (tracked.key === downloadKey && tracked.source !== 'annas') return true;
  }
  return false;
}

function getTrackedDownloadJob(downloadKey) {
  for (const [jobId, tracked] of state.trackedDownloadJobs.entries()) {
    if (tracked.key !== downloadKey) continue;
    const job = state.downloadJobs.find(candidate => String(candidate.job_id) === String(jobId));
    if (job) return job;
  }
  return null;
}

function setDownloadOutcome(downloadKey, status, persist = false) {
  const prevTimer = state.downloadOutcomeTimers.get(downloadKey);
  if (prevTimer) clearTimeout(prevTimer);

  state.downloadOutcomes.set(downloadKey, { status });
  renderSearchResults();

  if (persist) return;

  const timer = setTimeout(() => {
    state.downloadOutcomes.delete(downloadKey);
    state.downloadOutcomeTimers.delete(downloadKey);
    renderSearchResults();
  }, 2500);
  state.downloadOutcomeTimers.set(downloadKey, timer);
}

function isTerminalDownloadStatus(status) {
  return status === 'completed' || status === 'error' || status === 'dead_letter';
}

function isActiveDownloadStatus(status) {
  return status === 'queued' || status === 'searching' || status === 'downloading' || status === 'organizing' || status === 'importing' || status === 'retry_wait';
}

function trackDownloadJob(downloadKey, jobId, title, source, url = '') {
  if (!jobId) return;
  state.trackedDownloadJobs.set(String(jobId), { key: downloadKey, title, source, url });
  startDownloadPolling();
  refreshDownloads();
  renderSearchResults();
}

// ============================================================
// DOWNLOAD
// ============================================================
async function startDownload(result) {
  const downloadKey = getDownloadKey(result);
  if (state.pendingDownloads.has(downloadKey)) return;

  state.pendingDownloads.add(downloadKey);
  renderSearchResults();

  try {
    const body = {
      title: result.title,
      // Direct-download sources (Gutenberg, Standard Ebooks) carry their
      // link in epub_url — without this fallback their Download button 400s.
      download_url: result.download_url || result.url || result.epub_url || '',
      abb_url: result.abb_url || '',
      source: result.source,
      source_id: result.source_id || '',
      md5: result.md5 || '',
      author: result.author || '',
      info_hash: result.info_hash || '',
      magnet: result.magnet || '',
      // Preserve usenet-vs-torrent routing: Prowlarr proxy download URLs don't
      // match the backend's NZB URL heuristics, so without this a usenet grab
      // is misrouted to the torrent client and silently discarded.
      media_type: result.media_type || '',
      download_protocol: result.download_protocol || '',
      // The server refuses books already in the library. Sending force with a
      // result the user was shown as owned is the "Download anyway" click.
      force: !!result.in_library,
    };

    const resp = await api('/api/download', {
      method: 'POST',
      body: JSON.stringify(body),
    });
    let data = {};
    try {
      data = await resp.json();
    } catch {
      data = {};
    }

    // The library can gain the book between the search and the click, so a
    // result that was not flagged can still come back owned. Flag it now: the
    // re-render turns this button into "Download anyway".
    if (resp.status === 409 && data.in_library) {
      result.in_library = true;
      if (data.library_item_id) result.library_item_id = data.library_item_id;
      if (data.library_title) result.library_title = data.library_title;
      showToast(t('already_in_library', {title: data.library_title || result.title}), 'warning');
      return;
    }
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);

    if (result.source === 'annas' && data.job_id) {
      setDownloadOutcome(downloadKey, 'loading', true);
      trackAnnaDownload(data.job_id, downloadKey, result.title, result.download_url || result.url || '');
    } else if (data.success || data.job_id) {
      if (data.job_id) {
        setDownloadOutcome(downloadKey, 'loading', true);
        trackDownloadJob(downloadKey, data.job_id, result.title, result.source, result.download_url || result.url || '');
        showToast(t('download_started', {title: result.title}), 'info');
      } else {
        setDownloadOutcome(downloadKey, 'success');
        showToast(t('download_started', {title: result.title}), 'success');
      }
    } else {
      if (result.source === 'annas' && isAnnaNoMatchError(data.error || '')) {
        setDownloadOutcome(downloadKey, 'error', true);
        showAnnaNoMatchToast(result.title, result.download_url || result.url || '');
      } else {
        setDownloadOutcome(downloadKey, 'error');
        showToast(t('download_failed', {msg: data.error || t('unknown_error')}), 'error');
      }
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      if (result.source === 'annas' && isAnnaNoMatchError(err.message || '')) {
        setDownloadOutcome(downloadKey, 'error', true);
        showAnnaNoMatchToast(result.title, result.download_url || result.url || '');
      } else {
        setDownloadOutcome(downloadKey, 'error');
        showToast(t('download_failed', {msg: err.message}), 'error');
      }
    }
  } finally {
    state.pendingDownloads.delete(downloadKey);
    renderSearchResults();
  }
}

function setDownloadsRefreshLoading(loading) {
  const button = document.getElementById('downloads-refresh-btn');
  const icon = document.getElementById('downloads-refresh-icon');
  if (!button || !icon) return;

  button.disabled = loading;
  if (loading) {
    button.setAttribute('aria-busy', 'true');
    icon.classList.add('spin');
  } else {
    button.removeAttribute('aria-busy');
    icon.classList.remove('spin');
  }
}

async function refreshDownloads(manual = false) {
  if (manual) setDownloadsRefreshLoading(true);

  try {
    const data = await apiJson('/api/downloads');
    state.downloadJobs = data.downloads || data.jobs || [];
    syncTrackedDownloadJobs(state.downloadJobs);
    renderDownloadList();
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast(t('failed_load_downloads'), 'error');
    }
  } finally {
    if (manual) setDownloadsRefreshLoading(false);
  }
}

function renderDownloadList() {
  const jobs = state.downloadJobs || [];
  const container = document.getElementById('downloads-list');
  const emptyEl = document.getElementById('downloads-empty');

  // Update badge
  const activeCount = jobs.filter(j => isActiveDownloadStatus(j.status)).length;
  const badge = document.getElementById('dl-badge');
  if (activeCount > 0) {
    badge.textContent = activeCount;
    badge.classList.remove('hidden');
  } else {
    badge.classList.add('hidden');
  }

  if (jobs.length === 0) {
    container.innerHTML = '';
    emptyEl.classList.remove('hidden');
    return;
  }

  emptyEl.classList.add('hidden');
  container.innerHTML = jobs.map(renderDownloadJob).join('');
}

function renderDownloadJob(job) {
  const st = STATUS_STYLES[job.status] || STATUS_STYLES.queued;
  const progress = job.progress || 0;
  const showProgress = job.status === 'downloading' && progress > 0;
  const retryKey = String(job.job_id);
  const retryPending = state.pendingRetryDownloads.has(retryKey);

  let actions = '';
  if (job.status === 'error' || job.status === 'dead_letter') {
    actions = `
      <button
        data-action="retryDownload" data-job-id="${escapeHtml(job.job_id)}"
        ${retryPending ? 'disabled aria-busy="true"' : ''}
        class="px-2.5 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-slate-300 rounded transition-colors flex items-center gap-1 disabled:opacity-100"
      >
        ${retryPending ? '<svg class="w-3.5 h-3.5 spin" viewBox="0 0 24 24" fill="none" aria-hidden="true"><circle class="opacity-25" cx="12" cy="12" r="9" stroke="currentColor" stroke-width="2"></circle><path class="opacity-90" fill="currentColor" d="M12 3a9 9 0 0 1 9 9h-2.5A6.5 6.5 0 0 0 12 5.5V3z"></path></svg>' : ''}
        <span>${retryPending ? t('loading') : t('retry')}</span>
      </button>`;
  }

  return `
    <div class="bg-slate-900 border ${st.border} rounded-xl p-4 flex flex-col sm:flex-row sm:items-center gap-3">
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-2 mb-1">
          <span class="px-2 py-0.5 rounded text-xs font-medium ${st.bg} ${st.text}">${t('status_' + job.status)}</span>
          ${job.source ? `<span class="text-xs text-slate-500">${escapeHtml(job.source)}</span>` : ''}
        </div>
        <h4 class="text-sm font-medium text-white truncate" title="${escapeHtml(job.title || '')}">${escapeHtml(job.title || 'Unknown')}</h4>
        ${job.detail ? `<p class="text-xs text-slate-400 mt-1 truncate" title="${escapeHtml(job.detail)}">${escapeHtml(job.detail)}</p>` : ''}
        ${job.max_retries > 0 && job.retry_count > 0 ? `<p class="text-xs text-amber-400 mt-1">${escapeHtml(`Attempt ${Math.min(job.retry_count + 1, job.max_retries + 1)}/${job.max_retries + 1}`)}</p>` : ''}
        ${job.error ? `<p class="text-xs text-red-400 mt-1 truncate">${escapeHtml(job.error)}</p>` : ''}
        ${showProgress ? `
          <div class="mt-2 w-full bg-slate-800 rounded-full h-1.5">
            <div class="progress-bar bg-indigo-500 h-1.5 rounded-full" style="width:${progress}%"></div>
          </div>
          <span class="text-xs text-slate-500 mt-1">${progress.toFixed(1)}%</span>
        ` : ''}
      </div>
      <div class="flex items-center gap-2 flex-shrink-0">
        ${job.size ? `<span class="text-xs text-slate-500">${escapeHtml(formatSize(job.size))}</span>` : ''}
        ${actions}
      </div>
    </div>
  `;
}

function syncTrackedDownloadJobs(jobs) {
  const jobsById = new Map((jobs || []).map(job => [String(job.job_id), job]));
  let hasPendingTrackedJob = false;

  for (const [jobId, tracked] of [...state.trackedDownloadJobs.entries()]) {
    const job = jobsById.get(jobId);
    if (!job) {
      hasPendingTrackedJob = true;
      continue;
    }

    if (job.status === 'completed') {
      state.trackedDownloadJobs.delete(jobId);
      setDownloadOutcome(tracked.key, 'success');
      showToast(t('download_complete', {title: tracked.title || job.title || t('unknown_title')}), 'success');
      continue;
    }

    if (TERMINAL_DOWNLOAD_STATUSES.has(job.status)) {
      state.trackedDownloadJobs.delete(jobId);
      if (isAnnaNoMatchError(job.error || '')) {
        setDownloadOutcome(tracked.key, 'error', true);
        showAnnaNoMatchToast(tracked.title || job.title || 'Unknown', tracked.url || '');
      } else {
        setDownloadOutcome(tracked.key, 'error');
        showToast(t('download_failed', {msg: job.error || t('unknown_error')}), 'error');
      }
      continue;
    }

    hasPendingTrackedJob = true;
    Object.assign(tracked, {
      status: job.status,
      detail: job.detail || '',
      retryCount: job.retry_count || 0,
      maxRetries: job.max_retries || 0,
    });
    setDownloadOutcome(tracked.key, 'loading', true);
  }

  const hasActiveJob = (jobs || []).some(job => isActiveDownloadStatus(job.status));
  if (!hasPendingTrackedJob && state.trackedDownloadJobs.size === 0 && !hasActiveJob) {
    stopDownloadPolling();
  }
}

function trackAnnaDownload(jobId, downloadKey, title, url) {
  for (const [trackedJobId, tracked] of [...state.trackedDownloadJobs.entries()]) {
    if (tracked.key === downloadKey) {
      state.trackedDownloadJobs.delete(trackedJobId);
    }
  }
  state.trackedDownloadJobs.set(String(jobId), { key: downloadKey, title, url: url || '', source: 'annas' });
  startDownloadPolling();
  refreshDownloads();
}

async function retryDownload(jobId) {
  const key = String(jobId);
  if (state.pendingRetryDownloads.has(key)) return;

  state.pendingRetryDownloads.add(key);
  renderDownloadList();

  try {
    await apiJson(`/api/downloads/jobs/${jobId}/retry`, { method: 'POST' });
    showToast(t('retrying_download'), 'info');
    await refreshDownloads();
  } catch (err) {
    if (err.message !== 'Unauthorized') showToast(t('retry_failed'), 'error');
  } finally {
    state.pendingRetryDownloads.delete(key);
    renderDownloadList();
  }
}

async function clearCompleted() {
  const button = document.getElementById('downloads-clear-btn');
  const icon = document.getElementById('downloads-clear-icon');
  if (button && icon) {
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    icon.classList.remove('hidden');
  }

  try {
    await apiJson('/api/downloads/clear', { method: 'POST' });
    showToast(t('cleared_completed'), 'success');
    await refreshDownloads();
  } catch (err) {
    if (err.message !== 'Unauthorized') showToast(t('failed_clear'), 'error');
  } finally {
    if (button && icon) {
      button.disabled = false;
      button.removeAttribute('aria-busy');
      icon.classList.add('hidden');
    }
  }
}

function startDownloadPolling() {
  stopDownloadPolling();
  state.downloadPollTimer = setInterval(refreshDownloads, 5000);
}

function stopDownloadPolling() {
  if (state.downloadPollTimer) {
    clearInterval(state.downloadPollTimer);
    state.downloadPollTimer = null;
  }
}

// ============================================================
// LIBRARY
// ============================================================
let librarySearchTimeout = null;

document.getElementById('library-search').addEventListener('input', (e) => {
  clearTimeout(librarySearchTimeout);
  state.libraryPage = 1;
  librarySearchTimeout = setTimeout(() => loadLibrary(), 400);
});

async function loadLibrary() {
  const tab = state.libraryTab;
  const q = document.getElementById('library-search').value.trim();
  const page = state.libraryPage;

  const endpoints = {
    ebooks: `/api/library?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
    audiobooks: `/api/library/audiobooks?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
    manga: `/api/library/manga?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
  };

  const container = document.getElementById('library-results');
  const emptyEl = document.getElementById('library-empty');
  const paginationEl = document.getElementById('library-pagination');

  try {
    const data = await apiJson(endpoints[tab]);
    const items = data.items || [];
    state.libraryPages = data.pages || 1;

    if (items.length === 0) {
      container.innerHTML = '';
      emptyEl.classList.remove('hidden');
      paginationEl.classList.add('hidden');
      return;
    }

    emptyEl.classList.add('hidden');

    // Group items by series and render with section headers
    const renderFn = tab === 'ebooks' ? renderLibraryEbook : tab === 'audiobooks' ? renderLibraryAudiobook : renderLibraryManga;
    container.innerHTML = renderGroupedBySeries(items, renderFn);

    // Pagination
    if (state.libraryPages > 1) {
      paginationEl.classList.remove('hidden');
      renderPagination(paginationEl, page, state.libraryPages);
    } else {
      paginationEl.classList.add('hidden');
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      container.innerHTML = `<div class="col-span-full text-center py-10 text-slate-500">${t('failed_load_library')}</div>`;
    }
  }
}

function seriesBaseName(series) {
  // Strip "#N", "Book N", "(N)", "Vol N", "Volume N" suffixes to get the base series name
  if (!series) return '';
  return series
    .replace(/\s*#\s*[\d.]+.*$/, '')
    .replace(/\s*Book\s+[\d.]+.*$/i, '')
    .replace(/\s*Vol(ume)?\s*\.?\s*[\d.]+.*$/i, '')
    .replace(/\s*\([\d.]+\).*$/, '')
    .replace(/\s*,\s*$/, '')
    .trim();
}

function renderGroupedBySeries(items, renderFn) {
  const groups = new Map(); // base series name → items
  const standalone = [];
  let idx = 0;

  for (const item of items) {
    const raw = item.series || '';
    const base = seriesBaseName(raw);
    if (base) {
      if (!groups.has(base)) groups.set(base, []);
      groups.get(base).push(item);
    } else {
      standalone.push(item);
    }
  }

  let html = '';
  for (const [series, groupItems] of groups) {
    if (groupItems.length > 1) {
      // Series header
      html += `<div class="col-span-full mt-4 mb-2 first:mt-0">
        <div class="flex items-center gap-3">
          <h3 class="text-sm font-semibold text-indigo-400">${escapeHtml(series)}</h3>
          <span class="text-xs text-slate-600">${t('n_items', {n: groupItems.length})}</span>
          <div class="flex-1 border-t border-slate-800"></div>
        </div>
      </div>`;
    }
    for (const item of groupItems) {
      html += renderFn(item, idx++);
    }
    if (groupItems.length > 1) {
      // Spacer after series group
      html += `<div class="col-span-full h-2"></div>`;
    }
  }

  // Standalone items (no series)
  if (standalone.length > 0 && groups.size > 0) {
    html += `<div class="col-span-full mt-4 mb-2">
      <div class="flex items-center gap-3">
        <h3 class="text-sm font-semibold text-slate-500">${t('other')}</h3>
        <div class="flex-1 border-t border-slate-800"></div>
      </div>
    </div>`;
  }
  for (const item of standalone) {
    html += renderFn(item, idx++);
  }

  return html;
}

function renderLibraryEbook(item, index) {
  const coverHtml = item.cover_url
    ? `<img src="${escapeHtml(item.cover_url)}" alt="" class="w-full h-48 object-cover" loading="lazy" data-ph-title="${escapeHtml(item.title || '')}" data-ph-idx="${index}">`
    : makePlaceholderHtml(item.title || '?', index);

  const format = item.format || (item.file_path || '').split('.').pop() || '';

  const seriesBadge = item.series ? `<p class="text-xs text-indigo-400 line-clamp-1 mb-1">${escapeHtml(item.series)}</p>` : '';

  return `
    <div class="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 hover:border-slate-600 relative group">
      ${coverHtml}
      <button data-action="deleteLibraryItem" data-id="${escapeHtml(item.id || '')}" data-type="book" data-title="${escapeHtml(item.title || '')}"
        class="absolute top-2 right-2 w-7 h-7 bg-red-600/80 hover:bg-red-500 text-white rounded-full text-xs opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center" title="Remove from library">&#x2715;</button>
      <div class="p-3">
        <h3 class="text-sm font-semibold text-white line-clamp-2 mb-1">${escapeHtml(item.title || 'Unknown')}</h3>
        <p class="text-xs text-slate-400 line-clamp-1 mb-1">${escapeHtml(item.author || '')}</p>
        ${seriesBadge}
        <div class="flex items-center gap-2 text-xs text-slate-500">
          ${format ? `<span class="uppercase">${escapeHtml(format)}</span>` : ''}
          ${item.size ? `<span>${escapeHtml(formatSize(item.size))}</span>` : ''}
        </div>
      </div>
    </div>
  `;
}

function renderLibraryAudiobook(item, index) {
  const coverHtml = item.cover_url
    ? `<img src="${escapeHtml(item.cover_url)}" alt="" class="w-full h-48 object-cover" loading="lazy" data-ph-title="${escapeHtml(item.title || '')}" data-ph-idx="${index}">`
    : makePlaceholderHtml(item.title || '?', index);

  const duration = item.duration_hours ? `${item.duration_hours.toFixed(1)}h` : '';

  return `
    <div class="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 hover:border-slate-600 relative group">
      ${coverHtml}
      <button data-action="deleteLibraryItem" data-id="${escapeHtml(item.id || '')}" data-type="audiobook" data-title="${escapeHtml(item.title || '')}"
        class="absolute top-2 right-2 w-7 h-7 bg-red-600/80 hover:bg-red-500 text-white rounded-full text-xs opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center" title="Remove from library">&#x2715;</button>
      <div class="p-3">
        <h3 class="text-sm font-semibold text-white line-clamp-2 mb-1">${escapeHtml(item.title || 'Unknown')}</h3>
        <p class="text-xs text-slate-400 line-clamp-1 mb-1">${escapeHtml(item.author || '')}</p>
        ${item.series ? `<p class="text-xs text-indigo-400 line-clamp-1 mb-1">${escapeHtml(item.series)}</p>` : ''}
        <div class="flex items-center gap-2 text-xs text-slate-500 mb-2">
          ${duration ? `<span>${duration}</span>` : ''}
          ${item.num_files ? `<span>${t('n_files', {n: item.num_files})}</span>` : ''}
        </div>
        ${item.abs_url ? `<a href="${escapeHtml(item.abs_url)}" target="_blank" class="text-xs text-indigo-400 hover:text-indigo-300 transition-colors">${t('open_in_abs')}</a>` : ''}
      </div>
    </div>
  `;
}

function renderLibraryManga(item, index) {
  const coverHtml = item.cover_url
    ? `<img src="${escapeHtml(item.cover_url)}" alt="" class="w-full h-48 object-cover" loading="lazy" data-ph-title="${escapeHtml(item.name || '')}" data-ph-idx="${index}">`
    : makePlaceholderHtml(item.name || '?', index);

  return `
    <div class="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 hover:border-slate-600">
      ${coverHtml}
      <div class="p-3">
        <h3 class="text-sm font-semibold text-white line-clamp-2 mb-1">${escapeHtml(item.name || 'Unknown')}</h3>
        <div class="flex items-center gap-2 text-xs text-slate-500 mb-2">
          ${item.pages ? `<span>${t('n_pages', {n: item.pages})}</span>` : ''}
          ${item.library ? `<span>${escapeHtml(item.library)}</span>` : ''}
        </div>
        ${item.kavita_url ? `<a href="${escapeHtml(item.kavita_url)}" target="_blank" class="text-xs text-indigo-400 hover:text-indigo-300 transition-colors">${t('open_in_kavita')}</a>` : ''}
      </div>
    </div>
  `;
}

async function deleteLibraryItem(id, type, title) {
  if (!id) return;
  if (!confirm(`Remove "${title}" from library?`)) return;
  try {
    await apiJson(`/api/library/${type}/${id}`, { method: 'DELETE' });
    showToast(`Removed "${title}"`, 'success');
    loadLibrary();
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast(`Failed to remove: ${err.message}`, 'error');
    }
  }
}

function renderPagination(container, currentPage, totalPages) {
  let html = '';

  // Previous
  html += `<button data-action="goLibraryPage" data-page="${currentPage - 1}" class="px-3 py-1.5 text-sm rounded-lg ${currentPage <= 1 ? 'bg-slate-800 text-slate-600 cursor-not-allowed' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}" ${currentPage <= 1 ? 'disabled' : ''}>${t('prev')}</button>`;

  // Page numbers
  const maxVisible = 7;
  let start = Math.max(1, currentPage - Math.floor(maxVisible / 2));
  let end = Math.min(totalPages, start + maxVisible - 1);
  if (end - start < maxVisible - 1) start = Math.max(1, end - maxVisible + 1);

  if (start > 1) {
    html += `<button data-action="goLibraryPage" data-page="1" class="px-3 py-1.5 text-sm rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300">1</button>`;
    if (start > 2) html += `<span class="text-slate-600 px-1">...</span>`;
  }

  for (let i = start; i <= end; i++) {
    html += `<button data-action="goLibraryPage" data-page="${i}" class="px-3 py-1.5 text-sm rounded-lg ${i === currentPage ? 'bg-indigo-600 text-white' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}">${i}</button>`;
  }

  if (end < totalPages) {
    if (end < totalPages - 1) html += `<span class="text-slate-600 px-1">...</span>`;
    html += `<button data-action="goLibraryPage" data-page="${totalPages}" class="px-3 py-1.5 text-sm rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300">${totalPages}</button>`;
  }

  // Next
  html += `<button data-action="goLibraryPage" data-page="${currentPage + 1}" class="px-3 py-1.5 text-sm rounded-lg ${currentPage >= totalPages ? 'bg-slate-800 text-slate-600 cursor-not-allowed' : 'bg-slate-800 hover:bg-slate-700 text-slate-300'}" ${currentPage >= totalPages ? 'disabled' : ''}>${t('next')}</button>`;

  container.innerHTML = html;
}

function goLibraryPage(page) {
  if (page < 1 || page > state.libraryPages) return;
  state.libraryPage = page;
  loadLibrary();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ============================================================
// WISHLIST
// ============================================================
async function loadWishlist() {
  try {
    const data = await apiJson('/api/wishlist');
    const items = data.items || [];
    const container = document.getElementById('wishlist-list');
    const emptyEl = document.getElementById('wishlist-empty');

    if (items.length === 0) {
      container.innerHTML = '';
      emptyEl.classList.remove('hidden');
      return;
    }

    emptyEl.classList.add('hidden');
    container.innerHTML = items.map(renderWishlistItem).join('');
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast(t('failed_load_wishlist'), 'error');
    }
  }
}

function renderWishlistItem(item) {
  const typeColors = { ebook: 'bg-indigo-500/20 text-indigo-400', audiobook: 'bg-purple-500/20 text-purple-400', manga: 'bg-pink-500/20 text-pink-400' };
  const tc = typeColors[item.media_type] || typeColors.ebook;
  const date = item.created_at ? new Date(item.created_at).toLocaleDateString() : '';

  return `
    <div class="bg-slate-900 border border-slate-800 rounded-xl px-4 py-3 flex items-center gap-4">
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-2 mb-0.5">
          <span class="px-2 py-0.5 rounded text-xs font-medium ${tc}">${escapeHtml(item.media_type || 'ebook')}</span>
          ${date ? `<span class="text-xs text-slate-600">${date}</span>` : ''}
        </div>
        <h4 class="text-sm font-medium text-white truncate">${escapeHtml(item.title || '')}</h4>
        ${item.author ? `<p class="text-xs text-slate-400">${escapeHtml(item.author)}</p>` : ''}
      </div>
      <div class="flex items-center gap-2 flex-shrink-0">
        <button data-action="searchWishlistItem" data-title="${escapeHtml(item.title)}" data-media-type="${escapeHtml(item.media_type || 'ebook')}" class="px-2.5 py-1 text-xs bg-indigo-600 hover:bg-indigo-500 text-white rounded transition-colors" title="${t('wishlist_search')}">${t('wishlist_search')}</button>
        <button data-action="deleteWishlistItem" data-id="${item.id}" class="px-2.5 py-1 text-xs bg-slate-700 hover:bg-red-600 text-slate-300 hover:text-white rounded transition-colors" title="Remove">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
        </button>
      </div>
    </div>
  `;
}

function showWishlistForm() {
  document.getElementById('wishlist-form').classList.remove('hidden');
  document.getElementById('wl-title').focus();
}

function hideWishlistForm() {
  document.getElementById('wishlist-form').classList.add('hidden');
  document.getElementById('wl-title').value = '';
  document.getElementById('wl-author').value = '';
}

async function addWishlistItem() {
  const title = document.getElementById('wl-title').value.trim();
  const author = document.getElementById('wl-author').value.trim();
  const mediaType = document.getElementById('wl-type').value;

  if (!title) {
    showToast(t('err_title_required'), 'warning');
    return;
  }

  try {
    await apiJson('/api/wishlist', {
      method: 'POST',
      body: JSON.stringify({ title, author, media_type: mediaType }),
    });
    showToast(t('added_to_wishlist'), 'success');
    hideWishlistForm();
    loadWishlist();
  } catch (err) {
    if (err.message !== 'Unauthorized') showToast(t('failed_add_wishlist'), 'error');
  }
}

async function deleteWishlistItem(id) {
  try {
    await api(`/api/wishlist/${id}`, { method: 'DELETE' });
    showToast(t('removed_from_wishlist'), 'success');
    loadWishlist();
  } catch (err) {
    if (err.message !== 'Unauthorized') showToast(t('failed_delete'), 'error');
  }
}

function searchWishlistItem(title, mediaType) {
  const tabMap = { ebook: 'ebooks', audiobook: 'audiobooks', manga: 'manga' };
  switchTab('search');
  switchSearchTab(tabMap[mediaType] || 'ebooks');
  document.getElementById('search-input').value = title;
  doSearch(title);
}

// ============================================================
// SETTINGS
// ============================================================
async function loadSettings() {
  loadConfig();
  loadSources();
  loadTOTPStatus();
  loadSettingToggles();
  showChangePasswordIfMultiUser();
  if (state.currentRole === 'admin') {
    loadUsers();
    loadInviteCodes();
  }
}

// Show the self-service change-password card only when the user is logged in
// against a DB account. Env-credential single-user installs can't change pw
// here — those creds are sourced from AUTH_USERNAME / AUTH_PASSWORD.
async function showChangePasswordIfMultiUser() {
  try {
    const data = await apiJson('/api/auth/status');
    if (data.authenticated && data.user_id) {
      document.getElementById('change-password-section').classList.remove('hidden');
    }
  } catch (err) {}
}

// Guard with optional chaining: the change-password card is only present for
// DB-backed accounts, so this element is absent on env-credential / no-auth
// installs. Without the guard, the null deref threw at the top level and
// aborted the rest of this script — leaving later `const` declarations (e.g.
// INTEGRATION_FIELDS) uninitialized and breaking the integration Save button.
document.getElementById('change-password-form')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const errEl = document.getElementById('cp-error');
  errEl.classList.add('hidden');

  const current = document.getElementById('cp-current').value;
  const next = document.getElementById('cp-new').value;
  const confirm = document.getElementById('cp-confirm').value;

  if (!current || !next || !confirm) {
    errEl.textContent = 'All three fields are required';
    errEl.classList.remove('hidden');
    return;
  }
  if (next.length < 6) {
    errEl.textContent = 'New password must be at least 6 characters';
    errEl.classList.remove('hidden');
    return;
  }
  if (next !== confirm) {
    errEl.textContent = 'New password and confirmation do not match';
    errEl.classList.remove('hidden');
    return;
  }

  try {
    const res = await apiJson('/api/me/password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ current_password: current, new_password: next }),
    });
    if (res.success) {
      document.getElementById('cp-current').value = '';
      document.getElementById('cp-new').value = '';
      document.getElementById('cp-confirm').value = '';
      showToast('Password updated', 'success');
    } else {
      errEl.textContent = res.error || 'Failed to update password';
      errEl.classList.remove('hidden');
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      errEl.textContent = 'Failed to update password';
      errEl.classList.remove('hidden');
    }
  }
});

// Settings keys backed by an input in the Integrations section, grouped by the
// integration name passed to saveIntegration().
const INTEGRATION_FIELDS = {
  annas:          ['annas_archive_domain', 'annas_archive_secret_key'],
  prowlarr:       ['prowlarr_url', 'prowlarr_api_key'],
  qbittorrent:    ['qb_url', 'qb_user', 'qb_pass'],
  transmission:   ['transmission_url', 'transmission_user', 'transmission_pass', 'torrent_client'],
  sabnzbd:        ['sabnzbd_url', 'sabnzbd_api_key', 'sabnzbd_category'],
  audiobookshelf: ['abs_url', 'abs_token'],
  kavita:         ['kavita_url', 'kavita_user', 'kavita_pass', 'kavita_ebook_library_id', 'kavita_manga_library_id'],
  komga:          ['komga_url', 'komga_user', 'komga_pass'],
  calibre:        ['calibre_url', 'calibre_library_path'],
};

async function loadSettingToggles() {
  try {
    const data = await apiJson('/api/settings');
    const removeTorrent = document.getElementById('remove-torrent-toggle');
    if (removeTorrent && data.remove_torrent_after_import !== undefined) {
      removeTorrent.checked = data.remove_torrent_after_import;
    }
    const langFilter = document.getElementById('foreign-lang-filter-toggle');
    if (langFilter && data.foreign_lang_filter !== undefined) {
      langFilter.checked = data.foreign_lang_filter;
    }
    // Populate integration inputs. Sensitive values come back as "--------"
    // from the server; the save handler drops that sentinel before writing.
    for (const fields of Object.values(INTEGRATION_FIELDS)) {
      for (const key of fields) {
        const el = document.getElementById(`setting-${key}`);
        if (el && data[key] !== undefined && data[key] !== null) {
          el.value = data[key];
        }
      }
    }
  } catch (err) {}
}

async function saveIntegration(name) {
  const fields = INTEGRATION_FIELDS[name];
  if (!fields) return;
  const payload = {};
  for (const key of fields) {
    const el = document.getElementById(`setting-${key}`);
    if (!el) continue;
    payload[key] = el.value;
  }
  try {
    const res = await apiJson('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (res.success) {
      showToast('Saved. Restart the container for new URLs/credentials to take effect.', 'success');
    } else {
      showToast(res.error || 'Failed to save', 'error');
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast('Failed to save', 'error');
    }
  }
}

async function loadConfig() {
  try {
    const data = await apiJson('/api/config');
    state.config = data;

    const configEl = document.getElementById('config-info');
    const items = [];

    if (data.prowlarr) items.push(configItem('Prowlarr', data.prowlarr.url || t('not_configured')));
    if (data.qbittorrent) items.push(configItem('qBittorrent', data.qbittorrent.url || t('not_configured')));
    if (data.transmission) items.push(configItem('Transmission', data.transmission.url || t('not_configured')));
    if (data.sabnzbd) items.push(configItem('SABnzbd', data.sabnzbd.url || t('not_configured')));
    if (data.kavita_url) items.push(configItem('Kavita', data.kavita_url));
    if (data.audiobookshelf_url) items.push(configItem('Audiobookshelf', data.audiobookshelf_url));

    configEl.innerHTML = items.length > 0 ? items.join('') : `<p class="text-slate-500">${t('no_config_data')}</p>`;
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      document.getElementById('config-info').innerHTML = `<p class="text-red-400">${t('failed_load_config')}</p>`;
    }
  }
}

function configItem(label, value) {
  return `
    <div class="flex items-center justify-between py-2 border-b border-slate-800 last:border-0">
      <span class="text-slate-300">${escapeHtml(label)}</span>
      <span class="text-slate-500 text-xs truncate max-w-[60%] text-right">${escapeHtml(value)}</span>
    </div>
  `;
}

async function loadSources() {
  const container = document.getElementById('sources-list');
  const loadingEl = document.getElementById('sources-loading');

  try {
    const data = await apiJson('/api/sources');
    const sources = Array.isArray(data) ? data : (data.sources || []);
    loadingEl.classList.add('hidden');

    if (sources.length === 0) {
      container.innerHTML = `<p class="text-slate-500 text-sm">${t('no_sources')}</p>`;
      return;
    }

    container.innerHTML = sources.map(s => {
      const enabled = s.enabled !== false;
      const tabLabel = s.search_tab || s.download_type || '';
      return `
        <div class="flex items-center justify-between bg-slate-800/50 rounded-lg px-4 py-2.5">
          <div class="flex items-center gap-3">
            <div class="w-2 h-2 rounded-full ${enabled ? 'bg-emerald-400' : 'bg-slate-600'}"></div>
            <span class="text-sm text-slate-300">${escapeHtml(s.label || s.name || 'Unknown')}</span>
            ${tabLabel ? `<span class="text-xs text-slate-600">${escapeHtml(tabLabel)}</span>` : ''}
          </div>
          <span class="text-xs ${enabled ? 'text-emerald-400' : 'text-slate-500'}">${enabled ? t('enabled') : t('disabled')}</span>
        </div>
      `;
    }).join('');
  } catch (err) {
    loadingEl.classList.add('hidden');
    if (err.message !== 'Unauthorized') {
      container.innerHTML = `<p class="text-red-400 text-sm">${t('failed_load_sources')}</p>`;
    }
  }
}

async function toggleForeignLangFilter() {
  const toggle = document.getElementById('foreign-lang-filter-toggle');
  if (!toggle) return;
  const enabled = toggle.checked;
  try {
    const res = await apiJson('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ foreign_lang_filter: enabled })
    });
    if (res.success) {
      showToast(enabled ? t('filter_enabled_toast') : t('filter_disabled_toast'), 'success');
    } else {
      toggle.checked = !enabled;
      showToast(t('filter_update_failed'), 'error');
    }
  } catch (err) {
    toggle.checked = !enabled;
    if (err.message !== 'Unauthorized') {
      showToast(t('filter_save_failed'), 'error');
    }
  }
}

async function toggleRemoveTorrent() {
  const toggle = document.getElementById('remove-torrent-toggle');
  if (!toggle) return;
  const enabled = toggle.checked;
  try {
    const res = await apiJson('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ remove_torrent_after_import: enabled })
    });
    if (res.success) {
      showToast(enabled ? 'Torrents will be removed after import' : 'Torrents will keep seeding after import', 'success');
    } else {
      toggle.checked = !enabled;
      showToast('Failed to update setting', 'error');
    }
  } catch (err) {
    toggle.checked = !enabled;
    if (err.message !== 'Unauthorized') {
      showToast('Failed to save setting', 'error');
    }
  }
}

async function testConnection(service) {
  const statusEl = document.getElementById(`test-${service}-status`);
  statusEl.textContent = t('testing');
  statusEl.className = 'text-xs text-yellow-400';

  try {
    const data = await apiJson(`/api/test/${service}`, { method: 'POST' });
    if (data.success) {
      statusEl.textContent = t('connected');
      statusEl.className = 'text-xs text-emerald-400';
    } else {
      statusEl.textContent = data.error || t('conn_error');
      statusEl.className = 'text-xs text-red-400';
    }
  } catch (err) {
    statusEl.textContent = t('conn_error');
    statusEl.className = 'text-xs text-red-400';
  }
}

// ============================================================
// TOTP SETTINGS
// ============================================================
async function loadTOTPStatus() {
  const section = document.getElementById('totp-settings');
  // The TOTP settings markup is not rendered in every build/mode — without
  // this guard the Settings tab throws an uncaught TypeError on null.
  if (!section) return;
  try {
    const data = await apiJson('/api/totp/status');
    section.classList.remove('hidden');
    if (data.enabled) {
      document.getElementById('totp-disabled-section').classList.add('hidden');
      document.getElementById('totp-enabled-section').classList.remove('hidden');
    } else {
      document.getElementById('totp-disabled-section').classList.remove('hidden');
      document.getElementById('totp-enabled-section').classList.add('hidden');
    }
    document.getElementById('totp-setup-section').classList.add('hidden');
    document.getElementById('totp-disable-section').classList.add('hidden');
  } catch (err) {
    // Not in multi-user mode — hide TOTP settings.
    section.classList.add('hidden');
  }
}

async function setupTOTP() {
  try {
    const data = await apiJson('/api/totp/setup', { method: 'POST' });
    if (!data.success) {
      showToast(data.error || t('failed_setup_totp'), 'error');
      return;
    }
    document.getElementById('totp-disabled-section').classList.add('hidden');
    document.getElementById('totp-setup-section').classList.remove('hidden');
    document.getElementById('totp-secret-display').textContent = data.secret;

    // Generate QR code using a QR API.
    const qrUrl = 'https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=' + encodeURIComponent(data.qr_url);
    document.getElementById('totp-qr-img').src = qrUrl;

    // Display backup codes.
    const codesEl = document.getElementById('totp-backup-codes');
    codesEl.innerHTML = data.backup_codes.map(c => `<span class="bg-slate-700 px-2 py-1 rounded text-center">${escapeHtml(c)}</span>`).join('');
  } catch (err) {
    showToast(t('failed_setup_totp'), 'error');
  }
}

async function verifyTOTP() {
  const code = document.getElementById('totp-verify-code').value.trim();
  if (!code) {
    showToast(t('enter_6digit_code'), 'warning');
    return;
  }
  try {
    const data = await apiJson('/api/totp/verify', {
      method: 'POST',
      body: JSON.stringify({ code }),
    });
    if (data.success) {
      showToast(t('totp_enabled_success'), 'success');
      loadTOTPStatus();
    } else {
      showToast(data.error || t('err_invalid_code'), 'error');
    }
  } catch (err) {
    showToast(t('verification_failed'), 'error');
  }
}

function cancelTOTPSetup() {
  document.getElementById('totp-setup-section').classList.add('hidden');
  document.getElementById('totp-disabled-section').classList.remove('hidden');
}

function showDisableTOTP() {
  document.getElementById('totp-enabled-section').classList.add('hidden');
  document.getElementById('totp-disable-section').classList.remove('hidden');
  document.getElementById('totp-disable-code').focus();
}

function cancelDisableTOTP() {
  document.getElementById('totp-disable-section').classList.add('hidden');
  document.getElementById('totp-enabled-section').classList.remove('hidden');
}

async function disableTOTP() {
  const code = document.getElementById('totp-disable-code').value.trim();
  if (!code) {
    showToast(t('enter_totp_code'), 'warning');
    return;
  }
  try {
    const data = await apiJson('/api/totp/disable', {
      method: 'POST',
      body: JSON.stringify({ code }),
    });
    if (data.success) {
      showToast(t('totp_disabled_success'), 'success');
      loadTOTPStatus();
    } else {
      showToast(data.error || t('err_invalid_code'), 'error');
    }
  } catch (err) {
    showToast(t('failed_disable_totp'), 'error');
  }
}

// ============================================================
// USER MANAGEMENT (ADMIN)
// ============================================================
async function loadUsers() {
  const section = document.getElementById('user-management');
  try {
    const data = await apiJson('/api/users');
    if (!data.success) return;
    section.classList.remove('hidden');

    const container = document.getElementById('users-list');
    const users = data.users || [];
    container.innerHTML = users.map(u => {
      const roleOptions = `<select data-action-change="changeUserRole" data-id="${u.id}" class="bg-slate-700 text-sm text-slate-300 rounded px-2 py-1 border-0">
        <option value="user" ${u.role === 'user' ? 'selected' : ''}>${t('role_user')}</option>
        <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>${t('role_admin')}</option>
      </select>`;
      const totpBadge = u.totp_enabled ? '<span class="text-xs text-emerald-400 bg-emerald-400/10 px-1.5 py-0.5 rounded">2FA</span>' : '';
      const lastLogin = u.last_login ? new Date(u.last_login).toLocaleDateString() : t('never');
      return `
        <div class="flex items-center justify-between bg-slate-800/50 rounded-lg px-4 py-3">
          <div class="flex items-center gap-3">
            <span class="text-sm font-medium text-white">${escapeHtml(u.username)}</span>
            ${totpBadge}
            <span class="text-xs text-slate-600">${t('last_login', {date: lastLogin})}</span>
          </div>
          <div class="flex items-center gap-2">
            ${roleOptions}
            <button data-action="deleteUser" data-id="${u.id}" data-username="${escapeHtml(u.username)}" class="px-2 py-1 text-xs bg-slate-700 hover:bg-red-600 text-slate-400 hover:text-white rounded transition-colors" title="${t('failed_delete_user')}">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
            </button>
          </div>
        </div>
      `;
    }).join('');
  } catch (err) {
    // Not admin or multi-user not active — hide.
    section.classList.add('hidden');
  }
}

async function changeUserRole(id, role) {
  try {
    const data = await apiJson(`/api/users/${id}`, {
      method: 'PATCH',
      body: JSON.stringify({ role }),
    });
    if (data.success) {
      showToast(t('user_role_updated'), 'success');
    } else {
      showToast(data.error || t('failed_update_role'), 'error');
      loadUsers();
    }
  } catch (err) {
    showToast(t('failed_update_role'), 'error');
    loadUsers();
  }
}

async function deleteUser(id, username) {
  if (!confirm(t('confirm_delete_user', {username: username}))) return;
  try {
    const data = await apiJson(`/api/users/${id}`, { method: 'DELETE' });
    if (data.success) {
      showToast(t('user_deleted'), 'success');
      loadUsers();
    } else {
      showToast(data.error || t('failed_delete_user'), 'error');
    }
  } catch (err) {
    showToast(t('failed_delete_user'), 'error');
  }
}

async function addUser() {
  const username = document.getElementById('new-user-name').value.trim();
  const password = document.getElementById('new-user-pass').value;
  if (!username || !password) {
    showToast(t('err_credentials_required'), 'warning');
    return;
  }
  try {
    const data = await apiJson('/api/register', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    });
    if (data.success) {
      showToast(t('user_created'), 'success');
      document.getElementById('new-user-name').value = '';
      document.getElementById('new-user-pass').value = '';
      loadUsers();
    } else {
      showToast(data.error || t('failed_create_user'), 'error');
    }
  } catch (err) {
    showToast(t('failed_create_user'), 'error');
  }
}

// ============================================================
// INVITE CODES (admin)
// ============================================================
async function loadInviteCodes() {
  try {
    const data = await apiJson('/api/invites');
    const list = data.invites || [];
    const el = document.getElementById('invite-codes-list');
    if (list.length === 0) {
      el.innerHTML = `<p class="text-xs text-slate-500">No invite codes yet.</p>`;
      return;
    }
    el.innerHTML = list.map(inv => {
      const expires = inv.expires_at
        ? new Date(inv.expires_at * 1000).toLocaleDateString()
        : 'Never';
      const usesLabel = `${inv.uses} / ${inv.max_uses}`;
      const exhausted = inv.uses >= inv.max_uses;
      return `
        <div class="flex items-center justify-between bg-slate-800/50 rounded-lg px-3 py-2 text-sm">
          <div class="flex items-center gap-3 flex-1 min-w-0">
            <code class="text-indigo-400 font-mono text-xs truncate">${escapeHtml(inv.code)}</code>
            <span class="text-xs text-slate-500">role: ${escapeHtml(inv.role)}</span>
            <span class="text-xs ${exhausted ? 'text-amber-400' : 'text-slate-500'}">uses: ${usesLabel}</span>
            <span class="text-xs text-slate-500">expires: ${escapeHtml(expires)}</span>
          </div>
          <div class="flex items-center gap-1 flex-shrink-0">
            <button data-action="copyInviteCode" data-code="${escapeHtml(inv.code)}" class="px-2 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-slate-300 rounded transition-colors" title="Copy">Copy</button>
            <button data-action="revokeInviteCode" data-id="${inv.id}" class="px-2 py-1 text-xs bg-red-700 hover:bg-red-600 text-white rounded transition-colors" title="Revoke">Revoke</button>
          </div>
        </div>`;
    }).join('');
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      document.getElementById('invite-codes-list').innerHTML = `<p class="text-xs text-red-400">Failed to load invite codes</p>`;
    }
  }
}

async function generateInviteCode() {
  const role = document.getElementById('invite-role').value;
  const maxUses = parseInt(document.getElementById('invite-max-uses').value, 10) || 1;
  const expiresDays = parseInt(document.getElementById('invite-expires-days').value, 10) || 0;
  const expiresIn = expiresDays > 0 ? expiresDays * 86400 : 0;
  try {
    const res = await apiJson('/api/invites', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role, max_uses: maxUses, expires_in: expiresIn }),
    });
    if (res.success) {
      showToast('Invite code generated', 'success');
      loadInviteCodes();
    } else {
      showToast(res.error || 'Failed to generate', 'error');
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast('Failed to generate invite', 'error');
    }
  }
}

async function copyInviteCode(code) {
  try {
    await navigator.clipboard.writeText(code);
    showToast('Copied', 'success');
  } catch (err) {
    showToast('Copy failed — select the code manually', 'warning');
  }
}

async function revokeInviteCode(id) {
  if (!confirm('Revoke this invite code? Anyone with it will no longer be able to register.')) return;
  try {
    const res = await apiJson(`/api/invites/${id}`, { method: 'DELETE' });
    if (res.success) {
      showToast('Invite revoked', 'success');
      loadInviteCodes();
    } else {
      showToast(res.error || 'Failed to revoke', 'error');
    }
  } catch (err) {
    if (err.message !== 'Unauthorized') {
      showToast('Failed to revoke', 'error');
    }
  }
}

// ============================================================
// STATS
// ============================================================
async function loadStats() {
  try {
    const data = await apiJson('/api/stats');
    const statsEl = document.getElementById('header-stats');
    if (data.total_items !== undefined) {
      statsEl.textContent = t('n_items_in_library', {n: data.total_items});
      statsEl.classList.remove('hidden');
    }
  } catch (err) {
    // Silently ignore
  }
}

// ============================================================
// UTILITIES
// ============================================================
function escapeHtml(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.textContent = str;
  // textContent -> innerHTML escapes & < >, but not quotes, and most callers
  // interpolate into a quoted attribute (title=, alt=). A value containing a
  // quote would close the attribute early and drop the rest of the text.
  return div.innerHTML.replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

function escapeJs(str) {
  if (!str) return '';
  return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"').replace(/\n/g, '\\n');
}

// ============================================================
// KEYBOARD SHORTCUTS
// ============================================================
document.addEventListener('keydown', (e) => {
  // Ctrl/Cmd + K → focus search
  if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
    e.preventDefault();
    switchTab('search');
    document.getElementById('search-input').focus();
  }

  // Escape → close modals, clear search
  if (e.key === 'Escape') {
    hideWishlistForm();
  }
});

// ============================================================
// INIT
// ============================================================
async function init() {
  try {
    // Test auth by hitting config
    const cfg = await apiJson('/api/config');
    state.config = cfg;

    // Update user header.
    if (cfg.current_user) {
      updateUserHeader(cfg.current_user, cfg.current_role);
    }

    // Auth OK
    loadStats();
    // Show default tab
    document.getElementById('search-empty').classList.remove('hidden');
    // Apply language on load
    applyLanguage();
  } catch (err) {
    if (err.message === 'Unauthorized') {
      // Login modal already shown by api()
    }
  }
}

// Start
init();

// ============================================================
// EVENT DELEGATION
// ============================================================
// Replaces all inline on*= attributes so the UI runs under a strict
// Content-Security-Policy (script-src 'self'). Markup declares intent via
// data-action="..."; this explicit whitelist maps it to code — markup can
// never invoke anything that isn't registered here.
const CLICK_ACTIONS = {
  switchTab: el => switchTab(el.dataset.arg),
  switchSearchTab: el => switchSearchTab(el.dataset.arg),
  switchLibraryTab: el => switchLibraryTab(el.dataset.arg),
  setSortMode: el => setSortMode(el.dataset.arg),
  testConnection: el => testConnection(el.dataset.arg),
  saveIntegration: el => saveIntegration(el.dataset.arg),
  toggleMobileNav: () => toggleMobileNav(),
  toggleLanguage: () => toggleLanguage(),
  showLoginForm: () => showLoginForm(),
  showRegisterForm: () => showRegisterForm(),
  showWishlistForm: () => showWishlistForm(),
  hideWishlistForm: () => hideWishlistForm(),
  addWishlistItem: () => addWishlistItem(),
  generateInviteCode: () => generateInviteCode(),
  doLogout: () => doLogout(),
  clearCompleted: () => clearCompleted(),
  addUser: () => addUser(),
  refreshDownloads: () => refreshDownloads(true), // toolbar button forces a refresh
  // Dynamically rendered rows/cards:
  startDownload: el => {
    // data-idx indexes the *rendered* (sorted) list, not state.searchResults.
    const r = (state.renderedResults || [])[+el.dataset.idx];
    if (r) startDownload(r);
  },
  retryDownload: el => retryDownload(el.dataset.jobId),
  deleteLibraryItem: el => deleteLibraryItem(el.dataset.id, el.dataset.type, el.dataset.title),
  goLibraryPage: el => goLibraryPage(+el.dataset.page),
  searchWishlistItem: el => searchWishlistItem(el.dataset.title, el.dataset.mediaType),
  deleteWishlistItem: el => deleteWishlistItem(+el.dataset.id),
  deleteUser: el => deleteUser(+el.dataset.id, el.dataset.username),
  copyInviteCode: el => copyInviteCode(el.dataset.code),
  revokeInviteCode: el => revokeInviteCode(+el.dataset.id),
};

document.addEventListener('click', e => {
  const el = e.target.closest('[data-action]');
  if (!el) return;
  const fn = CLICK_ACTIONS[el.dataset.action];
  if (!fn) return;
  // Anchors previously used inline `return false` — keep them from navigating.
  if (el.tagName === 'A') e.preventDefault();
  fn(el, e);
});

const CHANGE_ACTIONS = {
  changeUserRole: el => changeUserRole(+el.dataset.id, el.value),
  toggleForeignLangFilter: () => toggleForeignLangFilter(),
  toggleRemoveTorrent: () => toggleRemoveTorrent(),
};

document.addEventListener('change', e => {
  const el = e.target.closest('[data-action-change]');
  if (!el) return;
  const fn = CHANGE_ACTIONS[el.dataset.actionChange];
  if (fn) fn(el, e);
});

// Cover-image fallback (replaces inline onerror=). 'error' events don't
// bubble, so listen in the capture phase.
document.addEventListener('error', e => {
  const img = e.target;
  if (img instanceof HTMLImageElement && img.dataset.phTitle !== undefined) {
    img.outerHTML = window.makePlaceholder(img.dataset.phTitle, +(img.dataset.phIdx || 0));
  }
}, true);
