// ============================================================
// I18N (INTERNATIONALIZATION)
// ============================================================
let currentLang = 'en';

const I18N = {
  en: {
    // Navigation
    nav_home: 'Home',
    nav_search: 'Search',
    nav_discover: 'Discover',
    nav_library: 'Library',
    nav_downloads: 'Downloads',
    nav_wishlist: 'Wishlist',
    nav_activity: 'Activity',
    nav_devices: 'Devices',
    nav_settings: 'Settings',
    sign_out: 'Sign out',
    // Header
    header_subtitle: 'Self-hosted book manager',
    home_kicker: 'Librarr 2.0',
    home_title: 'Welcome back',
    home_subtitle: 'Your self-hosted book library, downloads, and reading catalog in one place.',
    home_open_library: 'Browse Library',
    home_discover: 'Discover Books',
    home_welcome_title: 'Welcome to Librarr',
    home_welcome_subtitle: 'Your library is ready for its first book.',
    home_import_library: 'Configure Library',
    home_scan_library: 'Scan Library',
    home_import_hint: 'Set your folders, scan an existing collection, or discover a new book.',
    home_onboarding_title: 'Your library is ready for its first book',
    home_step_admin_done: 'Create administrator account',
    home_step_configure_folders: 'Configure library folders',
    home_step_scan_library: 'Scan existing library',
    home_step_review_books: 'Review imported books',
    home_step_opds: 'Connect an OPDS reader',
    dashboard_recent: 'Recently Added',
    dashboard_downloading: 'Download & Import Activity',
    dashboard_activity: 'Recent Activity',
    dashboard_totals: 'Library Summary',
    dashboard_formats: 'Format Distribution',
    dashboard_empty: 'No recent activity yet.',
    dashboard_attention: 'Needs Attention',
    dashboard_quick_actions: 'Quick Actions',
    dashboard_no_recent_books: 'Newly imported books will appear here.',
    dashboard_all_clear: 'No active downloads or imports.',
    dashboard_recent_count: 'Added in the last 30 days',
    dashboard_authors: 'Authors',
    dashboard_files: 'Files',
    dashboard_ready_to_import: 'Ready to import',
    dashboard_manual_review: 'Manual review',
    dashboard_waiting: 'Waiting for files',
    dashboard_failed: 'Failed',
    dashboard_importing: 'Importing',
    dashboard_downloading_count: 'Downloading',
    dashboard_open_opds: 'Open OPDS Catalog',
    library_kicker: 'Bookshelf',
    library_title: 'Your Library',
    library_formats: 'Formats',
    quick_details: 'Details',
    quick_more: 'More',
    details_kicker: 'Book Details',
    details_metadata: 'Metadata',
    metadata_edit: 'Edit Metadata',
    metadata_save: 'Save',
    metadata_cancel: 'Cancel',
    details_formats: 'Available Formats',
    details_history: 'History',
    details_description_placeholder: 'A richer description will appear here once normalized metadata and cover services are fully connected.',
    details_placeholder_value: 'Not available yet',
    metadata_source: 'Metadata source',
    metadata_confidence: 'Confidence',
    metadata_identifiers: 'Identifiers',
    metadata_series: 'Series',
    metadata_title: 'Title',
    metadata_edition_title: 'Edition Title',
    metadata_subtitle: 'Subtitle',
    metadata_description: 'Description',
    metadata_genres: 'Genres',
    metadata_language: 'Language',
    metadata_publication_date: 'Publication Date',
    metadata_publisher: 'Publisher',
    activity_kicker: 'Timeline',
    activity_title: 'Recent Activity',
    activity_open_downloads: 'Open Downloads',
    open_external: 'Open',
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
    // Library
    library_filter_placeholder: 'Filter library...',
    library_empty: 'Your library is empty',
    library_empty_hint: 'Import your folders or discover a new book to get started.',
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
    s_connection_tests: 'Connection Diagnostics',
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
    n_books_in_library: '{n} books in library',
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
    nav_home: 'Главная',
    nav_search: 'Поиск',
    nav_discover: 'Обзор',
    nav_library: 'Библиотека',
    nav_downloads: 'Загрузки',
    nav_wishlist: 'Желаемое',
    nav_activity: 'Активность',
    nav_devices: 'Устройства',
    nav_settings: 'Настройки',
    sign_out: 'Выйти',
    // Header
    header_subtitle: 'Менеджер книг для самохостинга',
    home_kicker: 'Librarr 2.0',
    home_title: 'Более тёплый, книжный интерфейс.',
    home_subtitle: 'Просматривайте коллекцию как личную библиотеку, а не как список файлов.',
    home_open_library: 'Открыть библиотеку',
    home_discover: 'Искать книги',
    home_welcome_title: 'Добро пожаловать в Librarr',
    home_welcome_subtitle: 'Импортируйте существующую коллекцию или начните искать книги.',
    home_import_library: 'Импортировать библиотеку',
    home_import_hint: 'Импорт начинается в Settings, где можно настроить папки и пути к существующей библиотеке.',
    home_onboarding_title: 'Подготовьте библиотеку',
    home_step_admin_done: 'Создать аккаунт администратора',
    home_step_configure_folders: 'Настроить папки библиотеки',
    home_step_scan_library: 'Просканировать существующую библиотеку',
    home_step_review_books: 'Проверить импортированные книги',
    dashboard_recent: 'Недавно добавлено',
    dashboard_downloading: 'Текущие загрузки',
    dashboard_wishlist: 'Желаемое',
    dashboard_activity: 'Недавняя активность',
    dashboard_totals: 'Итоги библиотеки',
    dashboard_formats: 'Распределение форматов',
    dashboard_empty: 'Пока ничего нет.',
    library_kicker: 'Книжная полка',
    library_title: 'Ваша библиотека',
    library_formats: 'Форматы',
    quick_details: 'Подробнее',
    quick_more: 'Ещё',
    details_kicker: 'О книге',
    details_metadata: 'Метаданные',
    details_formats: 'Доступные форматы',
    details_history: 'История',
    details_description_placeholder: 'Более подробное описание появится, когда сервисы нормализованных метаданных и обложек будут полностью подключены.',
    details_placeholder_value: 'Пока недоступно',
    metadata_source: 'Источник метаданных',
    metadata_confidence: 'Уверенность',
    metadata_identifiers: 'Идентификаторы',
    metadata_series: 'Серия',
    activity_kicker: 'Лента',
    activity_title: 'Недавняя активность',
    activity_open_downloads: 'Открыть загрузки',
    open_external: 'Открыть',
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
    // Library
    library_filter_placeholder: 'Фильтр библиотеки...',
    library_empty: 'Ваша библиотека пуста',
    library_empty_hint: 'Импортируйте папки или найдите новую книгу, чтобы начать.',
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
    s_connection_tests: 'Диагностика подключений',
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
    n_books_in_library: '{n} книг в библиотеке',
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

function refreshDynamicContent() {
  // Re-render current tab content with new language
  const tab = state.currentTab;
  if (tab === 'home') {
    loadHomeDashboard();
  } else if (tab === 'search' && state.searchResults.length > 0) {
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
  currentTab: 'home',
  searchTab: 'ebooks',
  libraryTab: 'ebooks',
  searchResults: [],
  libraryBooks: [],
  homeBooks: [],
  homeData: null,
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
  activeDetailContext: null,
  libraryMetadataEditor: {
    open: false,
    draft: null,
    errors: [],
  },
  bookDeleteDialog: {
    open: false,
    deleteFiles: false,
    loading: false,
    error: '',
  },
  libraryImport: {
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
  },
  libraryRepair: {
    nestedEbookPaths: {
      loading: false,
      running: false,
      plan: null,
      result: null,
      error: '',
    },
  },
};

const LIBRARY_IMPORT_FIELDS = ['incoming_dir', 'ebook_dir', 'audiobook_dir', 'manga_dir'];

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
  if (tab === 'home') loadHomeDashboard();
  if (tab === 'library') loadLibrary();
  if (tab === 'downloads') { refreshDownloads(); startDownloadPolling(); }
  else stopDownloadPolling();
  if (tab === 'wishlist') loadWishlist();
  if (tab === 'settings') loadSettings();
  if (tab === 'settings' && window.location.hash) {
    scrollToSettingsSection(window.location.hash.slice(1));
  }
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

function setupLibrarr2Shell() {
  document.body.classList.remove('bg-slate-950');
  document.body.classList.add('librarr-2-body');

  const header = document.querySelector('header');
  if (header) header.classList.add('librarr-2-header');

  const app = document.getElementById('app');
  if (app) app.classList.add('librarr-2-shell');

  const nav = document.getElementById('main-nav');
  if (nav) {
    nav.innerHTML = `
      <button data-action="switchTab" data-arg="home" class="nav-tab active px-4 py-2.5 text-sm font-medium border-b-2 border-transparent whitespace-nowrap" data-tab="home">
        <span data-i18n="nav_home">Home</span>
      </button>
      <button data-action="switchTab" data-arg="library" class="nav-tab px-4 py-2.5 text-sm font-medium border-b-2 border-transparent whitespace-nowrap" data-tab="library">
        <span data-i18n="nav_library">Library</span>
      </button>
      <button data-action="switchTab" data-arg="search" class="nav-tab px-4 py-2.5 text-sm font-medium border-b-2 border-transparent whitespace-nowrap" data-tab="search">
        <span data-i18n="nav_discover">Discover</span>
      </button>
      <button data-action="switchTab" data-arg="settings" class="nav-tab px-4 py-2.5 text-sm font-medium border-b-2 border-transparent whitespace-nowrap" data-tab="settings">
        <span data-i18n="nav_settings">Settings</span>
      </button>
    `;
  }

  const main = document.querySelector('main');
  const searchTab = document.getElementById('tab-search');
  if (main && searchTab && !document.getElementById('tab-home')) {
    const home = document.createElement('div');
    home.id = 'tab-home';
    home.className = 'tab-content active';
    home.innerHTML = `
      <section class="home-hero rounded-[2rem] p-4 sm:p-5 mb-4">
        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3">
          <div class="max-w-2xl">
            <p class="text-[11px] uppercase tracking-[0.26em] text-amber-300/80 mb-1.5" data-i18n="home_kicker">Librarr 2.0</p>
            <h2 id="home-hero-title" class="text-2xl sm:text-[1.7rem] font-semibold tracking-tight text-white mb-1.5" data-i18n="home_title">Welcome back</h2>
            <p id="home-hero-subtitle" class="text-sm sm:text-base text-stone-300/80 leading-6" data-i18n="home_subtitle">Your self-hosted book library, downloads, and reading catalog in one place.</p>
          </div>
          <div id="home-hero-actions" class="flex flex-wrap gap-2.5">
            <button data-action="switchTab" data-arg="library" class="px-4 py-2.5 rounded-2xl bg-amber-500 text-stone-950 font-medium hover:bg-amber-400 transition-colors" data-i18n="home_open_library">Open Library</button>
            <button data-action="switchTab" data-arg="search" class="px-4 py-2.5 rounded-2xl bg-white/10 text-white font-medium hover:bg-white/15 transition-colors" data-i18n="home_discover">Discover Books</button>
          </div>
        </div>
      </section>
      <div id="home-dashboard" class="grid gap-5 lg:grid-cols-12"></div>
    `;
    main.insertBefore(home, searchTab);

  }

  if (!document.getElementById('book-detail-modal')) {
    const modal = document.createElement('div');
    modal.id = 'book-detail-modal';
    modal.className = 'fixed inset-0 z-50 hidden items-stretch justify-end bg-black/70 backdrop-blur-sm';
    modal.innerHTML = `
      <div class="w-full max-w-4xl h-full overflow-y-auto bg-[#171412] border-l border-stone-800 shadow-2xl">
        <div class="sticky top-0 z-10 px-5 py-4 border-b border-stone-800 bg-[#171412]/95 backdrop-blur flex items-center justify-between">
          <div>
            <p class="text-xs uppercase tracking-[0.24em] text-amber-300/80 mb-1" data-i18n="details_kicker">Book Details</p>
            <h3 id="detail-heading" class="text-xl font-semibold text-white">Book</h3>
          </div>
          <button data-action="closeBookDetails" class="rounded-2xl p-2 text-stone-400 hover:text-white hover:bg-stone-800 transition-colors" aria-label="Close">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
          </button>
        </div>
        <div id="book-detail-content" class="p-5 sm:p-8"></div>
      </div>
    `;
    document.body.appendChild(modal);
  }

  const libraryResults = document.getElementById('library-results');
  if (libraryResults) {
    libraryResults.className = 'grid gap-5 sm:grid-cols-2 xl:grid-cols-3';
  }
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

  const seeders = result.seeders ? `<span class="text-emerald-400 text-xs font-medium">${t('n_seeds', {n: result.seeders})}</span>` : '';
  const leechers = result.leechers ? `<span class="text-amber-400 text-xs">${t('n_leech', {n: result.leechers})}</span>` : '';
  const sizeText = (result.size && result.size > 0) ? formatSize(result.size) : (result.size_human || result.sizeHuman || '');
  const size = sizeText ? `<span class="text-slate-400 text-xs font-medium">${escapeHtml(sizeText)}</span>` : '';
  const format = result.format ? `<span class="text-slate-500 text-xs uppercase">${escapeHtml(result.format)}</span>` : '';
  const indexer = result.indexer ? `<span class="text-slate-600 text-xs">${escapeHtml(result.indexer)}</span>` : '';

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
  const displayButtonText = buttonState === 'loading' && trackedJob?.detail
    ? escapeHtml(trackedJob.detail)
    : (buttonText[buttonState] || buttonText.idle);

  return `
    <div class="book-card bg-slate-900 rounded-xl overflow-hidden border border-slate-800 hover:border-slate-600 flex flex-col">
      <div class="relative">
        ${coverHtml}
        <span class="absolute top-2 left-2 px-2 py-0.5 rounded text-xs font-medium" style="background:${src.bg};color:${src.text}">${escapeHtml(src.label)}</span>
      </div>
      <div class="p-3 flex-1 flex flex-col">
        <h3 class="text-sm font-semibold text-white line-clamp-2 mb-1" title="${escapeHtml(result.title || '')}">${escapeHtml(result.title || 'Unknown')}</h3>
        <p class="text-xs text-slate-400 mb-2 line-clamp-1">${escapeHtml(result.author || '')}</p>
        <div class="flex items-center gap-2 flex-wrap mt-auto mb-2">
          ${seeders}${leechers}${size}${format}${indexer}
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

function makePlaceholderHtml(title, index, heightClass = 'h-48') {
  const gradient = COVER_GRADIENTS[index % COVER_GRADIENTS.length];
  const letter = (title || '?').charAt(0).toUpperCase();
  return `<div class="w-full ${heightClass} bg-gradient-to-br ${gradient} cover-placeholder">${escapeHtml(letter)}</div>`;
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
    };

    const data = await apiJson('/api/download', {
      method: 'POST',
      body: JSON.stringify(body),
    });

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
  const normalizedBooks = normalizedLibraryMode();

  const endpoints = {
    ebooks: `/api/library?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
    audiobooks: `/api/library/audiobooks?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
    manga: `/api/library/manga?page=${page}${q ? '&q=' + encodeURIComponent(q) : ''}`,
  };

  const container = document.getElementById('library-results');
  const emptyEl = document.getElementById('library-empty');
  const paginationEl = document.getElementById('library-pagination');

  try {
    let books = [];
    if (normalizedBooks) {
      const limit = 24;
      const offset = (page - 1) * limit;
      const mediaType = normalizedMediaTypeForTab(tab);
      const data = await apiJson(`/api/v1/books?media_type=${encodeURIComponent(mediaType)}&limit=${limit}&offset=${offset}&sort=title&order=asc${q ? '&search=' + encodeURIComponent(q) : ''}`);
      const total = data.pagination?.total || 0;
      state.libraryPages = Math.max(1, Math.ceil(total / limit));
      books = (data.items || []).map(mapV1BookToUIBook);
    } else {
      const data = await apiJson(endpoints[tab]);
      const rawItems = data.items || [];
      state.libraryPages = data.pages || 1;
      const items = filterLibraryItems(rawItems, q);
      books = groupLibraryItems(items, tab);
    }
    state.libraryBooks = books;

    if (books.length === 0) {
      container.innerHTML = '';
      emptyEl.classList.remove('hidden');
      paginationEl.classList.add('hidden');
      return;
    }

    emptyEl.classList.add('hidden');
    container.innerHTML = books.map((book, index) => renderLibraryBookCard(book, index)).join('');

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

function normalizedLibraryMode() {
  return (state.config?.library_repository_mode || '').toLowerCase() === 'normalized';
}

function normalizedMediaTypeForTab(tab) {
  switch (tab) {
    case 'audiobooks': return 'audiobook';
    case 'manga': return 'manga';
    default: return 'ebook';
  }
}

function mapV1BookToUIBook(book) {
  return {
    id: book.id,
    title: book.title || t('unknown_title'),
    author: book.primary_author?.name || (book.contributors?.find(c => c.role === 'author')?.name || ''),
    series: book.series?.name || '',
    coverUrl: book.cover?.available ? (book.cover?.url || '') : '',
    mediaType: book.media_type || 'ebook',
    formats: normalizeFormatLabels(book.formats || []),
    files: [],
    sourceRows: [],
    size: 0,
    description: book.description || '',
    externalUrl: '',
    placeholderIndex: 0,
    nativeV1: true,
  };
}

function normalizeFormatLabels(formats) {
  const order = new Map([
    ['EPUB', 10],
    ['PDF', 20],
    ['AZW3', 30],
    ['MOBI', 40],
    ['CBZ', 50],
    ['CBR', 51],
    ['M4B', 60],
    ['MP3', 61],
  ]);
  const seen = new Set();
  return (formats || [])
    .map(format => String(format || '').trim().toUpperCase())
    .filter(Boolean)
    .filter(format => {
      if (seen.has(format)) return false;
      seen.add(format);
      return true;
    })
    .sort((a, b) => (order.get(a) ?? 100) - (order.get(b) ?? 100) || a.localeCompare(b));
}

function filterLibraryItems(items, query) {
  if (!query) return items;
  const needle = query.toLowerCase();
  return items.filter(item => {
    const text = [
      item.title,
      item.name,
      item.author,
      item.series,
      item.library,
      item.file_format,
      item.format,
    ].filter(Boolean).join(' ').toLowerCase();
    return text.includes(needle);
  });
}

function groupLibraryItems(items, tab) {
  const groups = new Map();
  items.forEach((item, index) => {
    const title = item.title || item.name || t('unknown_title');
    const author = item.author || '';
    const key = `${tab}::${(title || '').trim().toLowerCase()}::${(author || '').trim().toLowerCase()}`;
    const format = (item.file_format || item.format || inferFormatFromPath(item.file_path || item.original_path || '') || '').toUpperCase();
    if (!groups.has(key)) {
      groups.set(key, {
        id: item.id,
        title,
        author,
        series: item.series || '',
        coverUrl: item.cover_url || '',
        mediaType: tab,
        formats: new Set(),
        files: [],
        sourceRows: [],
        size: 0,
        description: item.metadata?.description || item.metadata?.summary || '',
        externalUrl: item.abs_url || item.kavita_url || '',
        pages: item.pages || 0,
        durationHours: item.duration_hours || 0,
        numFiles: item.num_files || 0,
        placeholderIndex: index,
      });
    }
    const group = groups.get(key);
    if (format) group.formats.add(format);
    group.files.push({
      id: item.id,
      path: item.file_path || '',
      originalPath: item.original_path || '',
      format,
      source: item.source || '',
      sourceID: item.source_id || '',
      addedAt: item.added_at || '',
      size: item.file_size || 0,
    });
    group.sourceRows.push(item);
    group.size += item.file_size || 0;
  });

  return Array.from(groups.values()).map(group => ({
    ...group,
    formats: normalizeFormatLabels(Array.from(group.formats)),
  }));
}

function renderLibraryBookCard(book, index) {
  const coverHtml = renderBookCover(book, index, 'h-64');
  const subtitle = book.author || book.series || t('details_placeholder_value');
  const meta = [];
  if (book.series) meta.push(book.series);
  if (book.size) meta.push(formatSize(book.size));

  return `
    <article class="shelf-card group overflow-hidden rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 shadow-[0_12px_40px_rgba(0,0,0,0.22)]">
      <div class="relative">${coverHtml}</div>
      <div class="p-5">
        <div class="mb-4">
          <h3 class="text-xl font-semibold tracking-tight text-white line-clamp-2">${escapeHtml(book.title)}</h3>
          <p class="mt-1 text-sm text-stone-300 line-clamp-1">${escapeHtml(subtitle)}</p>
          ${meta.length ? `<p class="mt-2 text-xs uppercase tracking-[0.18em] text-stone-500">${escapeHtml(meta.join(' • '))}</p>` : ''}
        </div>
        <div class="mb-4 flex flex-wrap gap-2">
          ${book.formats.map(renderFormatBadge).join('')}
        </div>
        <div class="flex gap-2 flex-wrap">
          <button data-action="openBookDetails" data-index="${index}" class="px-3 py-2 rounded-xl bg-amber-500 text-stone-950 text-sm font-medium hover:bg-amber-400 transition-colors">${t('quick_details')}</button>
          ${book.externalUrl ? `<a href="${escapeHtml(book.externalUrl)}" target="_blank" rel="noreferrer" class="px-3 py-2 rounded-xl bg-white/10 text-white text-sm font-medium hover:bg-white/15 transition-colors">${t('open_external')}</a>` : `<button data-action="openBookDetails" data-index="${index}" class="px-3 py-2 rounded-xl bg-white/10 text-white text-sm font-medium hover:bg-white/15 transition-colors">${t('quick_more')}</button>`}
        </div>
      </div>
    </article>
  `;
}

function renderBookCover(book, index, heightClass = 'h-56') {
  if (book.coverUrl) {
    return `<img src="${escapeHtml(book.coverUrl)}" alt="" class="w-full ${heightClass} object-cover" loading="lazy" data-ph-title="${escapeHtml(book.title || '')}" data-ph-idx="${index}">`;
  }
  return makePlaceholderHtml(book.title || '?', index, heightClass);
}

function renderFormatBadge(format) {
  return `<span class="inline-flex items-center rounded-full border border-amber-500/20 bg-amber-500/10 px-3 py-1 text-xs font-semibold tracking-[0.18em] text-amber-200">${escapeHtml(format)}</span>`;
}

function inferFormatFromPath(path) {
  if (!path || !path.includes('.')) return '';
  return path.split('.').pop();
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

async function loadHomeDashboard() {
	const container = document.getElementById('home-dashboard');
	if (!container) return;
	try {
		const useNormalized = normalizedLibraryMode();
		const recentLibraryRequest = useNormalized
			? apiJson('/api/v1/books?limit=8&offset=0&sort=recently_added&order=desc')
			: apiJson('/api/library?limit=8');
		const statsRequest = useNormalized
			? apiJson('/api/v1/library/summary')
			: apiJson('/api/stats');
    const [statsRes, downloadsRes, activityRes, libraryRes] = await Promise.allSettled([
      statsRequest,
      apiJson('/api/downloads'),
      apiJson('/api/activity?limit=6'),
      recentLibraryRequest,
    ]);

		const stats = statsRes.status === 'fulfilled' ? statsRes.value : {};
		const downloads = downloadsRes.status === 'fulfilled' ? normalizeDownloadsResponse(downloadsRes.value) : [];
		const activity = activityRes.status === 'fulfilled' ? (activityRes.value.events || []) : [];
		const recentBooks = libraryRes.status === 'fulfilled'
			? (useNormalized
				? (libraryRes.value.items || []).map(mapV1BookToUIBook)
				: groupLibraryItems(libraryRes.value.items || [], 'ebooks').slice(0, 8))
			: [];
		state.homeBooks = recentBooks;
		const bookCount = currentLibraryCount(useNormalized, stats);
		const displayBookCount = bookCount || (recentBooks.length ? recentBooks.length : 0);
		const showOnboarding = bookCount === 0 && recentBooks.length === 0;
		updateHomeHero(showOnboarding, displayBookCount);
		const formatCounts = useNormalized
			? (stats.format_distribution || {})
			: buildFormatCounts(recentBooks);
		const activitySummary = buildDashboardActivitySummary(downloads, activity);
		const attention = buildDashboardAttention(activitySummary);

		container.innerHTML = buildHomeDashboardMarkup({
			showOnboarding,
			recentBooks,
			formatCounts,
			downloads,
			activity,
			activitySummary,
			attention,
			stats,
			bookCount: displayBookCount,
			isAdmin: isAdminUser(),
		});
	} catch (err) {
		updateHomeHero(false, 0);
		container.innerHTML = `<div class="dashboard-panel rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5 text-stone-400">${t('dashboard_empty')}</div>`;
	}
}

function buildHomeDashboardMarkup({ showOnboarding, recentBooks, formatCounts, downloads, activity, activitySummary, attention, stats, bookCount, isAdmin }) {
	if (showOnboarding) {
		return renderOnboardingChecklist(isAdmin);
	}

	const summary = activitySummary || {};
	const attentionItems = attention || [];
	return `
		${attentionItems.length ? renderNeedsAttention(attentionItems) : ''}
		<section class="dashboard-panel lg:col-span-8 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
			<div class="flex items-center justify-between gap-4 mb-4">
				<div>
					<h3 class="text-lg font-semibold text-white">${t('dashboard_recent')}</h3>
					<p class="text-sm text-stone-500">${t('dashboard_recent_count')}: ${escapeHtml(String(stats.recently_added ?? recentBooks.length ?? 0))}</p>
				</div>
				<button data-action="switchTab" data-arg="library" class="text-sm text-amber-300 hover:text-amber-200">${t('home_open_library')}</button>
			</div>
			${renderRecentlyAddedShelf(recentBooks)}
		</section>
		<section class="dashboard-panel lg:col-span-4 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
			<h3 class="text-lg font-semibold text-white mb-3">${t('dashboard_downloading')}</h3>
			${renderDashboardActivity(summary, isAdmin)}
		</section>
		<section class="dashboard-panel lg:col-span-5 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
			<h3 class="text-lg font-semibold text-white mb-4">${t('dashboard_totals')}</h3>
			<div class="grid grid-cols-2 gap-3">
				${renderMetricCard('Books', bookCount)}
				${renderMetricCard(t('dashboard_authors'), stats.authors ?? 0)}
				${renderMetricCard(t('dashboard_files'), stats.total_files ?? stats.total_items ?? 0)}
				${renderMetricCard('Ebooks', stats.ebooks ?? 0)}
				${renderMetricCard('Audiobooks', stats.audiobooks ?? 0)}
				${renderMetricCard('Manga', stats.manga ?? 0)}
			</div>
			<div class="mt-5">
				<h4 class="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500 mb-3">${t('dashboard_formats')}</h4>
				<div class="flex flex-wrap gap-2">
					${Object.entries(formatCounts || {}).length ? Object.entries(formatCounts).map(([format, count]) => `<span class="inline-flex items-center gap-2 rounded-full bg-stone-800 px-3 py-1.5 text-xs text-stone-200"><span class="text-amber-300">${escapeHtml(String(format).toUpperCase())}</span><span class="text-stone-500">${escapeHtml(String(count))}</span></span>`).join('') : `<span class="text-sm text-stone-500">${t('dashboard_empty')}</span>`}
				</div>
			</div>
		</section>
		<section class="dashboard-panel lg:col-span-4 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
			<h3 class="text-lg font-semibold text-white mb-4">${t('dashboard_quick_actions')}</h3>
			<div class="grid gap-2">
				<button data-action="switchTab" data-arg="library" class="rounded-2xl bg-amber-500 px-4 py-3 text-left text-sm font-semibold text-stone-950 hover:bg-amber-400 transition-colors">${t('home_open_library')}</button>
				<button data-action="switchTab" data-arg="search" class="rounded-2xl bg-stone-800 px-4 py-3 text-left text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('home_discover')}</button>
				${isAdmin ? `<button data-action="openImportSettings" class="rounded-2xl bg-stone-800 px-4 py-3 text-left text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('home_scan_library')}</button>` : ''}
				<a href="/opds" target="_blank" rel="noreferrer" class="rounded-2xl bg-stone-800 px-4 py-3 text-left text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('dashboard_open_opds')}</a>
			</div>
		</section>
		<section class="dashboard-panel lg:col-span-3 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
			<h3 class="text-lg font-semibold text-white mb-4">${t('dashboard_activity')}</h3>
			<div class="space-y-3">${(activity || []).slice(0, 3).map(renderActivityRow).join('') || renderDashboardEmpty()}</div>
		</section>
    `;
}

function renderCompactBookCard(book, index) {
	return `
    <button data-action="openHomeBookDetails" data-index="${index}" class="text-left rounded-[1.5rem] border border-stone-800 bg-stone-900/70 p-3 hover:border-amber-500/40 transition-colors">
      <div class="flex gap-4">
        <div class="w-20 shrink-0 overflow-hidden rounded-2xl">${renderBookCover(book, index, 'h-28')}</div>
        <div class="min-w-0">
          <h4 class="text-base font-semibold text-white line-clamp-2">${escapeHtml(book.title)}</h4>
          <p class="mt-1 text-sm text-stone-300 line-clamp-1">${escapeHtml(book.author || book.series || '')}</p>
          <div class="mt-3 flex flex-wrap gap-2">${book.formats.slice(0, 3).map(renderFormatBadge).join('')}</div>
        </div>
      </div>
    </button>
  `;
}

function renderRecentlyAddedShelf(books) {
	if (!books || books.length === 0) {
		return `<div class="rounded-[1.5rem] border border-dashed border-stone-800 bg-stone-900/30 p-6 text-sm text-stone-500">${t('dashboard_no_recent_books')}</div>`;
	}
	return `
		<div class="-mx-1 flex gap-4 overflow-x-auto pb-3 pr-2 snap-x" aria-label="${t('dashboard_recent')}">
			${books.map((book, index) => renderHomeBookCard(book, index)).join('')}
		</div>
	`;
}

function renderHomeBookCard(book, index) {
	const author = book.author || book.series || t('details_placeholder_value');
	return `
		<button data-action="openHomeBookDetails" data-index="${index}" class="group w-44 sm:w-48 shrink-0 snap-start rounded-[1.35rem] bg-stone-900/60 p-2 text-left outline-none transition-all hover:-translate-y-0.5 hover:bg-stone-900 focus-visible:ring-2 focus-visible:ring-amber-400/70" aria-label="${escapeHtml(`${book.title || t('unknown_title')} details`)}">
			<div class="relative overflow-hidden rounded-[1.1rem] shadow-[0_16px_40px_rgba(0,0,0,0.28)]">
				${renderBookCover(book, index, 'h-52')}
				<div class="pointer-events-none absolute inset-0 bg-gradient-to-t from-black/45 via-transparent to-transparent opacity-0 transition-opacity group-hover:opacity-100 group-focus-visible:opacity-100"></div>
				<div class="pointer-events-none absolute bottom-2 left-2 right-2 translate-y-2 rounded-full bg-black/60 px-3 py-1.5 text-center text-xs font-medium text-white opacity-0 backdrop-blur transition-all group-hover:translate-y-0 group-hover:opacity-100 group-focus-visible:translate-y-0 group-focus-visible:opacity-100">${t('quick_details')}</div>
			</div>
			<h4 class="mt-3 text-sm font-semibold leading-snug text-white line-clamp-2">${escapeHtml(book.title || t('unknown_title'))}</h4>
			<p class="mt-1 text-xs text-stone-400 line-clamp-1">${escapeHtml(author)}</p>
		</button>
	`;
}

function hasDashboardActivity(summary) {
	return Boolean(summary && [
		'downloading',
		'waiting',
		'ready',
		'manualReview',
		'importing',
		'failed',
	].some(key => Number(summary[key] || 0) > 0));
}

function renderDashboardActivity(summary, isAdmin) {
	if (!hasDashboardActivity(summary)) {
		return `<div class="rounded-2xl border border-dashed border-stone-800 bg-stone-900/30 px-4 py-5 text-sm text-stone-400">${t('dashboard_all_clear')}</div>`;
	}
	const chips = [
		{ key: 'downloading', label: t('dashboard_downloading_count'), action: 'switchTab', arg: 'downloads' },
		{ key: 'waiting', label: t('dashboard_waiting'), action: 'switchTab', arg: 'downloads' },
		{ key: 'ready', label: t('dashboard_ready_to_import'), action: isAdmin ? 'openImportSettings' : '', arg: '' },
		{ key: 'manualReview', label: t('dashboard_manual_review'), action: isAdmin ? 'openImportSettings' : '', arg: '' },
		{ key: 'importing', label: t('dashboard_importing'), action: isAdmin ? 'openImportSettings' : '', arg: '' },
		{ key: 'failed', label: t('dashboard_failed'), action: 'switchTab', arg: 'downloads' },
	];
	return `<div class="grid grid-cols-2 gap-2">${chips
		.filter(chip => Number(summary[chip.key] || 0) > 0)
		.map(chip => renderActivityChip(chip.label, summary[chip.key], chip.action, chip.arg))
		.join('')}</div>`;
}

function renderNeedsAttention(items) {
	return `
		<section class="dashboard-panel lg:col-span-12 rounded-[1.75rem] border border-amber-500/25 bg-amber-500/8 p-5">
			<div class="mb-4 flex items-center justify-between gap-3">
				<h3 class="text-lg font-semibold text-amber-100">${t('dashboard_attention')}</h3>
			</div>
			<div class="grid gap-3 md:grid-cols-3">
				${items.map(item => `
					<div class="rounded-2xl border border-amber-500/20 bg-stone-950/35 p-4">
						<p class="text-sm font-semibold text-white">${escapeHtml(item.title)}</p>
						<p class="mt-1 text-sm text-amber-100/75">${escapeHtml(item.reason)}</p>
						${item.action ? `<button data-action="${escapeHtml(item.action)}" ${item.arg ? `data-arg="${escapeHtml(item.arg)}"` : ''} class="mt-3 text-sm font-medium text-amber-300 hover:text-amber-200">${escapeHtml(item.label || t('quick_details'))}</button>` : ''}
					</div>
				`).join('')}
			</div>
		</section>
	`;
}

function renderActivityChip(label, count, action, arg) {
	const inner = `
		<span class="block text-2xl font-semibold text-white">${escapeHtml(String(count))}</span>
		<span class="mt-1 block text-xs text-stone-400">${escapeHtml(label)}</span>
	`;
	if (!action) {
		return `<div class="rounded-2xl border border-stone-800 bg-stone-900/60 px-3 py-3 text-left">${inner}</div>`;
	}
	const attrs = action ? `data-action="${escapeHtml(action)}"${arg ? ` data-arg="${escapeHtml(arg)}"` : ''}` : '';
	return `<button ${attrs} class="rounded-2xl border border-stone-800 bg-stone-900/60 px-3 py-3 text-left hover:border-amber-500/35 hover:bg-stone-800/90 transition-colors">${inner}</button>`;
}

function renderMetricCard(label, value) {
	return `<div class="rounded-[1.25rem] bg-stone-900/65 p-3.5"><p class="text-[10px] uppercase tracking-[0.18em] text-stone-500">${escapeHtml(label)}</p><p class="mt-1.5 text-3xl font-semibold tracking-tight text-white">${escapeHtml(String(value))}</p></div>`;
}

function currentLibraryCount(useNormalized, stats) {
	if (!stats) return 0;
	return useNormalized ? (stats.total_books ?? 0) : (stats.total_items ?? 0);
}

function normalizeDownloadsResponse(value) {
	if (Array.isArray(value)) return value;
	if (value && Array.isArray(value.downloads)) return value.downloads;
	return [];
}

function isAdminUser() {
	return String(state.currentRole || '').toLowerCase() === 'admin';
}

function homeDisplayName() {
	const name = String(state.currentUser || '').trim();
	if (!name) return '';
	return name.replace(/[_-]+/g, ' ').replace(/\b\w/g, ch => ch.toUpperCase());
}

function buildDashboardActivitySummary(downloads, activity) {
	const summary = {
		downloading: 0,
		waiting: 0,
		ready: 0,
		importing: 0,
		manualReview: 0,
		failed: 0,
	};
	(downloads || []).forEach(item => {
		const status = String(item.status || '').toLowerCase();
		if (['downloading', 'queued', 'searching'].includes(status)) summary.downloading++;
		if (['waiting', 'pending', 'retry_wait', 'sync_wait', 'missing_files'].includes(status)) summary.waiting++;
		if (['importing', 'organizing'].includes(status)) summary.importing++;
		if (['error', 'failed', 'dead_letter'].includes(status)) summary.failed++;
	});
	(activity || []).forEach(event => {
		const text = [event.action, event.event, event.detail, event.message, event.title, event.subject].filter(Boolean).join(' ').toLowerCase();
		if (text.includes('manual review')) summary.manualReview++;
		if (text.includes('ready to import')) summary.ready++;
		if (text.includes('failed') || text.includes('error')) summary.failed++;
	});
	return summary;
}

function buildDashboardAttention(summary) {
	const items = [];
	if ((summary.manualReview || 0) > 0) {
		items.push({
			title: `${summary.manualReview} ${summary.manualReview === 1 ? 'book needs' : 'books need'} manual review`,
			reason: 'Review metadata or destination before importing.',
			action: isAdminUser() ? 'openImportSettings' : '',
			label: 'Open Review',
		});
	}
	if ((summary.failed || 0) > 0) {
		items.push({
			title: `${summary.failed} ${summary.failed === 1 ? 'import failed' : 'imports failed'}`,
			reason: 'Check the failed download or import details.',
			action: 'switchTab',
			arg: 'downloads',
			label: 'View Details',
		});
	}
	if ((summary.waiting || 0) > 0) {
		items.push({
			title: `${summary.waiting} ${summary.waiting === 1 ? 'item is' : 'items are'} waiting for files`,
			reason: 'Librarr is waiting for synchronized files to appear locally.',
			action: 'switchTab',
			arg: 'downloads',
			label: 'Open Downloads',
		});
	}
	return items;
}

function updateHomeHero(isOnboarding, bookCount = 0) {
	const titleEl = document.getElementById('home-hero-title');
	const subtitleEl = document.getElementById('home-hero-subtitle');
	const actionsEl = document.getElementById('home-hero-actions');
	if (!titleEl || !subtitleEl || !actionsEl) return;

	if (isOnboarding) {
		titleEl.textContent = t('home_welcome_title');
		subtitleEl.textContent = t('home_welcome_subtitle');
		actionsEl.innerHTML = `
			${isAdminUser() ? `<button data-action="openImportSettings" class="px-4 py-2.5 rounded-2xl bg-amber-500 text-stone-950 font-medium hover:bg-amber-400 transition-colors">${t('home_import_library')}</button>` : ''}
			<button data-action="switchTab" data-arg="search" class="px-4 py-2.5 rounded-2xl bg-white/10 text-white font-medium hover:bg-white/15 transition-colors">${t('home_discover')}</button>
		`;
		return;
	}

	const displayName = homeDisplayName();
	titleEl.textContent = displayName ? `Welcome back, ${displayName}` : t('home_title');
	subtitleEl.textContent = `${t('home_subtitle')} ${bookCount === 1 ? 'Your library has 1 book.' : `Your library has ${bookCount} books.`}`;
	actionsEl.innerHTML = `
    <button data-action="switchTab" data-arg="library" class="px-4 py-2.5 rounded-2xl bg-amber-500 text-stone-950 font-medium hover:bg-amber-400 transition-colors">${t('home_open_library')}</button>
    <button data-action="switchTab" data-arg="search" class="px-4 py-2.5 rounded-2xl bg-white/10 text-white font-medium hover:bg-white/15 transition-colors">${t('home_discover')}</button>
  `;
}

function renderOnboardingChecklist(isAdmin = isAdminUser()) {
	const checklistItems = [
		{ done: Boolean(state.currentUser), label: t('home_step_admin_done') },
		{ done: Boolean(state.libraryImport?.completed), label: t('home_step_configure_folders') },
		{ done: false, label: t('home_step_scan_library') },
		{ done: false, label: t('home_step_review_books') },
		{ done: false, label: t('home_step_opds') },
	];

	return `
    <section class="dashboard-panel lg:col-span-12 rounded-[1.75rem] border border-stone-800 bg-[#1b1715]/95 p-5">
      <div class="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
        <div class="max-w-2xl">
          <h3 class="text-xl font-semibold text-white mb-2">${t('home_onboarding_title')}</h3>
          <p class="text-stone-400 leading-7">${t('home_import_hint')}</p>
        </div>
        <div class="flex flex-wrap gap-2">
          ${isAdmin ? `<button data-action="openImportSettings" class="rounded-2xl bg-amber-500 px-4 py-2.5 text-sm font-semibold text-stone-950 hover:bg-amber-400 transition-colors">${t('home_import_library')}</button>` : ''}
          ${isAdmin ? `<button data-action="openImportSettings" class="rounded-2xl bg-stone-800 px-4 py-2.5 text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('home_scan_library')}</button>` : ''}
          <button data-action="switchTab" data-arg="search" class="rounded-2xl bg-stone-800 px-4 py-2.5 text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('home_discover')}</button>
          <a href="/opds" target="_blank" rel="noreferrer" class="rounded-2xl bg-stone-800 px-4 py-2.5 text-sm font-semibold text-stone-100 hover:bg-stone-700 transition-colors">${t('dashboard_open_opds')}</a>
        </div>
      </div>
      <div class="mt-6 grid gap-3 md:grid-cols-2 lg:grid-cols-4">
        ${checklistItems.map(item => `
          <div class="flex items-center gap-3 rounded-2xl border ${item.done ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-100' : 'border-stone-800 bg-stone-900/60 text-stone-200'} px-4 py-3">
            <span class="text-base">${item.done ? '✅' : '⬜'}</span>
            <span class="text-sm font-medium">${escapeHtml(item.label)}</span>
          </div>
        `).join('')}
      </div>
    </section>
  `;
}

function openImportSettings() {
  const targetId = 'settings-library-import';
  window.location.hash = targetId;
  switchTab('settings');
  scrollToSettingsSection(targetId);
}

function scrollToSettingsSection(id) {
  const el = document.getElementById(id);
  if (!el) return;
  requestAnimationFrame(() => {
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    el.classList.add('ring-2', 'ring-amber-400/70');
    window.setTimeout(() => {
      el.classList.remove('ring-2', 'ring-amber-400/70');
    }, 1600);
  });
}

function renderCompactDownload(item) {
  return `<div class="rounded-[1.25rem] bg-stone-900/70 p-3"><p class="text-sm font-medium text-white line-clamp-1">${escapeHtml(item.title || t('unknown_title'))}</p><p class="mt-1 text-xs text-stone-400">${escapeHtml(item.status || '')}</p></div>`;
}

function renderCompactWishlist(item) {
  return `<div class="rounded-[1.25rem] bg-stone-900/70 p-3"><p class="text-sm font-medium text-white line-clamp-1">${escapeHtml(item.title || '')}</p><p class="mt-1 text-xs text-stone-400 line-clamp-1">${escapeHtml(item.author || item.media_type || '')}</p></div>`;
}

function renderActivityRow(event) {
  const timestamp = event.created_at || event.timestamp || '';
  const label = event.action || event.event || '';
  const detail = event.detail || event.message || '';
  const title = event.title || event.subject || '';
  return `<div class="rounded-[1rem] border border-stone-800 bg-stone-900/60 px-3.5 py-3">
    <div class="flex items-start justify-between gap-3">
      <div class="min-w-0">
        <p class="text-sm font-semibold leading-5 text-white line-clamp-1">${escapeHtml(title || label || 'Activity')}</p>
        <p class="mt-0.5 text-xs leading-5 text-stone-400 line-clamp-2">${escapeHtml(detail || label || '')}</p>
      </div>
      <span class="shrink-0 pt-0.5 text-[11px] text-stone-500">${escapeHtml(formatRelativeTime(timestamp))}</span>
    </div>
  </div>`;
}

function renderDashboardEmpty() {
  return `<div class="rounded-[1.25rem] border border-dashed border-stone-800 bg-stone-900/30 p-5 text-sm text-stone-500">${t('dashboard_empty')}</div>`;
}

function buildFormatCounts(books) {
  const counts = {};
  books.forEach(book => {
    book.formats.forEach(format => {
      counts[format] = (counts[format] || 0) + 1;
    });
  });
  return counts;
}

function formatRelativeTime(value) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const diffMs = Date.now() - date.getTime();
  const diffMin = Math.round(diffMs / 60000);
  if (diffMin < 1) return 'now';
  if (diffMin < 60) return `${diffMin}m`;
  const diffHr = Math.round(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h`;
  const diffDay = Math.round(diffHr / 24);
  return `${diffDay}d`;
}

async function openBookDetails(index, collection = 'libraryBooks') {
  const source = Array.isArray(state[collection]) ? state[collection] : [];
  const book = source[index];
  if (!book) return;
  const modal = document.getElementById('book-detail-modal');
  const content = document.getElementById('book-detail-content');
  const heading = document.getElementById('detail-heading');
  if (!modal || !content || !heading) return;
  let detailBook = book;
  let detailFiles = book.files || [];
  let detailMetadata = null;
  let detailProvenance = null;
  if (normalizedLibraryMode() && book.id) {
    try {
      const [detail, files, metadata, provenance] = await Promise.all([
        apiJson(`/api/v1/books/${book.id}`),
        apiJson(`/api/v1/books/${book.id}/files`),
        apiJson(`/api/v1/books/${book.id}/metadata`),
        apiJson(`/api/v1/books/${book.id}/provenance`),
      ]);
      detailBook = {
        ...mapV1BookToUIBook(detail),
        description: detail.description || '',
        identifiers: detail.identifiers || [],
        editions: detail.editions || [],
        series: detail.series?.name || '',
        metadataConfidence: 'Native',
      };
      detailFiles = (files.items || []).map(file => ({
        id: file.id,
        path: file.path || '',
        originalPath: file.original_path || '',
        format: (file.format || '').toUpperCase(),
        sourceID: file.source_id || '',
        size: file.size || 0,
        editionID: file.edition_id || 0,
      }));
      detailBook.files = detailFiles;
      detailMetadata = metadata;
      detailProvenance = provenance;
    } catch (err) {
      detailFiles = book.files || [];
    }
  }
  detailBook.metadata = detailMetadata;
  detailBook.provenance = detailProvenance;
  state.activeDetailBook = detailBook;
  state.activeDetailContext = { index, collection, bookId: detailBook.id || book.id || 0 };
  const preferredTitle = detailMetadata?.fields?.title?.value || detailBook.title || book.title;
  const preferredDescription = detailMetadata?.fields?.description?.value || detailBook.description || '';
  heading.textContent = preferredTitle;
  content.innerHTML = `
    <div class="grid gap-8 lg:grid-cols-[18rem_minmax(0,1fr)]">
      <div>
              <div class="overflow-hidden rounded-[2rem] shadow-[0_20px_50px_rgba(0,0,0,0.35)]">${renderBookCover(detailBook, index, 'h-[24rem]')}</div>
      </div>
      <div>
        <header class="mb-8">
          <h2 class="text-4xl font-semibold tracking-tight text-white">${escapeHtml(preferredTitle)}</h2>
          <p class="mt-2 text-lg text-stone-300">${escapeHtml(detailBook.author || t('details_placeholder_value'))}</p>
          ${detailBook.series ? `<p class="mt-3 text-sm uppercase tracking-[0.18em] text-amber-300/80">${escapeHtml(detailBook.series)}</p>` : ''}
          <p class="mt-5 text-stone-400 leading-7">${escapeHtml(preferredDescription || t('details_description_placeholder'))}</p>
        </header>
        <section class="mb-8">
          <div class="flex items-center justify-between gap-3 mb-4">
            <h3 class="text-lg font-semibold text-white">${t('details_metadata')}</h3>
            ${detailBook.id && normalizedLibraryMode() ? `<button data-action="openLibraryMetadataEditor" class="rounded-full border border-stone-700 px-3 py-1.5 text-xs font-medium text-stone-200 hover:border-amber-300 hover:text-amber-200 transition-colors">${t('metadata_edit')}</button>` : ''}
          </div>
          ${state.libraryMetadataEditor.open ? renderLibraryMetadataEditor(detailBook) : ''}
          <div class="grid gap-4 sm:grid-cols-2">
            ${renderMetadataFieldCards(detailMetadata)}
            ${renderDetailMetaCard(t('metadata_series'), detailBook.series || t('details_placeholder_value'))}
            ${renderDetailMetaCard(t('metadata_identifiers'), formatIdentifierList(detailBook.identifiers || []))}
          </div>
        </section>
        <section class="mb-8">
          <h3 class="text-lg font-semibold text-white mb-4">${t('details_formats')}</h3>
          <div class="space-y-3">
            ${detailFiles.map(file => `
              <div class="rounded-[1.25rem] border border-stone-800 bg-stone-900/60 px-4 py-3 flex items-center justify-between gap-3">
                <div>
                  <p class="text-sm font-medium text-white">${escapeHtml(file.format || 'FILE')}</p>
                  <p class="text-xs text-stone-500 line-clamp-1">${escapeHtml(file.path || file.originalPath || '')}</p>
                </div>
                <span class="text-xs text-stone-400">${escapeHtml(formatSize(file.size || 0))}</span>
              </div>
            `).join('')}
          </div>
        </section>
        <section>
          <h3 class="text-lg font-semibold text-white mb-4">${t('details_history')}</h3>
          <div id="detail-history" class="space-y-3">
            <div class="rounded-[1.25rem] border border-dashed border-stone-800 bg-stone-900/30 p-4 text-sm text-stone-500">${t('dashboard_empty')}</div>
          </div>
        </section>
        ${detailBook.id && normalizedLibraryMode() ? renderBookDeletionPanel(detailBook, detailFiles) : ''}
      </div>
    </div>
  `;
  modal.classList.remove('hidden');
  modal.classList.add('flex');
  loadBookHistory(detailBook);
}

function closeBookDetails() {
  const modal = document.getElementById('book-detail-modal');
  if (!modal) return;
  modal.classList.add('hidden');
  modal.classList.remove('flex');
  state.bookDeleteDialog = { open: false, deleteFiles: false, loading: false, error: '' };
}

function openHomeBookDetails(index) {
  openBookDetails(index, 'homeBooks');
}

function renderBookDeletionPanel(book, files = []) {
  const dialog = state.bookDeleteDialog || {};
  const formats = normalizeFormatLabels((book.formats || []).concat((files || []).map(file => file.format)));
  const fileList = (files || []).map(file => ({
    filename: (file.path || file.originalPath || '').split('/').pop() || file.format || 'file',
    format: String(file.format || '').toUpperCase(),
    size: file.size || 0,
  }));
  return `
    <section class="mt-8 rounded-[1.5rem] border border-red-500/20 bg-red-500/5 p-4">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h3 class="text-lg font-semibold text-white">Library Management</h3>
          <p class="mt-1 text-sm text-stone-400">Remove this book from Librarr, or delete its catalog entry and managed files.</p>
        </div>
        <div class="flex flex-wrap gap-2">
          ${isAdminUser() ? `<button data-action="mergeMatchingBookDuplicates" class="rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm font-medium text-amber-100 hover:bg-amber-500/20">Merge Matching Duplicates</button>` : ''}
          <button data-action="openBookDeleteDialog" data-delete-files="false" class="rounded-xl border border-stone-700 px-3 py-2 text-sm font-medium text-stone-200 hover:border-amber-300 hover:text-amber-200">Remove from Library</button>
          ${isAdminUser() ? `<button data-action="openBookDeleteDialog" data-delete-files="true" class="rounded-xl border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm font-medium text-red-100 hover:bg-red-500/20">Delete Book and Files</button>` : ''}
        </div>
      </div>
      ${dialog.open ? renderBookDeleteConfirmation(book, fileList, formats, dialog) : ''}
    </section>
  `;
}

function renderBookDeleteConfirmation(book, files, formats, dialog) {
  const deleteFiles = Boolean(dialog.deleteFiles);
  const title = book.title || t('unknown_title');
  const author = book.author || t('details_placeholder_value');
  const fileCount = files.length;
  if (!deleteFiles) {
    return `
      <div class="mt-4 rounded-2xl border border-amber-500/25 bg-stone-950/70 p-4">
        <p class="text-sm font-semibold text-white">Remove “${escapeHtml(title)}” from Librarr?</p>
        <p class="mt-2 text-sm text-stone-400">The ${escapeHtml(formats.join(' · ') || 'book')} files will remain on disk and may return during a future library scan.</p>
        ${dialog.error ? `<p class="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-100">${escapeHtml(dialog.error)}</p>` : ''}
        <div class="mt-4 flex flex-wrap gap-2">
          <button data-action="confirmBookDelete" ${dialog.loading ? 'disabled' : ''} class="rounded-xl bg-amber-500 px-4 py-2 text-sm font-semibold text-stone-950 hover:bg-amber-400 disabled:opacity-60">Remove from Library</button>
          <button data-action="cancelBookDeleteDialog" ${dialog.loading ? 'disabled' : ''} class="rounded-xl bg-stone-800 px-4 py-2 text-sm font-medium text-stone-200 hover:bg-stone-700 disabled:opacity-60">Cancel</button>
        </div>
      </div>
    `;
  }
  return `
    <div class="mt-4 rounded-2xl border border-red-500/35 bg-red-500/10 p-4">
      <p class="text-sm font-semibold text-red-50">Delete “${escapeHtml(title)}” and ${fileCount} ${fileCount === 1 ? 'file' : 'files'}?</p>
      <p class="mt-1 text-sm text-red-100/80">${escapeHtml(author)} · ${escapeHtml(formats.join(' · ') || 'Files')}</p>
      <p class="mt-3 text-sm text-red-100/80">This removes the catalog record and deletes the managed files listed below. This cannot be undone.</p>
      <div class="mt-3 max-h-44 overflow-y-auto rounded-xl border border-red-500/20 bg-stone-950/50">
        ${files.length ? files.map(file => `
          <div class="flex items-center justify-between gap-3 border-b border-red-500/10 px-3 py-2 last:border-0">
            <span class="min-w-0 truncate text-sm text-stone-100">${escapeHtml(file.filename)}</span>
            <span class="shrink-0 text-xs text-stone-400">${escapeHtml(file.format || '')}${file.size ? ` · ${escapeHtml(formatSize(file.size))}` : ''}</span>
          </div>
        `).join('') : `<p class="px-3 py-2 text-sm text-stone-400">No files are currently attached to this book.</p>`}
      </div>
      ${dialog.error ? `<p class="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-100">${escapeHtml(dialog.error)}</p>` : ''}
      <div class="mt-4 flex flex-wrap gap-2">
        <button data-action="confirmBookDelete" ${dialog.loading ? 'disabled' : ''} class="rounded-xl bg-red-500 px-4 py-2 text-sm font-semibold text-white hover:bg-red-400 disabled:opacity-60">Delete Book and ${fileCount} ${fileCount === 1 ? 'File' : 'Files'}</button>
        <button data-action="cancelBookDeleteDialog" ${dialog.loading ? 'disabled' : ''} class="rounded-xl bg-stone-800 px-4 py-2 text-sm font-medium text-stone-200 hover:bg-stone-700 disabled:opacity-60">Cancel</button>
      </div>
    </div>
  `;
}

async function mergeMatchingBookDuplicates() {
  const book = state.activeDetailBook;
  if (!book?.id) return;
  try {
    const response = await apiJson(`/api/v1/books/${book.id}/merge-matching`, { method: 'POST' });
    await loadLibrary();
    await loadStats();
    if (state.currentTab === 'home') {
      await loadHomeDashboard();
    }
    const targetID = response.target_book_id || book.id;
    const refreshedIndex = state.libraryBooks.findIndex(item => item.id === targetID);
    if (refreshedIndex >= 0) {
      await openBookDetails(refreshedIndex, 'libraryBooks');
    } else {
      closeBookDetails();
    }
    const mergedCount = response.merged_count || (Array.isArray(response.merged_book_ids) ? response.merged_book_ids.length : 0);
    showToast(mergedCount ? `Merged ${mergedCount} duplicate ${mergedCount === 1 ? 'book' : 'books'}.` : 'No matching duplicates found.', mergedCount ? 'success' : 'warning');
  } catch (err) {
    showToast(err.message || 'Failed to merge matching duplicates', 'error');
  }
}

function openBookDeleteDialog(deleteFiles) {
  state.bookDeleteDialog = {
    open: true,
    deleteFiles: deleteFiles === true || String(deleteFiles) === 'true',
    loading: false,
    error: '',
  };
  const context = state.activeDetailContext || {};
  openBookDetails(context.index || 0, context.collection || 'libraryBooks');
}

function cancelBookDeleteDialog() {
  state.bookDeleteDialog = { open: false, deleteFiles: false, loading: false, error: '' };
  const context = state.activeDetailContext || {};
  openBookDetails(context.index || 0, context.collection || 'libraryBooks');
}

async function confirmBookDelete() {
  const book = state.activeDetailBook;
  const dialog = state.bookDeleteDialog || {};
  if (!book?.id || dialog.loading) return;
  state.bookDeleteDialog = { ...dialog, loading: true, error: '' };
  const context = state.activeDetailContext || {};
  await openBookDetails(context.index || 0, context.collection || 'libraryBooks');
  try {
    const resp = await api(`/api/v1/books/${book.id}?delete_files=${dialog.deleteFiles ? 'true' : 'false'}`, { method: 'DELETE' });
    const response = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(formatBookDeleteError(response, resp.status));
    }
    state.bookDeleteDialog = { open: false, deleteFiles: false, loading: false, error: '' };
    closeBookDetails();
    await loadLibrary();
    await loadStats();
    if (state.currentTab === 'home') {
      await loadHomeDashboard();
    }
    const title = response.title || book.title || t('unknown_title');
    if (dialog.deleteFiles) {
      showToast(`Deleted “${title}” and ${response.deleted_files || 0} ${(response.deleted_files || 0) === 1 ? 'file' : 'files'}.`, 'success');
    } else {
      showToast(`Removed “${title}” from Librarr. Files were left on disk.`, 'success');
    }
  } catch (err) {
    state.bookDeleteDialog = { ...dialog, loading: false, error: err.message || 'Failed to delete book' };
    await openBookDetails(context.index || 0, context.collection || 'libraryBooks');
  }
}

function formatBookDeleteError(response = {}, status = 0) {
  const message = response.error || (status ? `API error: ${status}` : 'Failed to delete book');
  const files = Array.isArray(response.files) ? response.files : [];
  const details = files
    .filter(file => file?.error)
    .map(file => `${file.filename || 'file'}: ${file.error}`);
  if (details.length === 0) {
    return message;
  }
  return `${message}: ${details.join('; ')}`;
}

function renderDetailMetaCard(label, value) {
  return `<div class="rounded-[1.25rem] border border-stone-800 bg-stone-900/60 p-4"><p class="text-xs uppercase tracking-[0.18em] text-stone-500 mb-2">${escapeHtml(label)}</p><p class="text-sm text-stone-200">${escapeHtml(value)}</p></div>`;
}

function renderMetadataFieldCards(metadata) {
  if (!metadata?.fields) {
    return `
      ${renderDetailMetaCard(t('metadata_source'), t('details_placeholder_value'))}
      ${renderDetailMetaCard(t('metadata_confidence'), t('details_placeholder_value'))}
    `;
  }
  const fields = [
    ['title', t('metadata_title')],
    ['edition_title', t('metadata_edition_title')],
    ['subtitle', t('metadata_subtitle')],
    ['language', t('metadata_language')],
    ['publication_date', t('metadata_publication_date')],
    ['publisher', t('metadata_publisher')],
    ['genres', t('metadata_genres')],
  ];
  return fields.map(([key, label]) => renderMetadataFieldCard(label, metadata.fields[key])).join('');
}

function renderMetadataFieldCard(label, field) {
  if (!field || !field.value) {
    return renderDetailMetaCard(label, t('details_placeholder_value'));
  }
  const badge = field.manual_override ? '<span class="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] uppercase tracking-[0.18em] text-amber-200">Manual</span>' : '';
  return `
    <div class="rounded-[1.25rem] border border-stone-800 bg-stone-900/60 p-4">
      <div class="flex items-start justify-between gap-3 mb-2">
        <p class="text-xs uppercase tracking-[0.18em] text-stone-500">${escapeHtml(label)}</p>
        ${badge}
      </div>
      <p class="text-sm text-stone-100 mb-3">${escapeHtml(field.value)}</p>
      <div class="flex flex-wrap gap-2 text-[11px] text-stone-400">
        <span>${escapeHtml(field.source || t('details_placeholder_value'))}</span>
        <span>•</span>
        <span>${escapeHtml(formatConfidence(field))}</span>
      </div>
    </div>
  `;
}

function formatConfidence(field) {
  if (!field) return t('details_placeholder_value');
  if (typeof field.confidence_score === 'number' && field.confidence_score > 0) {
    return `${field.confidence_score}%`;
  }
  return field.confidence || t('details_placeholder_value');
}

function libraryMetadataDraftFromBook(book) {
  const fields = book?.metadata?.fields || {};
  return {
    title: fields.title?.value || book?.title || '',
    edition_title: fields.edition_title?.value || book?.editions?.[0]?.title || '',
    subtitle: fields.subtitle?.value || book?.editions?.[0]?.subtitle || '',
    author: book?.author || book?.primary_author?.name || '',
    series: book?.series || '',
    publisher: fields.publisher?.value || book?.editions?.[0]?.publisher || '',
    publication_year: publicationYearFromMetadata(fields.publication_date?.value || book?.editions?.[0]?.publication_date || ''),
    isbn: (book?.identifiers || []).find(identifier => String(identifier.type || '').toLowerCase() === 'isbn')?.value || '',
    language: fields.language?.value || book?.language || book?.editions?.[0]?.language || '',
    description: fields.description?.value || book?.description || '',
    tags: fields.genres?.value || '',
    library: book?.mediaType || book?.media_type || '',
  };
}

function renderLibraryMetadataEditor(book) {
  const draft = state.libraryMetadataEditor.draft || libraryMetadataDraftFromBook(book);
  const candidate = libraryMetadataCandidate(book);
  const preview = metadataEditorPreview(candidate, draft);
  const errors = validateLibraryMetadataDraft(book, draft);
  state.libraryMetadataEditor.errors = errors;
  const field = (name, label, value, attrs = '') => `
    <label class="block">
      <span class="text-[11px] uppercase tracking-wider text-stone-500">${escapeHtml(label)}</span>
      <input data-action-input="libraryMetadataEditorField" data-field="${escapeHtml(name)}" value="${escapeHtml(value || '')}" ${attrs} class="mt-1 w-full rounded-lg border border-stone-700 bg-stone-950 px-3 py-2 text-sm text-stone-100 placeholder-stone-600 focus:border-amber-400 focus:outline-none">
    </label>
  `;
  return `
    <div id="library-metadata-editor" class="mb-5 rounded-[1.5rem] border border-amber-500/25 bg-stone-950/75 p-4">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p class="text-sm font-semibold text-white">Metadata Editor</p>
          <p class="mt-1 text-xs text-stone-400">Update catalog metadata stored by Librarr. Ebook files are not modified.</p>
        </div>
        <button data-action="cancelLibraryMetadataEditor" class="rounded-md bg-stone-800 px-3 py-2 text-xs font-medium text-stone-300 hover:bg-stone-700">Cancel</button>
      </div>
      <div class="mt-4 grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
        <div class="space-y-4">
          <div class="grid gap-3 md:grid-cols-2">
            <div class="md:col-span-2">${field('title', 'Title', draft.title, 'required')}</div>
            ${field('edition_title', 'Edition Title', draft.edition_title)}
            ${field('subtitle', 'Subtitle', draft.subtitle)}
            ${field('publisher', 'Publisher', draft.publisher)}
            ${field('publication_year', 'Publication Year', draft.publication_year, 'inputmode="numeric" maxlength="4"')}
            ${field('language', 'Language', draft.language)}
            ${field('tags', 'Tags / Genres', draft.tags, 'placeholder="fantasy, fiction, owned"')}
          </div>
          <label class="block">
            <span class="text-[11px] uppercase tracking-wider text-stone-500">Description</span>
            <textarea data-action-input="libraryMetadataEditorField" data-field="description" rows="4" class="mt-1 w-full rounded-lg border border-stone-700 bg-stone-950 px-3 py-2 text-sm text-stone-100 placeholder-stone-600 focus:border-amber-400 focus:outline-none">${escapeHtml(draft.description || '')}</textarea>
          </label>
          <div class="rounded-lg border border-stone-800 bg-stone-900/60 p-3 text-xs text-stone-400">
            <p class="font-medium text-stone-200">Not editable here yet</p>
            <p class="mt-1">Author, series, ISBN, library, destination folder, and filename preview are shown for context but are not persisted by the current book metadata endpoint.</p>
          </div>
        </div>
        <div class="space-y-3">
          <div class="overflow-hidden rounded-lg border border-stone-800 bg-stone-900/80 p-3">
            <p class="mb-2 text-xs font-semibold text-stone-100">Cover Preview</p>
            <div class="w-28 overflow-hidden rounded-xl">${renderBookCover(book, 0, 'h-40')}</div>
          </div>
          <div class="rounded-lg border border-stone-800 bg-stone-900/80 p-3">
            <p class="text-xs font-semibold text-stone-100">Catalog Preview</p>
            <dl class="mt-3 space-y-2 text-xs">
              <div><dt class="text-stone-500">Author</dt><dd class="break-all text-stone-300">${escapeHtml(draft.author || t('details_placeholder_value'))}</dd></div>
              <div><dt class="text-stone-500">Series</dt><dd class="break-all text-stone-300">${escapeHtml(draft.series || t('details_placeholder_value'))}</dd></div>
              <div><dt class="text-stone-500">ISBN</dt><dd class="break-all text-stone-300">${escapeHtml(draft.isbn || t('details_placeholder_value'))}</dd></div>
              <div><dt class="text-stone-500">Library</dt><dd class="break-all text-stone-300">${escapeHtml(draft.library || t('details_placeholder_value'))}</dd></div>
              <div><dt class="text-stone-500">Current Folder</dt><dd class="break-all text-stone-300">${escapeHtml(preview.folder || t('details_placeholder_value'))}</dd></div>
              <div><dt class="text-stone-500">Filename Preview</dt><dd class="break-all text-stone-300">${escapeHtml(preview.filename)}</dd></div>
            </dl>
          </div>
          <div id="library-metadata-editor-validation" class="${errors.length ? '' : 'hidden'} rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-100">
            ${errors.map(error => `<p>${escapeHtml(error)}</p>`).join('')}
          </div>
        </div>
      </div>
      <div class="mt-4 flex flex-wrap gap-2">
        <button data-action="saveLibraryMetadataEditor" ${errors.length ? 'disabled' : ''} class="rounded-md bg-amber-500 px-4 py-2 text-sm font-medium text-stone-950 hover:bg-amber-400 disabled:cursor-not-allowed disabled:bg-stone-700 disabled:text-stone-400">Save</button>
        <button data-action="resetLibraryMetadataEditor" class="rounded-md bg-stone-800 px-4 py-2 text-sm font-medium text-stone-200 hover:bg-stone-700">Reset</button>
      </div>
    </div>
  `;
}

function openLibraryMetadataEditor() {
  const book = state.activeDetailBook;
  if (!book?.id || !normalizedLibraryMode()) return;
  state.libraryMetadataEditor = {
    open: true,
    draft: libraryMetadataDraftFromBook(book),
    errors: [],
  };
  const context = state.activeDetailContext || {};
  openBookDetails(context.index || 0, context.collection || 'libraryBooks');
}

function closeLibraryMetadataEditor() {
  state.libraryMetadataEditor = { open: false, draft: null, errors: [] };
  const context = state.activeDetailContext || {};
  openBookDetails(context.index || 0, context.collection || 'libraryBooks');
}

function resetLibraryMetadataEditor() {
  if (!state.activeDetailBook) return;
  state.libraryMetadataEditor.draft = libraryMetadataDraftFromBook(state.activeDetailBook);
  state.libraryMetadataEditor.errors = [];
  const context = state.activeDetailContext || {};
  openBookDetails(context.index || 0, context.collection || 'libraryBooks');
}

function updateLibraryMetadataEditorDraft(field, value) {
  if (!state.libraryMetadataEditor.open) return;
  state.libraryMetadataEditor.draft = state.libraryMetadataEditor.draft || {};
  state.libraryMetadataEditor.draft[field] = value;
  updateLibraryMetadataEditorValidation();
}

function updateLibraryMetadataEditorValidation() {
  const errors = validateLibraryMetadataDraft(state.activeDetailBook, state.libraryMetadataEditor.draft || {});
  state.libraryMetadataEditor.errors = errors;
  const validationEl = document.getElementById('library-metadata-editor-validation');
  if (validationEl) {
    validationEl.classList.toggle('hidden', errors.length === 0);
    validationEl.innerHTML = errors.map(error => `<p>${escapeHtml(error)}</p>`).join('');
  }
  const save = document.querySelector('[data-action="saveLibraryMetadataEditor"]');
  if (save) save.disabled = errors.length > 0;
}

function validateLibraryMetadataDraft(book, draft) {
  const candidate = libraryMetadataCandidate(book || {});
  return validateMetadataEditorDraft(candidate, {
    title: draft.title,
    author: draft.author || 'Existing author',
    publication_year: draft.publication_year,
    isbn: '',
  }).filter(error => !/destination/i.test(error));
}

function libraryMetadataPatchFromDraft(draft) {
  return {
    fields: {
      title: String(draft.title || '').trim(),
      edition_title: String(draft.edition_title || '').trim(),
      subtitle: String(draft.subtitle || '').trim(),
      publisher: String(draft.publisher || '').trim(),
      publication_date: String(draft.publication_year || '').trim(),
      language: String(draft.language || '').trim(),
      description: String(draft.description || '').trim(),
      genres: String(draft.tags || '').split(',').map(tag => tag.trim()).filter(Boolean),
    },
  };
}

async function saveLibraryMetadataEditor() {
  const book = state.activeDetailBook;
  const draft = state.libraryMetadataEditor.draft || {};
  if (!book?.id) return;
  const errors = validateLibraryMetadataDraft(book, draft);
  if (errors.length) {
    state.libraryMetadataEditor.errors = errors;
    updateLibraryMetadataEditorValidation();
    showToast(errors[0], 'error');
    return;
  }
  try {
    await apiJson(`/api/v1/books/${book.id}/metadata`, {
      method: 'PATCH',
      body: JSON.stringify(libraryMetadataPatchFromDraft(draft)),
    });
    state.libraryMetadataEditor = { open: false, draft: null, errors: [] };
    await loadLibrary();
    const context = state.activeDetailContext || {};
    const refreshedIndex = state.libraryBooks.findIndex(item => item.id === book.id);
    await openBookDetails(refreshedIndex >= 0 ? refreshedIndex : (context.index || 0), 'libraryBooks');
    showToast('Metadata saved', 'success');
  } catch (err) {
    showToast(err.message || 'Failed to update metadata', 'error');
  }
}

function libraryMetadataCandidate(book) {
  const files = Array.isArray(book?.files) ? book.files : [];
  const firstFile = files[0] || {};
  const format = String(firstFile.format || book?.formats?.[0] || 'book').toLowerCase();
  return {
    id: String(book?.id || 'book'),
    title: book?.title || '',
    author: book?.author || book?.primary_author?.name || '',
    format,
    path: firstFile.path || firstFile.originalPath || `${book?.title || 'book'}.${format}`,
    destination_path: firstFile.path || firstFile.originalPath || '',
    classification: 'library',
    metadata: {
      title: book?.metadata?.fields?.title?.value || book?.title || '',
      author: book?.author || '',
    },
  };
}

function publicationYearFromMetadata(value) {
  const match = String(value || '').match(/\d{4}/);
  return match ? match[0] : '';
}

function guessMetadataSource(book) {
  if (book.nativeV1) return 'Normalized library';
  if (book.coverUrl) return 'External library';
  if (book.files.some(file => file.source)) return 'Imported file';
  return t('details_placeholder_value');
}

function formatIdentifierList(identifiers) {
  if (!identifiers.length) return t('details_placeholder_value');
  return identifiers.map(identifier => `${identifier.type}: ${identifier.value}`).join(', ');
}

async function loadBookHistory(book) {
  const container = document.getElementById('detail-history');
  if (!container) return;
  try {
    const data = await apiJson('/api/activity?limit=100');
    const title = (book.title || '').toLowerCase();
    const matches = (data.events || []).filter(event => {
      const haystack = `${event.title || ''} ${event.detail || ''} ${event.subject || ''}`.toLowerCase();
      return title && haystack.includes(title);
    }).slice(0, 6);
    container.innerHTML = matches.length ? matches.map(renderActivityRow).join('') : `<div class="rounded-[1.25rem] border border-dashed border-stone-800 bg-stone-900/30 p-4 text-sm text-stone-500">${t('dashboard_empty')}</div>`;
  } catch (err) {
    container.innerHTML = `<div class="rounded-[1.25rem] border border-dashed border-stone-800 bg-stone-900/30 p-4 text-sm text-stone-500">${t('dashboard_empty')}</div>`;
  }
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
  bindLibraryImportChangeHandlers();
  loadConfig();
  loadSources();
  loadTOTPStatus();
  loadSettingToggles();
  updateLibraryRepairCardVisibility();
  showChangePasswordIfMultiUser();
  if (state.currentRole === 'admin') {
    loadUsers();
    loadInviteCodes();
  }
}

function updateLibraryRepairCardVisibility() {
  const card = document.getElementById('settings-library-repairs');
  if (!card) return;
  card.classList.toggle('hidden', !isAdminUser());
  renderNestedEbookPathRepair();
}

function renderNestedEbookPathRepair() {
  const out = document.getElementById('settings-library-repairs-output');
  if (!out) return;
  const repair = state.libraryRepair?.nestedEbookPaths || {};
  if (repair.loading || repair.running) {
    out.innerHTML = `<div class="rounded-lg border border-slate-700 bg-slate-800/40 px-4 py-3 text-sm text-slate-300">${repair.running ? 'Running repair…' : 'Building repair preview…'}</div>`;
    return;
  }
  if (repair.error) {
    out.innerHTML = `<div class="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">${escapeHtml(repair.error)}</div>`;
    return;
  }
  const plan = repair.result || repair.plan;
  if (!plan) {
    out.innerHTML = `<p class="text-sm text-slate-500">Run a preview to see cataloged files under the legacy nested ebook folder before making changes.</p>`;
    return;
  }
  const summary = plan.summary || {};
  const entries = Array.isArray(plan.entries) ? plan.entries : [];
  const visibleEntries = entries.slice(0, 25);
  out.innerHTML = `
    <div class="rounded-lg border border-slate-800 bg-slate-950/40 p-4">
      <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        ${renderRepairMetric('Affected files', plan.total_affected_files || 0)}
        ${renderRepairMetric('Ready', summary.ready || 0)}
        ${renderRepairMetric('Moved', summary.moved || 0)}
        ${renderRepairMetric('Needs attention', (summary.collision || 0) + (summary.missing || 0) + (summary.unsafe || 0) + (summary.failed || 0))}
      </div>
      <p class="mt-3 text-xs text-slate-500">Legacy root: ${escapeHtml(plan.legacy_root || '')}</p>
      ${plan.executed ? `<p class="mt-2 text-sm text-emerald-200">Repair complete. ${plan.legacy_root_removed ? 'The legacy nested directory was removed because it was empty.' : 'Non-empty legacy directories were preserved.'}</p>` : ''}
      <details class="mt-4">
        <summary class="cursor-pointer text-sm font-medium text-slate-200">File plan (${entries.length})</summary>
        <div class="mt-3 max-h-80 overflow-y-auto rounded-lg border border-slate-800">
          ${visibleEntries.length ? visibleEntries.map(renderRepairEntry).join('') : `<p class="px-3 py-2 text-sm text-slate-500">No nested ebook paths were found.</p>`}
        </div>
        ${entries.length > visibleEntries.length ? `<p class="mt-2 text-xs text-slate-500">Showing first ${visibleEntries.length} entries.</p>` : ''}
      </details>
    </div>
  `;
}

function renderRepairMetric(label, value) {
  return `
    <div class="rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-2">
      <p class="text-xs uppercase tracking-[0.18em] text-slate-500">${escapeHtml(label)}</p>
      <p class="mt-1 text-xl font-semibold text-white">${escapeHtml(value)}</p>
    </div>
  `;
}

function renderRepairEntry(entry) {
  const status = String(entry.status || '').replace(/_/g, ' ');
  const tone = repairStatusClass(entry.status);
  return `
    <div class="border-b border-slate-800 px-3 py-2 last:border-0">
      <div class="flex flex-wrap items-center gap-2">
        <span class="rounded-full px-2 py-0.5 text-[11px] font-medium ${tone}">${escapeHtml(status || 'unknown')}</span>
        <span class="text-sm font-medium text-slate-100">${escapeHtml(entry.book_title || `File ${entry.file_id || ''}`)}</span>
        ${entry.format ? `<span class="text-xs uppercase tracking-[0.18em] text-amber-300">${escapeHtml(entry.format)}</span>` : ''}
      </div>
      <p class="mt-1 truncate text-xs text-slate-500">${escapeHtml(entry.source_path || '')}</p>
      <p class="truncate text-xs text-slate-400">→ ${escapeHtml(entry.destination_path || '')}</p>
      ${entry.message ? `<p class="mt-1 text-xs text-slate-500">${escapeHtml(entry.message)}</p>` : ''}
    </div>
  `;
}

function repairStatusClass(status) {
  switch (status) {
    case 'ready':
      return 'bg-amber-500/15 text-amber-200';
    case 'moved':
      return 'bg-emerald-500/15 text-emerald-200';
    case 'already_repaired':
      return 'bg-sky-500/15 text-sky-200';
    case 'collision':
    case 'missing':
    case 'unsafe':
    case 'failed':
      return 'bg-red-500/15 text-red-200';
    default:
      return 'bg-slate-700 text-slate-200';
  }
}

async function previewNestedEbookPathRepair() {
  const repair = state.libraryRepair.nestedEbookPaths;
  repair.loading = true;
  repair.error = '';
  repair.result = null;
  renderNestedEbookPathRepair();
  try {
    repair.plan = await apiJson('/api/v1/library/repairs/nested-ebook-paths');
  } catch (err) {
    repair.error = err.message || 'Failed to preview nested ebook path repair';
  } finally {
    repair.loading = false;
    renderNestedEbookPathRepair();
  }
}

async function runNestedEbookPathRepair() {
  const repair = state.libraryRepair.nestedEbookPaths;
  const ready = repair.plan?.summary?.ready || 0;
  const affected = repair.plan?.total_affected_files || ready;
  const ok = window.confirm(`Move ${ready} cataloged files from the legacy nested ebook folder and update their Librarr paths?\n\nFiles with collisions or missing sources will be skipped.`);
  if (!ok) return;
  repair.running = true;
  repair.error = '';
  renderNestedEbookPathRepair();
  try {
    repair.result = await apiJson('/api/v1/library/repairs/nested-ebook-paths', { method: 'POST' });
    repair.plan = repair.result;
    await loadLibrary();
    showToast(`Nested ebook path repair complete: ${repair.result.summary?.moved || 0} moved, ${affected - ready} skipped.`, 'success');
  } catch (err) {
    repair.error = err.message || 'Failed to run nested ebook path repair';
    showToast(repair.error, 'error');
  } finally {
    repair.running = false;
    renderNestedEbookPathRepair();
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
  qbittorrent:    ['qb_url', 'qb_user', 'qb_pass', 'qb_save_path', 'qb_category', 'qb_audiobook_save_path', 'qb_audiobook_category', 'qb_manga_save_path', 'qb_manga_category'],
  transmission:   ['transmission_url', 'transmission_user', 'transmission_pass', 'torrent_client'],
  sabnzbd:        ['sabnzbd_url', 'sabnzbd_api_key', 'sabnzbd_category'],
  audiobookshelf: ['abs_url', 'abs_token'],
  kavita:         ['kavita_url', 'kavita_user', 'kavita_pass'],
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
    const fileOrgEnabled = document.getElementById('setting-file_org_enabled');
    if (fileOrgEnabled && data.file_org_enabled !== undefined) {
      fileOrgEnabled.checked = !!data.file_org_enabled;
    }
    for (const key of LIBRARY_IMPORT_FIELDS) {
      const el = document.getElementById(`setting-${key}`);
      if (el && data[key] !== undefined && data[key] !== null) {
        el.value = data[key];
      }
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
    applyLibraryImportLoadedState(getLibraryImportFormValues());
  } catch (err) {}
}

function getLibraryImportFormValues() {
  const values = {};
  for (const key of LIBRARY_IMPORT_FIELDS) {
    values[key] = document.getElementById(`setting-${key}`)?.value || '';
  }
  values.file_org_enabled = !!document.getElementById('setting-file_org_enabled')?.checked;
  return values;
}

function sanitizeLibraryImportValues(values) {
  return {
    incoming_dir: (values.incoming_dir || '').trim(),
    ebook_dir: (values.ebook_dir || '').trim(),
    audiobook_dir: (values.audiobook_dir || '').trim(),
    manga_dir: (values.manga_dir || '').trim(),
    file_org_enabled: !!values.file_org_enabled,
  };
}

function validateLibraryImportSettings(values) {
  const errors = [];
  const pathKeys = [
    ['incoming_dir', 'Import Folder'],
    ['ebook_dir', 'Ebook Library'],
    ['audiobook_dir', 'Audiobook Library'],
    ['manga_dir', 'Manga Library'],
  ];
  const normalized = [];

  for (const [key, label] of pathKeys) {
    const raw = values[key] || '';
    const trimmed = raw.trim();
    if (!trimmed) {
      errors.push(`${label} is required.`);
      continue;
    }
    if (raw !== trimmed) {
      errors.push(`${label} contains leading or trailing whitespace.`);
    }
    if (trimmed.includes('//')) {
      errors.push(`${label} contains a double slash.`);
    }
    normalized.push([label, trimmed]);
  }

  const seen = new Map();
  for (const [label, value] of normalized) {
    if (seen.has(value)) {
      errors.push(`${label} duplicates ${seen.get(value)}.`);
    } else {
      seen.set(value, label);
    }
  }

  return { errors };
}

function libraryImportSummaryLines(values) {
  return [
    ['Import Folder', values.incoming_dir || 'Not set'],
    ['Ebook Library', values.ebook_dir || 'Not set'],
    ['Audiobook Library', values.audiobook_dir || 'Not set'],
    ['Manga Library', values.manga_dir || 'Not set'],
  ];
}

function applyLibraryImportLoadedState(values) {
  const sanitized = sanitizeLibraryImportValues(values);
  const validation = validateLibraryImportSettings(sanitized);
  const completed = validation.errors.length === 0;
  state.libraryImport.completed = completed;
  state.libraryImport.dirty = false;
  state.libraryImport.lastSaved = sanitized;
  setLibraryImportStep2State(completed, completed ? sanitized : null);
  updateLibraryImportSaveState();
}

function setLibraryImportStep2State(unlocked, values = null) {
  const panel = document.getElementById('settings-library-import-step2');
  const icon = document.getElementById('settings-library-import-step2-icon');
  const copy = document.getElementById('settings-library-import-step2-copy');
  const summary = document.getElementById('settings-library-import-summary');
  if (!panel || !icon || !copy || !summary) return;

  panel.dataset.state = unlocked ? 'ready' : 'locked';
  panel.classList.toggle('opacity-75', !unlocked);
  panel.classList.toggle('border-slate-700', !unlocked);
  panel.classList.toggle('bg-slate-800/20', !unlocked);
  panel.classList.toggle('border-emerald-500/30', unlocked);
  panel.classList.toggle('bg-emerald-500/10', unlocked);

  icon.textContent = unlocked ? '✅' : '🔒';
  icon.className = unlocked ? 'mt-0.5 text-emerald-300' : 'mt-0.5 text-slate-500';
  copy.textContent = unlocked
    ? 'Your folders are saved. Librarr is ready to scan your existing collection.'
    : 'Available after saving your library folders.';
  renderLibraryScanWorkspace();

  if (!unlocked || !values) {
    summary.innerHTML = '';
    summary.classList.add('hidden');
    return;
  }

  summary.innerHTML = libraryImportSummaryLines(values).map(([label, value]) => `
    <div class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
      <span class="text-stone-400">${escapeHtml(label)}</span>
      <span class="font-medium text-stone-100">${escapeHtml(value)}</span>
    </div>
  `).join('');
  summary.classList.remove('hidden');
}

function renderLibraryScanWorkspace() {
  const workspace = document.getElementById('settings-library-scan-workspace');
  if (!workspace) return;
  if (!state.libraryImport.completed) {
    workspace.innerHTML = `
      <button disabled aria-disabled="true" class="cursor-not-allowed rounded-lg bg-slate-700/70 px-4 py-2 text-sm font-medium text-slate-400 opacity-70">Scan Library</button>
      <p class="mt-2 text-xs text-slate-500">Library scanning becomes available after folder configuration.</p>
    `;
    return;
  }
  const scan = state.libraryImport.scan;
  if (scan.running || scan.progress) {
    workspace.innerHTML = renderLibraryScanProgress(scan);
    return;
  }
  if (scan.result) {
    workspace.innerHTML = renderLibraryScanReview(scan.result);
    return;
  }
  if (scan.error) {
    workspace.innerHTML = renderLibraryScanError(scan.error);
    return;
  }
  workspace.innerHTML = renderLibraryScanReady();
}

function renderLibraryScanReady() {
  return `
    <div class="flex flex-wrap items-center gap-3">
      <button data-action="startLibraryScan" class="rounded-lg bg-amber-500 px-4 py-2 text-sm font-medium text-stone-950 transition-colors hover:bg-amber-400">Scan Library</button>
      <p class="text-xs text-slate-500">Review results here before importing. No files will be changed during scanning.</p>
    </div>
  `;
}

function renderLibraryScanProgress(scan) {
  const progress = scan.progress || {};
  const elapsed = formatLibraryScanElapsed(scan.startedAt || progress.started_at);
  const phase = progress.current_phase || progress.status || 'starting';
  return `
    <div class="rounded-lg border border-amber-500/20 bg-amber-500/8 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p class="text-sm font-medium text-amber-100">Scanning library...</p>
          <p class="mt-1 text-xs text-stone-400">${escapeHtml(formatLibraryScanPhase(phase))} · ${escapeHtml(elapsed)}</p>
        </div>
        <div class="h-5 w-5 animate-spin rounded-full border-2 border-amber-300 border-t-transparent" aria-label="Scanning"></div>
      </div>
      <div class="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
        ${renderLibraryScanProgressMetric('Directories', progress.directories_scanned || 0)}
        ${renderLibraryScanProgressMetric('Discovered', progress.files_discovered || 0)}
        ${renderLibraryScanProgressMetric('Processed', progress.files_processed || 0)}
        ${renderLibraryScanProgressMetric('Candidates', progress.candidates_ready || 0)}
      </div>
      ${progress.current_path ? `<p class="mt-3 truncate text-xs text-slate-500" title="${escapeHtml(progress.current_path)}">${escapeHtml(progress.current_path)}</p>` : ''}
    </div>
  `;
}

function renderLibraryScanProgressMetric(label, value) {
  return `
    <div class="rounded-md bg-slate-900/50 px-3 py-2">
      <p class="text-[11px] uppercase tracking-wider text-slate-500">${escapeHtml(label)}</p>
      <p class="mt-1 text-lg font-semibold text-white">${escapeHtml(String(value))}</p>
    </div>
  `;
}

function renderLibraryImportProgress(importState) {
  const progress = importState.progress || {};
  const total = progress.total || 0;
  const imported = progress.imported || 0;
  const duplicates = progress.duplicates || 0;
  const failed = progress.failed || 0;
  const completed = imported + duplicates + failed + (progress.skipped || 0);
  const percent = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 0;
  const elapsed = formatLibraryScanElapsed(importState.startedAt || progress.started_at);
  return `
    <div class="rounded-lg border border-emerald-500/20 bg-emerald-500/8 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p class="text-sm font-medium text-emerald-100">Importing selected books...</p>
          <p class="mt-1 text-xs text-stone-400">${escapeHtml(completed)} / ${escapeHtml(total)} books · ${escapeHtml(elapsed)}</p>
        </div>
        <div class="h-5 w-5 animate-spin rounded-full border-2 border-emerald-300 border-t-transparent" aria-label="Importing"></div>
      </div>
      <div class="mt-4 h-2 overflow-hidden rounded-full bg-slate-800">
        <div class="h-full rounded-full bg-emerald-400 transition-all" style="width:${percent}%"></div>
      </div>
      <div class="mt-4 grid grid-cols-3 gap-3">
        ${renderLibraryScanProgressMetric('Imported', imported)}
        ${renderLibraryScanProgressMetric('Duplicates', duplicates)}
        ${renderLibraryScanProgressMetric('Failed', failed)}
      </div>
      ${progress.current_title ? `<p class="mt-3 text-sm text-emerald-100">Current: ${escapeHtml(progress.current_title)}</p>` : ''}
    </div>
  `;
}

function renderLibraryImportSummary(result) {
  if (!result) return '';
  const summary = result.summary || {};
  const failed = summary.failed || 0;
  const failures = (result.items || []).filter(item => item.status === 'failed');
  const imported = summary.imported || 0;
  const skipped = summary.skipped || 0;
  const duplicates = summary.duplicates || 0;
  const total = summary.total || (result.items || []).length || 0;
  const elapsedSeconds = libraryImportElapsedSeconds(result.started_at, result.completed_at);
  const booksPerSecond = elapsedSeconds > 0 ? (imported / elapsedSeconds).toFixed(1) : '0.0';
  return `
    <div class="rounded-lg border border-emerald-500/20 bg-emerald-500/8 p-4">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p class="text-sm font-semibold text-emerald-100">Import Complete</p>
          <p class="mt-1 text-xs text-emerald-200">${escapeHtml(imported)} imported · ${escapeHtml(skipped)} skipped · ${escapeHtml(duplicates)} duplicates · ${escapeHtml(failed)} failed</p>
        </div>
        <div class="flex flex-wrap gap-2">
          <button data-action="viewImportedLibraryScan" class="rounded-md bg-emerald-500 px-3 py-2 text-xs font-medium text-stone-950 hover:bg-emerald-400">View Imported</button>
          <button data-action="retryFailedLibraryImport" ${failed ? '' : 'disabled'} class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700 disabled:cursor-not-allowed disabled:text-slate-500">Retry Failed</button>
          <button data-action="closeLibraryImportSummary" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700">Close</button>
        </div>
      </div>
      <div class="mt-3 grid grid-cols-2 gap-2 md:grid-cols-5">
        ${renderLibraryScanProgressMetric('Imported', imported)}
        ${renderLibraryScanProgressMetric('Skipped', skipped)}
        ${renderLibraryScanProgressMetric('Duplicates', duplicates)}
        ${renderLibraryScanProgressMetric('Failed', failed)}
        ${renderLibraryScanProgressMetric('Speed', `${booksPerSecond}/s`)}
      </div>
      <p class="mt-2 text-xs text-emerald-200">${escapeHtml(total)} reviewed · ${escapeHtml(formatDurationSeconds(elapsedSeconds))}</p>
      ${failed && failures.length ? `
        <details class="mt-3 rounded-md border border-red-500/20 bg-red-500/10 p-3">
          <summary class="cursor-pointer text-xs font-medium text-red-100">Show Details</summary>
          <div class="mt-2 space-y-2">
            ${failures.map(item => `<p class="text-xs text-red-200">${escapeHtml(item.title || item.path || 'Import candidate')}: ${escapeHtml(item.error || 'Import failed')}</p>`).join('')}
          </div>
        </details>
      ` : ''}
    </div>
  `;
}

function libraryImportElapsedSeconds(startedAt, completedAt) {
  const start = startedAt ? new Date(startedAt).getTime() : NaN;
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return 0;
  return Math.max(0, Math.round((end - start) / 1000));
}

function formatDurationSeconds(seconds) {
  const safe = Math.max(0, Number(seconds) || 0);
  if (safe < 60) return `${safe}s elapsed`;
  return `${Math.floor(safe / 60)}m ${safe % 60}s elapsed`;
}

function renderLibraryScanError(message) {
  return `
    <div class="rounded-lg border border-red-500/30 bg-red-500/10 p-4">
      <p class="text-sm font-medium text-red-100">Library scan failed</p>
      <p class="mt-1 text-xs text-red-200">${escapeHtml(message || 'Unknown scan error')}</p>
      <button data-action="startLibraryScan" class="mt-3 rounded-lg bg-red-500 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-red-400">Retry</button>
    </div>
  `;
}

function renderLibraryScanReview(result) {
  const candidates = Array.isArray(result.candidates) ? result.candidates : [];
  const totals = result.totals || {};
  const filtered = filterLibraryScanCandidates(candidates, state.libraryImport.scan.filter, state.libraryImport.scan.search);
  const grouped = groupLibraryScanCandidates(filtered);
  const importState = state.libraryImport.scan.import;
  if ((totals.found || 0) === 0) {
    return `
      ${renderLibraryScanToolbar()}
      <div class="rounded-lg border border-slate-700 bg-slate-900/60 p-5 text-center">
        <p class="text-sm font-medium text-white">No books found</p>
        <p class="mt-1 text-xs text-slate-500">Librarr scanned the configured folders but did not find supported files.</p>
        <button data-action="startLibraryScan" class="mt-4 rounded-lg bg-slate-800 px-4 py-2 text-sm font-medium text-slate-200 hover:bg-slate-700">Scan Again</button>
      </div>
    `;
  }
  const allDuplicates = (totals.ready_to_import || 0) === 0 && ((totals.duplicates || 0) + (totals.already_imported || 0)) === (totals.found || 0);
  return `
    <div class="space-y-4">
      ${allDuplicates ? `<div class="rounded-lg border border-amber-500/20 bg-amber-500/8 px-4 py-3 text-sm text-amber-100">Everything found in this scan already appears to be in the library.</div>` : ''}
      ${importState.running || importState.progress ? renderLibraryImportProgress(importState) : ''}
      ${importState.error ? `<div class="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">${escapeHtml(importState.error)}</div>` : ''}
      ${importState.result ? renderLibraryImportSummary(importState.result) : ''}
      ${renderLibraryScanTotals(totals)}
      ${renderLibraryScanImportActions(result)}
      ${renderLibraryScanToolbar()}
      <div class="space-y-3">
        ${renderLibraryScanSection('Ready to Import', 'new', grouped.new || [])}
        ${renderLibraryScanSection('Manual Review', 'manual_review', grouped.manual_review || [])}
        ${renderLibraryScanSection('Duplicates', 'duplicate', grouped.duplicate || [])}
        ${renderLibraryScanSection('Already Imported', 'already_imported', grouped.already_imported || [])}
        ${renderLibraryScanSection('Unsupported', 'unsupported', grouped.unsupported || [])}
        ${renderLibraryScanSection('Unreadable', 'unreadable', grouped.unreadable || [])}
      </div>
    </div>
  `;
}

function renderLibraryScanImportActions(result) {
  const ready = readyLibraryScanCandidates(result);
  const selected = selectedReadyLibraryScanCandidates(result);
  const importing = state.libraryImport.scan.import.running;
  const allSelected = ready.length > 0 && selected.length === ready.length;
  const disabledSelected = importing || selected.length === 0;
  const disabledReady = importing || ready.length === 0;
  return `
    <div class="flex flex-col gap-3 rounded-lg border border-slate-800 bg-slate-900/50 p-3 md:flex-row md:items-center md:justify-between">
      <label class="inline-flex items-center gap-2 text-sm text-slate-200">
        <input data-action-change="toggleLibraryScanSelectAllReady" type="checkbox" ${allSelected ? 'checked' : ''} ${ready.length === 0 || importing ? 'disabled' : ''} class="rounded border-slate-600 bg-slate-900 text-amber-500">
        <span>Select All Ready</span>
      </label>
      <div class="flex flex-wrap gap-2">
        <button data-action="startLibraryImportSelected" ${disabledSelected ? 'disabled' : ''} class="rounded-md bg-emerald-500 px-3 py-2 text-xs font-medium text-stone-950 transition-colors hover:bg-emerald-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400">Import Selected</button>
        <button data-action="startLibraryImportAllReady" ${disabledReady ? 'disabled' : ''} class="rounded-md bg-amber-500 px-3 py-2 text-xs font-medium text-stone-950 transition-colors hover:bg-amber-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400">Import All Ready</button>
        <button data-action="clearLibraryScanSelection" ${disabledSelected ? 'disabled' : ''} class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700 disabled:cursor-not-allowed disabled:text-slate-500">Clear Selection</button>
        <button data-action="skipLibraryScanSelected" ${disabledSelected ? 'disabled' : ''} class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700 disabled:cursor-not-allowed disabled:text-slate-500">Skip Selected</button>
      </div>
    </div>
  `;
}

function renderLibraryScanTotals(totals) {
  const cards = [
    ['Files Found', totals.found || 0],
    ['Ready to Import', totals.ready_to_import || 0],
    ['Needs Review', totals.manual_review || 0],
    ['Duplicates', totals.duplicates || 0],
    ['Already Imported', totals.already_imported || 0],
    ['Unsupported', totals.unsupported || 0],
    ['Unreadable', totals.unreadable || 0],
  ];
  return `<div class="grid grid-cols-2 gap-2 md:grid-cols-3">${cards.map(([label, value]) => `
    <div class="rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-3">
      <p class="text-[11px] uppercase tracking-wider text-slate-500">${escapeHtml(label)}</p>
      <p class="mt-1 text-2xl font-semibold text-white">${escapeHtml(String(value))}</p>
    </div>
  `).join('')}</div>`;
}

function renderLibraryScanToolbar() {
  const filter = state.libraryImport.scan.filter;
  const buttons = [
    ['all', 'All'],
    ['new', 'Ready'],
    ['manual_review', 'Review'],
    ['duplicate', 'Duplicates'],
    ['unsupported', 'Unsupported'],
    ['unreadable', 'Unreadable'],
  ];
  return `
    <div class="flex flex-col gap-3 rounded-lg border border-slate-800 bg-slate-900/50 p-3 md:flex-row md:items-center md:justify-between">
      <div class="flex flex-wrap gap-2">
        ${buttons.map(([value, label]) => `<button data-action="setLibraryScanFilter" data-filter="${escapeHtml(value)}" class="rounded-md px-3 py-1.5 text-xs font-medium ${filter === value ? 'bg-amber-500 text-stone-950' : 'bg-slate-800 text-slate-300 hover:bg-slate-700'}">${escapeHtml(label)}</button>`).join('')}
      </div>
      <div class="flex gap-2">
        <input id="settings-library-scan-search" data-action-input="libraryScanSearch" type="search" value="${escapeHtml(state.libraryImport.scan.search || '')}" placeholder="Search title, author, filename" class="w-full rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-200 placeholder-slate-600 md:w-72">
        <button data-action="startLibraryScan" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700">Scan Again</button>
      </div>
    </div>
  `;
}

function renderLibraryScanSection(title, classification, candidates) {
  const open = state.libraryImport.scan.sections[classification] !== false;
  return `
    <section class="rounded-lg border border-slate-800 bg-slate-900/45">
      <button data-action="toggleLibraryScanSection" data-section="${escapeHtml(classification)}" class="flex w-full items-center justify-between px-4 py-3 text-left">
        <span class="text-sm font-medium text-white">${escapeHtml(title)}</span>
        <span class="text-xs text-slate-500">${candidates.length} ${open ? 'Hide' : 'Show'}</span>
      </button>
      <div class="${open ? '' : 'hidden'} border-t border-slate-800">
        ${candidates.length ? candidates.map(renderLibraryScanCandidate).join('') : `<p class="px-4 py-4 text-sm text-slate-500">No items in this section.</p>`}
      </div>
    </section>
  `;
}

function renderLibraryScanCandidate(candidate) {
  const title = candidate.title || candidate.metadata?.title || candidate.filename || 'Unknown title';
  const author = candidate.author || candidate.metadata?.author || '';
  const format = (candidate.format || '').toUpperCase();
  const path = candidate.path || candidate.relative_path || '';
  const isReady = candidate.classification === 'new';
  const existingPath = candidate.existing_path || '';
  const selected = state.libraryImport.scan.selected.has(candidate.id);
  const importing = state.libraryImport.scan.import.running;
  const metadataSource = formatMetadataSource(candidate.metadata?.source || candidate.manual_review?.metadata_source || '');
  const confidence = formatMetadataConfidence(candidate.metadata?.confidence || candidate.manual_review?.confidence || '');
  return `
    <div class="grid gap-3 border-b border-slate-800 px-4 py-3 last:border-b-0 md:grid-cols-[auto_3rem_1fr]">
      <div class="flex items-start pt-1">${isReady ? `<input data-action-change="toggleLibraryScanCandidateSelection" data-candidate-id="${escapeHtml(candidate.id)}" type="checkbox" ${selected ? 'checked' : ''} ${importing ? 'disabled' : ''} aria-label="Select ${escapeHtml(title)}" class="rounded border-slate-600 bg-slate-900 text-amber-500">` : ''}</div>
      ${renderLibraryScanCover(candidate, format)}
      <div class="min-w-0">
        <div class="flex flex-wrap items-start justify-between gap-2">
          <div class="min-w-0">
            <p class="truncate text-sm font-medium text-white">${escapeHtml(title)}</p>
            <p class="mt-0.5 truncate text-xs text-slate-400">${escapeHtml(author || candidate.media_type || '')}</p>
          </div>
          <div class="flex flex-wrap justify-end gap-1">
            <span class="rounded-full bg-slate-800 px-2 py-1 text-[11px] uppercase tracking-wider text-slate-300">${escapeHtml(format || candidate.media_type || '')}</span>
            ${metadataSource ? `<span class="rounded-full bg-blue-500/10 px-2 py-1 text-[11px] text-blue-200">${escapeHtml(metadataSource)}</span>` : ''}
            ${confidence ? `<span class="rounded-full bg-amber-500/10 px-2 py-1 text-[11px] text-amber-200">${escapeHtml(confidence)}</span>` : ''}
          </div>
        </div>
        ${candidate.classification_reason ? `<p class="mt-2 text-xs text-amber-200/80">${escapeHtml(candidate.classification_reason)}</p>` : ''}
        ${candidate.error ? `<p class="mt-2 text-xs text-red-300">${escapeHtml(candidate.error)}</p>` : ''}
        ${candidate.destination_path ? renderLibraryScanDestination(candidate.destination_path) : ''}
        ${candidate.duplicate ? renderLibraryScanDuplicate(candidate.duplicate) : ''}
        ${candidate.manual_review ? renderLibraryScanManualReview(candidate, title, author) : ''}
        ${isReady ? `<div class="mt-3"><button data-action="editLibraryScanCandidateMetadata" data-candidate-id="${escapeHtml(candidate.id)}" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700">Edit Metadata</button></div>` : ''}
        ${state.libraryImport.scan.editor.candidateId === candidate.id ? renderLibraryScanMetadataEditor(candidate) : ''}
        ${existingPath ? `<p class="mt-2 truncate text-xs text-slate-400" title="${escapeHtml(existingPath)}">Existing: ${escapeHtml(existingPath)}</p>` : ''}
        <p class="mt-2 truncate text-xs text-slate-500" title="${escapeHtml(path)}">${escapeHtml(path)}</p>
      </div>
    </div>
  `;
}

function renderLibraryScanCover(candidate, format) {
  const title = candidate.title || candidate.metadata?.title || candidate.filename || '';
  if (candidate.cover_url) {
    return `<img src="${escapeHtml(candidate.cover_url)}" alt="" class="h-12 w-9 rounded-md object-cover" loading="lazy" data-ph-title="${escapeHtml(title)}" data-ph-idx="0">`;
  }
  return `<div class="flex h-12 w-9 items-center justify-center rounded-md bg-gradient-to-br from-stone-700 to-slate-900 text-xs font-semibold text-stone-300">${escapeHtml(format || '?')}</div>`;
}

function renderLibraryScanDestination(destination) {
  const parts = String(destination || '').split('/').filter(Boolean);
  const pretty = parts.length ? parts.join(' → ') : destination;
  return `
    <div class="mt-3 rounded-md border border-slate-800 bg-slate-950/60 p-3">
      <p class="text-[11px] uppercase tracking-wider text-slate-500">Destination</p>
      <p class="mt-1 break-all text-xs text-slate-300">${escapeHtml(pretty)}</p>
    </div>
  `;
}

function renderLibraryScanDuplicate(duplicate) {
  return `
    <div class="mt-3 rounded-md border border-amber-500/20 bg-amber-500/8 p-3">
      <p class="text-xs font-semibold text-amber-100">Duplicate</p>
      <dl class="mt-2 grid gap-1 text-xs text-amber-100/80 md:grid-cols-2">
        <div><dt class="text-amber-200/70">Reason</dt><dd>${escapeHtml(duplicate.reason || 'Duplicate detected')}</dd></div>
        ${duplicate.existing_title ? `<div><dt class="text-amber-200/70">Title</dt><dd>${escapeHtml(duplicate.existing_title)}</dd></div>` : ''}
        ${duplicate.existing_author ? `<div><dt class="text-amber-200/70">Author</dt><dd>${escapeHtml(duplicate.existing_author)}</dd></div>` : ''}
        ${duplicate.existing_format ? `<div><dt class="text-amber-200/70">Format</dt><dd>${escapeHtml(String(duplicate.existing_format).toUpperCase())}</dd></div>` : ''}
        ${duplicate.existing_path ? `<div class="md:col-span-2"><dt class="text-amber-200/70">Location</dt><dd class="break-all">${escapeHtml(duplicate.existing_path)}</dd></div>` : ''}
      </dl>
    </div>
  `;
}

function renderLibraryScanManualReview(candidate, title, author) {
  const review = candidate.manual_review || {};
  const canMergeMatches = /multiple existing books/i.test(review.reason || candidate.classification_reason || '');
  return `
    <div class="mt-3 rounded-md border border-orange-500/25 bg-orange-500/8 p-3">
      <p class="text-xs font-semibold text-orange-100">Manual Review Required</p>
      <dl class="mt-2 grid gap-1 text-xs text-orange-100/80 md:grid-cols-2">
        <div><dt class="text-orange-200/70">Reason</dt><dd>${escapeHtml(review.reason || candidate.classification_reason || 'Planner stopped for manual review')}</dd></div>
        <div><dt class="text-orange-200/70">Confidence</dt><dd>${escapeHtml(formatMetadataConfidence(review.confidence || candidate.metadata?.confidence || 'unknown'))}</dd></div>
        <div><dt class="text-orange-200/70">Metadata Source</dt><dd>${escapeHtml(formatMetadataSource(review.metadata_source || candidate.metadata?.source || 'unknown'))}</dd></div>
        ${review.suggested_destination ? `<div class="md:col-span-2"><dt class="text-orange-200/70">Suggested Destination</dt><dd class="break-all">${escapeHtml(review.suggested_destination)}</dd></div>` : ''}
      </dl>
      <div class="mt-3 flex flex-wrap gap-2">
        ${canMergeMatches ? `<button data-action="mergeMatchingLibraryScanBooks" data-candidate-id="${escapeHtml(candidate.id)}" class="rounded-md bg-amber-500 px-3 py-2 text-xs font-medium text-stone-950 hover:bg-amber-400">Merge Matching Books</button>` : ''}
        <button data-action="useSuggestedLibraryScanCandidate" data-candidate-id="${escapeHtml(candidate.id)}" class="rounded-md bg-orange-500 px-3 py-2 text-xs font-medium text-stone-950 hover:bg-orange-400">Use Suggested</button>
        <button data-action="editLibraryScanCandidateMetadata" data-candidate-id="${escapeHtml(candidate.id)}" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700">Edit Metadata</button>
        <button data-action="skipLibraryScanCandidate" data-candidate-id="${escapeHtml(candidate.id)}" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700">Skip</button>
      </div>
    </div>
  `;
}

function renderLibraryScanMetadataEditor(candidate) {
  const draft = state.libraryImport.scan.editor.draft || metadataEditorDraftFromCandidate(candidate);
  const preview = metadataEditorPreview(candidate, draft);
  const errors = validateMetadataEditorDraft(candidate, draft);
  state.libraryImport.scan.editor.errors = errors;
  const field = (name, label, value, attrs = '') => `
    <label class="block">
      <span class="text-[11px] uppercase tracking-wider text-slate-500">${escapeHtml(label)}</span>
      <input data-action-input="metadataEditorField" data-field="${escapeHtml(name)}" value="${escapeHtml(value || '')}" ${attrs} class="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 placeholder-slate-600 focus:border-amber-400 focus:outline-none">
    </label>
  `;
  return `
    <div id="metadata-editor-${escapeHtml(candidate.id)}" class="mt-3 rounded-xl border border-amber-500/25 bg-slate-950/80 p-4 shadow-xl shadow-black/20">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p class="text-sm font-semibold text-white">Metadata Editor</p>
          <p class="mt-1 text-xs text-slate-400">Adjust the metadata Librarr will use for this import. The source file is not modified.</p>
        </div>
        <button data-action="cancelMetadataEditor" class="rounded-md bg-slate-800 px-3 py-2 text-xs font-medium text-slate-300 hover:bg-slate-700">Cancel</button>
      </div>
      <div class="mt-4 grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
        <div class="space-y-4">
          <div class="grid gap-3 md:grid-cols-2">
            <div class="md:col-span-2">${field('title', 'Title', draft.title, 'required')}</div>
            ${field('subtitle', 'Subtitle', draft.subtitle)}
            ${field('author', 'Author', draft.author, 'required')}
            ${field('series', 'Series', draft.series)}
            ${field('series_number', 'Series Number', draft.series_number)}
            ${field('publisher', 'Publisher', draft.publisher)}
            ${field('publication_year', 'Publication Year', draft.publication_year, 'inputmode="numeric" maxlength="4"')}
            ${field('isbn', 'ISBN', draft.isbn)}
            ${field('language', 'Language', draft.language)}
            ${field('library', 'Library', draft.library)}
          </div>
          <label class="block">
            <span class="text-[11px] uppercase tracking-wider text-slate-500">Description</span>
            <textarea data-action-input="metadataEditorField" data-field="description" rows="4" class="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 placeholder-slate-600 focus:border-amber-400 focus:outline-none">${escapeHtml(draft.description || '')}</textarea>
          </label>
          ${field('tags', 'Tags', draft.tags, 'placeholder="fantasy, fiction, owned"')}
        </div>
        <div class="space-y-3">
          <div class="overflow-hidden rounded-lg border border-slate-800 bg-slate-900/80 p-3">
            <p class="mb-2 text-xs font-semibold text-slate-100">Cover Preview</p>
            <div class="w-24 overflow-hidden rounded-md">${renderLibraryScanCover(candidate, (candidate.format || '').toUpperCase())}</div>
          </div>
          <div class="rounded-lg border border-slate-800 bg-slate-900/80 p-3">
            <p class="text-xs font-semibold text-slate-100">Live Import Preview</p>
            <dl class="mt-3 space-y-2 text-xs">
              <div><dt class="text-slate-500">Destination Folder</dt><dd id="metadata-editor-destination-folder" class="break-all text-slate-200">${escapeHtml(preview.folder)}</dd></div>
              <div><dt class="text-slate-500">Filename</dt><dd id="metadata-editor-filename" class="break-all text-slate-200">${escapeHtml(preview.filename)}</dd></div>
              <div><dt class="text-slate-500">Import Location</dt><dd id="metadata-editor-import-location" class="break-all text-amber-100">${escapeHtml(preview.path)}</dd></div>
            </dl>
          </div>
          <div id="metadata-editor-validation" class="${errors.length ? '' : 'hidden'} rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-100">
            ${errors.map(error => `<p>${escapeHtml(error)}</p>`).join('')}
          </div>
          <div class="rounded-lg border border-slate-800 bg-slate-900/60 p-3 text-xs text-slate-400">
            <p>Metadata Source: ${escapeHtml(formatMetadataSource(candidate.metadata?.source || candidate.manual_review?.metadata_source || 'filename_fallback'))}</p>
            <p class="mt-1">Confidence: ${escapeHtml(formatMetadataConfidence(candidate.metadata?.confidence || candidate.manual_review?.confidence || 'unknown'))}</p>
          </div>
        </div>
      </div>
      <div class="mt-4 flex flex-wrap gap-2">
        <button data-action="saveMetadataEditor" ${errors.length ? 'disabled' : ''} class="rounded-md bg-amber-500 px-4 py-2 text-sm font-medium text-stone-950 hover:bg-amber-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400">Save</button>
        <button data-action="saveAndImportMetadataEditor" ${errors.length ? 'disabled' : ''} class="rounded-md bg-emerald-500 px-4 py-2 text-sm font-medium text-stone-950 hover:bg-emerald-400 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400">Save & Import</button>
        <button data-action="resetMetadataEditor" class="rounded-md bg-slate-800 px-4 py-2 text-sm font-medium text-slate-200 hover:bg-slate-700">Reset</button>
      </div>
    </div>
  `;
}

function findLibraryScanCandidate(candidateID) {
  return (state.libraryImport.scan.result?.candidates || []).find(candidate => candidate.id === candidateID) || null;
}

function metadataEditorDraftFromCandidate(candidate) {
  const metadata = candidate?.metadata || {};
  return {
    title: candidate?.title || metadata.title || candidate?.filename || '',
    subtitle: metadata.subtitle || '',
    author: candidate?.author || metadata.author || '',
    series: candidate?.series || metadata.series || '',
    series_number: metadata.series_number || candidate?.volume || metadata.volume || '',
    publisher: metadata.publisher || '',
    publication_year: metadata.publication_year || '',
    isbn: metadata.isbn || '',
    language: metadata.language || '',
    description: metadata.description || '',
    tags: Array.isArray(metadata.tags) ? metadata.tags.join(', ') : (metadata.tags || ''),
    library: metadata.library || candidate?.media_type || '',
  };
}

function openScanMetadataEditor(candidateID) {
  const candidate = findLibraryScanCandidate(candidateID);
  if (!candidate) return;
  state.libraryImport.scan.editor = {
    candidateId: candidateID,
    draft: metadataEditorDraftFromCandidate(candidate),
    errors: [],
  };
  renderLibraryScanWorkspace();
  document.getElementById(`metadata-editor-${candidateID}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function closeMetadataEditor() {
  state.libraryImport.scan.editor = { candidateId: '', draft: null, errors: [] };
  renderLibraryScanWorkspace();
}

function resetMetadataEditor() {
  const candidate = findLibraryScanCandidate(state.libraryImport.scan.editor.candidateId);
  if (!candidate) return;
  state.libraryImport.scan.editor.draft = metadataEditorDraftFromCandidate(candidate);
  renderLibraryScanWorkspace();
}

function updateMetadataEditorDraft(field, value) {
  const editor = state.libraryImport.scan.editor;
  if (!editor.candidateId) return;
  editor.draft = editor.draft || {};
  editor.draft[field] = value;
  updateMetadataEditorPreview();
}

function updateMetadataEditorPreview() {
  const editor = state.libraryImport.scan.editor;
  const candidate = findLibraryScanCandidate(editor.candidateId);
  if (!candidate || !editor.draft) return;
  const preview = metadataEditorPreview(candidate, editor.draft);
  const errors = validateMetadataEditorDraft(candidate, editor.draft);
  editor.errors = errors;
  const folderEl = document.getElementById('metadata-editor-destination-folder');
  const fileEl = document.getElementById('metadata-editor-filename');
  const pathEl = document.getElementById('metadata-editor-import-location');
  const validationEl = document.getElementById('metadata-editor-validation');
  if (folderEl) folderEl.textContent = preview.folder;
  if (fileEl) fileEl.textContent = preview.filename;
  if (pathEl) pathEl.textContent = preview.path;
  if (validationEl) {
    validationEl.classList.toggle('hidden', errors.length === 0);
    validationEl.innerHTML = errors.map(error => `<p>${escapeHtml(error)}</p>`).join('');
  }
  for (const action of ['saveMetadataEditor', 'saveAndImportMetadataEditor']) {
    const button = document.querySelector(`[data-action="${action}"]`);
    if (button) button.disabled = errors.length > 0;
  }
}

function metadataEditorPreview(candidate, draft) {
  const base = candidate.destination_path || candidate.path || candidate.filename || '';
  const folder = (pathDirname(base) || pathDirname(candidate.path || '') || '').replace(/\/(ebooks|audiobooks|manga)\/\1(?=\/|$)/gi, '/$1');
  const extension = (candidate.format || pathExtension(candidate.path || candidate.filename || '') || 'book').replace(/^\./, '').toLowerCase();
  const title = safeFilenamePart(draft.title || candidate.title || candidate.filename || 'Untitled');
  const author = safeFilenamePart(draft.author || candidate.author || '');
  const filename = `${author ? `${author} - ` : ''}${title}.${extension}`;
  return {
    folder,
    filename,
    path: folder ? `${folder.replace(/\/+$/, '')}/${filename}` : filename,
  };
}

function validateMetadataEditorDraft(candidate, draft) {
  const errors = [];
  if (!String(draft.title || '').trim()) errors.push('Title is required.');
  if (!String(draft.author || '').trim()) errors.push('Author is required.');
  const year = String(draft.publication_year || '').trim();
  if (year && !/^\d{4}$/.test(year)) errors.push('Publication year must be a four-digit year.');
  const isbn = String(draft.isbn || '').trim();
  if (isbn) {
    const normalized = isbn.replace(/[\s-]/g, '').toUpperCase();
    if (!/^[0-9X]+$/.test(normalized) || ![10, 13].includes(normalized.length)) {
      errors.push('ISBN must look like ISBN-10 or ISBN-13.');
    }
  }
  const preview = metadataEditorPreview(candidate, draft);
  if (!preview.folder || !preview.filename || !preview.path) errors.push('Destination preview is empty.');
  const duplicate = (state.libraryImport.scan.result?.candidates || []).some(other => (
    other.id !== candidate.id &&
    other.classification === 'new' &&
    String(other.destination_path || '').toLowerCase() === preview.path.toLowerCase()
  ));
  if (duplicate) errors.push('Another ready item already uses this destination filename.');
  return errors;
}

function metadataEditorPayload() {
  const editor = state.libraryImport.scan.editor;
  const draft = editor.draft || {};
  return {
    title: String(draft.title || '').trim(),
    subtitle: String(draft.subtitle || '').trim(),
    author: String(draft.author || '').trim(),
    series: String(draft.series || '').trim(),
    series_number: String(draft.series_number || '').trim(),
    publisher: String(draft.publisher || '').trim(),
    publication_year: String(draft.publication_year || '').trim(),
    isbn: String(draft.isbn || '').trim(),
    language: String(draft.language || '').trim(),
    description: String(draft.description || '').trim(),
    tags: String(draft.tags || '').split(',').map(tag => tag.trim()).filter(Boolean),
    library: String(draft.library || '').trim(),
  };
}

async function saveMetadataEditor(importAfterSave = false) {
  const editor = state.libraryImport.scan.editor;
  const candidate = findLibraryScanCandidate(editor.candidateId);
  if (!candidate) return;
  const errors = validateMetadataEditorDraft(candidate, editor.draft || {});
  if (errors.length) {
    editor.errors = errors;
    updateMetadataEditorPreview();
    showToast(errors[0], 'error');
    return;
  }
  const result = await resolveLibraryScanCandidate(editor.candidateId, 'edit_metadata', metadataEditorPayload(), { keepEditorOpen: false });
  const updated = result?.candidates?.find(item => item.id === editor.candidateId);
  if (importAfterSave && updated) {
    await startLibraryImport(false, [updated]);
  }
}

function pathExtension(path) {
  const name = String(path || '').split('/').pop() || '';
  const idx = name.lastIndexOf('.');
  return idx >= 0 ? name.slice(idx + 1) : '';
}

function pathDirname(path) {
  const clean = String(path || '').replace(/\/+$/, '');
  const idx = clean.lastIndexOf('/');
  if (idx <= 0) return idx === 0 ? '/' : '';
  return clean.slice(0, idx);
}

function safeFilenamePart(value) {
  return String(value || '')
    .trim()
    .replace(/[\\/:]/g, ' -')
    .replace(/\s+/g, ' ')
    .replace(/^[. ]+|[. ]+$/g, '') || 'Untitled';
}

function formatMetadataSource(source) {
  const labels = {
    embedded_metadata: 'Embedded metadata',
    filename_fallback: 'Filename parsing',
    manual_edit: 'Manual edit',
    pdf_metadata: 'PDF metadata',
  };
  return labels[source] || String(source || '').replace(/_/g, ' ');
}

function formatMetadataConfidence(confidence) {
  if (!confidence) return '';
  return String(confidence).replace(/\b\w/g, ch => ch.toUpperCase());
}

function filterLibraryScanCandidates(candidates, filter = 'all', search = '') {
  const query = String(search || '').trim().toLowerCase();
  const skipped = state.libraryImport.scan.skipped || new Set();
  return candidates.filter(candidate => {
    if (skipped.has(candidate.id)) return false;
    if (filter !== 'all' && candidate.classification !== filter) return false;
    if (!query) return true;
    const haystack = [
      candidate.title,
      candidate.author,
      candidate.filename,
      candidate.metadata?.title,
      candidate.metadata?.author,
    ].join(' ').toLowerCase();
    return haystack.includes(query);
  });
}

function groupLibraryScanCandidates(candidates) {
  return candidates.reduce((groups, candidate) => {
    const key = candidate.classification || 'new';
    if (!groups[key]) groups[key] = [];
    groups[key].push(candidate);
    return groups;
  }, {});
}

function readyLibraryScanCandidates(result) {
  const skipped = state.libraryImport.scan.skipped || new Set();
  return (result?.candidates || []).filter(candidate => candidate.classification === 'new' && !skipped.has(candidate.id));
}

function selectedReadyLibraryScanCandidates(result) {
  const selected = state.libraryImport.scan.selected || new Set();
  return readyLibraryScanCandidates(result).filter(candidate => selected.has(candidate.id));
}

function formatLibraryScanPhase(phase) {
  return String(phase || 'starting').replace(/_/g, ' ').replace(/\b\w/g, ch => ch.toUpperCase());
}

function formatLibraryScanElapsed(startedAt) {
  const start = startedAt ? new Date(startedAt).getTime() : Date.now();
  const elapsed = Math.max(0, Math.floor((Date.now() - start) / 1000));
  if (elapsed < 60) return `${elapsed}s elapsed`;
  return `${Math.floor(elapsed / 60)}m ${elapsed % 60}s elapsed`;
}

async function startLibraryScan() {
  const scan = state.libraryImport.scan;
  if (!state.libraryImport.completed || scan.running) return;
  if (scan.pollTimer) {
    window.clearTimeout(scan.pollTimer);
  }
  scan.running = true;
  scan.jobId = '';
  scan.startedAt = new Date().toISOString();
  scan.progress = { status: 'pending', current_phase: 'starting', started_at: scan.startedAt };
  scan.result = null;
  scan.error = '';
  scan.selected.clear();
  scan.skipped.clear();
  scan.editor = { candidateId: '', draft: null, errors: [] };
  scan.import = {
    running: false,
    jobId: '',
    pollTimer: null,
    startedAt: null,
    progress: null,
    result: null,
    error: '',
  };
  renderLibraryScanWorkspace();
  try {
    const data = await apiJson('/api/v1/library/scan', { method: 'POST' });
    scan.jobId = data.job_id || data.job?.id || '';
    if (data.job?.progress) {
      scan.progress = data.job.progress;
      scan.startedAt = data.job.started_at || scan.startedAt;
    }
    await pollLibraryScanJob();
  } catch (err) {
    scan.running = false;
    scan.progress = null;
    scan.error = err.message || 'Failed to start library scan';
    renderLibraryScanWorkspace();
    showToast(scan.error, 'error');
  }
}

async function pollLibraryScanJob() {
  const scan = state.libraryImport.scan;
  if (!scan.running || !scan.jobId) return;
  try {
    const job = await apiJson(`/api/v1/library/scan/${encodeURIComponent(scan.jobId)}`);
    scan.progress = job.progress || scan.progress;
    scan.startedAt = job.started_at || scan.startedAt;
    if (job.status === 'completed') {
      await loadLibraryScanResults(scan.jobId);
      return;
    }
    if (job.status === 'failed' || job.status === 'cancelled') {
      scan.running = false;
      scan.progress = null;
      scan.error = job.error || `Library scan ${job.status}`;
      renderLibraryScanWorkspace();
      showToast(scan.error, 'error');
      return;
    }
    renderLibraryScanWorkspace();
    scan.pollTimer = window.setTimeout(pollLibraryScanJob, 1200);
  } catch (err) {
    scan.running = false;
    scan.error = err.message || 'Failed to poll library scan';
    renderLibraryScanWorkspace();
    showToast(scan.error, 'error');
  }
}

async function loadLibraryScanResults(jobId) {
  const scan = state.libraryImport.scan;
  const result = await apiJson(`/api/v1/library/scan/${encodeURIComponent(jobId)}/results`);
  scan.running = false;
  scan.progress = null;
  scan.result = result;
  scan.error = '';
  renderLibraryScanWorkspace();
  showToast('Library scan complete', 'success');
}

async function startLibraryImport(allReady = false, overrideCandidates = null) {
  const scan = state.libraryImport.scan;
  const importState = scan.import;
  if (!scan.result || importState.running) return;
  const candidates = Array.isArray(overrideCandidates) ? overrideCandidates : (allReady ? readyLibraryScanCandidates(scan.result) : selectedReadyLibraryScanCandidates(scan.result));
  if (candidates.length === 0) {
    showToast('Select at least one ready book to import', 'error');
    return;
  }
  if (importState.pollTimer) {
    window.clearTimeout(importState.pollTimer);
  }
  importState.running = true;
  importState.jobId = '';
  importState.startedAt = new Date().toISOString();
  importState.progress = { status: 'pending', imported: 0, failed: 0, duplicates: 0, skipped: 0, total: candidates.length, started_at: importState.startedAt };
  importState.result = null;
  importState.error = '';
  renderLibraryScanWorkspace();
  try {
    const data = await apiJson('/api/v1/library/import', {
      method: 'POST',
      body: JSON.stringify({
        scan_job_id: scan.result.job_id || scan.jobId,
        all_ready: !!allReady && !Array.isArray(overrideCandidates),
        candidate_ids: allReady && !Array.isArray(overrideCandidates) ? [] : candidates.map(candidate => candidate.id),
      }),
    });
    importState.jobId = data.job_id || data.job?.id || '';
    if (data.job?.progress) {
      importState.progress = data.job.progress;
      importState.startedAt = data.job.started_at || importState.startedAt;
    }
    await pollLibraryImportJob();
  } catch (err) {
    importState.running = false;
    importState.progress = null;
    importState.error = err.message || 'Failed to start library import';
    renderLibraryScanWorkspace();
    showToast(importState.error, 'error');
  }
}

async function pollLibraryImportJob() {
  const importState = state.libraryImport.scan.import;
  if (!importState.running || !importState.jobId) return;
  try {
    const job = await apiJson(`/api/v1/library/import/${encodeURIComponent(importState.jobId)}`);
    importState.progress = job.progress || importState.progress;
    importState.startedAt = job.started_at || importState.startedAt;
    if (job.status === 'completed') {
      await loadLibraryImportResults(importState.jobId);
      return;
    }
    if (job.status === 'failed') {
      importState.running = false;
      importState.progress = null;
      importState.error = job.error || 'Library import failed';
      renderLibraryScanWorkspace();
      showToast(importState.error, 'error');
      return;
    }
    renderLibraryScanWorkspace();
    importState.pollTimer = window.setTimeout(pollLibraryImportJob, 1200);
  } catch (err) {
    importState.running = false;
    importState.progress = null;
    importState.error = err.message || 'Failed to poll library import';
    renderLibraryScanWorkspace();
    showToast(importState.error, 'error');
  }
}

async function loadLibraryImportResults(jobId) {
  const scan = state.libraryImport.scan;
  const importState = scan.import;
  const result = await apiJson(`/api/v1/library/import/${encodeURIComponent(jobId)}/results`);
  importState.running = false;
  importState.progress = null;
  importState.result = result;
  importState.error = '';
  const updatedScan = await apiJson(`/api/v1/library/scan/${encodeURIComponent(result.scan_job_id || scan.result?.job_id || scan.jobId)}/results`);
  scan.result = updatedScan;
  for (const candidate of updatedScan.candidates || []) {
    if (candidate.classification !== 'new') {
      scan.selected.delete(candidate.id);
    }
  }
  renderLibraryScanWorkspace();
  await refreshLibraryAfterScanImport();
  showToast('Library import complete', (result.summary?.failed || 0) > 0 ? 'error' : 'success');
}

async function resolveLibraryScanCandidate(candidateID, action, values = {}, options = {}) {
  const scan = state.libraryImport.scan;
  if (!scan.result || !candidateID) return;
  try {
    const result = await apiJson(`/api/v1/library/scan/${encodeURIComponent(scan.result.job_id || scan.jobId)}/resolve`, {
      method: 'POST',
      body: JSON.stringify({
        id: candidateID,
        action,
        title: values.title || '',
        subtitle: values.subtitle || '',
        author: values.author || '',
        series: values.series || '',
        series_number: values.series_number || '',
        publisher: values.publisher || '',
        publication_year: values.publication_year || '',
        isbn: values.isbn || '',
        language: values.language || '',
        description: values.description || '',
        tags: values.tags || [],
        library: values.library || '',
      }),
    });
    scan.result = result;
    scan.selected.add(candidateID);
    if (!options.keepEditorOpen) {
      scan.editor = { candidateId: '', draft: null, errors: [] };
    }
    renderLibraryScanWorkspace();
    await refreshLibraryAfterScanImport();
    const successMessage = action === 'edit_metadata'
      ? 'Metadata updated for import'
      : action === 'merge_matching_books'
        ? 'Matching books merged'
        : 'Candidate ready to import';
    showToast(successMessage, 'success');
    return result;
  } catch (err) {
    showToast(err.message || 'Could not resolve review item', 'error');
    return null;
  }
}

function retryFailedLibraryImport() {
  const scan = state.libraryImport.scan;
  const failedIDs = new Set((scan.import.result?.items || []).filter(item => item.status === 'failed').map(item => item.candidate_id));
  const candidates = readyLibraryScanCandidates(scan.result).filter(candidate => failedIDs.has(candidate.id));
  if (candidates.length === 0) {
    showToast('No failed ready books to retry', 'error');
    return;
  }
  startLibraryImport(false, candidates);
}

async function refreshLibraryAfterScanImport() {
  const tasks = [];
  if (state.currentTab === 'home' && typeof loadHomeDashboard === 'function') {
    tasks.push(loadHomeDashboard());
  }
  if (state.currentTab === 'library' && typeof loadLibrary === 'function') {
    tasks.push(loadLibrary());
  }
  if (tasks.length) {
    await Promise.allSettled(tasks);
  }
}

function updateLibraryImportSaveState(flashMode = '') {
  const standardSaveButton = document.getElementById('settings-library-import-save-standard');
  const saveButton = document.getElementById('settings-library-import-save-continue');
  const completeState = document.getElementById('settings-library-import-complete');
  const completeTitle = document.getElementById('settings-library-import-complete-title');
  const completeCopy = document.getElementById('settings-library-import-complete-copy');
  const unsaved = document.getElementById('settings-library-import-unsaved');
  const validationEl = document.getElementById('settings-library-import-validation');
  if (!standardSaveButton || !saveButton || !completeState || !completeTitle || !completeCopy || !unsaved || !validationEl) return;

  const values = getLibraryImportFormValues();
  const validation = validateLibraryImportSettings(values);
  const sanitized = sanitizeLibraryImportValues(values);
  const dirty = state.libraryImport.lastSaved
    ? JSON.stringify(sanitized) !== JSON.stringify(state.libraryImport.lastSaved)
    : true;
  state.libraryImport.dirty = dirty;

  if (validation.errors.length) {
    validationEl.innerHTML = validation.errors.map(err => `<div>${escapeHtml(err)}</div>`).join('');
    validationEl.classList.remove('hidden');
  } else {
    validationEl.innerHTML = '';
    validationEl.classList.add('hidden');
  }

  standardSaveButton.disabled = validation.errors.length > 0;
  standardSaveButton.classList.toggle('opacity-50', standardSaveButton.disabled);
  standardSaveButton.classList.toggle('cursor-not-allowed', standardSaveButton.disabled);

  if (state.libraryImport.completed) {
    saveButton.classList.add('hidden');
    completeState.classList.remove('hidden');
    completeTitle.textContent = flashMode === 'saved' ? 'Changes Saved' : 'Step 1 Complete';
    completeCopy.textContent = flashMode === 'saved'
      ? 'Library folder changes saved successfully.'
      : 'Library folders configured successfully.';
    if (dirty) {
      unsaved.classList.remove('hidden');
    } else {
      unsaved.classList.add('hidden');
    }
    return;
  }

  unsaved.classList.add('hidden');
  completeState.classList.add('hidden');
  saveButton.textContent = 'Save & Continue';
  saveButton.disabled = validation.errors.length > 0;
  saveButton.classList.toggle('opacity-50', saveButton.disabled);
  saveButton.classList.toggle('cursor-not-allowed', saveButton.disabled);
  saveButton.classList.remove('hidden');
}

function bindLibraryImportChangeHandlers() {
  const fields = [...LIBRARY_IMPORT_FIELDS.map(key => document.getElementById(`setting-${key}`)), document.getElementById('setting-file_org_enabled')].filter(Boolean);
  for (const field of fields) {
    if (field.dataset.libraryImportBound === 'true') continue;
    const eventName = field.type === 'checkbox' ? 'change' : 'input';
    field.addEventListener(eventName, () => updateLibraryImportSaveState());
    field.dataset.libraryImportBound = 'true';
  }
}

async function saveLibraryImportSettings(continueAfterSave = false) {
  const values = getLibraryImportFormValues();
  const validation = validateLibraryImportSettings(values);
  if (validation.errors.length) {
    updateLibraryImportSaveState();
    showToast(validation.errors[0], 'error');
    return;
  }
  const payload = sanitizeLibraryImportValues(values);

  try {
    const res = await apiJson('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (res.success) {
      showToast('Library and import settings saved', 'success');
      state.libraryImport.completed = state.libraryImport.completed || continueAfterSave;
      state.libraryImport.lastSaved = payload;
      setLibraryImportStep2State(state.libraryImport.completed, state.libraryImport.completed ? payload : null);
      if (state.libraryImport.flashTimer) window.clearTimeout(state.libraryImport.flashTimer);
      updateLibraryImportSaveState(state.libraryImport.completed && !continueAfterSave ? 'saved' : '');
      state.libraryImport.flashTimer = window.setTimeout(() => {
        if (!state.libraryImport.dirty) {
          updateLibraryImportSaveState();
        }
      }, continueAfterSave ? 0 : 1800);
      if (continueAfterSave) {
        scrollToSettingsSection('settings-library-import-step2');
        document.getElementById('settings-library-import-step2')?.focus();
      }
    } else {
      setLibraryImportStep2State(state.libraryImport.completed, state.libraryImport.completed ? state.libraryImport.lastSaved : null);
      updateLibraryImportSaveState();
      showToast(res.error || 'Failed to save', 'error');
    }
  } catch (err) {
    setLibraryImportStep2State(state.libraryImport.completed, state.libraryImport.completed ? state.libraryImport.lastSaved : null);
    updateLibraryImportSaveState();
    if (err.message !== 'Unauthorized') {
      showToast('Failed to save', 'error');
    }
  }
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
		const [data, health] = await Promise.all([
			apiJson('/api/config'),
			apiJson('/api/health').catch(() => null),
		]);
		state.config = data;

		const configEl = document.getElementById('config-info');
		const items = [];

    if (data.prowlarr) items.push(configItem('Prowlarr', data.prowlarr.url || t('not_configured')));
    if (data.qbittorrent) items.push(configItem('qBittorrent', data.qbittorrent.url || t('not_configured')));
    if (data.transmission) items.push(configItem('Transmission', data.transmission.url || t('not_configured')));
		if (data.sabnzbd) items.push(configItem('SABnzbd', data.sabnzbd.url || t('not_configured')));
		if (data.kavita_url) items.push(configItem('Kavita', data.kavita_url));
		if (data.audiobookshelf_url) items.push(configItem('Audiobookshelf', data.audiobookshelf_url));
		if (health) {
			items.push(configItem('Version', health.version || 'unknown'));
			items.push(configItem('Channel', health.channel || 'development'));
			items.push(configItem('Commit', health.commit || 'unknown'));
			items.push(configItem('Built', health.build_time || 'unknown'));
		}

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

function diagnosticPayload(service) {
  if (service === 'prowlarr') {
    return {
      url: document.getElementById('setting-prowlarr_url')?.value || '',
      api_key: document.getElementById('setting-prowlarr_api_key')?.value || '',
    };
  }
  if (service === 'qbittorrent') {
    return {
      url: document.getElementById('setting-qb_url')?.value || '',
      username: document.getElementById('setting-qb_user')?.value || '',
      password: document.getElementById('setting-qb_pass')?.value || '',
    };
  }
  return {};
}

function renderDiagnosticResult(result) {
  const steps = Array.isArray(result?.steps) ? result.steps : [];
  const status = result?.status || (result?.success ? 'connected' : 'failed');
  const summaryClass = diagnosticStatusClass(status);
  return `
    <div class="rounded-lg border ${summaryClass.border} ${summaryClass.bg} p-3">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p class="text-xs uppercase tracking-wider ${summaryClass.label}">Status</p>
          <p class="mt-1 text-sm font-medium ${summaryClass.text}">${escapeHtml(result?.summary || diagnosticStatusLabel(status))}</p>
        </div>
        ${Number.isFinite(result?.duration_ms) ? `<span class="rounded-full bg-slate-900/70 px-2 py-1 text-[11px] text-slate-300">${escapeHtml(result.duration_ms)} ms</span>` : ''}
      </div>
      <div class="mt-3 space-y-2">
        ${steps.map(renderDiagnosticStep).join('')}
      </div>
    </div>
  `;
}

function renderDiagnosticStep(step) {
  const status = step?.status || 'skipped';
  const style = diagnosticStatusClass(status);
  return `
    <div class="rounded-md border border-slate-800 bg-slate-950/50 px-3 py-2">
      <div class="flex flex-wrap items-start justify-between gap-2">
        <div class="flex min-w-0 items-start gap-2">
          <span class="${style.text}" aria-hidden="true">${diagnosticStatusIcon(status)}</span>
          <div class="min-w-0">
            <p class="text-xs font-medium text-slate-100">${escapeHtml(step?.name || 'Diagnostic step')}</p>
            ${step?.message ? `<p class="mt-0.5 text-xs text-slate-400">${escapeHtml(step.message)}</p>` : ''}
            ${step?.suggestion ? `<p class="mt-1 text-xs text-amber-200">Suggestion: ${escapeHtml(step.suggestion)}</p>` : ''}
          </div>
        </div>
        ${Number.isFinite(step?.duration_ms) ? `<span class="shrink-0 text-[11px] text-slate-500">${escapeHtml(step.duration_ms)} ms</span>` : ''}
      </div>
    </div>
  `;
}

function diagnosticStatusClass(status) {
  switch (status) {
    case 'success':
    case 'connected':
      return { text: 'text-emerald-300', label: 'text-emerald-200', border: 'border-emerald-500/20', bg: 'bg-emerald-500/8' };
    case 'warning':
      return { text: 'text-amber-300', label: 'text-amber-200', border: 'border-amber-500/20', bg: 'bg-amber-500/8' };
    case 'failed':
    case 'error':
      return { text: 'text-red-300', label: 'text-red-200', border: 'border-red-500/25', bg: 'bg-red-500/8' };
    default:
      return { text: 'text-slate-400', label: 'text-slate-500', border: 'border-slate-800', bg: 'bg-slate-900/40' };
  }
}

function diagnosticStatusIcon(status) {
  switch (status) {
    case 'success':
    case 'connected':
      return '✓';
    case 'warning':
      return '!';
    case 'failed':
    case 'error':
      return '✗';
    default:
      return '○';
  }
}

function diagnosticStatusLabel(status) {
  switch (status) {
    case 'connected':
    case 'success':
      return 'Connected';
    case 'warning':
      return 'Connected with warnings';
    case 'failed':
    case 'error':
      return 'Diagnostics failed';
    default:
      return 'Not tested';
  }
}

async function testConnection(service) {
  const statusEl = document.getElementById(`test-${service}-status`);
  const resultEl = document.getElementById(`diagnostic-${service}-result`);
  const cardEl = document.getElementById(`diagnostic-${service}`);
  const button = cardEl?.querySelector('[data-action="testConnection"]');
  if (!statusEl || !resultEl) return;
  statusEl.textContent = 'Running diagnostics...';
  statusEl.className = 'mt-1 text-xs text-yellow-400';
  resultEl.innerHTML = `
    <div class="rounded-lg border border-slate-800 bg-slate-950/50 p-3 text-xs text-slate-400">
      <span class="inline-block h-3 w-3 animate-spin rounded-full border-2 border-amber-300 border-t-transparent align-[-2px]"></span>
      Testing configuration, network, authentication, and API access...
    </div>
  `;
  if (button) button.disabled = true;

  try {
    const data = await apiJson(`/api/test/${service}`, {
      method: 'POST',
      body: JSON.stringify(diagnosticPayload(service)),
    });
    resultEl.innerHTML = renderDiagnosticResult(data);
    if (data.success) {
      statusEl.textContent = data.summary || 'Connected';
      statusEl.className = 'mt-1 text-xs text-emerald-400';
    } else {
      statusEl.textContent = data.summary || 'Diagnostics failed';
      statusEl.className = 'mt-1 text-xs text-red-400';
    }
  } catch (err) {
    statusEl.textContent = 'Diagnostics failed';
    statusEl.className = 'mt-1 text-xs text-red-400';
    resultEl.innerHTML = renderDiagnosticResult({
      status: 'failed',
      success: false,
      summary: err.message || 'Diagnostics failed',
      steps: [{
        name: 'Request',
        status: 'failed',
        message: err.message || 'Librarr could not run diagnostics.',
        suggestion: 'Verify your session and try again.',
      }],
    });
  } finally {
    if (button) button.disabled = false;
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
    if (users.length === 0) {
      container.innerHTML = `<p class="px-4 py-3 text-xs text-slate-500">No users yet.</p>`;
      return;
    }
    container.innerHTML = users.map(u => {
      const isCurrent = state.currentUser && u.username === state.currentUser;
      const isAdmin = u.role === 'admin';
      const roleOptions = `<select data-action-change="changeUserRole" data-id="${u.id}" class="bg-slate-800 text-sm text-slate-300 rounded px-2 py-1 border border-slate-700">
        <option value="user" ${u.role === 'user' ? 'selected' : ''}>${t('role_user')}</option>
        <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>${t('role_admin')}</option>
      </select>`;
      const totpBadge = u.totp_enabled ? '<span class="text-xs text-emerald-400 bg-emerald-400/10 px-1.5 py-0.5 rounded">2FA</span>' : '';
      const created = u.created_at ? new Date(u.created_at).toLocaleDateString() : '—';
      const lastLogin = u.last_login ? new Date(u.last_login).toLocaleString() : t('never');
      const statusBadge = u.enabled === false
        ? '<span class="rounded-full bg-amber-500/10 px-2 py-0.5 text-xs font-medium text-amber-300">Disabled</span>'
        : '<span class="rounded-full bg-emerald-500/10 px-2 py-0.5 text-xs font-medium text-emerald-300">Enabled</span>';
      const deleteDisabled = isAdmin || isCurrent ? 'disabled aria-disabled="true"' : '';
      const deleteClass = isAdmin || isCurrent ? 'bg-slate-800 text-slate-600 cursor-not-allowed' : 'bg-slate-800 hover:bg-red-600 text-slate-300 hover:text-white';
      const toggleLabel = u.enabled === false ? 'Enable' : 'Disable';
      return `
        <div class="grid min-w-[820px] grid-cols-[1.1fr_0.8fr_0.75fr_0.8fr_1fr_1.4fr] items-center gap-3 border-b border-slate-800 px-4 py-3 text-sm last:border-b-0">
          <div class="min-w-0">
            <div class="flex items-center gap-2">
              <span class="truncate font-medium text-white">${escapeHtml(u.username)}</span>
              ${isCurrent ? '<span class="rounded-full bg-indigo-500/10 px-2 py-0.5 text-[11px] text-indigo-300">You</span>' : ''}
              ${totpBadge}
            </div>
          </div>
          <div>${roleOptions}</div>
          <div>${statusBadge}</div>
          <div class="text-xs text-slate-500">${escapeHtml(created)}</div>
          <div class="text-xs text-slate-500">${escapeHtml(lastLogin)}</div>
          <div class="flex flex-wrap items-center gap-1.5">
            <button data-action="editUser" data-id="${u.id}" data-username="${escapeHtml(u.username)}" data-role="${escapeHtml(u.role)}" class="rounded bg-slate-800 px-2 py-1 text-xs text-slate-300 hover:bg-slate-700">Edit</button>
            <button data-action="resetUserPassword" data-id="${u.id}" data-username="${escapeHtml(u.username)}" class="rounded bg-slate-800 px-2 py-1 text-xs text-slate-300 hover:bg-slate-700">Reset Password</button>
            <button data-action="toggleUserEnabled" data-id="${u.id}" data-username="${escapeHtml(u.username)}" data-enabled="${u.enabled !== false}" class="rounded bg-slate-800 px-2 py-1 text-xs text-slate-300 hover:bg-slate-700">${toggleLabel}</button>
            <button data-action="deleteUser" data-id="${u.id}" data-username="${escapeHtml(u.username)}" ${deleteDisabled} class="rounded px-2 py-1 text-xs transition-colors ${deleteClass}">Delete</button>
          </div>
        </div>
      `;
    }).join('');
  } catch (err) {
    // Not admin or multi-user not active — hide.
    section.classList.add('hidden');
  }
}

async function editUser(id, username, role) {
  const nextUsername = prompt('Username', username);
  if (nextUsername === null) return;
  const cleaned = nextUsername.trim();
  if (!cleaned) {
    showToast('Username is required', 'warning');
    return;
  }
  const nextRole = prompt('Role: user or admin', role);
  if (nextRole === null) return;
  const cleanedRole = nextRole.trim().toLowerCase();
  if (cleanedRole !== 'user' && cleanedRole !== 'admin') {
    showToast("Role must be 'user' or 'admin'", 'warning');
    return;
  }
  await updateUser(id, { username: cleaned, role: cleanedRole }, 'User updated');
}

async function changeUserRole(id, role) {
  await updateUser(id, { role }, t('user_role_updated'));
}

async function resetUserPassword(id, username) {
  const password = prompt(`New password for ${username}`);
  if (password === null) return;
  if (password.length < 6) {
    showToast('Password must be at least 6 characters', 'warning');
    return;
  }
  await updateUser(id, { password }, 'Password reset');
}

async function toggleUserEnabled(id, username, enabled) {
  enabled = enabled === true || enabled === 'true';
  const nextEnabled = !enabled;
  const verb = nextEnabled ? 'enable' : 'disable';
  if (!confirm(`Are you sure you want to ${verb} ${username}?`)) return;
  await updateUser(id, { enabled: nextEnabled }, nextEnabled ? 'User enabled' : 'User disabled');
}

async function updateUser(id, payload, successMessage) {
  try {
    const data = await apiJson(`/api/users/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
    if (data.success) {
      showToast(successMessage || 'User updated', 'success');
      loadUsers();
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

function cancelCreateUser() {
  document.getElementById('new-user-name').value = '';
  document.getElementById('new-user-pass').value = '';
  const role = document.getElementById('new-user-role');
  if (role) role.value = 'user';
  const admin = document.getElementById('new-user-admin');
  if (admin) admin.checked = false;
}

async function addUser() {
  const username = document.getElementById('new-user-name').value.trim();
  const password = document.getElementById('new-user-pass').value;
  const adminChecked = document.getElementById('new-user-admin')?.checked;
  const selectedRole = document.getElementById('new-user-role')?.value || 'user';
  const role = adminChecked ? 'admin' : selectedRole;
  if (!username || !password) {
    showToast(t('err_credentials_required'), 'warning');
    return;
  }
  try {
    const data = await apiJson('/api/register', {
      method: 'POST',
      body: JSON.stringify({ username, password, role }),
    });
    if (data.success) {
      showToast(t('user_created'), 'success');
      cancelCreateUser();
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
    const nowSeconds = Date.now() / 1000;
    el.innerHTML = list.map(inv => {
      const expires = inv.expires_at
        ? new Date(inv.expires_at * 1000).toLocaleDateString()
        : 'Never';
      const usesLabel = `${inv.uses} / ${inv.max_uses}`;
      const exhausted = inv.uses >= inv.max_uses;
      const expired = Boolean(inv.expires_at && inv.expires_at < nowSeconds);
      const created = inv.created_at ? new Date(inv.created_at * 1000).toLocaleDateString() : '—';
      const used = inv.uses > 0 ? 'Yes' : 'No';
      return `
        <div class="rounded-lg bg-slate-800/50 px-3 py-2 text-sm">
          <div class="flex items-center justify-between gap-3">
            <code class="min-w-0 flex-1 truncate font-mono text-xs text-indigo-400">${escapeHtml(inv.code)}</code>
            <div class="flex items-center gap-1 flex-shrink-0">
              <button data-action="copyInviteCode" data-code="${escapeHtml(inv.code)}" class="px-2 py-1 text-xs bg-slate-700 hover:bg-slate-600 text-slate-300 rounded transition-colors" title="Copy">Copy</button>
              <button data-action="revokeInviteCode" data-id="${inv.id}" class="px-2 py-1 text-xs bg-red-700 hover:bg-red-600 text-white rounded transition-colors" title="Delete">Delete</button>
            </div>
          </div>
          <div class="mt-2 grid grid-cols-2 gap-2 text-xs text-slate-500 md:grid-cols-6">
            <span>Role: <span class="text-slate-300">${escapeHtml(inv.role)}</span></span>
            <span class="${exhausted ? 'text-amber-400' : ''}">Uses: ${escapeHtml(usesLabel)}</span>
            <span>Expiration: ${escapeHtml(expires)}</span>
            <span>Created: ${escapeHtml(created)}</span>
            <span>Used: ${used}</span>
            <span class="${expired ? 'text-amber-400' : 'text-emerald-400'}">Expired: ${expired ? 'Yes' : 'No'}</span>
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
    const useNormalized = normalizedLibraryMode();
    const data = await apiJson(useNormalized ? '/api/v1/library/summary' : '/api/stats');
    const statsEl = document.getElementById('header-stats');
    const count = useNormalized ? data.total_books : data.total_items;
    if (count !== undefined) {
      statsEl.textContent = t('n_books_in_library', {n: count});
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
  return div.innerHTML;
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
    closeBookDetails();
  }
});

// ============================================================
// INIT
// ============================================================
async function init() {
  try {
    const authResp = await fetch('/api/auth/status', { credentials: 'include' });
    const auth = await authResp.json().catch(() => ({}));

    if (auth.setup_required) {
      showLoginModal();
      return;
    }

    if (auth.has_users && !auth.authenticated) {
      showLoginModal();
      return;
    }

    // Test auth by hitting config
    const cfg = await apiJson('/api/config');
    state.config = cfg;
    setupLibrarr2Shell();

    // Update user header.
    if (cfg.current_user) {
      updateUserHeader(cfg.current_user, cfg.current_role);
    }

    // Auth OK
    loadStats();
    // Apply language on load
    applyLanguage();
    document.getElementById('search-empty').classList.remove('hidden');
    switchTab('home');
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
  openImportSettings: () => openImportSettings(),
  saveLibraryImportStandard: () => saveLibraryImportSettings(false),
  saveLibraryImportContinue: () => saveLibraryImportSettings(true),
  previewNestedEbookPathRepair: () => previewNestedEbookPathRepair(),
  runNestedEbookPathRepair: () => runNestedEbookPathRepair(),
  startLibraryScan: () => startLibraryScan(),
  startLibraryImportSelected: () => startLibraryImport(false),
  startLibraryImportAllReady: () => startLibraryImport(true),
  viewImportedLibraryScan: () => {
    state.libraryImport.scan.filter = 'already_imported';
    state.libraryImport.scan.sections.already_imported = true;
    renderLibraryScanWorkspace();
  },
  retryFailedLibraryImport: () => retryFailedLibraryImport(),
  closeLibraryImportSummary: () => {
    state.libraryImport.scan.import.result = null;
    state.libraryImport.scan.import.error = '';
    renderLibraryScanWorkspace();
  },
  useSuggestedLibraryScanCandidate: el => resolveLibraryScanCandidate(el.dataset.candidateId, 'use_suggested'),
  mergeMatchingLibraryScanBooks: el => resolveLibraryScanCandidate(el.dataset.candidateId, 'merge_matching_books'),
  editLibraryScanCandidateMetadata: el => openScanMetadataEditor(el.dataset.candidateId),
  saveMetadataEditor: () => saveMetadataEditor(false),
  saveAndImportMetadataEditor: () => saveMetadataEditor(true),
  cancelMetadataEditor: () => closeMetadataEditor(),
  resetMetadataEditor: () => resetMetadataEditor(),
  skipLibraryScanCandidate: el => {
    const id = el.dataset.candidateId;
    if (!id) return;
    state.libraryImport.scan.skipped.add(id);
    state.libraryImport.scan.selected.delete(id);
    renderLibraryScanWorkspace();
  },
  clearLibraryScanSelection: () => {
    state.libraryImport.scan.selected.clear();
    renderLibraryScanWorkspace();
  },
  skipLibraryScanSelected: () => {
    for (const candidate of selectedReadyLibraryScanCandidates(state.libraryImport.scan.result)) {
      state.libraryImport.scan.skipped.add(candidate.id);
      state.libraryImport.scan.selected.delete(candidate.id);
    }
    renderLibraryScanWorkspace();
  },
  setLibraryScanFilter: el => {
    state.libraryImport.scan.filter = el.dataset.filter || 'all';
    renderLibraryScanWorkspace();
  },
  toggleLibraryScanSection: el => {
    const section = el.dataset.section;
    if (!section) return;
    state.libraryImport.scan.sections[section] = state.libraryImport.scan.sections[section] === false;
    renderLibraryScanWorkspace();
  },
  switchSearchTab: el => switchSearchTab(el.dataset.arg),
  switchLibraryTab: el => switchLibraryTab(el.dataset.arg),
  setSortMode: el => setSortMode(el.dataset.arg),
  testConnection: el => testConnection(el.dataset.arg),
  saveIntegration: el => saveIntegration(el.dataset.arg),
  toggleMobileNav: () => toggleMobileNav(),
  showLoginForm: () => showLoginForm(),
  showRegisterForm: () => showRegisterForm(),
  showWishlistForm: () => showWishlistForm(),
  hideWishlistForm: () => hideWishlistForm(),
  addWishlistItem: () => addWishlistItem(),
  generateInviteCode: () => generateInviteCode(),
  doLogout: () => doLogout(),
  clearCompleted: () => clearCompleted(),
  addUser: () => addUser(),
  cancelCreateUser: () => cancelCreateUser(),
  editUser: el => editUser(+el.dataset.id, el.dataset.username, el.dataset.role),
  resetUserPassword: el => resetUserPassword(+el.dataset.id, el.dataset.username),
  toggleUserEnabled: el => toggleUserEnabled(+el.dataset.id, el.dataset.username, el.dataset.enabled),
  refreshDownloads: () => refreshDownloads(true), // toolbar button forces a refresh
  openBookDetails: el => openBookDetails(+el.dataset.index),
  openHomeBookDetails: el => openHomeBookDetails(+el.dataset.index),
  closeBookDetails: () => closeBookDetails(),
  openLibraryMetadataEditor: () => openLibraryMetadataEditor(),
  cancelLibraryMetadataEditor: () => closeLibraryMetadataEditor(),
  resetLibraryMetadataEditor: () => resetLibraryMetadataEditor(),
  saveLibraryMetadataEditor: () => saveLibraryMetadataEditor(),
  mergeMatchingBookDuplicates: () => mergeMatchingBookDuplicates(),
  openBookDeleteDialog: el => openBookDeleteDialog(el.dataset.deleteFiles),
  cancelBookDeleteDialog: () => cancelBookDeleteDialog(),
  confirmBookDelete: () => confirmBookDelete(),
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
  toggleLibraryScanCandidateSelection: el => {
    const id = el.dataset.candidateId;
    if (!id) return;
    if (el.checked) {
      state.libraryImport.scan.selected.add(id);
    } else {
      state.libraryImport.scan.selected.delete(id);
    }
    renderLibraryScanWorkspace();
  },
  toggleLibraryScanSelectAllReady: el => {
    const ready = readyLibraryScanCandidates(state.libraryImport.scan.result);
    if (el.checked) {
      for (const candidate of ready) {
        state.libraryImport.scan.selected.add(candidate.id);
      }
    } else {
      for (const candidate of ready) {
        state.libraryImport.scan.selected.delete(candidate.id);
      }
    }
    renderLibraryScanWorkspace();
  },
};

document.addEventListener('change', e => {
  const el = e.target.closest('[data-action-change]');
  if (!el) return;
  const fn = CHANGE_ACTIONS[el.dataset.actionChange];
  if (fn) fn(el, e);
});

document.addEventListener('input', e => {
  const el = e.target.closest('[data-action-input]');
  if (!el) return;
  if (el.dataset.actionInput === 'libraryScanSearch') {
    state.libraryImport.scan.search = el.value || '';
    renderLibraryScanWorkspace();
    document.getElementById('settings-library-scan-search')?.focus();
  } else if (el.dataset.actionInput === 'metadataEditorField') {
    updateMetadataEditorDraft(el.dataset.field, el.value || '');
  }
});

// Cover-image fallback (replaces inline onerror=). 'error' events don't
// bubble, so listen in the capture phase.
document.addEventListener('error', e => {
  const img = e.target;
  if (img instanceof HTMLImageElement && img.dataset.phTitle !== undefined) {
    img.outerHTML = window.makePlaceholder(img.dataset.phTitle, +(img.dataset.phIdx || 0));
  }
}, true);
