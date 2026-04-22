package api

import "github.com/JeremiahM37/librarr/internal/db"

// SettingsStore abstracts runtime settings persistence from handlers.
// It allows API code to depend on behavior instead of the concrete DB type.
type SettingsStore interface {
	HasAnySettings() (bool, error)
	GetAllSettings() (map[string]string, error)
	BulkSetSettings(settings map[string]interface{}, updatedBy string, sensitiveKeys map[string]bool) error
}

type dbSettingsStore struct {
	db *db.DB
}

func newDBSettingsStore(database *db.DB) SettingsStore {
	return &dbSettingsStore{db: database}
}

func (s *dbSettingsStore) HasAnySettings() (bool, error) {
	return s.db.HasAnySettings()
}

func (s *dbSettingsStore) GetAllSettings() (map[string]string, error) {
	return s.db.GetAllSettings()
}

func (s *dbSettingsStore) BulkSetSettings(settings map[string]interface{}, updatedBy string, sensitiveKeys map[string]bool) error {
	return s.db.BulkSetSettings(settings, updatedBy, sensitiveKeys)
}
