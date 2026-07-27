package db

import (
	"fmt"

	"github.com/jamie75/librarr/internal/webhook"
)

// --- Webhook Configs ---

// GetWebhookConfigs returns all webhook configurations.
func (d *DB) GetWebhookConfigs() ([]webhook.Config, error) {
	rows, err := d.db.Query("SELECT id, name, url, type, enabled, events FROM webhook_configs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []webhook.Config
	for rows.Next() {
		var c webhook.Config
		var enabled int
		if err := rows.Scan(&c.ID, &c.Name, &c.URL, &c.Type, &enabled, &c.Events); err != nil {
			continue
		}
		c.Enabled = enabled == 1
		configs = append(configs, c)
	}
	return configs, nil
}

// CreateWebhookConfig inserts a new webhook config.
func (d *DB) CreateWebhookConfig(c *webhook.Config) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	enabledInt := 0
	if c.Enabled {
		enabledInt = 1
	}

	result, err := d.db.Exec(
		`INSERT INTO webhook_configs (name, url, type, enabled, events) VALUES (?, ?, ?, ?, ?)`,
		c.Name, c.URL, c.Type, enabledInt, c.Events,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// DeleteWebhookConfig removes a webhook config by ID.
func (d *DB) DeleteWebhookConfig(id int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	result, err := d.db.Exec("DELETE FROM webhook_configs WHERE id = ?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("webhook not found")
	}
	return nil
}
