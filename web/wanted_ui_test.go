package web

import (
	"regexp"
	"strings"
	"testing"
)

// The wanted list, quality-profile editor and author monitor are rendered by
// app.js into containers that index.html must provide, and every
// data-action / data-action-change the JS emits must be registered in the
// delegation tables. These tests pin that contract so a stray rename fails
// the build instead of producing a silently dead button.

func TestWantedMarkupExists(t *testing.T) {
	html := string(IndexHTML)
	for _, id := range []string{
		// Wanted tab
		"tab-wishlist", "wanted-summary", "wanted-filter", "wanted-search-all",
		"wanted-upgrades-off", "wishlist-empty", "wishlist-list", "wishlist-form",
		"wl-title", "wl-author", "wl-type", "wl-profile",
		// Settings card
		"wanted-settings", "setting-scheduler_enabled", "setting-scheduler_auto_download",
		"setting-scheduler_interval_hours", "setting-scheduler_min_score",
		"setting-scheduler_item_delay_seconds", "setting-auto_upgrade_enabled",
		"setting-upgrade_keep_old_files", "scheduler-status", "quality-profiles-list",
		"setting-author_monitor_enabled", "author-name", "author-interval", "author-auto-add", "authors-list",
	} {
		if !strings.Contains(html, `id="`+id+`"`) {
			t.Errorf("index.html is missing element #%s", id)
		}
	}
}

// TestWantedActionsAreRegistered checks that every data-action the markup or
// the JS templates emit has a handler in CLICK_ACTIONS / CHANGE_ACTIONS.
func TestWantedActionsAreRegistered(t *testing.T) {
	js := appJS(t)
	html := string(IndexHTML)
	click := regexp.MustCompile(`data-action="([a-zA-Z]+)"`)
	change := regexp.MustCompile(`data-action-change="([a-zA-Z]+)"`)

	clickTable := js[strings.Index(js, "const CLICK_ACTIONS = {"):]
	clickTable = clickTable[:strings.Index(clickTable, "\n};")]
	changeTable := js[strings.Index(js, "const CHANGE_ACTIONS = {"):]
	changeTable = changeTable[:strings.Index(changeTable, "\n};")]

	seen := map[string]bool{}
	for _, src := range []string{html, js} {
		for _, m := range click.FindAllStringSubmatch(src, -1) {
			name := m[1]
			if seen["c:"+name] {
				continue
			}
			seen["c:"+name] = true
			if !strings.Contains(clickTable, "\n  "+name+":") {
				t.Errorf("data-action=%q has no CLICK_ACTIONS entry", name)
			}
		}
		for _, m := range change.FindAllStringSubmatch(src, -1) {
			name := m[1]
			if seen["x:"+name] {
				continue
			}
			seen["x:"+name] = true
			if !strings.Contains(changeTable, "\n  "+name+":") {
				t.Errorf("data-action-change=%q has no CHANGE_ACTIONS entry", name)
			}
		}
	}
	for _, name := range []string{"searchWantedNow", "explainWanted", "runSchedulerNow", "saveWantedSettings", "qpNew", "qpMove", "qpSave", "qpDelete", "addAuthor", "checkAuthor", "deleteAuthor"} {
		if !seen["c:"+name] {
			t.Errorf("expected the UI to emit data-action=%q somewhere", name)
		}
	}
	for _, name := range []string{"toggleWantedMonitored", "setWantedProfile", "qpToggleFormat", "qpField", "filterWanted", "authorAutoAdd", "authorInterval"} {
		if !seen["x:"+name] {
			t.Errorf("expected the UI to emit data-action-change=%q somewhere", name)
		}
	}
}

// TestWantedI18nKeysExist: every t('key') the wanted code uses resolves in the
// English table, so no raw key names leak into the UI.
func TestWantedI18nKeysExist(t *testing.T) {
	js := appJS(t)
	en := js[strings.Index(js, "const I18N = {"):]
	en = en[:strings.Index(en, "\n  ru: {")]
	re := regexp.MustCompile(`\bt\('([a-z_]+)'`)
	missing := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(js, -1) {
		key := m[1]
		if strings.HasSuffix(key, "_") {
			continue // dynamic prefix such as t('wanted_state_' + st)
		}
		if !strings.Contains(en, "\n    "+key+":") {
			missing[key] = true
		}
	}
	for k := range missing {
		t.Errorf("t(%q) has no English translation", k)
	}
}

// TestSchedulerSettingsFieldsMatchAPI: the settings inputs are named after
// the /api/settings keys they round-trip, so a renamed key surfaces here.
func TestSchedulerSettingsFieldsMatchAPI(t *testing.T) {
	js := appJS(t)
	block := js[strings.Index(js, "const WANTED_SETTING_KEYS = ["):]
	block = block[:strings.Index(block, "];")]
	html := string(IndexHTML)
	for _, m := range regexp.MustCompile(`'([a-z_]+)'`).FindAllStringSubmatch(block, -1) {
		if !strings.Contains(html, `id="setting-`+m[1]+`"`) {
			t.Errorf("WANTED_SETTING_KEYS lists %q but index.html has no #setting-%s", m[1], m[1])
		}
	}
}
