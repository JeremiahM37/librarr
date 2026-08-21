package config

import "strings"

// Import modes decide how an organized file gets from the download folder into
// the library.
//
//   - ImportModeMove consumes the download: the payload is gone from the
//     download folder afterwards, so the torrent can no longer be seeded even
//     when its record is kept in the client (a force-recheck drops it to 0%).
//   - ImportModeHardlink adds a second directory entry for the same data. The
//     library and the download folder share the bytes, so seeding continues and
//     no extra disk is used. Requires both paths to live on one filesystem;
//     otherwise librarr falls back to a copy.
//   - ImportModeCopy duplicates the bytes. Seeding continues at the cost of
//     twice the disk.
//   - ImportModeAuto (the default, and what an unrecognized value becomes)
//     resolves from RemoveTorrentAfterImport: keeping torrents means the user
//     wants them seedable, so imports hardlink; removing them means nothing
//     needs the payload, so imports move.
const (
	ImportModeAuto     = ""
	ImportModeMove     = "move"
	ImportModeHardlink = "hardlink"
	ImportModeCopy     = "copy"
)

// NormalizeImportMode maps user input onto a supported import mode. Anything
// unrecognized (including an empty value) becomes ImportModeAuto, so a typo
// lands on the default rather than silently picking a mode nobody asked for.
func NormalizeImportMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case ImportModeMove:
		return ImportModeMove
	case ImportModeHardlink, "hard_link", "hard-link", "link":
		return ImportModeHardlink
	case ImportModeCopy:
		return ImportModeCopy
	default:
		return ImportModeAuto
	}
}

// EffectiveImportMode is the mode imports actually run in. An explicit
// IMPORT_MODE always wins; otherwise the mode follows the one setting whose
// whole purpose is seeding, so keeping torrents is enough on its own to keep
// them seedable.
func (c *Config) EffectiveImportMode() string {
	if mode := NormalizeImportMode(c.ImportMode); mode != ImportModeAuto {
		return mode
	}
	if !c.RemoveTorrentAfterImport {
		return ImportModeHardlink
	}
	return ImportModeMove
}

// ImportModeKeepsPayload reports whether mode leaves the downloaded payload in
// place, which is what a torrent needs in order to keep seeding after import.
func ImportModeKeepsPayload(mode string) bool {
	return mode == ImportModeHardlink || mode == ImportModeCopy
}

// KeepsPayload reports whether imports leave the download payload where the
// client wrote it.
func (c *Config) KeepsPayload() bool {
	return ImportModeKeepsPayload(c.EffectiveImportMode())
}
