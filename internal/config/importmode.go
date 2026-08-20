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
const (
	ImportModeMove     = "move"
	ImportModeHardlink = "hardlink"
	ImportModeCopy     = "copy"
)

// NormalizeImportMode maps user input onto a supported import mode. Anything
// unrecognized (including an empty value) becomes ImportModeMove, which is the
// historical behavior.
func NormalizeImportMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case ImportModeHardlink, "hard_link", "hard-link", "link":
		return ImportModeHardlink
	case ImportModeCopy:
		return ImportModeCopy
	default:
		return ImportModeMove
	}
}

// ImportModeKeepsPayload reports whether mode leaves the downloaded payload in
// place, which is what a torrent needs in order to keep seeding after import.
func ImportModeKeepsPayload(mode string) bool {
	return NormalizeImportMode(mode) != ImportModeMove
}
