package libraryimport

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/jamie75/librarr/internal/library"
	"github.com/jamie75/librarr/internal/organize"
)

type MetadataResolver struct{}

var plannerParenRe = regexp.MustCompile(`\([^)]*\)`)
var plannerBracketRe = regexp.MustCompile(`\[[^\]]*\]`)
var plannerWhitespaceRe = regexp.MustCompile(`\s+`)

func (r *MetadataResolver) Resolve(ctx context.Context, candidate *ImportCandidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch {
	case candidate.IsDirectory && candidate.MediaType == library.MediaTypeAudiobook:
		r.resolveAudiobookDirectory(candidate)
	case candidate.MediaType == library.MediaTypeAudiobook:
		r.resolveAudiobookFile(candidate)
	case isEbookFormat(candidate.Format):
		r.resolveEbook(candidate)
	default:
		r.resolveFilenameOnly(candidate)
	}
	return nil
}

func (r *MetadataResolver) resolveEbook(candidate *ImportCandidate) {
	embedded := organize.ExtractEmbeddedEbookMetadata(candidate.Path)
	filename := organize.ExtractFilenameEbookMetadata(candidate.Path)
	if strings.EqualFold(candidate.Format, "mobi") && embedded.Title == "" && embedded.Author == "" {
		filename = parsePlannerEbookFilename(candidate.Path)
	} else if !strings.EqualFold(candidate.Format, "mobi") {
		plannerFilename := parsePlannerEbookFilename(candidate.Path)
		if shouldUsePlannerFilename(filename, plannerFilename) {
			filename = plannerFilename
		}
	}
	selected := organize.ExtractEbookMetadata(candidate.Path)
	if strings.EqualFold(candidate.Format, "mobi") && embedded.Title == "" && embedded.Author == "" {
		selected = filename
	}
	selected = selectEbookMetadata(embedded, filename, selected)
	candidate.Metadata = CandidateMetadata{
		EmbeddedTitle:  strings.TrimSpace(embedded.Title),
		EmbeddedAuthor: strings.TrimSpace(embedded.Author),
		FilenameTitle:  strings.TrimSpace(filename.Title),
		FilenameAuthor: strings.TrimSpace(filename.Author),
		SelectedTitle:  strings.TrimSpace(selected.Title),
		SelectedAuthor: strings.TrimSpace(selected.Author),
	}
	r.applyHints(candidate)
	r.applyOverrides(candidate)
	candidate.Evidence = append(candidate.Evidence,
		metadataEvidence("embedded_title", candidate.Metadata.EmbeddedTitle, "embedded_metadata"),
		metadataEvidence("embedded_author", candidate.Metadata.EmbeddedAuthor, "embedded_metadata"),
		metadataEvidence("filename_title", candidate.Metadata.FilenameTitle, "filename_fallback"),
		metadataEvidence("filename_author", candidate.Metadata.FilenameAuthor, "filename_fallback"),
		metadataEvidence("selected_title", candidate.Metadata.SelectedTitle, selectedTitleSource(candidate.Metadata), "selected metadata title for planning"),
		metadataEvidence("selected_author", candidate.Metadata.SelectedAuthor, selectedAuthorSource(candidate.Metadata), "selected metadata author for planning"),
	)
}

func (r *MetadataResolver) resolveFilenameOnly(candidate *ImportCandidate) {
	title := strings.TrimSpace(strings.TrimSuffix(filepath.Base(candidate.Path), filepath.Ext(candidate.Path)))
	title = strings.Join(strings.Fields(title), " ")
	candidate.Metadata = CandidateMetadata{
		FilenameTitle: title,
		SelectedTitle: title,
	}
	r.applyHints(candidate)
	r.applyOverrides(candidate)
	candidate.Evidence = append(candidate.Evidence,
		metadataEvidence("filename_title", candidate.Metadata.FilenameTitle, "filename_fallback"),
		metadataEvidence("selected_title", candidate.Metadata.SelectedTitle, "filename_fallback", "selected filename-derived title for planning"),
	)
}

func (r *MetadataResolver) resolveAudiobookFile(candidate *ImportCandidate) {
	meta := organize.ExtractAudiobookPathMetadata(candidate.Path)
	selectedTitle := ""
	selectedAuthor := ""
	if meta != nil {
		selectedTitle = strings.TrimSpace(meta.Title)
		selectedAuthor = strings.TrimSpace(meta.Artist)
	}
	if selectedTitle == "" {
		selectedTitle = strings.TrimSpace(strings.TrimSuffix(filepath.Base(candidate.Path), filepath.Ext(candidate.Path)))
	}
	candidate.Metadata = CandidateMetadata{
		FilenameTitle:  selectedTitle,
		FilenameAuthor: selectedAuthor,
		SelectedTitle:  selectedTitle,
		SelectedAuthor: selectedAuthor,
	}
	r.applyHints(candidate)
	r.applyOverrides(candidate)
	candidate.Evidence = append(candidate.Evidence,
		metadataEvidence("filename_title", candidate.Metadata.FilenameTitle, "audiobook_path_fallback"),
		metadataEvidence("filename_author", candidate.Metadata.FilenameAuthor, "audiobook_path_fallback"),
		metadataEvidence("selected_title", candidate.Metadata.SelectedTitle, "audiobook_path_fallback", "selected audiobook title from path for planning"),
		metadataEvidence("selected_author", candidate.Metadata.SelectedAuthor, "audiobook_path_fallback", "selected audiobook author from path for planning"),
	)
}

func (r *MetadataResolver) resolveAudiobookDirectory(candidate *ImportCandidate) {
	folderName := strings.TrimSpace(filepath.Base(candidate.Path))
	meta := organize.ExtractAudioMetaFromDir(candidate.Path)
	selectedTitle := folderName
	selectedAuthor := ""
	if meta != nil {
		if strings.TrimSpace(meta.Album) != "" {
			selectedTitle = strings.TrimSpace(meta.Album)
		} else if strings.TrimSpace(meta.Title) != "" {
			selectedTitle = strings.TrimSpace(meta.Title)
		}
		selectedAuthor = strings.TrimSpace(meta.Artist)
	}
	candidate.Metadata = CandidateMetadata{
		FilenameTitle:  folderName,
		FilenameAuthor: selectedAuthor,
		SelectedTitle:  selectedTitle,
		SelectedAuthor: selectedAuthor,
	}
	r.applyHints(candidate)
	r.applyOverrides(candidate)
	candidate.Evidence = append(candidate.Evidence,
		metadataEvidence("filename_title", candidate.Metadata.FilenameTitle, "directory_name"),
		metadataEvidence("selected_title", candidate.Metadata.SelectedTitle, "audio_metadata", "selected audiobook title for planning"),
		metadataEvidence("selected_author", candidate.Metadata.SelectedAuthor, "audio_metadata", "selected audiobook author for planning"),
	)
}

func isEbookFormat(format string) bool {
	switch strings.ToLower(format) {
	case "epub", "mobi", "azw3", "pdf":
		return true
	default:
		return false
	}
}

func (r *MetadataResolver) applyHints(candidate *ImportCandidate) {
	if candidate.Metadata.SelectedTitle == "" && strings.TrimSpace(candidate.TitleHint) != "" {
		candidate.Metadata.SelectedTitle = strings.TrimSpace(candidate.TitleHint)
		candidate.Evidence = append(candidate.Evidence, metadataEvidence("selected_title_hint", candidate.Metadata.SelectedTitle, "import_request_hint", "selected request title hint for planning"))
	}
	if candidate.Metadata.SelectedAuthor == "" && strings.TrimSpace(candidate.AuthorHint) != "" {
		candidate.Metadata.SelectedAuthor = strings.TrimSpace(candidate.AuthorHint)
		candidate.Evidence = append(candidate.Evidence, metadataEvidence("selected_author_hint", candidate.Metadata.SelectedAuthor, "import_request_hint", "selected request author hint for planning"))
	}
}

func (r *MetadataResolver) applyOverrides(candidate *ImportCandidate) {
	override := candidate.MetadataOverride
	applied := false
	if value := strings.TrimSpace(override.SelectedTitle); value != "" {
		candidate.Metadata.SelectedTitle = value
		applied = true
	}
	if value := strings.TrimSpace(override.SelectedAuthor); value != "" {
		candidate.Metadata.SelectedAuthor = value
		applied = true
	}
	if value := strings.TrimSpace(override.Subtitle); value != "" {
		candidate.Metadata.Subtitle = value
		applied = true
	}
	if value := strings.TrimSpace(override.Series); value != "" {
		candidate.Metadata.Series = value
		applied = true
	}
	if value := strings.TrimSpace(override.SeriesNumber); value != "" {
		candidate.Metadata.SeriesNumber = value
		applied = true
	}
	if value := strings.TrimSpace(override.Publisher); value != "" {
		candidate.Metadata.Publisher = value
		applied = true
	}
	if value := strings.TrimSpace(override.PublicationYear); value != "" {
		candidate.Metadata.PublicationYear = value
		applied = true
	}
	if value := strings.TrimSpace(override.ISBN); value != "" {
		candidate.Metadata.ISBN = value
		candidate.Metadata.Identifiers = append(candidate.Metadata.Identifiers, library.Identifier{
			Provider:   "isbn",
			Value:      value,
			Scope:      library.IdentifierScopeEdition,
			Confidence: library.ConfidenceHigh,
		})
		applied = true
	}
	if value := strings.TrimSpace(override.Language); value != "" {
		candidate.Metadata.Language = value
		applied = true
	}
	if value := strings.TrimSpace(override.Description); value != "" {
		candidate.Metadata.Description = value
		applied = true
	}
	if len(override.Tags) > 0 {
		candidate.Metadata.Tags = cleanTags(override.Tags)
		applied = true
	}
	if value := strings.TrimSpace(override.Library); value != "" {
		candidate.Metadata.Library = value
		applied = true
	}
	if applied {
		candidate.Evidence = append(candidate.Evidence, PlanningEvidence{
			Signal:      "manual_metadata_override",
			Value:       candidate.Metadata.SelectedTitle,
			Source:      "manual_edit",
			Confidence:  library.ConfidenceHigh,
			Explanation: "User-edited metadata selected during import review",
		})
	}
}

func cleanTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := map[string]struct{}{}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		key := strings.ToLower(tag)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func parsePlannerEbookFilename(path string) organize.EbookMetadata {
	name := strings.TrimSpace(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	name = plannerBracketRe.ReplaceAllString(name, "")
	name = plannerParenRe.ReplaceAllString(name, "")
	name = plannerWhitespaceRe.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	if name == "" {
		return organize.EbookMetadata{}
	}
	rawParts := strings.Split(name, " - ")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.Trim(strings.TrimSpace(part), "- ")
		if part != "" {
			parts = append(parts, part)
		}
	}
	switch len(parts) {
	case 0:
		return organize.EbookMetadata{}
	case 1:
		return organize.EbookMetadata{Title: parts[0]}
	case 2:
		if looksLikePersonName(parts[0]) {
			return organize.EbookMetadata{Title: parts[1], Author: parts[0]}
		}
		if looksLikePersonName(parts[1]) {
			return organize.EbookMetadata{Title: parts[0], Author: parts[1]}
		}
		return organize.EbookMetadata{Title: parts[0], Author: parts[1]}
	default:
		return organize.EbookMetadata{Title: parts[len(parts)-2], Author: parts[len(parts)-1]}
	}
}

func shouldUsePlannerFilename(current, planner organize.EbookMetadata) bool {
	if strings.TrimSpace(planner.Title) == "" && strings.TrimSpace(planner.Author) == "" {
		return false
	}
	if looksLikePublisherName(current.Author) && strings.TrimSpace(planner.Author) != "" {
		return true
	}
	if !looksLikePersonName(current.Author) && looksLikePersonName(planner.Author) {
		return true
	}
	if strings.TrimSpace(current.Author) == "" && strings.TrimSpace(planner.Author) != "" {
		return true
	}
	return false
}

func selectEbookMetadata(embedded, filename, fallback organize.EbookMetadata) organize.EbookMetadata {
	selected := fallback
	if strings.TrimSpace(selected.Title) == "" {
		selected.Title = filename.Title
	}
	if strings.TrimSpace(selected.Author) == "" {
		selected.Author = filename.Author
	}
	if shouldPreferFilenameAuthor(embedded.Author, filename.Author) {
		selected.Author = strings.TrimSpace(filename.Author)
	}
	return selected
}

func shouldPreferFilenameAuthor(embeddedAuthor, filenameAuthor string) bool {
	embeddedAuthor = strings.TrimSpace(embeddedAuthor)
	filenameAuthor = strings.TrimSpace(filenameAuthor)
	if filenameAuthor == "" {
		return false
	}
	if embeddedAuthor == "" {
		return true
	}
	return looksLikePublisherName(embeddedAuthor) && !looksLikePublisherName(filenameAuthor)
}

func looksLikePublisherName(value string) bool {
	normalized := library.NormalizeKey(value)
	if normalized == "" {
		return false
	}
	terms := []string{
		"book group",
		"books",
		"publisher",
		"publishers",
		"publishing",
		"press",
		"media",
		"group",
		"company",
		"corporation",
		"corp",
		"inc",
		"llc",
		"ltd",
	}
	for _, term := range terms {
		if strings.Contains(normalized, term) {
			return true
		}
	}
	return false
}

func looksLikePersonName(value string) bool {
	if looksLikePublisherName(value) {
		return false
	}
	words := strings.Fields(value)
	if len(words) < 2 || len(words) > 4 {
		return false
	}
	first := strings.ToLower(strings.Trim(words[0], ".,;:!?\"'"))
	if first == "a" || first == "an" || first == "the" {
		return false
	}
	for _, word := range words {
		word = strings.Trim(word, ".,;:!?\"'")
		if word == "" {
			return false
		}
		runes := []rune(word)
		if len(runes) == 1 {
			if !unicode.IsUpper(runes[0]) {
				return false
			}
			continue
		}
		if strings.HasSuffix(word, ".") && len([]rune(strings.TrimSuffix(word, "."))) == 1 {
			continue
		}
		if !unicode.IsUpper(runes[0]) {
			return false
		}
	}
	return true
}

func metadataEvidence(signal, value, source string, explanation ...string) PlanningEvidence {
	exp := ""
	if len(explanation) > 0 {
		exp = explanation[0]
	}
	return PlanningEvidence{
		Signal:      signal,
		Value:       value,
		Source:      source,
		Confidence:  library.ConfidenceHigh,
		Explanation: exp,
	}
}

func selectedTitleSource(meta CandidateMetadata) string {
	if meta.EmbeddedTitle != "" && strings.EqualFold(meta.SelectedTitle, meta.EmbeddedTitle) {
		return "embedded_metadata"
	}
	if meta.FilenameTitle != "" && strings.EqualFold(meta.SelectedTitle, meta.FilenameTitle) {
		return "filename_fallback"
	}
	if meta.EmbeddedTitle != "" {
		return "embedded_metadata"
	}
	if meta.FilenameTitle != "" {
		return "filename_fallback"
	}
	return "unknown"
}

func selectedAuthorSource(meta CandidateMetadata) string {
	if meta.EmbeddedAuthor != "" && strings.EqualFold(meta.SelectedAuthor, meta.EmbeddedAuthor) {
		return "embedded_metadata"
	}
	if meta.FilenameAuthor != "" && strings.EqualFold(meta.SelectedAuthor, meta.FilenameAuthor) {
		return "filename_fallback"
	}
	if meta.EmbeddedAuthor != "" {
		return "embedded_metadata"
	}
	if meta.FilenameAuthor != "" {
		return "filename_fallback"
	}
	return "unknown"
}
