package organize

import (
	"archive/zip"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
)

// ErrCoverTooLarge is returned when an EPUB's cover image exceeds the size cap.
var ErrCoverTooLarge = errors.New("epub cover too large")

type opfCoverPackage struct {
	Metas []opfCoverMeta `xml:"metadata>meta"`
	Items []opfCoverItem `xml:"manifest>item"`
}

type opfCoverMeta struct {
	Name    string `xml:"name,attr"`
	Content string `xml:"content,attr"`
}

type opfCoverItem struct {
	ID         string `xml:"id,attr"`
	Href       string `xml:"href,attr"`
	Properties string `xml:"properties,attr"`
}

// ExtractEPUBCover returns the cover image bytes declared by an EPUB's OPF.
// The EPUB is untrusted: entries are looked up by exact name and reads are
// capped at maxBytes whatever the zip headers claim.
func ExtractEPUBCover(epubPath string, maxBytes int64) ([]byte, error) {
	r, err := zip.OpenReader(epubPath)
	if err != nil {
		return nil, fmt.Errorf("open epub zip: %w", err)
	}
	defer r.Close()

	opfFile := findOPF(&r.Reader)
	if opfFile == nil {
		return nil, fmt.Errorf("no .opf file found in epub")
	}
	rc, err := opfFile.Open()
	if err != nil {
		return nil, fmt.Errorf("open opf: %w", err)
	}
	var pkg opfCoverPackage
	err = xml.NewDecoder(io.LimitReader(rc, 4<<20)).Decode(&pkg)
	rc.Close()
	if err != nil {
		return nil, fmt.Errorf("parse opf: %w", err)
	}

	href := coverHref(&pkg)
	if href == "" {
		return nil, fmt.Errorf("epub declares no cover")
	}
	if u, err := url.PathUnescape(href); err == nil {
		href = u
	}
	name := path.Join(path.Dir(opfFile.Name), href)

	for _, f := range r.File {
		if f.Name != name {
			continue
		}
		cr, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open cover: %w", err)
		}
		defer cr.Close()
		data, err := io.ReadAll(io.LimitReader(cr, maxBytes+1))
		if err != nil {
			return nil, fmt.Errorf("read cover: %w", err)
		}
		if int64(len(data)) > maxBytes {
			return nil, ErrCoverTooLarge
		}
		return data, nil
	}
	return nil, fmt.Errorf("cover %q not found in epub", name)
}

// coverHref returns the manifest href of the declared cover: an EPUB 3
// cover-image item first, then the EPUB 2 <meta name="cover"> reference.
func coverHref(pkg *opfCoverPackage) string {
	for _, it := range pkg.Items {
		for _, p := range strings.Fields(it.Properties) {
			if p == "cover-image" {
				return it.Href
			}
		}
	}
	for _, m := range pkg.Metas {
		if m.Name != "cover" && m.Name != "cover-image" {
			continue
		}
		for _, it := range pkg.Items {
			if it.ID == m.Content {
				return it.Href
			}
		}
	}
	return ""
}
