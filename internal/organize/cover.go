package organize

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strings"
)

const maxEmbeddedCoverBytes = 8 << 20

// ExtractedCover is artwork read from a user's local book file.
type ExtractedCover struct {
	Data     []byte
	MimeType string
	Ext      string
	Width    int
	Height   int
	Source   string
}

// ExtractEmbeddedCover extracts local artwork when the format can be parsed
// safely without external providers or heavyweight renderers. Unsupported
// formats return (nil, nil) so callers can continue using placeholders.
func ExtractEmbeddedCover(filePath string) (*ExtractedCover, error) {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".epub":
		return ExtractEPUBCover(filePath)
	default:
		return nil, nil
	}
}

// ExtractEPUBCover reads the cover image referenced by an EPUB OPF package.
func ExtractEPUBCover(filePath string) (*ExtractedCover, error) {
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("open epub zip: %w", err)
	}
	defer r.Close()

	opfFile, err := findOPFFile(r.File)
	if err != nil {
		return nil, err
	}
	opf, err := readOPFPackage(opfFile)
	if err != nil {
		return nil, err
	}
	coverHref := selectEPUBCoverHref(opf)
	if coverHref == "" {
		return nil, nil
	}
	coverName := path.Clean(path.Join(path.Dir(opfFile.Name), coverHref))
	coverFile := findZipFile(r.File, coverName)
	if coverFile == nil {
		return nil, fmt.Errorf("epub cover image not found")
	}
	if coverFile.UncompressedSize64 > maxEmbeddedCoverBytes {
		return nil, fmt.Errorf("epub cover image is too large")
	}
	rc, err := coverFile.Open()
	if err != nil {
		return nil, fmt.Errorf("open epub cover image: %w", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, maxEmbeddedCoverBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read epub cover image: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("epub cover image is empty")
	}
	if len(data) > maxEmbeddedCoverBytes {
		return nil, fmt.Errorf("epub cover image is too large")
	}
	mimeType := http.DetectContentType(data)
	if !strings.HasPrefix(mimeType, "image/") {
		return nil, fmt.Errorf("epub cover is not an image")
	}
	normalized, normalizedMime, width, height := normalizeCoverThumbnail(data)
	if len(normalized) > 0 {
		data = normalized
		mimeType = normalizedMime
	}
	if width == 0 || height == 0 {
		width, height = imageSize(data)
	}
	ext := extensionForMime(mimeType)
	if ext == "" {
		ext = strings.ToLower(filepath.Ext(coverFile.Name))
	}
	if ext == "" {
		ext = ".img"
	}
	return &ExtractedCover{
		Data:     data,
		MimeType: mimeType,
		Ext:      ext,
		Width:    width,
		Height:   height,
		Source:   "embedded_epub",
	}, nil
}

func findOPFFile(files []*zip.File) (*zip.File, error) {
	for _, f := range files {
		if strings.EqualFold(f.Name, "META-INF/container.xml") {
			rc, err := f.Open()
			if err != nil {
				continue
			}
			var container containerXML
			decodeErr := xml.NewDecoder(rc).Decode(&container)
			_ = rc.Close()
			if decodeErr != nil {
				continue
			}
			for _, rf := range container.Rootfiles {
				if opf := findZipFile(files, rf.FullPath); opf != nil {
					return opf, nil
				}
			}
		}
	}
	for _, f := range files {
		if strings.EqualFold(filepath.Ext(f.Name), ".opf") {
			return f, nil
		}
	}
	return nil, fmt.Errorf("no .opf file found in epub")
}

func readOPFPackage(f *zip.File) (*opfPackage, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("open opf: %w", err)
	}
	defer rc.Close()
	var pkg opfPackage
	if err := xml.NewDecoder(rc).Decode(&pkg); err != nil {
		return nil, fmt.Errorf("parse opf: %w", err)
	}
	return &pkg, nil
}

func selectEPUBCoverHref(pkg *opfPackage) string {
	if pkg == nil {
		return ""
	}
	manifest := map[string]opfManifestItem{}
	for _, item := range pkg.Manifest.Items {
		manifest[item.ID] = item
	}
	for _, meta := range pkg.Metadata.Meta {
		if strings.EqualFold(strings.TrimSpace(meta.Name), "cover") {
			if item, ok := manifest[strings.TrimSpace(meta.Content)]; ok {
				return item.Href
			}
		}
	}
	for _, item := range pkg.Manifest.Items {
		if strings.Contains(strings.ToLower(item.Properties), "cover-image") {
			return item.Href
		}
	}
	for _, ref := range pkg.Guide.References {
		if strings.EqualFold(ref.Type, "cover") {
			return ref.Href
		}
	}
	for _, item := range pkg.Manifest.Items {
		if strings.HasPrefix(strings.ToLower(item.MediaType), "image/") {
			return item.Href
		}
	}
	return ""
}

func findZipFile(files []*zip.File, name string) *zip.File {
	name = strings.TrimLeft(path.Clean(strings.ReplaceAll(name, "\\", "/")), "/")
	for _, f := range files {
		if path.Clean(f.Name) == name {
			return f
		}
	}
	return nil
}

func imageSize(data []byte) (int, int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

func normalizeCoverThumbnail(data []byte) ([]byte, string, int, int) {
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", 0, 0
	}
	bounds := src.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return nil, "", 0, 0
	}
	maxWidth := 480
	maxHeight := 720
	targetWidth, targetHeight := width, height
	if targetWidth > maxWidth || targetHeight > maxHeight {
		scaleW := float64(maxWidth) / float64(targetWidth)
		scaleH := float64(maxHeight) / float64(targetHeight)
		scale := scaleW
		if scaleH < scale {
			scale = scaleH
		}
		targetWidth = maxInt(1, int(float64(targetWidth)*scale))
		targetHeight = maxInt(1, int(float64(targetHeight)*scale))
	}
	dst := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
	for y := 0; y < targetHeight; y++ {
		srcY := bounds.Min.Y + y*height/targetHeight
		for x := 0; x < targetWidth; x++ {
			srcX := bounds.Min.X + x*width/targetWidth
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}
	var out bytes.Buffer
	if err := jpeg.Encode(&out, dst, &jpeg.Options{Quality: 82}); err != nil {
		return nil, "", 0, 0
	}
	return out.Bytes(), "image/jpeg", targetWidth, targetHeight
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func extensionForMime(mimeType string) string {
	if exts, err := mime.ExtensionsByType(mimeType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	switch mimeType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	default:
		return ""
	}
}
