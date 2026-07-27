package library

import (
	"context"
	"io"
)

type PathResolution struct {
	RequestedPath string
	ResolvedPath  string
	Root          string
	Managed       bool
}

type MoveRequest struct {
	Source      string
	Destination string
	Overwrite   bool
}

type DeliveryDestination struct {
	ID               string
	Name             string
	SupportedFormats []string
	PreferredFormats []string
}

type FileService interface {
	ResolvePath(context.Context, string) (PathResolution, error)
	ValidatePath(context.Context, string) error
	DeleteManagedFile(context.Context, BookFile) error
	MoveManagedFile(context.Context, MoveRequest) (PathResolution, error)
	OpenDownload(context.Context, BookFile) (io.ReadCloser, error)
}

type DeviceDeliveryService interface {
	SendToKindle(context.Context, Book, DeliveryDestination) error
	Export(context.Context, Book, DeliveryDestination) error
}
