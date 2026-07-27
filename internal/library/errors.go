package library

import "errors"

var (
	ErrInvalidDomainObject = errors.New("invalid library domain object")
	ErrNotFound            = errors.New("library object not found")
	ErrAmbiguousMatch      = errors.New("library match is ambiguous")
	ErrUnsupportedFormat   = errors.New("unsupported library file format")
	ErrUnsafePath          = errors.New("unsafe library path")
	ErrReadOnlyRepository  = errors.New("repository is read-only")

	ErrBookNotFound         = ErrNotFound
	ErrDuplicateBook        = errors.New("duplicate book")
	ErrInvalidIdentifier    = errors.New("invalid identifier")
	ErrRepositoryReadOnly   = ErrReadOnlyRepository
	ErrUnsupportedOperation = errors.New("unsupported library operation")
)
