package handlererror

import (
	"github.com/cockroachdb/errors"
)

var ErrInvalidParameter = errors.New("invalid parameter")

var ErrNotFound = errors.New("not found")

var ErrDuplicateRecord = errors.New("duplicate record")

// ErrConflict indicates that the request conflicts with the current state of
// a resource, e.g. an operation that is already in progress with another owner.
var ErrConflict = errors.New("conflict")
