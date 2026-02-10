package core

import (
	"sync"
	"unsafe"
)

// RepoIntern handles string interning for repository paths
// Replaces 16-byte string pointers with 2-byte uint16 IDs
type RepoIntern struct {
	mu       sync.RWMutex
	pathToID map[string]uint16
	idToPath []string
	nextID   uint16
}

func NewRepoIntern() *RepoIntern {
	return &RepoIntern{
		pathToID: make(map[string]uint16, RepoInternCapacity),
		idToPath: make([]string, 0, RepoInternCapacity),
	}
}

func (r *RepoIntern) Intern(path string) uint16 {
	r.mu.RLock()
	if id, exists := r.pathToID[path]; exists {
		r.mu.RUnlock()
		return id
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if id, exists := r.pathToID[path]; exists {
		return id
	}

	id := r.nextID
	r.nextID++
	r.pathToID[path] = id
	r.idToPath = append(r.idToPath, path)
	return id
}

func (r *RepoIntern) Lookup(id uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(id) < len(r.idToPath) {
		return r.idToPath[id]
	}
	return ""
}

// AuthorIntern handles string interning for author emails
// Replaces 16-byte string pointers with 4-byte uint32 IDs
// Uses double-checked locking for thread-safe interning
type AuthorIntern struct {
	mu        sync.RWMutex
	emailToID map[string]uint32
	idToEmail []string
	nextID    uint32
}

func NewAuthorIntern() *AuthorIntern {
	return &AuthorIntern{
		emailToID: make(map[string]uint32, AuthorInternCapacity),
		idToEmail: make([]string, 1, AuthorInternCapacity), // slot 0 reserved as sentinel
		nextID:    1,                                       // 0 is sentinel for "no author"
	}
}

func (a *AuthorIntern) Intern(email string) uint32 {
	a.mu.RLock()
	if id, exists := a.emailToID[email]; exists {
		a.mu.RUnlock()
		return id
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()

	if id, exists := a.emailToID[email]; exists {
		return id
	}

	id := a.nextID
	a.nextID++
	a.emailToID[email] = id
	a.idToEmail = append(a.idToEmail, email)
	return id
}

// InternBytes interns a byte slice as a string without allocating unless necessary
// Uses zero-copy string conversion for lookup (Go 1.20+)
func (a *AuthorIntern) InternBytes(email []byte) uint32 {
	a.mu.RLock()
	emailStr := unsafe.String(unsafe.SliceData(email), len(email))
	if id, exists := a.emailToID[emailStr]; exists {
		a.mu.RUnlock()
		return id
	}
	a.mu.RUnlock()

	emailStrCopy := string(email)
	return a.Intern(emailStrCopy)
}

func (a *AuthorIntern) Lookup(id uint32) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if int(id) < len(a.idToEmail) {
		return a.idToEmail[id]
	}
	return ""
}

// Global state
var Verbose bool
var GlobalMetrics Metrics
var RepoInterns = NewRepoIntern()
var AuthorInterns = NewAuthorIntern()
