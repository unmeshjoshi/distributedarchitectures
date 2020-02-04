package types

import "strconv"

// ID represents a generic identifier which is canonically
// stored as a uint64 but is typically represented as a
// base-16 string for input/output
type ID uint64

func (i ID) String() string {
	return strconv.FormatUint(uint64(i), 16)
}

// IDFromString attempts to create an ID from a base-16 string.
func IDFromString(s string) (ID, error) {
	i, err := strconv.ParseUint(s, 16, 64)
	return ID(i), err
}

// IDSlice implements the sort interface
type IDSlice []ID

func (p IDSlice) Len() int           { return len(p) }
func (p IDSlice) Less(i, j int) bool { return uint64(p[i]) < uint64(p[j]) }
func (p IDSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
