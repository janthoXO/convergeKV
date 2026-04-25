// Package crdt implements the Add-Wins Last-Write-Wins Map (AWLWWMap) CRDT.
// Each key stores a field-wise AWLWWMap where individual JSON object fields
// compete independently under a Last-Write-Wins rule broken by replica ID.
package crdt

// AWLWWMap is the per-key CRDT state: a map of field name -> FieldEntry.
// The zero value is a valid empty map.
type AWLWWMap struct {
	Fields map[string]FieldEntry // never nil after initialisation
}

// NewAWLWWMap returns an empty, ready-to-use AWLWWMap.
func NewAWLWWMap() AWLWWMap {
	return AWLWWMap{Fields: make(map[string]FieldEntry)}
}
