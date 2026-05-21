package autopoiesis

import (
	"crypto/sha3"
	"sort"
	"wavicle/internal/core"
)

type CausalEdge struct {
	SourcePath        string
	TargetPath        string
	StructuralBinding float32
	TemporalLift      float32
	Confidence        float32
	DiscoveredBy      string
}

const (
	ChannelStructuralDiffraction = "StructuralDiffraction"
)

type DiffractionTree struct {
	Path     string
	Hash     core.Hash
	Children []*DiffractionTree
}

func DiffractValue(path string, value core.Value, shadow *CausalShadow) *DiffractionTree {
	switch v := value.(type) {
	case core.VRecord:
		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		children := make([]*DiffractionTree, 0, len(v))
		for _, key := range keys {
			childPath := path + "." + key
			child := DiffractValue(childPath, v[key], shadow)
			children = append(children, child)

			// Register structural dependency edge
			shadow.AddEdge(&CausalEdge{
				SourcePath:        childPath,
				TargetPath:        path,
				StructuralBinding: 1.0,
				Confidence:        1.0,
				DiscoveredBy:      ChannelStructuralDiffraction,
			})
		}
		return &DiffractionTree{
			Path:     path,
			Hash:     merkleHashChildren(children),
			Children: children,
		}

	case core.VString:
		return &DiffractionTree{
			Path: path,
			Hash: sha3.Sum256([]byte(v)),
		}
	default:
		return &DiffractionTree{
			Path: path,
			Hash: sha3.Sum256(value.Serialize()),
		}
	}
}

func merkleHashChildren(children []*DiffractionTree) core.Hash {
	h := sha3.New256()
	for _, child := range children {
		h.Write(child.Hash[:])
	}
	var res core.Hash
	copy(res[:], h.Sum(nil))
	return res
}

type CausalShadow struct {
	Edges []*CausalEdge
}

func (s *CausalShadow) AddEdge(e *CausalEdge) {
	s.Edges = append(s.Edges, e)
}
