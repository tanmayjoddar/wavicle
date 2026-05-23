package semantic

import (
	"math"
	"wavicle/internal/core"
)

func CircularConvolution(a, b core.Vector) core.Vector {
	n := len(a)
	var result core.Vector
	for k := 0; k < n; k++ {
		var sum float32
		for j := 0; j < n; j++ {
			idx := (k - j) % n
			if idx < 0 {
				idx += n
			}
			sum += a[j] * b[idx]
		}
		result[k] = sum
	}
	return normalize(result)
}

func CircularCorrelation(a, b core.Vector) core.Vector {
	n := len(a)
	var result core.Vector
	for k := 0; k < n; k++ {
		var sum float32
		for j := 0; j < n; j++ {
			idx := (k + j) % n
			sum += a[j] * b[idx]
		}
		result[k] = sum
	}
	return normalize(result)
}

func Bind(key, value core.Vector) core.Vector {
	return normalize(CircularConvolution(key, value))
}

func Unbind(bundle, key core.Vector) core.Vector {
	return normalize(CircularCorrelation(bundle, key))
}

func Superpose(vectors []core.Vector) core.Vector {
	n := len(vectors[0])
	var sum core.Vector
	for _, v := range vectors {
		for i := 0; i < n; i++ {
			sum[i] += v[i]
		}
	}
	norm := float32(0)
	for i := 0; i < n; i++ {
		norm += sum[i] * sum[i]
	}
	norm = float32(math.Sqrt(float64(norm)))
	if norm > 0 {
		for i := 0; i < n; i++ {
			sum[i] /= norm
		}
	}
	return sum
}

func normalize(v core.Vector) core.Vector {
	var norm float64
	for i := 0; i < len(v); i++ {
		norm += float64(v[i]) * float64(v[i])
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return v
	}
	var result core.Vector
	for i := 0; i < len(v); i++ {
		result[i] = float32(float64(v[i]) / norm)
	}
	return result
}

func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

func EmbedString(s string) core.Vector {
	var v core.Vector
	h := uint64(0)
	for i, c := range s {
		h = h*31 + uint64(c)
		for j := 0; j < 4; j++ {
			idx := (i*4 + j) % len(v)
			v[idx] += float32(int8(h & 0xFF)) / 128.0
			h >>= 8
		}
	}
	return normalize(v)
}
