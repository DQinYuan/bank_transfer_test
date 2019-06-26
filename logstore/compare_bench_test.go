package logstore

import "testing"

func BenchmarkCompare(b *testing.B) {
	start1 := 1
	start2 := 2
	for n := 0; n < b.N; n++ {
		sn1 := start1 + n
		sn2 := start2 + n
		compare(sn1, sn2)
	}
}

func compare(b1 int, b2 int) bool  {
	return b1 == b2
}
