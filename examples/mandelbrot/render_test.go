package main

import (
	"image"
	"testing"
)

func TestEscape(t *testing.T) {
	const maxIter = 1000
	cases := []struct {
		cr, ci float64
		want   int
	}{
		{0, 0, maxIter},
		{-1, 0, maxIter},
		{-2, 0, maxIter},
		{0.25, 0, maxIter},
		{0, 1, maxIter},
		{1, 0, 3},
		{2, 0, 2},
		{3, 0, 1},
		{1, 1, 2},
	}
	for _, c := range cases {
		if got := escape(c.cr, c.ci, maxIter); got != c.want {
			t.Errorf("escape(%v, %v) = %d, want %d", c.cr, c.ci, got, c.want)
		}
	}
}

func TestGridCoversImageOnce(t *testing.T) {
	const size, side = 100, 7
	seen := make([]int, size*size)
	for _, r := range grid(size, side) {
		for y := r.Min.Y; y < r.Max.Y; y++ {
			for x := r.Min.X; x < r.Max.X; x++ {
				seen[y*size+x]++
			}
		}
	}
	for i, n := range seen {
		if n != 1 {
			t.Fatalf("pixel (%d, %d) covered %d times", i%size, i/size, n)
		}
	}
}

func TestRenderIndependentOfPartition(t *testing.T) {
	const size, maxIter = 64, 100
	whole := renderTile(Tile{Rect: image.Rect(0, 0, size, size), Size: size, MaxIter: maxIter})
	for _, r := range grid(size, 4) {
		pix := renderTile(Tile{Rect: r, Size: size, MaxIter: maxIter})
		for i, p := range pix {
			x, y := r.Min.X+i%r.Dx(), r.Min.Y+i/r.Dx()
			if p != whole[y*size+x] {
				t.Fatalf("pixel (%d, %d) differs", x, y)
			}
		}
	}
}
