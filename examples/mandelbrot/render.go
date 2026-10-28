package main

import (
	"image"
	"image/color"
	"math"
)

const (
	background = 0 // pixel not computed yet
	interior   = 1
	firstBand  = 2
	bands      = 256 - firstBand
	bandStep   = 3

	centreRe = -0.75
	centreIm = 0.0
	span     = 3.0
)

// escape returns the iterations z = z²+c takes to leave the disc of radius 2, or maxIter if it
// stays inside.
func escape(cr, ci float64, maxIter int) int {
	var zr, zi, zr2, zi2 float64
	for i := range maxIter {
		if zr2+zi2 > 4 {
			return i
		}
		zi = 2*zr*zi + ci
		zr = zr2 - zi2 + cr
		zr2, zi2 = zr*zr, zi*zi
	}
	return maxIter
}

func shade(iter, maxIter int) byte {
	if iter >= maxIter {
		return interior
	}
	return byte(firstBand + iter*bandStep%bands)
}

// renderTile returns t's pixels row by row. They depend only on absolute image coordinates, so the
// image does not depend on how it is partitioned or placed.
func renderTile(t Tile) []byte {
	step := span / float64(t.Size)
	left, top := centreRe-span/2, centreIm-span/2
	pix := make([]byte, 0, t.Rect.Dx()*t.Rect.Dy())
	for y := t.Rect.Min.Y; y < t.Rect.Max.Y; y++ {
		ci := top + (float64(y)+0.5)*step
		for x := t.Rect.Min.X; x < t.Rect.Max.X; x++ {
			cr := left + (float64(x)+0.5)*step
			pix = append(pix, shade(escape(cr, ci, t.MaxIter), t.MaxIter))
		}
	}
	return pix
}

// tileSide returns the side of the square grid holding n tiles, rounded to the nearest integer.
func tileSide(n int) int {
	return int(math.Round(math.Sqrt(float64(n))))
}

// grid splits a square image into side×side tiles, in raster order.
func grid(size, side int) []image.Rectangle {
	rs := make([]image.Rectangle, 0, side*side)
	for y := range side {
		for x := range side {
			rs = append(rs, image.Rect(x*size/side, y*size/side, (x+1)*size/side, (y+1)*size/side))
		}
	}
	return rs
}

var gradient = []color.RGBA{
	{0, 7, 100, 255},
	{32, 107, 203, 255},
	{237, 255, 255, 255},
	{255, 170, 0, 255},
	{0, 2, 0, 255},
}

func palette() color.Palette {
	p := make(color.Palette, 256)
	p[background] = color.RGBA{64, 64, 64, 255}
	p[interior] = color.RGBA{0, 0, 0, 255}
	for i := range bands {
		pos := i * len(gradient)
		s, r := pos/bands, pos%bands
		a, b := gradient[s], gradient[(s+1)%len(gradient)]
		mix := func(x, y uint8) uint8 { return uint8((int(x)*(bands-r) + int(y)*r) / bands) }
		p[firstBand+i] = color.RGBA{mix(a.R, b.R), mix(a.G, b.G), mix(a.B, b.B), 255}
	}
	return p
}
