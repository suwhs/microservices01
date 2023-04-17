package utils_test

import (
	"strconv"
	"testing"

	"whs.su/svcs/utils"
)

func TestStringFifo(t *testing.T) {

	fifo := utils.NewStringFifo()

	if fifo.Size() != 0 {
		t.Fatalf("invalid size")
	}

	fifo.Put("1")

	if fifo.String() != "[1/100]<[1]>" {
		t.Fatal()
	}

	for x := 0; x < 20; x++ {
		fifo.Put(strconv.Itoa(x * 100))
	}

	if fifo.String() != "[21/100]<[1 0 100 200 300 400 500 600 700 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900]>" {
		t.Fatal()
	}

	for x := 2000; x < 2010; x++ {
		fifo.GetAndPut(strconv.Itoa(x))
	}

	if fifo.String() != "[21/90]<[900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009]>" {
		t.Fatal()
	}

	for x := 0; x < 10; x++ {
		_ = fifo.Get()
	}

	if fifo.String() != "[11/80]<[1900 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009]>" {
		t.Fatal()
	}

	for x := 0; x < 10; x++ {
		_ = fifo.Get()
	}

	if fifo.Size() != 1 {
		t.Fatal()
	}
}
