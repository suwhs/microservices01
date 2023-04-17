package utils

import "fmt"

type StringFifo struct {
	queue []string
}

func NewStringFifo() *StringFifo {
	return &StringFifo{queue: make([]string, 0, 100)}
}

func (this *StringFifo) Get() string {
	val := this.queue[0]
	this.queue = this.queue[1:]
	return val
}

func (this *StringFifo) Put(value string) {
	this.queue = append(this.queue, value)
}

func (this *StringFifo) GetAndPut(value string) string {
	res := this.queue[0]
	this.queue = append(this.queue[1:], value)
	return res
}

func (this *StringFifo) Size() int {
	return len(this.queue)
}

func (this StringFifo) String() string {
	return fmt.Sprintf("[%d/%d]<%v>", len(this.queue), cap(this.queue), this.queue)
}
