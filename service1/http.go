package service1

import (
	"bytes"
	"context"
	"net/http"
)

type HttpInterface interface {
	POST(ctx context.Context, url string, contentType string, body []byte) error
}

type DefaultHttpInterface struct {
}

func (this *DefaultHttpInterface) POST(ctx context.Context, url string, contentType string, body []byte) error {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	r.Header.Add("Content-Type", contentType)
	client := &http.Client{}
	_, err = client.Do(r)
	return err
}
