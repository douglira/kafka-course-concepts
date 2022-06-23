package sse

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

type SSEClient struct {
	url         string
	successes   chan []byte
	errors      chan error
	eventReader *EventReader
}

type MessageEvent struct {
	Id   string
	Data string
}

type EventReader struct {
	BodyReader   bufio.Reader
	MessageEvent *MessageEvent
	body         io.ReadCloser
}

func newEventReader(body io.ReadCloser) *EventReader {
	bodyReader := bufio.NewReader(body)
	return &EventReader{
		body:         body,
		BodyReader:   *bodyReader,
		MessageEvent: &MessageEvent{},
	}
}

type HandlerSuccess func(me *MessageEvent)

type HandlerError func(err error)

type MessageHandler func(er *EventReader)

func NewClient(url string) *SSEClient {

	sse := &SSEClient{
		url: url,
	}
	sse.successes = make(chan []byte)
	sse.errors = make(chan error)
	return sse
}

func (sse *SSEClient) Listen(handler MessageHandler) {
	go run(sse, handler)
}

func (sse *SSEClient) Successes() chan []byte {
	return sse.successes
}

func (sse *SSEClient) Errors() chan error {
	return sse.errors
}

func (sse *SSEClient) Close() {
	sse.eventReader.body.Close()

	close(sse.successes)
	close(sse.errors)
}

func (sse *SSEClient) SendEvent(er *EventReader) {
	messageBytes, err := json.Marshal(er.MessageEvent)
	if err != nil {
		sse.errors <- err
		return
	}
	sse.successes <- messageBytes
}

func (sse *SSEClient) Error(err error) {
	sse.errors <- err
}

func run(sse *SSEClient, h MessageHandler) {
	res, err := http.Get(sse.url)
	if err != nil {
		log.Println("Request Error:", err)
		return
	}

	sse.eventReader = newEventReader(res.Body)

	for {
		h(sse.eventReader)
	}
}
