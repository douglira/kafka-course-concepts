package eventsourcing

import (
	"log"
	"strings"

	"github.com/douglira/kafka-producer-wikimedia/adapters/sse"
)

type WikimediaEventSource struct {
	sseClient *sse.SSEClient
}

func NewWikimedia(sse *sse.SSEClient) WikimediaEventSource {
	return WikimediaEventSource{
		sseClient: sse,
	}
}

func (w *WikimediaEventSource) HandleMessage() {
	w.sseClient.Listen(func(er *sse.EventReader) {
		line, err := er.BodyReader.ReadBytes('\n')
		if err != nil {
			log.Println("Reader error:", err)
			w.sseClient.Error(err)
			return
		}
		eventLine := string(line)
		if strings.Contains(eventLine, "id: ") {
			s := strings.Split(eventLine, "id: ")
			er.MessageEvent.Id = s[1]
		}
		if strings.Contains(eventLine, "data: ") {
			l := strings.Split(eventLine, "data: ")
			s := strings.Trim(l[1], "\n")
			er.MessageEvent.Data = s
			w.sseClient.SendEvent(er)
		}
	})
}

func (w *WikimediaEventSource) Successes() chan []byte {
	return w.sseClient.Successes()
}

func (w *WikimediaEventSource) Errors() chan error {
	return w.sseClient.Errors()
}

func (w *WikimediaEventSource) Close() {
	w.sseClient.Close()
}
