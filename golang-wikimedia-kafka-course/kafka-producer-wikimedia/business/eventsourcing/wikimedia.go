package eventsourcing

import (
	"log"
	"strings"

	"github.com/douglira/kafka-producer-wikimedia/adapters/sse"
)

type WikimediaEventSource struct {
	*sse.SSEClient
}

func NewWikimedia(sse *sse.SSEClient) *WikimediaEventSource {
	return &WikimediaEventSource{sse}
}

func (w *WikimediaEventSource) HandleMessage(er *sse.EventReader) error {
	line, err := er.BodyReader.ReadBytes('\n')
	if err != nil {
		log.Println("Reader error:", err)
		w.OnError(err)
		return err
	}
	eventLine := string(line)
	if strings.Contains(eventLine, "id: ") {
		s := strings.Split(eventLine, "id: ")
		er.MessageEvent.Id = s[1]
	}
	if strings.Contains(eventLine, "data: ") {
		l := strings.Split(eventLine, "data: ")
		s := strings.TrimSpace(l[1])
		er.MessageEvent.Data = s
		w.OnSend(er)
	}
	return nil
}
