package db

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"

	opensearch "github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type ElasticSearchClient struct {
	client *opensearch.Client
	index  string
}

func NewElasticSearchClient(index string) *ElasticSearchClient {

	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // For testing only. Use certificate for validation.
		},
		Addresses: []string{"http://localhost:9200"},
		Username:  "admin", // For testing only. Don't store credentials in code.
		Password:  "admin",
	})
	if err != nil {
		log.Println("cannot initialize", err)
	}
	log.Println(client.Info())
	return &ElasticSearchClient{
		client,
		index,
	}
}

func (e *ElasticSearchClient) CreateIndex() error {
	indexExistRequest := opensearchapi.IndicesExistsRequest{
		Index: []string{e.index},
	}
	resIndexExists, err := indexExistRequest.Do(context.Background(), e.client)
	if err != nil {
		log.Println("[OpenSearchClient]Error at checking if index exists", err)
		return err
	}
	if resIndexExists.StatusCode == 404 {
		createIndex := opensearchapi.IndicesCreateRequest{
			Index: e.index,
		}
		_, err := createIndex.Do(context.Background(), e.client)
		if err != nil {
			log.Print("[OpenSearchClient]Error at index creation", err)
		}
	}
	return nil
}
