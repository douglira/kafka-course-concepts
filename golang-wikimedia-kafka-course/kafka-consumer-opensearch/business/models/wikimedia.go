package models

type WikimediaMeta struct {
	Id string `json:"id"`
}

type Wikimedia struct {
	Id   string        `json:"_id"`
	Meta WikimediaMeta `json:"meta"`
}
