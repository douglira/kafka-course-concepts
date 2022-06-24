package models

type WikimediaMeta struct {
	Id string `json:"id"`
}

type Wikimedia struct {
	Meta WikimediaMeta `json:"meta"`
}
