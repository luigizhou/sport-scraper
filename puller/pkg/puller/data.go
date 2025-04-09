package puller

type ScheduledEvents struct {
	Events []*Event `json:"events"`
}

type Event struct {
	ID                       int  `json:"id"`
	HasEventPlayerStatistics bool `json:"hasEventPlayerStatistics"`
}
