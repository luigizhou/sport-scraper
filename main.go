package main

import (
	"sspuller/pkg/puller"

	"github.com/sirupsen/logrus"
)

var daysInMonth = map[int]int{
	1:  31,
	2:  28,
	3:  31,
	4:  30,
	5:  31,
	6:  30,
	7:  31,
	8:  31,
	9:  30,
	10: 31,
	11: 30,
	12: 31,
}

const (
	SCHEMA        = "https"
	BASE_URL      = "www.sofascore.com"
	EXPORT_FOLDER = "../data"
)

func main() {
	// add cli

	puller := puller.NewPuller(SCHEMA, BASE_URL, EXPORT_FOLDER)
	reconciliateScheduledEvents := false
	if reconciliateScheduledEvents {
		err := puller.ExportScheduledEvents("basketball", 2008, 10, 1, 25)
		if err != nil {
			logrus.Fatal("failed to reconciliate event data")
		}
	}

	err := puller.ExportEventData("basketball", "../data", "2022", 25, 10)
	if err != nil {
		logrus.Fatal("failed to export event data")
	}
}
