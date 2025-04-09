package main

import (
	"flag"
	"sspuller/pkg/puller"
	"strings"

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

var (
	VALID_SPORTS = [2]string{"basketball", "football"}
)

func main() {

	year := flag.String("year", "", "desired year")
	sport := flag.String("sport", "", "desired sport")
	dataPath := flag.String("data", "../data", "data folder")
	events := flag.Bool("scheduled-events", false, "pull scheduled events")
	maxErrors := flag.Int("max-errors", 25, "max upstream errors tolerated (non-200 non-404)")
	maxParallelism := flag.Int("max-parallelism", 25, "max number of parallel pulling processes")

	flag.Parse()

	if !strings.HasPrefix(*year, "20") && len(*year) != 4 {
		logrus.Fatal("you need to pass a valid year (from 2000 - 2025)")
	}

	invalidSport := true
	for _, vsport := range VALID_SPORTS {
		if vsport == *sport {
			invalidSport = false
			break
		}
	}
	if invalidSport {
		logrus.Fatal("you need to specify a valid sport (basketball, football)")
	}

	puller := puller.NewPuller(SCHEMA, BASE_URL, *dataPath)

	if *events {
		err := puller.ExportScheduledEvents("basketball", 2008, 10, 1, 25)
		if err != nil {
			logrus.Fatal("failed to reconciliate event data")
		}
	}

	err := puller.ExportEventData(*sport, *dataPath, *year, *maxParallelism, *maxErrors)
	if err != nil {
		logrus.Fatal("failed to export event data", err)
	}
}
