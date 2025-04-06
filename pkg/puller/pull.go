package puller

import (
	"archive/tar"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

type Puller struct {
	client       *http.Client
	schema       string
	baseUrl      string
	exportFolder string
	force        bool
}

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

func NewPuller(sch, bUrl, ef string) *Puller {

	cl := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	return &Puller{
		client:       cl,
		schema:       sch,
		baseUrl:      bUrl,
		exportFolder: ef,
	}
}

func (p *Puller) ExportScheduledEvents(sport string, startingYear, startingMonth, startingDay int, maxErrors int) error {

	multiPuller := func(sport, year, month, day string, ch chan error) {
		ch <- p.download(
			fmt.Sprintf("%s://%s/api/v1/sport/%s/scheduled-events", p.schema, p.baseUrl, fmt.Sprintf("%s-%s-%s", year, month, day)),
			fmt.Sprintf("%s/%s/%s/%s/%s", p.exportFolder, year, month, day, sport),
			"scheduled-events.json",
		)
	}

	errorsCount := 0
	ch := make(chan error, 10)
	yearLimit, _, _ := time.Now().Date()

	for year := startingYear; year <= yearLimit; year++ {
		for month := 1; month <= 12; month++ {

			for day := 1; day <= daysInMonth[month]; day++ {
				if year == startingYear && month <= startingMonth && day < startingDay {
					continue
				}

				yearStr := fmt.Sprintf("%d", year)
				monthStr := fmt.Sprintf("%d", month)
				if month < 10 {
					monthStr = fmt.Sprintf("0%d", month)
				}
				dayStr := fmt.Sprintf("%d", day)
				if day < 10 {
					dayStr = fmt.Sprintf("0%d", day)
				}

				multiPuller(sport, yearStr, monthStr, dayStr, ch)
				err := <-ch
				if err != nil {
					errorsCount++
					logrus.Errorln("error while downloading historical data", err)
					//TODO log to file
				}

				if errorsCount > maxErrors {
					return fmt.Errorf("scheduled events: too many download errors")
				}
			}
		}
	}
	return nil
}

func (p *Puller) ExportEventData(sport, dataPah, yearFilter string, maxErrors, parallelism int) error {

	multiPuller := func(year, month, day, sport string, eventid int, etype string, ch chan error) {

		ch <- p.download(
			fmt.Sprintf("%s://%s/api/v1/event/%d/%s", p.schema, p.baseUrl, eventid, etype),
			fmt.Sprintf("%s/%s/%s/%s/%s", p.exportFolder, year, month, day, sport),
			fmt.Sprintf("%d-%s.json", eventid, etype),
		)
	}

	ch := make(chan error, parallelism)
	errors := 0
	years, err := os.ReadDir(dataPah)
	if err != nil {
		return fmt.Errorf("failed to read filesystem \"years\": %v", err)
	}

	for _, year := range years {
		yearStr := year.Name()
		fmt.Println(yearStr)
		months, err := os.ReadDir(fmt.Sprintf("%s/%s", dataPah, year.Name()))
		if err != nil {
			return fmt.Errorf("failed to read filesystem for \"months\": %v", err)
		}
		for _, month := range months {
			monthStr := month.Name()

			days, err := os.ReadDir(fmt.Sprintf("%s/%s/%s", dataPah, year.Name(), month.Name()))
			if err != nil {
				return fmt.Errorf("failed to read filesystem for \"days\": %v", err)
			}

			for _, day := range days {

				dayStr := day.Name()
				if yearFilter != "" {
					if year.Name() != yearFilter {
						continue
					}
				}

				baseDir := fmt.Sprintf("%s/%s/%s/%s/%s", p.exportFolder, yearStr, monthStr, dayStr, sport)
				fmt.Println(baseDir)
				logrus.Info("processing baseDir", baseDir)
				filename := fmt.Sprintf("%s/scheduled-events.json", baseDir)
				cmd := exec.Command(
					"tar", "-xvf", fmt.Sprintf("%s.tar.gz", filename),
					"-C", baseDir, "--strip-components", "3",
				)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					errors++
					logrus.Errorln("failed to decompress", filename)
					continue
				}

				if _, err := os.Stat(filename); os.IsNotExist(err) {
					continue
				}
				file, err := os.ReadFile(filename)
				if err != nil {
					return fmt.Errorf("failed to read file %s: %v", filename, err)
				}
				// Declaration of the instance of the struct that we want to fill
				var sched ScheduledEvents

				// Fill the instance from the JSON file content
				err = json.Unmarshal(file, &sched)
				if err != nil {
					return fmt.Errorf("failed unmarshal file %s: %v", filename, err)
				}

				err = os.Remove(filename)
				if err != nil {
					logrus.Errorln("failed to remove raw file", filename)
					errors++
				}

				for _, event := range sched.Events {
					multiPuller(yearStr, monthStr, dayStr, sport, event.ID, "statistics", ch)
					err := <-ch
					if err != nil {
						errors++
					}
					multiPuller(yearStr, monthStr, dayStr, sport, event.ID, "incidents", ch)
					err = <-ch
					if err != nil {
						errors++
					}
					multiPuller(yearStr, monthStr, dayStr, sport, event.ID, "votes", ch)
					err = <-ch
					if err != nil {
						errors++
					}
					multiPuller(yearStr, monthStr, dayStr, sport, event.ID, "graph", ch)
					err = <-ch
					if err != nil {
						errors++
					}
					if errors > maxErrors {
						return fmt.Errorf("event metadata: too many download errors")
					}
				}
			}
		}
	}

	return nil
}

func (p *Puller) download(url, exportDir, filename string) error {

	fullPathFile := fmt.Sprintf("%s/%s", exportDir, filename)
	compressedFile := fmt.Sprintf("%s.tar.gz", fullPathFile)
	_, err := os.Stat(fullPathFile)

	if err == nil && !p.force {
		logrus.Infoln("file exists already, skipping", fullPathFile)
		return nil
	}

	_, err = os.Stat(compressedFile)
	if err == nil && !p.force {
		logrus.Infoln("file exists already, skipping", compressedFile)
		return nil
	}

	logrus.Infoln("processing url >>", url)
	resp, err := p.client.Get(url)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		logrus.Errorf("error in response (status code not in [200, 404]) %d", resp.StatusCode)
		return fmt.Errorf("error in response (status code not in [200, 404])")
	}

	if _, err := os.Stat(exportDir); os.IsNotExist(err) {
		logrus.Infoln("creating folder", exportDir)
		err := os.MkdirAll(exportDir, 0755)
		if err != nil {
			return err
		}
	}

	logrus.Infoln("downloading data into file", fullPathFile)
	f, err := os.Create(fullPathFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}

	// Files which to include in the tar.gz archive
	files := []string{fullPathFile}

	// Create output file
	out, err := os.Create(compressedFile)
	if err != nil {
		return fmt.Errorf("failed to create archive file: %v", err)
	}
	defer out.Close()

	// Create the archive and write the output to the "out" Writer
	err = createArchive(files, out)
	if err != nil {
		return fmt.Errorf("failed to provision archive: %v", err)
	}

	err = os.Remove(fullPathFile)
	if err != nil {
		return fmt.Errorf("failed to remove file %v", err)
	}

	return nil
}

func createArchive(files []string, buf io.Writer) error {
	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "buf" writer
	gw := gzip.NewWriter(buf)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Iterate over files and add them to the tar archive
	for _, file := range files {
		err := addToArchive(tw, file)
		if err != nil {
			return err
		}
	}

	return nil
}

func addToArchive(tw *tar.Writer, filename string) error {
	// Open the file which will be written into the archive
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get FileInfo about our file providing file size, mode, etc.
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a tar Header from the FileInfo data
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	// Use full path as name (FileInfoHeader only takes the basename)
	// If we don't do this the directory strucuture would
	// not be preserved
	// https://golang.org/src/archive/tar/common.go?#L626
	header.Name = filename

	// Write file header to the tar archive
	err = tw.WriteHeader(header)
	if err != nil {
		return err
	}

	// Copy file content to tar archive
	_, err = io.Copy(tw, file)
	if err != nil {
		return err
	}

	return nil
}
