package athena

import (
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
)

type Config struct {
	Session        *session.Session
	Database       string
	OutputLocation string

	PollFrequency time.Duration
}

func configFromConnectionString(connStr string) (*Config, error) {
	args, err := url.ParseQuery(connStr)
	if err != nil {
		return nil, err
	}

	var cfg Config

	cfg.Session, err = session.NewSession()
	if err != nil {
		return nil, err
	}

	cfg.Database = args.Get("db")
	cfg.OutputLocation = args.Get("output_location")

	frequencyStr := args.Get("poll_frequency")
	if frequencyStr != "" {
		cfg.PollFrequency, err = time.ParseDuration(frequencyStr)
		if err != nil {
			return nil, fmt.Errorf("invalid poll_frequency parameter: %s", frequencyStr)
		}
	}

	return &cfg, nil
}
