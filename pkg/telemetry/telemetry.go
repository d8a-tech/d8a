package telemetry

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type triggerType int

const (
	triggerOnStartup triggerType = iota
	triggerPeriodic
)

type trigger struct {
	typ      triggerType
	duration time.Duration
}

type eventConfig struct {
	trigger      triggerType
	duration     time.Duration
	eventName    string
	clientIDFunc func() string
	params       map[string]func() any
}

type option func(*telemetryClient)

type telemetryClient struct {
	url    string
	events []eventConfig
}

// OnStartup is a trigger type for immediate execution.
var OnStartup = trigger{typ: triggerOnStartup}

// EveryXHours returns a trigger for periodic execution.
func EveryXHours(duration time.Duration) trigger {
	return trigger{typ: triggerPeriodic, duration: duration}
}

// WithURL sets the telemetry endpoint URL.
func WithURL(url string) option {
	return func(tc *telemetryClient) {
		tc.url = url
	}
}

// WithEvent adds an event configuration.
func WithEvent(trig trigger, event *eventBuilder) option {
	return func(tc *telemetryClient) {
		cfg := eventConfig{
			trigger:      trig.typ,
			eventName:    event.name,
			clientIDFunc: event.clientIDFunc,
			params:       event.params,
		}
		if trig.typ == triggerPeriodic {
			cfg.duration = trig.duration
		}
		tc.events = append(tc.events, cfg)
	}
}

// SimpleEvent creates an event builder.
func SimpleEvent(name string, clientIDFunc func() string, paramBuilders ...*paramBuilder) *eventBuilder {
	eb := &eventBuilder{
		name:         name,
		clientIDFunc: clientIDFunc,
		params:       make(map[string]func() any),
	}
	for _, pb := range paramBuilders {
		eb.params[pb.key] = pb.valueFunc
	}
	return eb
}

// Raw returns a wrapper for static values.
func Raw(value any) *paramBuilder {
	return &paramBuilder{
		key:       "",
		valueFunc: func() any { return value },
	}
}

type eventBuilder struct {
	name         string
	clientIDFunc func() string
	params       map[string]func() any
}

type paramBuilder struct {
	key       string
	valueFunc func() any
}

// WithParam adds a parameter to an event.
func (eb *eventBuilder) WithParam(key string, value *paramBuilder) *eventBuilder {
	value.key = key
	eb.params[key] = value.valueFunc
	return eb
}

var (
	startTimeOnce sync.Once
	startTime     int64
)

// NumberOfSecsSinceStarted returns a paramBuilder that calculates seconds since app start.
// Start time is stored in memory on first call.
func NumberOfSecsSinceStarted() *paramBuilder {
	return &paramBuilder{
		key: "",
		valueFunc: func() any {
			startTimeOnce.Do(func() {
				startTime = getCurrentUnixTime()
			})
			return getCurrentUnixTime() - startTime
		},
	}
}

// Start initializes and starts the telemetry client.
func Start(opts ...option) {
	tc := &telemetryClient{}

	for _, opt := range opts {
		opt(tc)
	}

	for _, event := range tc.events {
		switch event.trigger {
		case triggerOnStartup:
			tc.sendEvent(event)
		case triggerPeriodic:
			go tc.runPeriodicEvent(event)
		}
	}
}

func (tc *telemetryClient) sendEvent(event eventConfig) {
	params := make(map[string]any)
	for key, valueFunc := range event.params {
		params[key] = valueFunc()
	}

	if err := sendEvent(tc.url, event.eventName, event.clientIDFunc(), params); err != nil {
		logrus.Debugf("telemetry: failed to send event %s: %v", event.eventName, err)
	}
}

func (tc *telemetryClient) runPeriodicEvent(event eventConfig) {
	for {
		time.Sleep(event.duration)
		tc.sendEvent(event)
	}
}

func getCurrentUnixTime() int64 {
	return time.Now().Unix()
}
