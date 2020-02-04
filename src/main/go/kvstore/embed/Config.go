package embed

import "go.uber.org/zap"

type Config struct {
	TickMs     int
	ElectionMs int
	Logger     *zap.Logger
}


// NewConfig creates a new Config populated with default values.
func NewConfig() *Config {
	cfg := &Config{
		Logger:  zap.NewExample(),
		TickMs:     100,
		ElectionMs: 1000,
	}
	return cfg
}