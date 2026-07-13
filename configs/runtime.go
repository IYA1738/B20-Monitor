package configs

import (
	"token-discover-demo/common/idgen"
	"token-discover-demo/database"
)

func (c DBConfig) ToDatabaseConfig() database.Config {
	return database.Config{
		DSN:             c.DSN,
		MaxOpenConns:    c.MaxOpenConns,
		MaxIdleConns:    c.MaxIdleConns,
		ConnMaxLifetime: c.ConnMaxLifetime(),
		ConnMaxIdleTime: c.ConnMaxIdleTime(),
		PingTimeout:     c.PingTimeout(),
		LogLevel:        database.ParseLogLevel(c.LogLevel),
	}
}

func (c IDGenConfig) ToIDGenConfig() idgen.Config {
	return idgen.Config{
		WorkerID:         c.WorkerIDValue(),
		EpochMillis:      c.EpochMillis,
		MaxClockBackward: c.MaxClockBackward(),
	}
}
