package time_precision_mode

import "data-replication/internal/enum"

const (
	Adaptive                 enum.TimePrecisionMode = "adaptive"
	AdaptiveTimeMicroseconds enum.TimePrecisionMode = "adaptive_time_microseconds"
	Connect                  enum.TimePrecisionMode = "connect"
)
