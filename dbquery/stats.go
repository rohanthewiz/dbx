package dbquery

import (
	"fmt"
	"strings"
	"time"
)

type Stats struct {
	RowsCount int64
	Timing    struct {
		QueryTime,
		FetchTime,
		Total time.Duration
	}
}

func (st Stats) String() (out string) {
	sb := strings.Builder{}
	sb.WriteString(strings.Repeat("-", 50) + "\n")
	sb.WriteString(fmt.Sprintf("%d rows returned in %s\n", st.RowsCount, st.Timing.Total.String()))
	sb.WriteString(fmt.Sprintf("Query time: %s, Fetch time: %s\n",
		st.Timing.QueryTime.String(), st.Timing.FetchTime.String()))

	sb.WriteString(strings.Repeat("-", 50) + "\n")
	return sb.String()
}
