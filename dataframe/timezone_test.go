package dataframe

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "time/tzdata" // keep the tests hermetic on systems without tzdata

	"github.com/go-gota/gota/series"
)

func mustConstraintFrame(t *testing.T, records [][]string) DataFrame {
	t.Helper()
	df := LoadRecords(records)
	if df.Err != nil {
		t.Fatalf("constraint table failed to load: %v", df.Err)
	}
	return df
}

func utcTime(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
	return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
}

func utcNanoStr(y int, mo time.Month, d, h, mi, s, ns int) string {
	return strconv.FormatInt(utcTime(y, mo, d, h, mi, s, ns).UnixNano(), 10)
}

func utcNanos(ts []time.Time) []int64 {
	out := make([]int64, len(ts))
	for i, t := range ts {
		out[i] = t.UnixNano()
	}
	return out
}

func asTimezoneError(t *testing.T, err error) *TimezoneNormalizationError {
	t.Helper()
	var tzErr *TimezoneNormalizationError
	if err == nil || !errors.As(err, &tzErr) {
		t.Fatalf("expected a *TimezoneNormalizationError, got: %v", err)
	}
	return tzErr
}

func issuesOfKind(tzErr *TimezoneNormalizationError, kind TimezoneErrorKind) []TimezoneIssue {
	var out []TimezoneIssue
	for _, is := range tzErr.Issues {
		if is.Kind == kind {
			out = append(out, is)
		}
	}
	return out
}

func TestTimezoneNormalization_BasicLoad(t *testing.T) {
	cons := mustConstraintFrame(t, [][]string{
		{"event", "min", "max"},
		{"start", "5m", "20m"},
	})
	records := [][]string{
		{"entity", "seq", "event", "local_time", "tz", "amount", "note"},
		{"A", "1", "start", "2025-01-10 08:00:00.123456789", "America/New_York", "1.5", "NA"},
		{"A", "2", "end", "2025-01-10 08:10:00", "America/New_York", "2.5", "ok"},
		{"B", "1", "start", "2025-01-10 20:00:00", "Asia/Shanghai", "3.5", "x"},
	}
	df := LoadRecords(records,
		NaNValues([]string{"NA"}),
		WithTypes(map[string]series.Type{"amount": series.Float}),
		WithTimezoneNormalization(TimezoneNormalization{
			LocalTimeColumn: "local_time",
			TimezoneColumn:  "tz",
			EntityColumn:    "entity",
			SequenceColumn:  "seq",
			EventColumn:     "event",
			Constraints: &DurationConstraintTable{
				Frame:       cons,
				EventColumn: "event",
				MinColumn:   "min",
				MaxColumn:   "max",
			},
		}),
	)
	if df.Err != nil {
		t.Fatalf("unexpected error: %v", df.Err)
	}

	// The UTC column is appended last with the default name.
	wantNames := []string{"entity", "seq", "event", "local_time", "tz", "amount", "note", "local_time_utc"}
	if !reflect.DeepEqual(df.Names(), wantNames) {
		t.Fatalf("column names: got %v, want %v", df.Names(), wantNames)
	}
	if df.Nrow() != 3 {
		t.Fatalf("nrow: got %d, want 3", df.Nrow())
	}

	// UTC values are exact to the nanosecond and follow the original row order.
	gotUTC := df.Col("local_time_utc").Records()
	wantUTC := []string{
		utcNanoStr(2025, time.January, 10, 13, 0, 0, 123456789), // 08:00 EST = UTC-5
		utcNanoStr(2025, time.January, 10, 13, 10, 0, 0),
		utcNanoStr(2025, time.January, 10, 12, 0, 0, 0), // 20:00 CST = UTC+8
	}
	if !reflect.DeepEqual(gotUTC, wantUTC) {
		t.Fatalf("utc column: got %v, want %v", gotUTC, wantUTC)
	}
	if typ := df.Col("local_time_utc").Type(); typ != series.Int {
		t.Fatalf("utc column type: got %v, want int", typ)
	}

	// Original row order and other columns are preserved.
	if got := df.Col("entity").Records(); !reflect.DeepEqual(got, []string{"A", "A", "B"}) {
		t.Fatalf("entity column: got %v", got)
	}
	if got := df.Col("local_time").Records()[0]; got != "2025-01-10 08:00:00.123456789" {
		t.Fatalf("local_time column changed: got %q", got)
	}

	// NaNValues and WithTypes still apply to the other columns.
	if got := df.Col("note").Records()[0]; got != "NaN" {
		t.Fatalf("NaNValues not applied to note column: got %q", got)
	}
	if typ := df.Col("amount").Type(); typ != series.Float {
		t.Fatalf("WithTypes not applied to amount column: got %v", typ)
	}
	// ... and never produce NaN in the generated UTC column.
	for _, v := range df.Col("local_time_utc").Records() {
		if v == "NaN" {
			t.Fatalf("generated UTC column contains NaN")
		}
	}
}

func TestTimezoneNormalization_DSTFoldDisambiguated(t *testing.T) {
	// America/New_York falls back on 2025-11-02: 01:00-02:00 happens twice,
	// first in EDT (UTC-4), then in EST (UTC-5).
	cons := mustConstraintFrame(t, [][]string{
		{"event", "min", "max"},
		{"prep", "30m", "90m"},
		{"job", "5m", "30m"},
	})
	// Rows of entities A and B are interleaved on purpose: grouping must not
	// change the output row order.
	records := [][]string{
		{"entity", "seq", "event", "local_time", "tz"},
		{"A", "1", "prep", "2025-11-02 00:30:00", "America/New_York"},
		{"B", "1", "prep", "2025-11-02 03:00:00", "America/New_York"},
		{"A", "2", "job", "2025-11-02 01:30:00", "America/New_York"},
		{"B", "2", "job", "2025-11-02 03:40:00", "America/New_York"},
		{"A", "3", "job", "2025-11-02 01:45:00", "America/New_York"},
	}
	df := LoadRecords(records,
		WithTimezoneNormalization(TimezoneNormalization{
			LocalTimeColumn: "local_time",
			TimezoneColumn:  "tz",
			EntityColumn:    "entity",
			SequenceColumn:  "seq",
			EventColumn:     "event",
			Constraints: &DurationConstraintTable{
				Frame:       cons,
				EventColumn: "event",
				MinColumn:   "min",
				MaxColumn:   "max",
			},
		}),
	)
	if df.Err != nil {
		t.Fatalf("unexpected error: %v", df.Err)
	}
	got := df.Col("local_time_utc").Records()
	want := []string{
		utcNanoStr(2025, time.November, 2, 4, 30, 0, 0), // A prep: 00:30 EDT
		utcNanoStr(2025, time.November, 2, 8, 0, 0, 0),  // B prep: 03:00 EST
		utcNanoStr(2025, time.November, 2, 5, 30, 0, 0), // A job: first 01:30 (EDT)
		utcNanoStr(2025, time.November, 2, 8, 40, 0, 0), // B job: 03:40 EST
		utcNanoStr(2025, time.November, 2, 5, 45, 0, 0), // A job: first 01:45 (EDT)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("utc column: got %v, want %v", got, want)
	}
}

func TestTimezoneNormalization_AmbiguousReported(t *testing.T) {
	t.Run("no sequence information", func(t *testing.T) {
		records := [][]string{
			{"local_time", "tz"},
			{"2025-11-02 01:30:00", "America/New_York"},
		}
		df := LoadRecords(records,
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
			}),
		)
		tzErr := asTimezoneError(t, df.Err)
		amb := issuesOfKind(tzErr, TimezoneErrAmbiguousLocalTime)
		if len(amb) != 1 {
			t.Fatalf("expected 1 ambiguous issue, got %d (%v)", len(amb), tzErr)
		}
		// Both valid interpretations are reported, earliest first; none is
		// chosen by default.
		want := []int64{
			utcTime(2025, time.November, 2, 5, 30, 0, 0).UnixNano(),
			utcTime(2025, time.November, 2, 6, 30, 0, 0).UnixNano(),
		}
		if got := utcNanos(amb[0].Candidates); !reflect.DeepEqual(got, want) {
			t.Fatalf("candidates: got %v, want %v", got, want)
		}
	})

	t.Run("constraints leave both interpretations valid", func(t *testing.T) {
		cons := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"job", "0s", "3h"},
		})
		records := [][]string{
			{"entity", "seq", "event", "local_time", "tz"},
			{"A", "1", "job", "2025-11-02 01:30:00", "America/New_York"},
			{"A", "2", "job", "2025-11-02 01:45:00", "America/New_York"},
		}
		df := LoadRecords(records,
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				EntityColumn:    "entity",
				SequenceColumn:  "seq",
				EventColumn:     "event",
				Constraints: &DurationConstraintTable{
					Frame:       cons,
					EventColumn: "event",
					MinColumn:   "min",
					MaxColumn:   "max",
				},
			}),
		)
		tzErr := asTimezoneError(t, df.Err)
		amb := issuesOfKind(tzErr, TimezoneErrAmbiguousLocalTime)
		if len(amb) != 2 {
			t.Fatalf("expected 2 ambiguous issues, got %d (%v)", len(amb), tzErr)
		}
		want0 := []int64{
			utcTime(2025, time.November, 2, 5, 30, 0, 0).UnixNano(),
			utcTime(2025, time.November, 2, 6, 30, 0, 0).UnixNano(),
		}
		want1 := []int64{
			utcTime(2025, time.November, 2, 5, 45, 0, 0).UnixNano(),
			utcTime(2025, time.November, 2, 6, 45, 0, 0).UnixNano(),
		}
		if got := utcNanos(amb[0].Candidates); !reflect.DeepEqual(got, want0) {
			t.Fatalf("row 0 candidates: got %v, want %v", got, want0)
		}
		if got := utcNanos(amb[1].Candidates); !reflect.DeepEqual(got, want1) {
			t.Fatalf("row 1 candidates: got %v, want %v", got, want1)
		}
		if !strings.Contains(df.Err.Error(), "ambiguous_local_time") {
			t.Fatalf("error message does not mention the kind: %v", df.Err)
		}
	})
}

func TestTimezoneNormalization_NonExistent(t *testing.T) {
	// America/New_York springs forward on 2025-03-09: 02:00-03:00 does not exist.
	cfg := TimezoneNormalization{
		LocalTimeColumn: "local_time",
		TimezoneColumn:  "tz",
	}
	for _, local := range []string{
		"2025-03-09 02:00:00",
		"2025-03-09 02:30:00",
		"2025-03-09 02:59:59.999999999",
	} {
		df := LoadRecords([][]string{
			{"local_time", "tz"},
			{local, "America/New_York"},
		}, WithTimezoneNormalization(cfg))
		tzErr := asTimezoneError(t, df.Err)
		nonEx := issuesOfKind(tzErr, TimezoneErrNonExistentLocalTime)
		if len(nonEx) != 1 {
			t.Fatalf("%s: expected 1 nonexistent issue, got %d (%v)", local, len(nonEx), tzErr)
		}
		if nonEx[0].LocalTime != local || nonEx[0].Timezone != "America/New_York" {
			t.Fatalf("%s: issue carries wrong context: %+v", local, nonEx[0])
		}
	}

	// The instants around the gap exist and keep nanosecond precision.
	for local, want := range map[string]string{
		"2025-03-09 01:59:59.999999999": utcNanoStr(2025, time.March, 9, 6, 59, 59, 999999999),
		"2025-03-09 03:00:00":           utcNanoStr(2025, time.March, 9, 7, 0, 0, 0),
	} {
		df := LoadRecords([][]string{
			{"local_time", "tz"},
			{local, "America/New_York"},
		}, WithTimezoneNormalization(cfg))
		if df.Err != nil {
			t.Fatalf("%s: unexpected error: %v", local, df.Err)
		}
		if got := df.Col("local_time_utc").Records()[0]; got != want {
			t.Fatalf("%s: got UTC %s, want %s", local, got, want)
		}
	}
}

func TestTimezoneNormalization_ConstraintConflict(t *testing.T) {
	cons := mustConstraintFrame(t, [][]string{
		{"event", "min", "max"},
		{"start", "1h", "2h"},
	})
	records := [][]string{
		{"entity", "seq", "event", "local_time", "tz"},
		{"A", "1", "start", "2025-06-01 10:00:00", "America/New_York"},
		{"A", "2", "end", "2025-06-01 10:05:00", "America/New_York"},
	}
	df := LoadRecords(records,
		WithTimezoneNormalization(TimezoneNormalization{
			LocalTimeColumn: "local_time",
			TimezoneColumn:  "tz",
			EntityColumn:    "entity",
			SequenceColumn:  "seq",
			EventColumn:     "event",
			Constraints: &DurationConstraintTable{
				Frame:       cons,
				EventColumn: "event",
				MinColumn:   "min",
				MaxColumn:   "max",
			},
		}),
	)
	tzErr := asTimezoneError(t, df.Err)
	conflicts := issuesOfKind(tzErr, TimezoneErrConstraintConflict)
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict issue, got %d (%v)", len(conflicts), tzErr)
	}
	if conflicts[0].Entity != "A" {
		t.Fatalf("conflict issue carries wrong entity: %+v", conflicts[0])
	}
	if !strings.Contains(conflicts[0].Message, "1h0m0s") {
		t.Fatalf("conflict message does not mention the constraint bounds: %q", conflicts[0].Message)
	}
}

func TestTimezoneNormalization_InvalidData(t *testing.T) {
	baseCfg := TimezoneNormalization{
		LocalTimeColumn: "local_time",
		TimezoneColumn:  "tz",
		EntityColumn:    "entity",
		SequenceColumn:  "seq",
	}
	tests := []struct {
		name    string
		records [][]string
		wantMsg string
	}{
		{
			name: "unknown timezone",
			records: [][]string{
				{"entity", "seq", "local_time", "tz"},
				{"A", "1", "2025-01-10 08:00:00", "Not/A_Zone"},
			},
			wantMsg: "unknown IANA timezone",
		},
		{
			name: "unparseable local time",
			records: [][]string{
				{"entity", "seq", "local_time", "tz"},
				{"A", "1", "not-a-time", "America/New_York"},
			},
			wantMsg: "cannot parse local time",
		},
		{
			name: "unparseable sequence",
			records: [][]string{
				{"entity", "seq", "local_time", "tz"},
				{"A", "abc", "2025-01-10 08:00:00", "America/New_York"},
			},
			wantMsg: "cannot parse sequence number",
		},
		{
			name: "duplicate sequence",
			records: [][]string{
				{"entity", "seq", "local_time", "tz"},
				{"A", "1", "2025-01-10 08:00:00", "America/New_York"},
				{"A", "1", "2025-01-10 08:10:00", "America/New_York"},
			},
			wantMsg: "duplicate sequence number",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			df := LoadRecords(tc.records, WithTimezoneNormalization(baseCfg))
			tzErr := asTimezoneError(t, df.Err)
			invalid := issuesOfKind(tzErr, TimezoneErrInvalidTimeData)
			if len(invalid) == 0 {
				t.Fatalf("expected an invalid_time_data issue, got %v", tzErr)
			}
			if !strings.Contains(invalid[0].Message, tc.wantMsg) {
				t.Fatalf("issue message %q does not contain %q", invalid[0].Message, tc.wantMsg)
			}
		})
	}
}

func TestTimezoneNormalization_ConfigErrors(t *testing.T) {
	cons := mustConstraintFrame(t, [][]string{
		{"event", "min", "max"},
		{"start", "5m", "20m"},
	})
	records := [][]string{
		{"entity", "seq", "event", "local_time", "tz"},
		{"A", "1", "start", "2025-01-10 08:00:00", "America/New_York"},
	}
	tests := []struct {
		name    string
		cfg     TimezoneNormalization
		extra   []LoadOption
		wantMsg string
	}{
		{
			name:    "missing timezone column config",
			cfg:     TimezoneNormalization{LocalTimeColumn: "local_time"},
			wantMsg: "timezone column is required",
		},
		{
			name: "column not found",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "no_such_column",
			},
			wantMsg: "not found",
		},
		{
			name: "entity without sequence",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				EntityColumn:    "entity",
			},
			wantMsg: "requires a sequence column",
		},
		{
			name: "constraints without event column",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				Constraints: &DurationConstraintTable{
					Frame: cons, EventColumn: "event", MinColumn: "min", MaxColumn: "max",
				},
			},
			wantMsg: "requires an event column",
		},
		{
			name: "event column without constraints",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				EventColumn:     "event",
			},
			wantMsg: "requires a duration constraint table",
		},
		{
			name: "UTC column name collision",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				UTCColumn:       "event",
			},
			wantMsg: "collides with an existing column",
		},
		{
			name: "WithTypes targets the UTC column",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
			},
			extra:   []LoadOption{WithTypes(map[string]series.Type{"local_time_utc": series.String})},
			wantMsg: "WithTypes must not target the generated UTC column",
		},
		{
			name: "constraint table with unknown column",
			cfg: TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				EventColumn:     "event",
				Constraints: &DurationConstraintTable{
					Frame: cons, EventColumn: "event", MinColumn: "nope", MaxColumn: "max",
				},
			},
			wantMsg: "constraint table min duration column",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := append([]LoadOption{WithTimezoneNormalization(tc.cfg)}, tc.extra...)
			df := LoadRecords(records, options...)
			if df.Err == nil {
				t.Fatalf("expected an error, got none")
			}
			var tzErr *TimezoneNormalizationError
			if errors.As(df.Err, &tzErr) {
				t.Fatalf("config problems must not be reported as data issues: %v", df.Err)
			}
			if !strings.Contains(df.Err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not contain %q", df.Err.Error(), tc.wantMsg)
			}
		})
	}
}

func TestTimezoneNormalization_FormatsAndLayouts(t *testing.T) {
	t.Run("RFC3339Nano string column", func(t *testing.T) {
		records := [][]string{
			{"local_time", "tz"},
			{"2025-01-10 08:00:00.123456789", "America/New_York"},
		}
		df := LoadRecords(records,
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				UTCFormat:       UTCFormatRFC3339Nano,
			}),
		)
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
		if typ := df.Col("local_time_utc").Type(); typ != series.String {
			t.Fatalf("utc column type: got %v, want string", typ)
		}
		if got := df.Col("local_time_utc").Records()[0]; got != "2025-01-10T13:00:00.123456789Z" {
			t.Fatalf("utc value: got %q", got)
		}
	})

	t.Run("custom UTC column name and layout", func(t *testing.T) {
		records := [][]string{
			{"local_time", "tz"},
			{"2025/01/10 08:00:00.5", "America/New_York"},
		}
		df := LoadRecords(records,
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				UTCColumn:       "ts_utc",
				Layouts:         []string{"2006/01/02 15:04:05"},
			}),
		)
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
		if got := df.Col("ts_utc").Records()[0]; got != utcNanoStr(2025, time.January, 10, 13, 0, 0, 500000000) {
			t.Fatalf("utc value: got %q", got)
		}
	})

	t.Run("NaNValues must not swallow the generated UTC column", func(t *testing.T) {
		records := [][]string{
			{"local_time", "tz"},
			{"2025-01-10 08:00:00", "America/New_York"},
		}
		df := LoadRecords(records,
			NaNValues([]string{"2025-01-10T13:00:00Z"}),
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
				UTCFormat:       UTCFormatRFC3339Nano,
			}),
		)
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
		if got := df.Col("local_time_utc").Records()[0]; got != "2025-01-10T13:00:00Z" {
			t.Fatalf("generated UTC column was harmed by NaNValues: got %q", got)
		}
	})
}

func TestTimezoneNormalization_ConstraintTableCellTypes(t *testing.T) {
	mkCfg := func(cons DataFrame) TimezoneNormalization {
		return TimezoneNormalization{
			LocalTimeColumn: "local_time",
			TimezoneColumn:  "tz",
			EntityColumn:    "entity",
			SequenceColumn:  "seq",
			EventColumn:     "event",
			Constraints: &DurationConstraintTable{
				Frame:       cons,
				EventColumn: "event",
				MinColumn:   "min",
				MaxColumn:   "max",
			},
		}
	}

	t.Run("integer cells are nanoseconds", func(t *testing.T) {
		cons := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "300000000000", "1200000000000"}, // 5m .. 20m
		})
		records := [][]string{
			{"entity", "seq", "event", "local_time", "tz"},
			{"A", "1", "start", "2025-06-01 10:00:00", "America/New_York"},
			{"A", "2", "end", "2025-06-01 10:10:00", "America/New_York"},
		}
		df := LoadRecords(records, WithTimezoneNormalization(mkCfg(cons)))
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
	})

	t.Run("float cells are seconds", func(t *testing.T) {
		cons := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "0.4", "0.6"},
		})
		records := [][]string{
			{"entity", "seq", "event", "local_time", "tz"},
			{"A", "1", "start", "2025-06-01 10:00:00", "America/New_York"},
			{"A", "2", "end", "2025-06-01 10:00:00.5", "America/New_York"},
		}
		df := LoadRecords(records, WithTimezoneNormalization(mkCfg(cons)))
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
	})

	t.Run("nanosecond-exact boundaries are inclusive", func(t *testing.T) {
		cons := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "5", "5"}, // exactly 5ns
		})
		records := [][]string{
			{"entity", "seq", "event", "local_time", "tz"},
			{"A", "1", "start", "2025-06-01 10:00:00", "America/New_York"},
			{"A", "2", "end", "2025-06-01 10:00:00.000000005", "America/New_York"},
		}
		df := LoadRecords(records, WithTimezoneNormalization(mkCfg(cons)))
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
		if got, want := df.Col("local_time_utc").Records()[1], utcNanoStr(2025, time.June, 1, 14, 0, 0, 5); got != want {
			t.Fatalf("utc value: got %s, want %s", got, want)
		}

		// One nanosecond less than required is a conflict.
		consTooTight := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "6", "5"},
		})
		_ = consTooTight // min > max is a config error, checked below instead
		cons6 := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "6", "10"},
		})
		df = LoadRecords(records, WithTimezoneNormalization(mkCfg(cons6)))
		tzErr := asTimezoneError(t, df.Err)
		if got := issuesOfKind(tzErr, TimezoneErrConstraintConflict); len(got) != 1 {
			t.Fatalf("expected 1 conflict issue, got %v", tzErr)
		}
	})

	t.Run("min greater than max is a config error", func(t *testing.T) {
		cons := mustConstraintFrame(t, [][]string{
			{"event", "min", "max"},
			{"start", "20m", "5m"},
		})
		records := [][]string{
			{"entity", "seq", "event", "local_time", "tz"},
			{"A", "1", "start", "2025-06-01 10:00:00", "America/New_York"},
		}
		df := LoadRecords(records, WithTimezoneNormalization(mkCfg(cons)))
		if df.Err == nil || !strings.Contains(df.Err.Error(), "greater than max") {
			t.Fatalf("expected min>max config error, got %v", df.Err)
		}
	})
}

func TestTimezoneNormalization_ReadCSV(t *testing.T) {
	cons := mustConstraintFrame(t, [][]string{
		{"event", "min", "max"},
		{"prep", "30m", "90m"},
		{"job", "5m", "30m"},
	})
	csvData := `entity,seq,event,local_time,tz
A,1,prep,2025-11-02 00:30:00,America/New_York
A,2,job,2025-11-02 01:30:00,America/New_York
A,3,job,2025-11-02 01:45:00,America/New_York
`
	df := ReadCSV(strings.NewReader(csvData),
		WithTimezoneNormalization(TimezoneNormalization{
			LocalTimeColumn: "local_time",
			TimezoneColumn:  "tz",
			EntityColumn:    "entity",
			SequenceColumn:  "seq",
			EventColumn:     "event",
			Constraints: &DurationConstraintTable{
				Frame:       cons,
				EventColumn: "event",
				MinColumn:   "min",
				MaxColumn:   "max",
			},
		}),
	)
	if df.Err != nil {
		t.Fatalf("unexpected error: %v", df.Err)
	}
	want := []string{
		utcNanoStr(2025, time.November, 2, 4, 30, 0, 0),
		utcNanoStr(2025, time.November, 2, 5, 30, 0, 0),
		utcNanoStr(2025, time.November, 2, 5, 45, 0, 0),
	}
	if got := df.Col("local_time_utc").Records(); !reflect.DeepEqual(got, want) {
		t.Fatalf("utc column: got %v, want %v", got, want)
	}
}

func TestTimezoneNormalization_OtherLoadPaths(t *testing.T) {
	t.Run("no header with explicit names", func(t *testing.T) {
		records := [][]string{
			{"2025-01-10 08:00:00", "America/New_York"},
			{"2025-01-10 09:00:00", "America/New_York"},
		}
		df := LoadRecords(records,
			HasHeader(false),
			Names("local_time", "tz"),
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
			}),
		)
		if df.Err != nil {
			t.Fatalf("unexpected error: %v", df.Err)
		}
		want := []string{
			utcNanoStr(2025, time.January, 10, 13, 0, 0, 0),
			utcNanoStr(2025, time.January, 10, 14, 0, 0, 0),
		}
		if got := df.Col("local_time_utc").Records(); !reflect.DeepEqual(got, want) {
			t.Fatalf("utc column: got %v, want %v", got, want)
		}
	})

	t.Run("LoadMaps", func(t *testing.T) {
		df := LoadMaps([]map[string]interface{}{
			{"local_time": "2025-11-02 01:30:00", "tz": "America/New_York"},
		},
			WithTimezoneNormalization(TimezoneNormalization{
				LocalTimeColumn: "local_time",
				TimezoneColumn:  "tz",
			}),
		)
		tzErr := asTimezoneError(t, df.Err)
		if got := issuesOfKind(tzErr, TimezoneErrAmbiguousLocalTime); len(got) != 1 {
			t.Fatalf("expected 1 ambiguous issue, got %v", tzErr)
		}
	})
}

func TestTimezoneNormalization_DisabledNoOp(t *testing.T) {
	records := [][]string{
		{"entity", "seq", "local_time", "tz"},
		{"A", "1", "2025-11-02 01:30:00", "America/New_York"},
	}
	df := LoadRecords(records)
	if df.Err != nil {
		t.Fatalf("unexpected error: %v", df.Err)
	}
	if df.Ncol() != 4 {
		t.Fatalf("ncol changed without the option: got %d", df.Ncol())
	}
	for _, n := range df.Names() {
		if strings.HasSuffix(n, "_utc") {
			t.Fatalf("unexpected UTC column without the option: %v", df.Names())
		}
	}
}
