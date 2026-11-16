package dataframe

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-gota/gota/series"
)

// This file implements optional timezone normalization for the LoadRecords
// based readers (LoadRecords, ReadCSV, ReadJSON and LoadMaps). When enabled
// via WithTimezoneNormalization, a column of local wall-clock timestamps is
// combined with a per-row IANA timezone, a per-entity business sequence and
// an event duration constraint table to derive a UTC instant for every row.
//
// A row is only assigned a UTC value when its interpretation is unique:
//
//   - Local times that do not exist in their timezone (spring-forward gaps)
//     are reported as TimezoneErrNonExistentLocalTime.
//   - Local times with more than one valid UTC interpretation (fall-back
//     folds) are resolved using the monotonic business sequence and the
//     duration constraints. If several interpretations remain valid, the row
//     is reported as TimezoneErrAmbiguousLocalTime and ALL of them are
//     listed; neither the earliest nor the latest is chosen by default.
//   - If the sequence/duration constraints cannot be satisfied by any
//     combination of candidates, the entity is reported as
//     TimezoneErrConstraintConflict.
//
// If any issue is found, the load fails with a *TimezoneNormalizationError
// stored in DataFrame.Err and no UTC column is produced.

// TimezoneErrorKind classifies why a row (or an entity chain of rows) could
// not be assigned a unique UTC instant during timezone normalization.
type TimezoneErrorKind string

const (
	// TimezoneErrNonExistentLocalTime means the local wall-clock time does
	// not exist in the row's IANA timezone (it falls inside a spring-forward
	// gap), so there is no valid UTC interpretation at all.
	TimezoneErrNonExistentLocalTime TimezoneErrorKind = "nonexistent_local_time"

	// TimezoneErrAmbiguousLocalTime means more than one UTC interpretation
	// remains valid after applying the business sequence and the duration
	// constraints (typically a fall-back fold). All remaining
	// interpretations are reported in TimezoneIssue.Candidates; none is
	// selected implicitly.
	TimezoneErrAmbiguousLocalTime TimezoneErrorKind = "ambiguous_local_time"

	// TimezoneErrConstraintConflict means the business sequence ordering
	// and/or the event duration constraints rule out every candidate
	// assignment for an entity.
	TimezoneErrConstraintConflict TimezoneErrorKind = "constraint_conflict"

	// TimezoneErrInvalidTimeData means the row data itself is invalid: an
	// unknown IANA timezone name, an unparseable local timestamp, an
	// unparseable sequence number, or a duplicated sequence number within
	// the same entity.
	TimezoneErrInvalidTimeData TimezoneErrorKind = "invalid_time_data"
)

// TimezoneIssue describes a single row (or, for constraint conflicts, an
// entity chain) that could not be normalized to a unique UTC instant.
type TimezoneIssue struct {
	Kind        TimezoneErrorKind // category of the problem
	Row         int               // 0-based index among the data rows (header excluded)
	Entity      string            // value of the entity column, if configured
	Sequence    int64             // value of the sequence column, if configured and parseable
	HasSequence bool              // whether Sequence holds a meaningful value
	LocalTime   string            // raw local timestamp as found in the input
	Timezone    string            // raw IANA timezone name as found in the input

	// Candidates lists every UTC interpretation that remains valid for the
	// row. It is populated for TimezoneErrAmbiguousLocalTime issues and
	// always contains at least two instants in chronological order. No
	// interpretation is preferred over the others.
	Candidates []time.Time

	Message string // human-readable details
}

func (i TimezoneIssue) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s at row %d", i.Kind, i.Row)
	if i.Entity != "" {
		fmt.Fprintf(&b, " entity %q", i.Entity)
	}
	if i.HasSequence {
		fmt.Fprintf(&b, " seq %d", i.Sequence)
	}
	if i.LocalTime != "" {
		fmt.Fprintf(&b, " local time %q", i.LocalTime)
	}
	if i.Timezone != "" {
		fmt.Fprintf(&b, " tz %q", i.Timezone)
	}
	if len(i.Candidates) > 0 {
		b.WriteString(" valid UTC interpretations:")
		for _, c := range i.Candidates {
			fmt.Fprintf(&b, " %s", c.UTC().Format(time.RFC3339Nano))
		}
	}
	if i.Message != "" {
		fmt.Fprintf(&b, ": %s", i.Message)
	}
	return b.String()
}

// TimezoneNormalizationError is the error stored in DataFrame.Err when
// timezone normalization is enabled and at least one row could not be
// assigned a unique UTC instant. Use errors.As to inspect the individual
// issues:
//
//	var tzErr *dataframe.TimezoneNormalizationError
//	if errors.As(df.Err, &tzErr) {
//	    for _, issue := range tzErr.Issues { ... }
//	}
type TimezoneNormalizationError struct {
	Issues []TimezoneIssue
}

func (e *TimezoneNormalizationError) Error() string {
	counts := make(map[TimezoneErrorKind]int)
	for _, is := range e.Issues {
		counts[is.Kind]++
	}
	var b strings.Builder
	fmt.Fprintf(&b, "timezone normalization failed: %d issue(s)", len(e.Issues))
	for _, k := range []TimezoneErrorKind{
		TimezoneErrNonExistentLocalTime,
		TimezoneErrConstraintConflict,
		TimezoneErrAmbiguousLocalTime,
		TimezoneErrInvalidTimeData,
	} {
		if counts[k] > 0 {
			fmt.Fprintf(&b, ", %d %s", counts[k], k)
		}
	}
	const maxShown = 10
	for i, is := range e.Issues {
		if i >= maxShown {
			fmt.Fprintf(&b, "\n  ... and %d more issue(s)", len(e.Issues)-maxShown)
			break
		}
		fmt.Fprintf(&b, "\n  - %s", is.String())
	}
	return b.String()
}

// UTCFormat selects the representation of the UTC column generated by
// timezone normalization.
type UTCFormat int

const (
	// UTCFormatUnixNano stores UTC instants as Unix nanoseconds in an Int
	// column. It is exact to the nanosecond and directly usable for
	// ordering and duration arithmetic. It requires a 64-bit platform
	// (series.Int is the platform int) and instants between the years 1678
	// and 2262.
	UTCFormatUnixNano UTCFormat = iota

	// UTCFormatRFC3339Nano stores UTC instants as RFC3339Nano strings
	// (e.g. "2025-11-02T05:30:00.123456789Z") in a String column. It is
	// exact to the nanosecond on any platform.
	UTCFormatRFC3339Nano
)

// DurationConstraintTable describes the event duration constraint table used
// to disambiguate local times. The table is itself a DataFrame (for example
// loaded with ReadCSV) with one row per event name.
//
// For two consecutive events of the same entity (consecutive business
// sequence numbers), the gap between their UTC instants must lie within
// [MinColumn, MaxColumn] of the EARLIER event's row. Events without a row in
// the table only constrain the order (UTC must not decrease with increasing
// sequence numbers).
//
// Duration cells are interpreted according to their column type:
//   - String: parsed with time.ParseDuration (e.g. "15m", "1h30m", "2.5s")
//   - Int:    nanoseconds (the Go time.Duration convention)
//   - Float:  seconds (rounded to the nearest nanosecond)
type DurationConstraintTable struct {
	Frame       DataFrame // the constraint table
	EventColumn string    // name of the event column in Frame
	MinColumn   string    // name of the minimum duration column in Frame
	MaxColumn   string    // name of the maximum duration column in Frame
}

// TimezoneNormalization configures the optional timezone normalization
// performed while loading records. See WithTimezoneNormalization.
type TimezoneNormalization struct {
	// LocalTimeColumn is the name of the column holding local wall-clock
	// timestamps without zone information. Required.
	LocalTimeColumn string

	// TimezoneColumn is the name of the column holding an IANA timezone
	// name per row (e.g. "America/New_York"). Required. The system tz
	// database is used (see time.LoadLocation); import _ "time/tzdata" in
	// your program to embed it instead.
	TimezoneColumn string

	// EntityColumn is the name of the column identifying the entity an
	// event belongs to. Events of the same entity form a chain ordered by
	// SequenceColumn. Optional; if empty, all rows form a single chain
	// (only meaningful together with SequenceColumn).
	EntityColumn string

	// SequenceColumn is the name of the column holding the business
	// sequence number (an integer) of the event within its entity. UTC
	// instants must not decrease with increasing sequence numbers.
	// Optional; if empty, every row is normalized independently.
	SequenceColumn string

	// EventColumn is the name of the column holding the event name used to
	// look up duration constraints. Required when Constraints is set.
	EventColumn string

	// Constraints is the event duration constraint table. Optional; if
	// nil, only the sequence ordering is used for disambiguation.
	Constraints *DurationConstraintTable

	// UTCColumn is the name of the generated UTC column. Defaults to
	// LocalTimeColumn + "_utc". The name must not collide with an existing
	// column and must not be targeted by WithTypes.
	UTCColumn string

	// UTCFormat selects the representation of the generated UTC column.
	// Defaults to UTCFormatUnixNano.
	UTCFormat UTCFormat

	// Layouts are the layouts (in the sense of time.Parse) used to parse
	// the local timestamps, tried in order. Fractional seconds are always
	// accepted, even if the layout does not mention them. Layouts must not
	// contain zone offsets: the zone always comes from TimezoneColumn.
	// Defaults to ["2006-01-02 15:04:05", "2006-01-02T15:04:05"].
	Layouts []string
}

// WithTimezoneNormalization enables optional timezone normalization while
// loading records. It is honored by LoadRecords and every reader built on it
// (ReadCSV, ReadJSON, LoadMaps); it is ignored by LoadStructs.
//
// The local timestamp column (LocalTimeColumn) is interpreted in the IANA
// timezone given per row (TimezoneColumn). Because local times can be
// non-existent (spring-forward gap) or ambiguous (fall-back fold), the
// per-entity business sequence (EntityColumn + SequenceColumn) and the event
// duration constraint table (Constraints) are used to prune the candidate
// UTC instants:
//
//   - UTC instants must not decrease with increasing sequence numbers of the
//     same entity.
//   - For two consecutive events of the same entity, the gap between their
//     UTC instants must lie within the [min, max] duration registered for
//     the earlier event's name in the constraint table.
//
// A UTC value is loaded only when exactly one interpretation remains. If any
// row is non-existent, ambiguous or part of a conflicting chain, the whole
// load fails and DataFrame.Err holds a *TimezoneNormalizationError whose
// Issues distinguish the three categories; ambiguous rows report all valid
// UTC interpretations and none is chosen by default.
//
// The generated UTC column (UTCColumn, default LocalTimeColumn+"_utc") is
// appended after the existing columns. The original row order and the
// loading rules of every other column are preserved: NaNValues replacement
// and type detection/WithTypes apply to the input columns exactly as before
// and never touch the generated UTC column. WithTypes must not name the
// generated column. When this option is not used, loading behaves exactly as
// before.
func WithTimezoneNormalization(cfg TimezoneNormalization) LoadOption {
	return func(c *loadOptions) {
		c.tzNorm = &cfg
	}
}

// tzDurationRange is an inclusive [min, max] duration interval.
type tzDurationRange struct {
	min, max time.Duration
}

// tzRowInfo holds the per-row normalization state.
type tzRowInfo struct {
	cands     []time.Time // valid UTC interpretations, ascending; nil if the row failed
	assigned  time.Time   // final unique UTC instant (valid only when ok && unique)
	ok        bool        // whether the row passed row-level validation
	seq       int64
	seqParsed bool
}

// tzEngine carries the state of one normalization run.
type tzEngine struct {
	cfg     TimezoneNormalization
	records [][]string
	cons    map[string]tzDurationRange
	infos   []tzRowInfo
	issues  []TimezoneIssue

	ltIdx, tzIdx   int
	entIdx, seqIdx int
	evtIdx         int
	hasEnt, hasSeq bool
	hasEvt         bool
}

// defaultLocalTimeLayouts are used when TimezoneNormalization.Layouts is
// empty. time.Parse accepts fractional seconds even when the layout does not
// mention them, so both second and nanosecond precision inputs are covered.
var defaultLocalTimeLayouts = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
}

// applyTimezoneNormalization runs timezone normalization over the raw data
// records (header already excluded) of an otherwise fully loaded DataFrame
// and appends the generated UTC column. df is only mutated on success.
func (df *DataFrame) applyTimezoneNormalization(lo loadOptions, records [][]string) error {
	cfg := *lo.tzNorm

	names := df.Names()
	colIdx := make(map[string]int, len(names))
	for i, n := range names {
		colIdx[n] = i
	}

	// ---- configuration validation (programmer errors, plain error values)
	if cfg.LocalTimeColumn == "" {
		return fmt.Errorf("timezone normalization: a local time column is required")
	}
	if cfg.TimezoneColumn == "" {
		return fmt.Errorf("timezone normalization: a timezone column is required")
	}
	for _, c := range []struct{ label, name string }{
		{"local time", cfg.LocalTimeColumn},
		{"timezone", cfg.TimezoneColumn},
		{"entity", cfg.EntityColumn},
		{"sequence", cfg.SequenceColumn},
		{"event", cfg.EventColumn},
	} {
		if c.name == "" {
			continue
		}
		if _, ok := colIdx[c.name]; !ok {
			return fmt.Errorf("timezone normalization: %s column %q not found", c.label, c.name)
		}
	}
	if cfg.EntityColumn != "" && cfg.SequenceColumn == "" {
		return fmt.Errorf("timezone normalization: entity column %q requires a sequence column", cfg.EntityColumn)
	}
	if cfg.Constraints != nil && cfg.EventColumn == "" {
		return fmt.Errorf("timezone normalization: a duration constraint table requires an event column")
	}
	if cfg.Constraints == nil && cfg.EventColumn != "" {
		return fmt.Errorf("timezone normalization: event column %q requires a duration constraint table", cfg.EventColumn)
	}
	utcCol := cfg.UTCColumn
	if utcCol == "" {
		utcCol = cfg.LocalTimeColumn + "_utc"
	}
	if _, ok := colIdx[utcCol]; ok {
		return fmt.Errorf("timezone normalization: UTC column name %q collides with an existing column", utcCol)
	}
	if _, ok := lo.types[utcCol]; ok {
		return fmt.Errorf("timezone normalization: WithTypes must not target the generated UTC column %q; its type is managed by the UTCFormat option", utcCol)
	}
	if cfg.UTCFormat != UTCFormatUnixNano && cfg.UTCFormat != UTCFormatRFC3339Nano {
		return fmt.Errorf("timezone normalization: unknown UTC format %d", cfg.UTCFormat)
	}
	if cfg.UTCFormat == UTCFormatUnixNano && strconv.IntSize < 64 {
		return fmt.Errorf("timezone normalization: UTCFormatUnixNano requires a 64-bit platform; use UTCFormatRFC3339Nano instead")
	}
	layouts := cfg.Layouts
	if len(layouts) == 0 {
		layouts = defaultLocalTimeLayouts
	}
	cons, err := parseDurationConstraints(cfg.Constraints)
	if err != nil {
		return err
	}

	e := &tzEngine{
		cfg:     cfg,
		records: records,
		cons:    cons,
		infos:   make([]tzRowInfo, len(records)),
		ltIdx:   colIdx[cfg.LocalTimeColumn],
		tzIdx:   colIdx[cfg.TimezoneColumn],
		hasEnt:  cfg.EntityColumn != "",
		hasSeq:  cfg.SequenceColumn != "",
		hasEvt:  cfg.EventColumn != "",
	}
	if e.hasEnt {
		e.entIdx = colIdx[cfg.EntityColumn]
	}
	if e.hasSeq {
		e.seqIdx = colIdx[cfg.SequenceColumn]
	}
	if e.hasEvt {
		e.evtIdx = colIdx[cfg.EventColumn]
	}

	e.rowPass(layouts)
	if e.hasSeq {
		e.groupPass()
	} else {
		for j := range records {
			if !e.infos[j].ok {
				continue
			}
			if len(e.infos[j].cands) > 1 {
				e.issues = append(e.issues, e.ambiguousIssue(j, e.infos[j].cands))
				continue
			}
			e.infos[j].assigned = e.infos[j].cands[0]
		}
	}

	if len(e.issues) > 0 {
		return &TimezoneNormalizationError{Issues: e.issues}
	}

	// ---- build the UTC column in the original row order
	var utcSeries series.Series
	switch cfg.UTCFormat {
	case UTCFormatUnixNano:
		vals := make([]int, len(records))
		for j := range records {
			t := e.infos[j].assigned
			if y := t.Year(); y < 1678 || y > 2262 {
				return fmt.Errorf("timezone normalization: UTC instant %s at row %d cannot be represented as Unix nanoseconds; use UTCFormatRFC3339Nano", t.UTC().Format(time.RFC3339Nano), j)
			}
			vals[j] = int(t.UnixNano())
		}
		utcSeries = series.New(vals, series.Int, utcCol)
	case UTCFormatRFC3339Nano:
		vals := make([]string, len(records))
		for j := range records {
			vals[j] = e.infos[j].assigned.UTC().Format(time.RFC3339Nano)
		}
		utcSeries = series.New(vals, series.String, utcCol)
	}
	if utcSeries.Err != nil {
		return utcSeries.Err
	}
	df.columns = append(df.columns, utcSeries)
	df.ncols++
	return nil
}

// rowPass parses the timezone, the local timestamp and the sequence number
// of every row and computes the set of valid UTC interpretations.
func (e *tzEngine) rowPass(layouts []string) {
	locs := make(map[string]*time.Location)
	locFailed := make(map[string]bool)
	candCache := make(map[string][]time.Time)

	for j, rec := range e.records {
		base := e.issueBase(j)

		if e.hasSeq {
			sv, err := strconv.ParseInt(strings.TrimSpace(rec[e.seqIdx]), 10, 64)
			if err != nil {
				e.issues = append(e.issues, tzIssue(TimezoneErrInvalidTimeData, base,
					fmt.Sprintf("cannot parse sequence number %q", rec[e.seqIdx])))
				continue
			}
			e.infos[j].seq = sv
			e.infos[j].seqParsed = true
			base.Sequence = sv
			base.HasSequence = true
		}

		tzName := rec[e.tzIdx]
		loc, ok := locs[tzName]
		if !ok && !locFailed[tzName] {
			l, err := time.LoadLocation(tzName)
			if err != nil {
				locFailed[tzName] = true
			} else {
				locs[tzName] = l
				loc = l
				ok = true
			}
		}
		if !ok {
			e.issues = append(e.issues, tzIssue(TimezoneErrInvalidTimeData, base,
				fmt.Sprintf("unknown IANA timezone %q", tzName)))
			continue
		}

		localStr := rec[e.ltIdx]
		wall, err := parseWallClock(localStr, layouts)
		if err != nil {
			e.issues = append(e.issues, tzIssue(TimezoneErrInvalidTimeData, base,
				fmt.Sprintf("cannot parse local time %q: %v", localStr, err)))
			continue
		}

		key := tzName + "\x00" + localStr
		cands, cached := candCache[key]
		if !cached {
			cands = utcCandidates(wall, loc)
			candCache[key] = cands
		}
		if len(cands) == 0 {
			e.issues = append(e.issues, tzIssue(TimezoneErrNonExistentLocalTime, base,
				fmt.Sprintf("local time %q does not exist in timezone %q", localStr, tzName)))
			continue
		}
		e.infos[j].cands = cands
		e.infos[j].ok = true
	}
}

// groupPass solves the per-entity event chains: events are ordered by their
// business sequence number and the candidates are pruned with the monotonic
// order and the duration constraints until every event has a unique UTC
// interpretation, or a conflict/ambiguity is reported.
func (e *tzEngine) groupPass() {
	type group struct{ rows []int }
	groups := make(map[string]*group)
	var order []string
	tainted := make(map[string]bool)
	for j, rec := range e.records {
		key := ""
		if e.hasEnt {
			key = rec[e.entIdx]
		}
		g, ok := groups[key]
		if !ok {
			g = &group{}
			groups[key] = g
			order = append(order, key)
		}
		g.rows = append(g.rows, j)
		if !e.infos[j].ok {
			tainted[key] = true
		}
	}

	for _, key := range order {
		g := groups[key]
		if tainted[key] {
			// Rows of this entity already carry row-level issues; solving
			// the chain would only produce cascading noise.
			continue
		}
		rows := append([]int(nil), g.rows...)
		sort.SliceStable(rows, func(a, b int) bool { return e.infos[rows[a]].seq < e.infos[rows[b]].seq })

		dup := false
		for k := 1; k < len(rows); k++ {
			if e.infos[rows[k]].seq == e.infos[rows[k-1]].seq {
				e.issues = append(e.issues, tzIssue(TimezoneErrInvalidTimeData, e.issueBase(rows[k]),
					fmt.Sprintf("duplicate sequence number %d within the same entity", e.infos[rows[k]].seq)))
				dup = true
			}
		}
		if dup {
			continue
		}
		e.solveChain(rows)
	}
}

// solveChain prunes the candidate domains of one entity chain (rows sorted
// by sequence number) with arc consistency and either assigns the unique
// solution or reports a constraint conflict / remaining ambiguities.
func (e *tzEngine) solveChain(rows []int) {
	n := len(rows)
	domains := make([][]time.Time, n)
	for k, j := range rows {
		// Copy: domains are filtered in place below and must not alias the
		// per-row candidate lists.
		domains[k] = append([]time.Time(nil), e.infos[j].cands...)
	}

	eventName := func(k int) string {
		if !e.hasEvt {
			return ""
		}
		return e.records[rows[k]][e.evtIdx]
	}
	// rel reports whether u1 (position k) and u2 (position k+1) satisfy the
	// monotonic order and the duration constraint of the event at k.
	rel := func(k int, u1, u2 time.Time) bool {
		if u2.Before(u1) {
			return false
		}
		if e.cons != nil {
			if c, ok := e.cons[eventName(k)]; ok {
				if d := u2.Sub(u1); d < c.min || d > c.max {
					return false
				}
			}
		}
		return true
	}
	supported := func(k int, v time.Time) bool {
		if k > 0 {
			ok := false
			for _, u := range domains[k-1] {
				if rel(k-1, u, v) {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
		if k+1 < n {
			ok := false
			for _, w := range domains[k+1] {
				if rel(k, v, w) {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}

	// Arc consistency to a fixpoint. The chain is a path graph, so once no
	// domain changes anymore every remaining value participates in at least
	// one globally valid assignment.
	conflictAt := -1
	for {
		changed := false
		for k := 0; k < n; k++ {
			kept := domains[k][:0]
			for _, v := range domains[k] {
				if supported(k, v) {
					kept = append(kept, v)
				}
			}
			if len(kept) != len(domains[k]) {
				domains[k] = kept
				changed = true
			}
		}
		conflictAt = -1
		for k := range domains {
			if len(domains[k]) == 0 {
				conflictAt = k
				break
			}
		}
		if conflictAt >= 0 || !changed {
			break
		}
	}

	if conflictAt >= 0 {
		j := rows[conflictAt]
		var b strings.Builder
		fmt.Fprintf(&b, "no valid UTC assignment for the %d event(s) of this entity; constraints eliminated every candidate at seq %d", n, e.infos[j].seq)
		if e.hasEvt {
			if c, ok := e.cons[eventName(conflictAt)]; ok && conflictAt+1 < n {
				fmt.Fprintf(&b, "; allowed gap after event %q is [%v, %v]", eventName(conflictAt), c.min, c.max)
			} else if c, ok := e.cons[eventName(conflictAt-1)]; ok && conflictAt > 0 {
				fmt.Fprintf(&b, "; allowed gap after event %q is [%v, %v]", eventName(conflictAt-1), c.min, c.max)
			}
		}
		e.issues = append(e.issues, tzIssue(TimezoneErrConstraintConflict, e.issueBase(j), b.String()))
		return
	}

	unique := true
	for k := range domains {
		if len(domains[k]) != 1 {
			unique = false
			break
		}
	}
	if unique {
		// Defensive verification of the assignment before loading it.
		for k := 0; k+1 < n; k++ {
			if !rel(k, domains[k][0], domains[k+1][0]) {
				j := rows[k]
				e.issues = append(e.issues, tzIssue(TimezoneErrConstraintConflict, e.issueBase(j),
					"internal verification of the unique assignment failed"))
				return
			}
		}
		for k, j := range rows {
			e.infos[j].assigned = domains[k][0]
		}
		return
	}

	// Ambiguity: report every event that still has more than one valid
	// interpretation, with all of them. Nothing is chosen by default.
	for k, j := range rows {
		if len(domains[k]) > 1 {
			e.issues = append(e.issues, e.ambiguousIssue(j, domains[k]))
		}
	}
}

// issueBase builds the common part of a TimezoneIssue for row j.
func (e *tzEngine) issueBase(j int) TimezoneIssue {
	rec := e.records[j]
	base := TimezoneIssue{
		Row:       j,
		LocalTime: rec[e.ltIdx],
		Timezone:  rec[e.tzIdx],
	}
	if e.hasEnt {
		base.Entity = rec[e.entIdx]
	}
	if e.hasSeq && e.infos[j].seqParsed {
		base.Sequence = e.infos[j].seq
		base.HasSequence = true
	}
	return base
}

// ambiguousIssue builds a TimezoneErrAmbiguousLocalTime issue listing every
// remaining valid UTC interpretation of row j.
func (e *tzEngine) ambiguousIssue(j int, cands []time.Time) TimezoneIssue {
	iss := tzIssue(TimezoneErrAmbiguousLocalTime, e.issueBase(j),
		fmt.Sprintf("%d valid UTC interpretations remain; none was chosen", len(cands)))
	iss.Candidates = append([]time.Time(nil), cands...)
	return iss
}

func tzIssue(kind TimezoneErrorKind, base TimezoneIssue, msg string) TimezoneIssue {
	base.Kind = kind
	base.Message = msg
	return base
}

// parseDurationConstraints validates the constraint table and converts it to
// a map from event name to the allowed [min, max] duration interval.
func parseDurationConstraints(t *DurationConstraintTable) (map[string]tzDurationRange, error) {
	if t == nil {
		return nil, nil
	}
	if t.Frame.Err != nil {
		return nil, fmt.Errorf("timezone normalization: constraint table has error: %v", t.Frame.Err)
	}
	if t.EventColumn == "" || t.MinColumn == "" || t.MaxColumn == "" {
		return nil, fmt.Errorf("timezone normalization: constraint table requires event, min and max column names")
	}
	names := t.Frame.Names()
	for _, c := range []struct{ label, name string }{
		{"event", t.EventColumn},
		{"min duration", t.MinColumn},
		{"max duration", t.MaxColumn},
	} {
		found := false
		for _, n := range names {
			if n == c.name {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("timezone normalization: constraint table %s column %q not found", c.label, c.name)
		}
	}

	events := t.Frame.Col(t.EventColumn).Records()
	mins := t.Frame.Col(t.MinColumn)
	maxs := t.Frame.Col(t.MaxColumn)
	minRecs, maxRecs := mins.Records(), maxs.Records()
	if len(events) != len(minRecs) || len(events) != len(maxRecs) {
		return nil, fmt.Errorf("timezone normalization: constraint table columns have different lengths")
	}

	out := make(map[string]tzDurationRange, len(events))
	for i, ev := range events {
		if _, dup := out[ev]; dup {
			return nil, fmt.Errorf("timezone normalization: duplicate constraint for event %q", ev)
		}
		lo, err := parseDurationCell(mins.Type(), minRecs[i])
		if err != nil {
			return nil, fmt.Errorf("timezone normalization: constraint table event %q min duration: %v", ev, err)
		}
		hi, err := parseDurationCell(maxs.Type(), maxRecs[i])
		if err != nil {
			return nil, fmt.Errorf("timezone normalization: constraint table event %q max duration: %v", ev, err)
		}
		if lo > hi {
			return nil, fmt.Errorf("timezone normalization: constraint table event %q has min duration %v greater than max duration %v", ev, lo, hi)
		}
		out[ev] = tzDurationRange{min: lo, max: hi}
	}
	return out, nil
}

// parseDurationCell interprets one constraint table cell as a duration
// according to the column type: String via time.ParseDuration, Int as
// nanoseconds, Float as seconds.
func parseDurationCell(typ series.Type, raw string) (time.Duration, error) {
	switch typ {
	case series.String:
		return time.ParseDuration(raw)
	case series.Int:
		ns, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid integer nanoseconds %q", raw)
		}
		return time.Duration(ns), nil
	case series.Float:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid float seconds %q", raw)
		}
		return time.Duration(f * float64(time.Second)), nil
	}
	return 0, fmt.Errorf("unsupported column type %q for durations", typ)
}

// parseWallClock parses a local timestamp with the given layouts and returns
// its wall-clock reading as a UTC time (i.e. the zone-agnostic calendar
// fields). Fractional seconds are accepted even if the layout does not
// mention them.
func parseWallClock(s string, layouts []string) (time.Time, error) {
	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			y, mo, d := t.Date()
			h, mi, sec := t.Clock()
			return time.Date(y, mo, d, h, mi, sec, t.Nanosecond(), time.UTC), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

const (
	// tzProbeWindow is the half-width of the window scanned for zone
	// offsets around a wall-clock time. It comfortably covers every offset
	// ever present in the IANA database (at most 14 hours) plus margin.
	tzProbeWindow = 26 * time.Hour
	// tzProbeStep is the granularity of the offset scan. Real offset
	// regimes last far longer, so no offset in effect at a candidate
	// instant is missed.
	tzProbeStep = 10 * time.Minute
)

// utcCandidates returns every UTC instant whose local reading in loc equals
// the given wall-clock time, in chronological order:
//
//   - 0 candidates: the wall time does not exist in loc (spring-forward gap)
//   - 1 candidate:  the common, unique case
//   - 2 candidates: the wall time happens twice (fall-back fold)
//
// Seconds and nanoseconds of the wall time are preserved exactly.
func utcCandidates(wallUTC time.Time, loc *time.Location) []time.Time {
	offsets := make(map[int]struct{})
	for d := -tzProbeWindow; d <= tzProbeWindow; d += tzProbeStep {
		_, off := wallUTC.Add(d).In(loc).Zone()
		offsets[off] = struct{}{}
	}

	wy, wmo, wd := wallUTC.Date()
	wH, wM, wS := wallUTC.Clock()
	wN := wallUTC.Nanosecond()

	var out []time.Time
	seen := make(map[time.Time]struct{})
	for off := range offsets {
		cand := wallUTC.Add(-time.Duration(off) * time.Second)
		c := cand.In(loc)
		cy, cmo, cd := c.Date()
		cH, cM, cS := c.Clock()
		if cy == wy && cmo == wmo && cd == wd && cH == wH && cM == wM && cS == wS && c.Nanosecond() == wN {
			if _, dup := seen[cand]; !dup {
				seen[cand] = struct{}{}
				out = append(out, cand)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}
