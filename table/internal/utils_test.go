// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsModePairs(t *testing.T) {
	tests := []struct {
		str      string
		expected internal.MetricsMode
	}{
		{"none", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"nOnE", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"Counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"full", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{" FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"truncate(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(7)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
		{"trUncatE(07)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			mode, err := internal.MatchMetricsMode(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}

	_, err := internal.MatchMetricsMode("truncatE(-7)")
	assert.ErrorContains(t, err, "malformed truncate metrics mode: truncatE(-7)")

	_, err = internal.MatchMetricsMode("truncatE(0)")
	assert.ErrorContains(t, err, "invalid truncate length: 0")
}

func TestTruncateUpperBoundString(t *testing.T) {
	assert.Equal(t, "ab", internal.TruncateUpperBoundText("aaaa", 2))
	// \u10FFFF\u10FFFF\x00
	assert.Equal(t, "", internal.TruncateUpperBoundText("\xf4\x8f\xbf\xbf\xf4\x8f\xbf\xbf\x00", 2))
}

func TestTruncateUpperBoundBinary(t *testing.T) {
	assert.Equal(t, []byte{0x01, 0x03}, internal.TruncateUpperBoundBinary([]byte{0x01, 0x02, 0x03}, 2))
	assert.Nil(t, internal.TruncateUpperBoundBinary([]byte{0xff, 0xff, 0x00}, 2))
}

// The input iterator reports errors by panicking (binPackRecords does exactly
// this), and it is drained on MapExec's own producer goroutine, out of reach of
// any recover in the caller. MapExec must turn that into the error it already
// reports rather than letting it kill the process.
func TestMapExecRecoversPanicFromIterator(t *testing.T) {
	boom := errors.New("cast column 10 (month) from utf8 to int64")

	panicking := func(yield func(int) bool) {
		if !yield(1) {
			return
		}
		panic(boom)
	}

	done := make(chan error, 1)
	go func() {
		var last error
		for _, err := range internal.MapExec(3, panicking, func(i int) (int, error) {
			return i * 2, nil
		}) {
			if err != nil {
				last = err
			}
		}
		done <- last
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a panic in the input iterator must surface as an error")
		assert.ErrorIs(t, err, boom, "the original error must be preserved for the caller")
	case <-time.After(10 * time.Second):
		// Skipping close(ch) on the panic path strands the workers, so out never
		// closes and the consumer blocks forever instead of seeing the error.
		t.Fatal("MapExec never returned: consumer deadlocked after the panic")
	}
}

// A non-error panic value must not be dropped either.
func TestMapExecRecoversNonErrorPanic(t *testing.T) {
	panicking := func(yield func(int) bool) {
		panic("something went very wrong")
	}

	done := make(chan error, 1)
	go func() {
		var last error
		for _, err := range internal.MapExec(2, panicking, func(i int) (int, error) {
			return i, nil
		}) {
			if err != nil {
				last = err
			}
		}
		done <- last
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "something went very wrong")
	case <-time.After(10 * time.Second):
		t.Fatal("MapExec never returned after a non-error panic")
	}
}

func TestMapExecFinish(t *testing.T) {
	var (
		ch = make(chan struct{}, 1)
		f  = func(i int) (int, error) {
			return i * 2, nil
		}
	)

	go func() {
		defer close(ch)
		for _, err := range internal.MapExec(3, slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), f) {
			assert.NoError(t, err)
		}
	}()

	assert.Eventually(
		t,
		func() bool {
			<-ch

			return true
		},
		time.Second, 10*time.Millisecond,
	)
}

type mockStatsAgg struct {
	min, max iceberg.Literal
}

var _ internal.StatsAgg = (*mockStatsAgg)(nil)

func (m *mockStatsAgg) Min() iceberg.Literal                       { return m.min }
func (m *mockStatsAgg) Max() iceberg.Literal                       { return m.max }
func (m *mockStatsAgg) Update(stats interface{ HasMinMax() bool }) {}
func (m *mockStatsAgg) MinAsBytes() ([]byte, error)                { return nil, nil }
func (m *mockStatsAgg) MaxAsBytes() ([]byte, error)                { return nil, nil }

func TestPartitionValue_LinearTransforms(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "ts_col",
		Type: iceberg.TimestampType{},
	})

	cases := []struct {
		name      string
		transform iceberg.Transform
		min       time.Time
		max       time.Time
	}{
		{
			name:      "day",
			transform: iceberg.DayTransform{},
			min:       time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC),
			max:       time.Date(2023, 5, 15, 23, 59, 59, 0, time.UTC),
		},
		{
			name:      "month",
			transform: iceberg.MonthTransform{},
			min:       time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC),
			max:       time.Date(2023, 5, 31, 23, 59, 59, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			minLit := iceberg.NewLiteral(iceberg.Timestamp(tc.min.UnixMicro()))
			maxLit := iceberg.NewLiteral(iceberg.Timestamp(tc.max.UnixMicro()))

			partitionField := iceberg.PartitionField{
				SourceID:  1,
				FieldID:   100,
				Name:      tc.name + "_part",
				Transform: tc.transform,
			}

			stats := internal.DataFileStatistics{
				ColAggs: map[int]internal.StatsAgg{
					1: &mockStatsAgg{min: minLit, max: maxLit},
				},
			}

			minLitOpt := iceberg.Optional[iceberg.Literal]{Val: minLit, Valid: true}
			expected := tc.transform.Apply(minLitOpt).Val.Any()
			got := stats.PartitionValue(partitionField, schema)
			assert.Equal(t, expected, got)
		})
	}
}

func TestPartitionValue_MismatchPanics(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "ts_col",
		Type: iceberg.TimestampType{},
	})
	partitionField := iceberg.PartitionField{
		SourceID:  1,
		FieldID:   100,
		Name:      "date_part",
		Transform: iceberg.DayTransform{},
	}

	day1 := iceberg.NewLiteral(iceberg.Timestamp(time.Date(2023, 5, 15, 12, 0, 0, 0, time.UTC).UnixMicro()))
	day2 := iceberg.NewLiteral(iceberg.Timestamp(time.Date(2023, 5, 16, 12, 0, 0, 0, time.UTC).UnixMicro()))

	stats := internal.DataFileStatistics{
		ColAggs: map[int]internal.StatsAgg{
			1: &mockStatsAgg{min: day1, max: day2},
		},
	}

	assert.Panics(t, func() { stats.PartitionValue(partitionField, schema) })
}
