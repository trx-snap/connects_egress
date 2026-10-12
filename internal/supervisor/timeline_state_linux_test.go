//go:build linux

package supervisor

import (
	"errors"
	"reflect"
	"testing"
)

func TestRoomStateApplyBindConsumesReservedPendingGeneration(t *testing.T) {
	const (
		room     = "room_test"
		reserved = uint64(5)
	)
	key := EpochKey{
		Run:    "run-test",
		Source: "source-test",
		Epoch:  1,
	}

	st := roomState{
		Version:        stateVersion,
		Room:           room,
		RecordHigh:     10,
		NextGeneration: reserved + 1,
		Pending: &PendingSwitch{
			Incoming: RoomBinding{
				Generation: reserved,
				Key:        key,
			},
		},
	}

	rec := roomRecord{
		Version: stateVersion,
		Room:    room,
		Seq:     st.RecordHigh + 1,
		Op:      opBind,
		Bind: &RoomBinding{
			Generation: reserved,
			Key:        key,
		},
	}

	if err := st.apply(rec); err != nil {
		t.Fatalf("apply(opBind): unexpected error: %v", err)
	}

	if len(st.Bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(st.Bindings))
	}
	if b := st.Bindings[0]; b.Generation != reserved || b.Key != key {
		t.Fatalf("unexpected binding: %+v, want generation %d key %+v", b, reserved, key)
	}
	if st.Pending != nil {
		t.Fatalf("expected pending switch to be cleared, got %+v", st.Pending)
	}
	if st.NextGeneration != reserved+1 {
		t.Fatalf("expected NextGeneration %d to remain reserved+1, got %d", reserved+1, st.NextGeneration)
	}
	if st.RecordHigh != 11 {
		t.Fatalf("expected RecordHigh to advance to 11, got %d", st.RecordHigh)
	}
}

func TestRoomStateApplyBindRejectsMismatchedPendingBind(t *testing.T) {
	const (
		room     = "room_test"
		reserved = uint64(5)
	)
	key := EpochKey{
		Run:    "run-test",
		Source: "source-test",
		Epoch:  1,
	}

	basePendingState := func() roomState {
		return roomState{
			Version:        stateVersion,
			Room:           room,
			RecordHigh:     10,
			NextGeneration: reserved + 1,
			Pending: &PendingSwitch{
				Incoming: RoomBinding{
					Generation: reserved,
					Key:        key,
				},
			},
		}
	}

	tests := []struct {
		name string
		bind RoomBinding
	}{
		{
			name: "wrong generation higher",
			bind: RoomBinding{
				Generation: reserved + 1,
				Key:        key,
			},
		},
		{
			name: "wrong generation lower",
			bind: RoomBinding{
				Generation: reserved - 1,
				Key:        key,
			},
		},
		{
			name: "same generation wrong run",
			bind: RoomBinding{
				Generation: reserved,
				Key: EpochKey{
					Run:    "wrong-run",
					Source: key.Source,
					Epoch:  key.Epoch,
				},
			},
		},
		{
			name: "same generation wrong source",
			bind: RoomBinding{
				Generation: reserved,
				Key: EpochKey{
					Run:    key.Run,
					Source: "wrong-source",
					Epoch:  key.Epoch,
				},
			},
		},
		{
			name: "same generation wrong epoch",
			bind: RoomBinding{
				Generation: reserved,
				Key: EpochKey{
					Run:    key.Run,
					Source: key.Source,
					Epoch:  key.Epoch + 1,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := basePendingState()
			expected := basePendingState()

			rec := roomRecord{
				Version: stateVersion,
				Room:    room,
				Seq:     st.RecordHigh + 1,
				Op:      opBind,
				Bind:    &tc.bind,
			}

			err := st.apply(rec)
			if !errors.Is(err, ErrTimelineInvalid) {
				t.Fatalf("apply(opBind): got error %v, want errors.Is ErrTimelineInvalid", err)
			}
			if !reflect.DeepEqual(st, expected) {
				t.Fatalf("state mutated on rejected bind:\ngot:  %+v\nwant: %+v", st, expected)
			}
		})
	}

	t.Run("stale no-pending bind", func(t *testing.T) {
		st := roomState{
			Version:        stateVersion,
			Room:           room,
			RecordHigh:     4,
			NextGeneration: 3,
			Pending:        nil,
		}
		expectedInitial := st

		staleRec := roomRecord{
			Version: stateVersion,
			Room:    room,
			Seq:     st.RecordHigh + 1,
			Op:      opBind,
			Bind: &RoomBinding{
				Generation: 2,
				Key:        key,
			},
		}

		err := st.apply(staleRec)
		if !errors.Is(err, ErrTimelineInvalid) {
			t.Fatalf("apply(opBind generation=2): got error %v, want errors.Is ErrTimelineInvalid", err)
		}
		if !reflect.DeepEqual(st, expectedInitial) {
			t.Fatalf("state mutated on stale bind rejection:\ngot:  %+v\nwant: %+v", st, expectedInitial)
		}

		validRec := roomRecord{
			Version: stateVersion,
			Room:    room,
			Seq:     st.RecordHigh + 1,
			Op:      opBind,
			Bind: &RoomBinding{
				Generation: 3,
				Key:        key,
			},
		}

		if err := st.apply(validRec); err != nil {
			t.Fatalf("apply(opBind generation=3): unexpected error: %v", err)
		}
		if len(st.Bindings) != 1 {
			t.Fatalf("expected 1 binding, got %d", len(st.Bindings))
		}
		if b := st.Bindings[0]; b.Generation != 3 || b.Key != key {
			t.Fatalf("unexpected binding: %+v, want generation 3 key %+v", b, key)
		}
		if st.NextGeneration != 4 {
			t.Fatalf("expected NextGeneration to advance to 4, got %d", st.NextGeneration)
		}
		if st.RecordHigh != 5 {
			t.Fatalf("expected RecordHigh to advance to 5, got %d", st.RecordHigh)
		}
		if st.Pending != nil {
			t.Fatalf("expected pending to remain nil, got %+v", st.Pending)
		}
	})
}
