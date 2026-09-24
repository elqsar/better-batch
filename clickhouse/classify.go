package clickhouse

import (
	"errors"
	"fmt"

	batch "github.com/elqsar/better-batch"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// Server error codes, from ClickHouse's src/Common/ErrorCodes.cpp.
//
// rejected is the data being wrong: the same batch will fail the same way
// however often it is sent. misdirected is the destination being wrong: every
// batch will fail until someone fixes the configuration or the schema, and
// then all of them will succeed.
var (
	rejected = map[int32]bool{
		6:   true, // CANNOT_PARSE_TEXT
		27:  true, // CANNOT_PARSE_INPUT_ASSERTION_FAILED
		38:  true, // CANNOT_PARSE_DATE
		41:  true, // CANNOT_PARSE_DATETIME
		53:  true, // TYPE_MISMATCH
		70:  true, // CANNOT_CONVERT_TYPE
		72:  true, // CANNOT_PARSE_NUMBER
		117: true, // INCORRECT_DATA
		349: true, // CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
		469: true, // VIOLATED_CONSTRAINT
	}
	misdirected = map[int32]bool{
		16:  true, // NO_SUCH_COLUMN_IN_TABLE
		60:  true, // UNKNOWN_TABLE
		81:  true, // UNKNOWN_DATABASE
		192: true, // UNKNOWN_USER
		193: true, // WRONG_PASSWORD
		497: true, // ACCESS_DENIED
		516: true, // AUTHENTICATION_FAILED
	}
)

// classifyServer marks a server rejection of the data as permanent, so the
// buffer dead-letters it with nothing configured.
func classifyServer(err error) error {
	if code, ok := exceptionCode(err); ok && rejected[code] {
		return fmt.Errorf("%w: %w", batch.ErrPermanent, err)
	}
	return err
}

func exceptionCode(err error) (int32, bool) {
	if ex, ok := errors.AsType[*ch.Exception](err); ok {
		return ex.Code, true
	}
	return 0, false
}

// Classify is a handler for batch.WithOnSinkError. It stops the buffer, with
// the backlog kept, when ClickHouse says the destination is wrong — bad
// credentials, no such database, table or column — or the row function
// returns the wrong number of values. Without it those are retried on the
// configured ladder, which is safe but never gets anywhere.
//
// A batch the sink marked with batch.ErrPermanent is dead-lettered, as it
// would be without a handler; anything else is retried.
func Classify(f batch.SinkFailure) batch.RetryDecision {
	if errors.Is(f.Err, errRowShape) {
		return batch.RetryDecision{Action: batch.FailBuffer}
	}
	if code, ok := exceptionCode(f.Err); ok && misdirected[code] {
		return batch.RetryDecision{Action: batch.FailBuffer}
	}
	if errors.Is(f.Err, batch.ErrPermanent) {
		return batch.RetryDecision{Action: batch.DeadLetterBatch}
	}
	return batch.RetryDecision{}
}
