package callgraph

import (
	"encoding/binary"
	"errors"
)

const (
	ackHeaderLength = 24
	ackRangeLength = 16
	maxAckRanges = 8
)

var ErrMalformedACK = errors.New("callgraph: malformed ACK1 frame")

type AckRange struct {
	First uint64
	Last uint64
}

type AckFrame struct {
	LargestAcked uint64
	AckDelayMicros uint32
	Ranges []AckRange
}

func (a AckFrame) Validate() error {
	if len(a.Ranges) == 0 || len(a.Ranges) > maxAckRanges { return ErrMalformedACK }
	if a.Ranges[0].First > a.Ranges[0].Last || a.Ranges[0].Last != a.LargestAcked { return ErrMalformedACK }
	for i := 1; i < len(a.Ranges); i++ {
		newer, older := a.Ranges[i-1], a.Ranges[i]
		if older.First > older.Last || older.Last == ^uint64(0) || older.Last+1 >= newer.First {
			return ErrMalformedACK
		}
	}
	return nil
}

func (a AckFrame) Encode() ([]byte, error) {
	if err := a.Validate(); err != nil { return nil, err }
	out := make([]byte, ackHeaderLength+len(a.Ranges)*ackRangeLength)
	copy(out[:4], "ACK1")
	out[4] = 1
	binary.BigEndian.PutUint64(out[8:16], a.LargestAcked)
	binary.BigEndian.PutUint32(out[16:20], a.AckDelayMicros)
	out[20] = byte(len(a.Ranges))
	for i, r := range a.Ranges {
		offset := ackHeaderLength+i*ackRangeLength
		binary.BigEndian.PutUint64(out[offset:offset+8], r.First)
		binary.BigEndian.PutUint64(out[offset+8:offset+16], r.Last)
	}
	return out, nil
}

func DecodeACK(input []byte) (AckFrame, error) {
	if len(input) < ackHeaderLength || string(input[:4]) != "ACK1" || input[4] != 1 ||
		input[5] != 0 || input[6] != 0 || input[7] != 0 ||
		input[21] != 0 || input[22] != 0 || input[23] != 0 {
		return AckFrame{}, ErrMalformedACK
	}
	count := int(input[20])
	if count == 0 || count > maxAckRanges || len(input) != ackHeaderLength+count*ackRangeLength {
		return AckFrame{}, ErrMalformedACK
	}
	frame := AckFrame{
		LargestAcked: binary.BigEndian.Uint64(input[8:16]),
		AckDelayMicros: binary.BigEndian.Uint32(input[16:20]),
		Ranges: make([]AckRange, count),
	}
	for i := range frame.Ranges {
		offset := ackHeaderLength+i*ackRangeLength
		frame.Ranges[i] = AckRange{
			First: binary.BigEndian.Uint64(input[offset:offset+8]),
			Last: binary.BigEndian.Uint64(input[offset+8:offset+16]),
		}
	}
	if err := frame.Validate(); err != nil { return AckFrame{}, err }
	return frame, nil
}
