package items

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *TupleRecord) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "T":
			z.T, err = dc.ReadTime()
			if err != nil {
				return
			}
		case "V":
			err = z.V.DecodeMsg(dc)
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *TupleRecord) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "T"
	err = en.Append(0x82, 0xa1, 0x54)
	if err != nil {
		return
	}
	err = en.WriteTime(z.T)
	if err != nil {
		return
	}
	// write "V"
	err = en.Append(0xa1, 0x56)
	if err != nil {
		return
	}
	err = z.V.EncodeMsg(en)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TupleRecord) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "T"
	o = append(o, 0x82, 0xa1, 0x54)
	o = msgp.AppendTime(o, z.T)
	// string "V"
	o = append(o, 0xa1, 0x56)
	o, err = z.V.MarshalMsg(o)
	if err != nil {
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TupleRecord) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "T":
			z.T, bts, err = msgp.ReadTimeBytes(bts)
			if err != nil {
				return
			}
		case "V":
			bts, err = z.V.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *TupleRecord) Msgsize() (s int) {
	s = 1 + 2 + msgp.TimeSize + 2 + z.V.Msgsize()
	return
}
