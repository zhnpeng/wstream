package types

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *MapRecord) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "K":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.K) >= int(zb0002) {
				z.K = (z.K)[:zb0002]
			} else {
				z.K = make([]interface{}, zb0002)
			}
			for za0001 := range z.K {
				z.K[za0001], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		case "V":
			var zb0003 uint32
			zb0003, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.V == nil && zb0003 > 0 {
				z.V = make(map[string]interface{}, zb0003)
			} else if len(z.V) > 0 {
				for key := range z.V {
					delete(z.V, key)
				}
			}
			for zb0003 > 0 {
				zb0003--
				var za0002 string
				var za0003 interface{}
				za0002, err = dc.ReadString()
				if err != nil {
					return
				}
				za0003, err = dc.ReadIntf()
				if err != nil {
					return
				}
				z.V[za0002] = za0003
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
func (z *MapRecord) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "T"
	err = en.Append(0x83, 0xa1, 0x54)
	if err != nil {
		return
	}
	err = en.WriteTime(z.T)
	if err != nil {
		return
	}
	// write "K"
	err = en.Append(0xa1, 0x4b)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.K)))
	if err != nil {
		return
	}
	for za0001 := range z.K {
		err = en.WriteIntf(z.K[za0001])
		if err != nil {
			return
		}
	}
	// write "V"
	err = en.Append(0xa1, 0x56)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.V)))
	if err != nil {
		return
	}
	for za0002, za0003 := range z.V {
		err = en.WriteString(za0002)
		if err != nil {
			return
		}
		err = en.WriteIntf(za0003)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MapRecord) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "T"
	o = append(o, 0x83, 0xa1, 0x54)
	o = msgp.AppendTime(o, z.T)
	// string "K"
	o = append(o, 0xa1, 0x4b)
	o = msgp.AppendArrayHeader(o, uint32(len(z.K)))
	for za0001 := range z.K {
		o, err = msgp.AppendIntf(o, z.K[za0001])
		if err != nil {
			return
		}
	}
	// string "V"
	o = append(o, 0xa1, 0x56)
	o = msgp.AppendMapHeader(o, uint32(len(z.V)))
	for za0002, za0003 := range z.V {
		o = msgp.AppendString(o, za0002)
		o, err = msgp.AppendIntf(o, za0003)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MapRecord) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "K":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.K) >= int(zb0002) {
				z.K = (z.K)[:zb0002]
			} else {
				z.K = make([]interface{}, zb0002)
			}
			for za0001 := range z.K {
				z.K[za0001], bts, err = msgp.ReadIntfBytes(bts)
				if err != nil {
					return
				}
			}
		case "V":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.V == nil && zb0003 > 0 {
				z.V = make(map[string]interface{}, zb0003)
			} else if len(z.V) > 0 {
				for key := range z.V {
					delete(z.V, key)
				}
			}
			for zb0003 > 0 {
				var za0002 string
				var za0003 interface{}
				zb0003--
				za0002, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				za0003, bts, err = msgp.ReadIntfBytes(bts)
				if err != nil {
					return
				}
				z.V[za0002] = za0003
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
func (z *MapRecord) Msgsize() (s int) {
	s = 1 + 2 + msgp.TimeSize + 2 + msgp.ArrayHeaderSize
	for za0001 := range z.K {
		s += msgp.GuessSize(z.K[za0001])
	}
	s += 2 + msgp.MapHeaderSize
	if z.V != nil {
		for za0002, za0003 := range z.V {
			_ = za0003
			s += msgp.StringPrefixSize + len(za0002) + msgp.GuessSize(za0003)
		}
	}
	return
}
