package callgraph
import("encoding/binary";"errors")
var ErrInvalidOCTETFrame=errors.New("callgraph: invalid OCTET frame")
const OCTETHeaderSize=10
type OCTETFrame struct{Flags uint8;Payload []byte}
// OCTET v1: OCT1 magic, version, flags, u32 big-endian payload length, payload.
func EncodeOCTET(flags uint8,payload []byte,max int)([]byte,error){if max<0||len(payload)>max{return nil,ErrPayloadTooLarge};b:=make([]byte,OCTETHeaderSize+len(payload));copy(b[:4],"OCT1");b[4]=1;b[5]=flags;binary.BigEndian.PutUint32(b[6:10],uint32(len(payload)));copy(b[10:],payload);return b,nil}
func DecodeOCTET(b []byte,max int)(OCTETFrame,error){var f OCTETFrame;if len(b)<OCTETHeaderSize||string(b[:4])!="OCT1"||b[4]!=1{return f,ErrInvalidOCTETFrame};n:=uint64(binary.BigEndian.Uint32(b[6:10]));if max<0||n>uint64(max)||uint64(len(b))!=OCTETHeaderSize+n{return f,ErrInvalidOCTETFrame};f.Flags=b[5];f.Payload=b[10:];return f,nil}
