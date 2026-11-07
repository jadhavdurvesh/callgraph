package callgraph
import("encoding/binary";"errors")
var ErrMalformedFrame=errors.New("callgraph: malformed RPC frame")
const FrameHeaderSize=38
type Frame struct{Kind uint8;RequestID [16]byte;OperationID uint32;Major uint16;Minor uint16;DeadlineMillis uint32;Payload []byte}
// CGR1 header: magic/version/kind/request-id/op-id/major/minor/deadline/payload-length.
func EncodeFrame(f Frame,max int)([]byte,error){if max<0||len(f.Payload)>max{return nil,ErrPayloadTooLarge};b:=make([]byte,FrameHeaderSize+len(f.Payload));copy(b[:4],"CGR1");b[4]=1;b[5]=f.Kind;copy(b[6:22],f.RequestID[:]);binary.BigEndian.PutUint32(b[22:26],f.OperationID);binary.BigEndian.PutUint16(b[26:28],f.Major);binary.BigEndian.PutUint16(b[28:30],f.Minor);binary.BigEndian.PutUint32(b[30:34],f.DeadlineMillis);binary.BigEndian.PutUint32(b[34:38],uint32(len(f.Payload)));copy(b[38:],f.Payload);return b,nil}
func DecodeFrame(b []byte,max int)(Frame,error){var f Frame;if len(b)<FrameHeaderSize||string(b[:4])!="CGR1"||b[4]!=1{return f,ErrMalformedFrame};n:=uint64(binary.BigEndian.Uint32(b[34:38]));if max<0||n>uint64(max)||uint64(len(b))!=uint64(FrameHeaderSize)+n{return f,ErrMalformedFrame};f.Kind=b[5];copy(f.RequestID[:],b[6:22]);f.OperationID=binary.BigEndian.Uint32(b[22:26]);f.Major=binary.BigEndian.Uint16(b[26:28]);f.Minor=binary.BigEndian.Uint16(b[28:30]);f.DeadlineMillis=binary.BigEndian.Uint32(b[30:34]);f.Payload=b[38:];return f,nil}
