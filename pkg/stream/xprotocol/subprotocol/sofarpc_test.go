package subprotocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/alipay/sofa-mosn/pkg/protocol/serialize"
	"github.com/alipay/sofa-mosn/pkg/types"
)

type base struct {
	proto     uint8
	kind      uint8
	code      uint16
	ver2      uint8
	id        uint32
	codec     uint8
	className string
	header    map[string]string
	content   string
}

type v1Request struct {
	base
	timeout uint32
}

type v1Response struct {
	base
	status uint16
}

type v2Request struct {
	base
	timeout  uint32
	ver1     uint8
	switcher uint8
}

type v2Response struct {
	base
	ver1     uint8
	switcher uint8
	status   uint16
}

var header = map[string]string{
	ServiceKey: "io.sofa.target",
	MethodKey:  "say",
}

var v1Req = &v1Request{
	base: base{
		proto:     1,
		kind:      1,
		code:      1,
		ver2:      1,
		id:        1,
		codec:     1,
		className: "io.me.sofa",
		header:    header,
		content:   "hello",
	},
	timeout: 8,
}

var v1Res = &v1Response{
	base: base{
		proto:     1,
		kind:      0,
		code:      1,
		ver2:      1,
		id:        1,
		codec:     1,
		className: "io.me.sofa",
		header:    header,
		content:   "hello",
	},
	status: 8,
}

var v2Req = &v2Request{
	base: base{
		proto:     2,
		kind:      1,
		code:      1,
		ver2:      1,
		id:        1,
		codec:     1,
		className: "io.me.sofa",
		header:    header,
		content:   "hello",
	},
	ver1:     1,
	switcher: 1,
	timeout:  8,
}

var v2Res = &v2Response{
	base: base{
		proto:     2,
		kind:      0,
		code:      1,
		ver2:      1,
		id:        1,
		codec:     1,
		className: "io.me.sofa",
		header:    header,
		content:   "hello",
	},
	ver1:     1,
	switcher: 1,
	status:   8,
}

func TestSofaCodec_SplitFrame(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 1 {
		t.Error("v1 request frame should be split")
	}
}

func TestSofaCodec_SplitFrame_Min_Len(t *testing.T) {
	data := []byte{}
	fmt.Print(data[0])
	// codec := newCodec()
	// frames := codec.SplitFrame(data)
	//
	// if len(frames) != 1 {
	// 	t.Errorf("v1 heart beat reponse should be split")
	// }
}

func TestSofaCodec_SplitFrame_More(t *testing.T) {
	data := buildV1Request(v1Req)
	buffer := new(bytes.Buffer)
	buffer.Write(data)
	buffer.Write([]byte{1, 3, 4})
	codec := newCodec()
	frames := codec.SplitFrame(buffer.Bytes())

	if len(frames) != 1 {
		t.Errorf("expect frame is splited if the packges are merged")
	}
}

func TestSofaCodec_SplitFrame_less(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	frames := codec.SplitFrame(data[:len(data)-5])

	if len(frames) != 0 {
		t.Errorf("expect no frame splited if it is not ready")
	}
}

func TestSofaCodec_SplitFrame_Unknow_Proto(t *testing.T) {
	var newReq = *v1Req
	newReq.proto = 3
	data := buildV1Request(&newReq)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 0 {
		t.Errorf("expect no frame splited if the protocol is unkonwn")
	}
}

func TestSofaCodec_SplitFrame_Unknow_Type(t *testing.T) {
	var newReq = *v1Req
	newReq.kind = 3
	data := buildV1Request(&newReq)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 0 {
		t.Errorf("expect no frame splited if the type is unkonwn")
	}
}

func TestSofaCodec_SplitFrame_V1_response(t *testing.T) {
	data := buildV1Response(v1Res)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 1 {
		t.Errorf("expect frame is splited if the packges are merged")
	}
}

func TestSofaCodec_SplitFrame_V2(t *testing.T) {
	data := buildV2Request(v2Req)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 1 {
		t.Errorf("expect frame is splited")
	}
}

func TestSofaCodec_SplitFrame_V2_response(t *testing.T) {
	data := buildV2Response(v2Res)
	codec := newCodec()
	frames := codec.SplitFrame(data)

	if len(frames) != 1 {
		t.Errorf("expect frame is splited if the packges are merged")
	}
}

func TestSofaCodec_GetStreamID(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	id := codec.GetStreamID(data)
	if id != "1" {
		t.Errorf("expect %s, but got %s", "1", id)
	}
}

func TestSofaCodec_GetStreamID_Unknow_Proto(t *testing.T) {
	var newReq = *v1Req
	newReq.proto = 3
	data := buildV1Request(&newReq)
	codec := newCodec()
	id := codec.GetStreamID(data)
	if id != "" {
		t.Errorf("expect %s, but got %s", "", id)
	}
}

func TestSofaCodec_GetStreamID_V1_Response(t *testing.T) {
	data := buildV1Response(v1Res)
	codec := newCodec()
	id := codec.GetStreamID(data)
	if id != "1" {
		t.Errorf("expect %s, but got %s", "1", id)
	}
}

func TestSofaCodec_GetStreamID_V2_Request(t *testing.T) {
	data := buildV2Request(v2Req)
	codec := newCodec()
	id := codec.GetStreamID(data)
	if id != "1" {
		t.Errorf("expect %s, but got %s", "1", id)
	}
}

func TestSofaCodec_GetStreamID_V2_Response(t *testing.T) {
	data := buildV2Response(v2Res)
	codec := newCodec()
	id := codec.GetStreamID(data)
	if id != "1" {
		t.Errorf("expect %s, but got %s", "1", id)
	}
}

func TestSofaCodec_SetStreamID(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	expected := "2"
	codec.SetStreamID(data, expected)
	idBytes := data[5:9]
	id := binary.BigEndian.Uint32(idBytes)
	actual := strconv.FormatUint(uint64(id), 10)

	if actual != expected {
		t.Errorf("expected stream id %s, but got %s", expected, actual)
	}
}

func TestSofaCodec_GetMetas(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	routing := codec.(types.RequestRouting)
	headers := routing.GetMetas(data)

	assertHeaders(headers, t)
}

func TestSofaCodec_GetMetas_V1_Response(t *testing.T) {
	data := buildV1Response(v1Res)
	codec := newCodec()
	routing := codec.(types.RequestRouting)
	headers := routing.GetMetas(data)

	assertHeaders(headers, t)
}

func TestSofaCodec_GetMethodName(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	tracing := codec.(types.Tracing)
	method := tracing.GetMethodName(data)

	if method != "say" {
		t.Errorf("expect %s, but got %s", "say", method)
	}
}

func TestSofaCodec_GetServiceName(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	tracing := codec.(types.Tracing)
	service := tracing.GetServiceName(data)

	if service != "io.sofa.target" {
		t.Errorf("expect %s, but got %s", "io.sofa.target", service)
	}
}

func TestSofaCodec_Convert(t *testing.T) {
	data := buildV1Request(v1Req)
	codec := newCodec()
	convertor := codec.(types.ProtocolConvertor)
	headers, content := convertor.Convert(data)

	assertHeaders(headers, t)

	if string(content) != "hello" {
		t.Errorf("expect %s, but got %s", "hello", string(content))
	}
}

func assertHeaders(headers map[string]string, t *testing.T) {
	if len(headers) == 2 {
		service := headers[ServiceKey]
		method := headers[MethodKey]
		if service != "io.sofa.target" {
			t.Errorf("expect %s, but got %s", "io.sofa.target", service)
		}
		if method != "say" {
			t.Errorf("expect %s, but got %s", "say", method)
		}
	} else {
		t.Errorf("expect %d entries, but got %d", 2, len(headers))
	}
}

func buildV1HeartBeat(request *v1Response) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, request.proto)
	binary.Write(buffer, binary.BigEndian, request.kind)
	binary.Write(buffer, binary.BigEndian, request.code)
	binary.Write(buffer, binary.BigEndian, request.ver2)
	binary.Write(buffer, binary.BigEndian, request.id)
	binary.Write(buffer, binary.BigEndian, request.codec)
	binary.Write(buffer, binary.BigEndian, request.status)
	binary.Write(buffer, binary.BigEndian, uint16(0))
	binary.Write(buffer, binary.BigEndian, uint16(0))
	binary.Write(buffer, binary.BigEndian, uint32(0))
	return buffer.Bytes()
}

func buildV1Request(request *v1Request) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, request.proto)
	binary.Write(buffer, binary.BigEndian, request.kind)
	binary.Write(buffer, binary.BigEndian, request.code)
	binary.Write(buffer, binary.BigEndian, request.ver2)
	binary.Write(buffer, binary.BigEndian, request.id)
	binary.Write(buffer, binary.BigEndian, request.codec)
	binary.Write(buffer, binary.BigEndian, request.timeout)
	classBytes := []byte(request.className)
	headerBytes, _ := serialize.Instance.Serialize(request.header)
	contentBytes := []byte(request.content)
	binary.Write(buffer, binary.BigEndian, uint16(len(classBytes)))
	binary.Write(buffer, binary.BigEndian, uint16(len(headerBytes)))
	binary.Write(buffer, binary.BigEndian, uint32(len(contentBytes)))
	buffer.Write(classBytes)
	buffer.Write(headerBytes)
	buffer.Write(contentBytes)
	return buffer.Bytes()
}

func buildV1Response(response *v1Response) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, response.proto)
	binary.Write(buffer, binary.BigEndian, response.kind)
	binary.Write(buffer, binary.BigEndian, response.code)
	binary.Write(buffer, binary.BigEndian, response.ver2)
	binary.Write(buffer, binary.BigEndian, response.id)
	binary.Write(buffer, binary.BigEndian, response.codec)
	binary.Write(buffer, binary.BigEndian, response.status)
	classBytes := []byte(response.className)
	headerBytes, _ := serialize.Instance.Serialize(response.header)
	contentBytes := []byte(response.content)
	binary.Write(buffer, binary.BigEndian, uint16(len(classBytes)))
	binary.Write(buffer, binary.BigEndian, uint16(len(headerBytes)))
	binary.Write(buffer, binary.BigEndian, uint32(len(contentBytes)))
	buffer.Write(classBytes)
	buffer.Write(headerBytes)
	buffer.Write(contentBytes)
	return buffer.Bytes()
}

func buildV2Request(request *v2Request) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, request.proto)
	binary.Write(buffer, binary.BigEndian, request.ver1)
	binary.Write(buffer, binary.BigEndian, request.kind)
	binary.Write(buffer, binary.BigEndian, request.code)
	binary.Write(buffer, binary.BigEndian, request.ver2)
	binary.Write(buffer, binary.BigEndian, request.id)
	binary.Write(buffer, binary.BigEndian, request.codec)
	binary.Write(buffer, binary.BigEndian, request.switcher)
	binary.Write(buffer, binary.BigEndian, request.timeout)
	classBytes := []byte(request.className)
	headerBytes, _ := serialize.Instance.Serialize(request.header)
	contentBytes := []byte(request.content)
	binary.Write(buffer, binary.BigEndian, uint16(len(classBytes)))
	binary.Write(buffer, binary.BigEndian, uint16(len(headerBytes)))
	binary.Write(buffer, binary.BigEndian, uint32(len(contentBytes)))
	buffer.Write(classBytes)
	buffer.Write(headerBytes)
	buffer.Write(contentBytes)
	return buffer.Bytes()
}

func buildV2Response(res *v2Response) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, res.proto)
	binary.Write(buffer, binary.BigEndian, res.ver1)
	binary.Write(buffer, binary.BigEndian, res.kind)
	binary.Write(buffer, binary.BigEndian, res.code)
	binary.Write(buffer, binary.BigEndian, res.ver2)
	binary.Write(buffer, binary.BigEndian, res.id)
	binary.Write(buffer, binary.BigEndian, res.codec)
	binary.Write(buffer, binary.BigEndian, res.switcher)
	binary.Write(buffer, binary.BigEndian, res.status)
	classBytes := []byte(res.className)
	headerBytes, _ := serialize.Instance.Serialize(res.header)
	contentBytes := []byte(res.content)
	binary.Write(buffer, binary.BigEndian, uint16(len(classBytes)))
	binary.Write(buffer, binary.BigEndian, uint16(len(headerBytes)))
	binary.Write(buffer, binary.BigEndian, uint32(len(contentBytes)))
	buffer.Write(classBytes)
	buffer.Write(headerBytes)
	buffer.Write(contentBytes)
	return buffer.Bytes()
}
