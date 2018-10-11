/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package subprotocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/alipay/sofa-mosn/pkg/protocol/serialize"
	"github.com/alipay/sofa-mosn/pkg/protocol/sofarpc"
	"github.com/alipay/sofa-mosn/pkg/types"
	"strconv"
)

func init() {
	Register("sofa", newCodecFactory())
}

const (
	ServiceKey 	 = "sofa_head_target_service"
	MethodKey 	 = "sofa_head_method_name"
)

var (
	unknownProtocolError = fmt.Errorf("unknown protocol")
	unKnowRequestTypeError = fmt.Errorf("unknow request type")
)

type sofaCodecFactory struct {}

type sofaCodec struct {}

func newCodecFactory() CodecFactory {
	return &sofaCodecFactory{}
}

func newCodec() types.Multiplexing {
	return &sofaCodec{}
}

func (factory *sofaCodecFactory) CreateSubProtocolCodec(context context.Context) types.Multiplexing {
	return newCodec()
}

type frameMeta struct {
	requestIDIdx int
	headLen int
	classLen int
	headerLen int
	contentLen int
}

func (m *frameMeta) frameLen() int {
	return m.headLen + m.classLen + m.contentLen + m.headerLen
}

// SplitFrame split raw data to stream frame according to different protocol spec
func (codec *sofaCodec) SplitFrame(data []byte) [][]byte {
	frames := make([][]byte, 0)
	fb := data[:]
	for len(fb) > 20 {
		if m, ok, _ := codec.detectFrame(fb); ok {
			l := m.frameLen()
			frame := fb[: l]
			fb = fb[l:]
			frames = append(frames, frame)
		} else {
			break // just break out if buffer is not ready
		}
	}
	return frames
}

// GetStreamID gets stream id of a request or a response
func (codec *sofaCodec)  GetStreamID(data []byte) string {

	if m, ok, _ := codec.detectFrame(data); ok {
		i := m.requestIDIdx
		idBytes := data[i : i+4]
		id := binary.BigEndian.Uint32(idBytes)
		return fmt.Sprintf("%d", id)
	}
	return ""
}

// SetStreamID sets stream id of the request or response to a new value
func (codec *sofaCodec) SetStreamID(data []byte, streamID string) []byte {
	requestId, err := strconv.ParseUint(streamID, 10, 64)
	if err != nil {
		return data
	}

	buffer := new(bytes.Buffer)
	err = binary.Write(buffer, binary.BigEndian, uint32(requestId))
	if err != nil {
		return data
	}

	if m, ok, _ := codec.detectFrame(data); ok {
		requestIdBytes := buffer.Bytes()
		for i := 0; i < len(requestIdBytes) && i < 4; i++ {
			offset := m.requestIDIdx + i
			data[offset] = requestIdBytes[i]
		}
	}

	return data
}

// GetServiceName returns service name, normally it is the interface name of rpc service
func (codec *sofaCodec) GetServiceName(data []byte) string {
	headers := codec.GetMetas(data)
	serviceName, exist := headers[ServiceKey]
	if exist {
		return serviceName
	}
	return ""
}

// GetMethodName returns target method name for rpc service
func (codec *sofaCodec) GetMethodName(data []byte) string {
	headers := codec.GetMetas(data)
	serviceName, exist := headers[MethodKey]
	if exist {
		return serviceName
	}
	return ""
}


// GetMetas returns header as map
func (codec *sofaCodec) GetMetas(data []byte) map[string]string {
	header := make(map[string]string)
	if m, ok, _ := codec.detectFrame(data); ok {
		headerIdx := m.headLen + m.classLen
		headerBytes := data[headerIdx : headerIdx + m.headerLen]
		serialize.Instance.DeSerialize(headerBytes, &header)
	}
	return header
}

// Convert returns header as map and body as byte slice
func (codec *sofaCodec) Convert(data []byte) (map[string]string, []byte) {
	var body []byte
	if m, ok, _ := codec.detectFrame(data); ok {
		contentIdx := m.headLen + m.classLen + m.headerLen
		body = data[contentIdx : contentIdx + m.contentLen]
	}
	header := codec.GetMetas(data)
	return header, body
}

func (codec *sofaCodec) detectFrame(data []byte) (meta *frameMeta, ready bool, err error) {
	var headLen, idx int
	proto := sofarpc.ProtocolType(data[0])
	switch proto {
	case sofarpc.BOLT_V1:
		kind := data[1]
		idx = 5
		switch kind {
		case sofarpc.REQUEST, sofarpc.REQUEST_ONEWAY:
			headLen = 22
		case sofarpc.RESPONSE:
			headLen = 20
		default:
			err = unKnowRequestTypeError
		}
	case sofarpc.BOLT_V2:
		kind := data[2]
		idx = 6
		switch kind {
		case sofarpc.REQUEST, sofarpc.REQUEST_ONEWAY:
			headLen = 24
		case sofarpc.RESPONSE:
			headLen = 22
		default:
			err = unKnowRequestTypeError
		}
	default:
		err = unknownProtocolError
	}
	// 20 is the min head length
	if headLen >= 20 {
		classLen := binary.BigEndian.Uint16(data[headLen-8 : headLen-6])
		headerLen := binary.BigEndian.Uint16(data[headLen-6 : headLen-4])
		contentLen := binary.BigEndian.Uint32(data[headLen-4 : headLen])
		frameLen := headLen + int(classLen) + int(headerLen) + int(contentLen)
		meta = &frameMeta{
			requestIDIdx: idx,
			headLen: headLen,
			classLen: int(classLen),
			headerLen: int(headerLen),
			contentLen: int(contentLen),
		}
		ready = frameLen > 0 && frameLen <= len(data)
	}
	return
}