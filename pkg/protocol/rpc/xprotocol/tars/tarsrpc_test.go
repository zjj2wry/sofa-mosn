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

package tars

import "testing"

func Test_tars_SplitFrame_Request(t *testing.T) {
	msg := []byte{0, 0, 0, 31, 16, 1, 44, 49, 44, 146, 76, 92, 109, 0, 0, 9, 12, 45, 0, 0, 4, 1, 1, 1, 1, 120, 12, 134, 0, 152, 12, 0, 0, 0, 31, 16, 1, 44, 49, 36, 65, 76, 92, 109, 0, 0, 9, 12, 45, 0, 0, 4, 1, 1, 1, 1, 120, 12, 134, 0, 152, 12, 0, 0, 0, 31, 16, 1, 44, 49, 36, 66, 76, 92, 109, 0, 0, 9, 12, 45, 0, 0, 4, 1, 1, 1, 1, 120, 12, 134, 0, 152, 12}
	rpc := NewRpcTars()
	resps := rpc.SplitFrame(msg)
	respLen := len(resps)
	if respLen != 3 {
		t.Errorf("%d != 3", respLen)
	} else {
		t.Log("split mulit-request ok")
	}
}

func Test_tars_SplitFrame_Response(t *testing.T) {
	msg := []byte{0, 0, 0, 75, 16, 1, 44, 60, 65, 44, 142, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 44, 143, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 44, 144, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 44, 145, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 44, 146, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12}
	rpc := NewRpcTars()
	reqs := rpc.SplitFrame(msg)
	reqsLen := len(reqs)
	if reqsLen != 5 {
		t.Errorf("%d != 5", reqsLen)
	} else {
		t.Log("split mulit-request ok")
	}
}

func Test_tars_GetStreamID_Response(t *testing.T) {
	msg := []byte{0, 0, 0, 31, 16, 1, 44, 49, 44, 146, 76, 92, 109, 0, 0, 9, 12, 45, 0, 0, 4, 1, 1, 1, 1, 120, 12, 134, 0, 152, 12}
	rpc := NewRpcTars()
	strId := rpc.GetStreamID(msg)
	if strId != "11410" {
		t.Errorf("%s != 11410", strId)
	} else {
		t.Log("get stream-id from request ok")
	}
}

func Test_tars_GetStreamID_Request(t *testing.T) {
	msg := []byte{0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 236, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 237, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12}
	rpc := NewRpcTars()
	strId := rpc.GetStreamID(msg)
	if strId != "12012" {
		t.Errorf("%s != 12012", strId)
	} else {
		t.Log("get stream-id from request ok")
	}
}

func Test_tars_GetServiceName_01(t *testing.T) {
	msg := []byte{0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 236, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 237, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12}
	rpc := NewRpcTars()
	name := "StressTest.EchoTestServer.EchoTestObj"
	getName := rpc.GetServiceName(msg)
	if name != getName {
		t.Errorf("%s != %s", name, getName)
	} else {
		t.Log("get service-name succ ok")
	}
}

func Test_Tars_SetStreamID_Request(t *testing.T) {
	msg := []byte{0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 236, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 237, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12}
	rpc := NewRpcTars()
	newMsg := rpc.SetStreamID(msg, "111111")
	newStrId := rpc.GetStreamID(newMsg)
	if newStrId != "111111" {
		t.Errorf("%s != 111111", newStrId)
	} else {
		t.Log("set stream-id by Request ok")
	}
}

func Test_Tars_SetStreamID_01(t *testing.T) {
	msg := []byte{0, 0, 0, 31, 16, 1, 44, 49, 44, 146, 76, 92, 109, 0, 0, 9, 12, 45, 0, 0, 4, 1, 1, 1, 1, 120, 12, 134, 0, 152, 12}
	rpc := NewRpcTars()
	newMsg := rpc.SetStreamID(msg, "111111")
	newStrId := rpc.GetStreamID(newMsg)
	if newStrId != "111111" {
		t.Errorf("%s != 111111", newStrId)
	} else {
		t.Log("set stream-id by Response ok")
	}
}

func Test_tars_GetMethodName_01(t *testing.T) {
	msg := []byte{0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 236, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12, 0, 0, 0, 75, 16, 1, 44, 60, 65, 46, 237, 86, 37, 83, 116, 114, 101, 115, 115, 84, 101, 115, 116, 46, 69, 99, 104, 111, 84, 101, 115, 116, 83, 101, 114, 118, 101, 114, 46, 69, 99, 104, 111, 84, 101, 115, 116, 79, 98, 106, 102, 4, 101, 99, 104, 111, 125, 0, 0, 8, 29, 0, 0, 4, 1, 1, 1, 1, 129, 11, 184, 152, 12, 168, 12}
	rpc := NewRpcTars()
	name := "echo"
	getName := rpc.GetMethodName(msg)
	if name != getName {
		t.Errorf("%s != %s", name, getName)
	} else {
		t.Log("get method-name succ ok")
	}
}
