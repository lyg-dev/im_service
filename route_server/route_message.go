/**
 * Copyright (c) 2014-2015, GoBelieve     
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main
import "bytes"
import "encoding/binary"
import "fmt"
import "io"
import log "github.com/golang/glog"
import "errors"

const MSG_VOIP_CONTROL = 64

//路由服务器消息
const MSG_PUBLISH_OFFLINE = 128
const MSG_SUBSCRIBE = 130
const MSG_UNSUBSCRIBE = 131
const MSG_PUBLISH = 132

const MSG_SUBSCRIBE_GROUP = 133
const MSG_UNSUBSCRIBE_GROUP = 134
const MSG_PUBLISH_GROUP = 135

const MSG_SUBSCRIBE_ROOM = 136
const MSG_UNSUBSCRIBE_ROOM = 137
const MSG_PUBLISH_ROOM = 138

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func()IMessage
var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func()IVersionMessage
var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)

func init() {
	message_creators[MSG_SUBSCRIBE] = func()IMessage{return new(AppUserID)}
	message_creators[MSG_UNSUBSCRIBE] = func()IMessage{return new(AppUserID)}
	message_creators[MSG_PUBLISH] = func()IMessage{return new(AppMessage)}
	message_creators[MSG_PUBLISH_OFFLINE] = func()IMessage{return new(AppMessage)}

	message_creators[MSG_SUBSCRIBE_GROUP] = func()IMessage{return new(AppGroupMemberID)}
	message_creators[MSG_UNSUBSCRIBE_GROUP] = func()IMessage{return new(AppGroupMemberID)}
	message_creators[MSG_PUBLISH_GROUP] = func()IMessage{return new(AppMessage)}
	
	message_creators[MSG_SUBSCRIBE_ROOM] = func()IMessage{return new(AppRoomID)}
	message_creators[MSG_UNSUBSCRIBE_ROOM] = func()IMessage{return new(AppRoomID)}
	message_creators[MSG_PUBLISH_ROOM] = func()IMessage{return new(AppMessage)}

	message_descriptions[MSG_PUBLISH_OFFLINE] = "MSG_PUBLISH_OFFLINE"
	message_descriptions[MSG_SUBSCRIBE] = "MSG_SUBSCRIBE"
	message_descriptions[MSG_UNSUBSCRIBE] = "MSG_UNSUBSCRIBE"
	message_descriptions[MSG_PUBLISH] = "MSG_PUBLISH"

	message_descriptions[MSG_SUBSCRIBE_GROUP] = "MSG_SUBSCRIBE_GROUP"
	message_descriptions[MSG_UNSUBSCRIBE_GROUP] = "MSG_UNSUBSCRIBE_GROUP"
	message_descriptions[MSG_PUBLISH_GROUP] = "MSG_PUBLISH_GROUP"

	message_descriptions[MSG_SUBSCRIBE_ROOM] = "MSG_SUBSCRIBE_ROOM"
	message_descriptions[MSG_UNSUBSCRIBE_ROOM] = "MSG_UNSUBSCRIBE_ROOM"
	message_descriptions[MSG_PUBLISH_ROOM] = "MSG_PUBLISH_ROOM"
}

type AppMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	device_id int64
	msg      *Message
}


func (amsg *AppMessage) ToData() []byte {
	if amsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, amsg.appid)
	binary.Write(buffer, binary.BigEndian, amsg.receiver)
	binary.Write(buffer, binary.BigEndian, amsg.msgid)
	binary.Write(buffer, binary.BigEndian, amsg.device_id)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, amsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	buf := buffer.Bytes()
	return buf
}

func (amsg *AppMessage) FromData(buff []byte) bool {
	if len(buff) < 34 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &amsg.appid)
	binary.Read(buffer, binary.BigEndian, &amsg.receiver)
	binary.Read(buffer, binary.BigEndian, &amsg.msgid)
	binary.Read(buffer, binary.BigEndian, &amsg.device_id)

	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)

	mbuffer := bytes.NewBuffer(msg_buf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return false
	}
	amsg.msg = msg

	return true
}

type Message struct {
	cmd  int
	seq  int
	version int
	
	body interface{}
}

func (message *Message) ToData() []byte {
	if message.body != nil {
		if m, ok := message.body.(IMessage); ok {
			return m.ToData()
		}
		if m, ok := message.body.(IVersionMessage); ok {
			return m.ToData(message.version)
		}
		return nil
	} else {
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.cmd
	if creator, ok := message_creators[cmd]; ok {
		c := creator()
		r := c.FromData(buff)
		message.body = c
		return r
	}
	if creator, ok := vmessage_creators[cmd]; ok {
		c := creator()
		r := c.FromData(message.version, buff)
		message.body = c
		return r
	}

	return len(buff) == 0
}

type IMessage interface {
	ToData() []byte
	FromData(buff []byte) bool
}

type IVersionMessage interface {
	ToData(version int) []byte
	FromData(version int, buff []byte) bool
}

type AppUserID struct {
	appid    int64
	uid      int64
}

func (id *AppUserID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppUserID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

type AppGroupMemberID struct {
	appid  int64
	gid    int64
	uid    int64
}

func (id *AppGroupMemberID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.gid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppGroupMemberID) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.gid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

type AppRoomID struct {
	appid    int64
	room_id      int64
}

func (id *AppRoomID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.room_id)
	buf := buffer.Bytes()
	return buf
}

func (id *AppRoomID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.room_id)

	return true
}

type Command int
func (cmd Command) String() string {
	c := int(cmd)
	if desc, ok := message_descriptions[c]; ok {
		return desc
	} else {
		return fmt.Sprintf("%d", c)
	}
}

type VOIPControl struct {
	sender   int64
	receiver int64
	content  []byte
}

func (ctl *VOIPControl) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ctl.sender)
	binary.Write(buffer, binary.BigEndian, ctl.receiver)
	buffer.Write([]byte(ctl.content))
	buf := buffer.Bytes()
	return buf
}

func (ctl *VOIPControl) FromData(buff []byte) bool {
	if len(buff) <= 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff[:16])
	binary.Read(buffer, binary.BigEndian, &ctl.sender)
	binary.Read(buffer, binary.BigEndian, &ctl.receiver)
	ctl.content = buff[16:]
	return true
}

func WriteHeader(len int32, seq int32, cmd byte, version byte, buffer io.Writer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	t := []byte{cmd, byte(version), 0, 0}
	buffer.Write(t)
}

func ReadHeader(buff []byte) (int, int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	version, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd), int(version)
}

func WriteMessage(w *bytes.Buffer, msg *Message) {
	body := msg.ToData()
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), byte(msg.version), w)
	w.Write(body)
}

func SendMessage(conn io.Writer, msg *Message) error {
	buffer := new(bytes.Buffer)
	WriteMessage(buffer, msg)
	buf := buffer.Bytes()
	n, err := conn.Write(buf)
	if err != nil {
		log.Info("sock write error:", err)
		return err
	}
	if n != len(buf) {
		log.Infof("write less:%d %d", n, len(buf))
		return errors.New("write less")
	}
	return nil
}

func ReceiveMessage(conn io.Reader) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version := ReadHeader(buff)
	if length < 0 || length >= 32*1024 {
		log.Info("invalid len:", length)
		return nil
	}
	buff = make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	message := new(Message)
	message.cmd = cmd
	message.seq = seq
	message.version = version
	if !message.FromData(buff) {
		log.Warning("parse error")
		return nil
	}
	return message
}