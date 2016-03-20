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

import "io"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import "fmt"
import "errors"

//deprecated
const MSG_HEARTBEAT = 1
const MSG_AUTH = 2

const MSG_AUTH_STATUS = 3

//persistent
const MSG_IM = 4

const MSG_ACK = 5
const MSG_RST = 6

//persistent
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8

//deprecated
const MSG_PEER_ACK = 9
const MSG_INPUTING = 10

//deprecated
const MSG_SUBSCRIBE_ONLINE_STATE = 11
const MSG_ONLINE_STATE = 12

const MSG_PING = 13
const MSG_PONG = 14
const MSG_AUTH_TOKEN = 15
const MSG_LOGIN_POINT = 16
const MSG_RT = 17
const MSG_ENTER_ROOM = 18
const MSG_LEAVE_ROOM = 19
const MSG_ROOM_IM = 20

//persistent
const MSG_SYSTEM = 21

const MSG_UNREAD_COUNT = 22

//persistent
const MSG_CUSTOMER_SERVICE = 23

//透传消息
const MSG_TRANSMIT_USER = 24
const MSG_TRANSMIT_GROUP = 25
const MSG_TRANSMIT_ROOM = 26

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

const MSG_SERVER_REGISTER = 139
const MSG_SERVER_REGISTER_STORAGE = 140

//好友
const MSG_CONTACT_INVITE = 10200
const MSG_CONTACT_INVITE_RESP = 10201

const MSG_CONTACT_ACCEPT = 10202
const MSG_CONTACT_ACCEPT_RESP = 10203

const MSG_CONTACT_REFUSE = 10204
const MSG_CONTACT_REFUSE_RESP = 10205

const MSG_CONTACT_DEL = 10206
const MSG_CONTACT_DEL_RESP = 10207

const MSG_CONTACT_BLACK = 10208
const MSG_CONTACT_BLACK_RESP = 10209
const MSG_CONTACT_UNBLACK = 10210
const MSG_CONTACT_UNBLACK_RESP = 10211

//群
const MSG_GROUP_CREATE = 10300  //创建
const MSG_GROUP_CREATE_RESP = 10301
const MSG_GROUP_SELF_JOIN = 10302 //加入
const MSG_GROUP_SELF_JOIN_RESP = 10303
const MSG_GROUP_INVITE_JOIN = 10304 //群成员拉人入群
const MSG_GROUP_INVITE_JOIN_RESP =10305
const MSG_GROUP_REMOVE = 10306 //移除
const MSG_GROUP_REMOVE_RESP = 10307
const MSG_GROUP_QUIT = 10308 //退出，群主不可以退出
const MSG_GROUP_QUIT_RESP = 10309
const MSG_GROUP_DEL = 10310 //解散
const MSG_GROUP_DEL_RESP = 10311

//平台号
const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3

const DEFAULT_VERSION = 1


//好友操作回调
/*
{
	cmd : 1,
	from: 1,
	to : 2,
	msg : "" //自定义
}
*/
const CMD_CALLBACK_FRIEND_INVITE = 1 //被请求添加好友回调  {from: 1, to:2, reason: "加好友吧"}
const CMD_CALLBACK_FRIEND_DEL = 2 //被删除好友回调	{from: 1, to: 2}
const CMD_CALLBACK_FRIEND_ACCEPT = 3 //好友请求被接受回调	{from: 2, to:1}
const CMD_CALLBACK_FRIEND_REFUSE = 4 //好友请求被拒绝回调	{from:2, to:1}
const CMD_CALLBACK_FRIEND_ADD = 5 //成功添加好友回调		{from:2, to: 1}

//群操作回调
const CMD_CALLBACK_GROUP_REMOVE = 101 //被移除群
const CMD_CALLBACK_GROUP_JOIN = 102 //被加入群
const CMD_CALLBACK_GROUP_DEL = 103 //群被解散

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func() IMessage

var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func() IVersionMessage

var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)

func init() {
	message_creators[MSG_AUTH] = func() IMessage { return new(Authentication) }
	message_creators[MSG_ACK] = func() IMessage { return new(MessageACK) }
	message_creators[MSG_GROUP_NOTIFICATION] = func() IMessage { return new(GroupNotification) }

	message_creators[MSG_PEER_ACK] = func() IMessage { return new(MessagePeerACK) }
	message_creators[MSG_INPUTING] = func() IMessage { return new(MessageInputing) }
	message_creators[MSG_SUBSCRIBE_ONLINE_STATE] = func() IMessage { return new(MessageSubscribeState) }
	message_creators[MSG_ONLINE_STATE] = func() IMessage { return new(MessageOnlineState) }
	message_creators[MSG_AUTH_TOKEN] = func() IMessage { return new(AuthenticationToken) }

	message_creators[MSG_LOGIN_POINT] = func() IMessage { return new(LoginPoint) }
	message_creators[MSG_RT] = func() IMessage { return new(RTMessage) }
	message_creators[MSG_ENTER_ROOM] = func() IMessage { return new(Room) }
	message_creators[MSG_LEAVE_ROOM] = func() IMessage { return new(Room) }
	message_creators[MSG_ROOM_IM] = func() IMessage { return &RoomMessage{new(RTMessage)} }
	message_creators[MSG_SYSTEM] = func() IMessage { return new(SystemMessage) }
	message_creators[MSG_UNREAD_COUNT] = func() IMessage { return new(MessageUnreadCount) }
	message_creators[MSG_CUSTOMER_SERVICE] = func() IMessage { return new(CustomerServiceMessage) }
	message_creators[MSG_VOIP_CONTROL] = func() IMessage { return new(VOIPControl) }

	vmessage_creators[MSG_GROUP_IM] = func() IVersionMessage { return new(IMMessage) }
	vmessage_creators[MSG_IM] = func() IVersionMessage { return new(IMMessage) }
	vmessage_creators[MSG_TRANSMIT_USER] = func() IVersionMessage { return new(IMMessage) }
	vmessage_creators[MSG_TRANSMIT_GROUP] = func() IVersionMessage { return new(IMMessage) }
	message_creators[MSG_TRANSMIT_ROOM] = func() IMessage { return &RoomMessage{new(RTMessage)} }

	vmessage_creators[MSG_AUTH_STATUS] = func() IVersionMessage { return new(AuthenticationStatus) }

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
	
	message_creators[MSG_SERVER_REGISTER] = func() IMessage { return new(ServerID) }
	message_creators[MSG_SERVER_REGISTER_STORAGE] = func() IMessage { return new(ServerID) }
	
	message_creators[MSG_CONTACT_ACCEPT] = func() IMessage { return new(ContactAccept) }
	message_creators[MSG_CONTACT_ACCEPT_RESP] = func() IMessage { return new(ContactAcceptResp) }
	message_creators[MSG_CONTACT_INVITE] = func() IMessage { return new(ContactInvite) }
	message_creators[MSG_CONTACT_INVITE_RESP] = func() IMessage { return new(ContactInviteResp) }
	message_creators[MSG_CONTACT_REFUSE] = func() IMessage { return new(ContactRefuse) }
	message_creators[MSG_CONTACT_REFUSE_RESP] = func() IMessage { return new(ContactRefuseResp) }
	message_creators[MSG_CONTACT_DEL] = func() IMessage { return new(ContactDel) }
	message_creators[MSG_CONTACT_DEL_RESP] = func() IMessage { return new(ContactDelResp) }
	message_creators[MSG_CONTACT_BLACK] = func() IMessage { return new(ContactBlack) }
	message_creators[MSG_CONTACT_BLACK_RESP] = func() IMessage { return new(ContactBlackResp) }
	message_creators[MSG_CONTACT_UNBLACK] = func() IMessage { return new(ContactUnBlack) }
	message_creators[MSG_CONTACT_UNBLACK_RESP] = func() IMessage { return new(ContactUnBlackResp) }
	
	message_creators[MSG_GROUP_CREATE] = func() IMessage { return new(GroupCreate) }
	message_creators[MSG_GROUP_CREATE_RESP] = func() IMessage { return new(GroupCreateResp) }
	message_creators[MSG_GROUP_SELF_JOIN] = func() IMessage { return new(GroupSelfJoin) }
	message_creators[MSG_GROUP_SELF_JOIN_RESP] = func() IMessage { return new(SimpleResp) }
	message_creators[MSG_GROUP_INVITE_JOIN] = func() IMessage { return new(GroupInviteJoin) }
	message_creators[MSG_GROUP_INVITE_JOIN_RESP] = func() IMessage { return new(SimpleResp) }
	message_creators[MSG_GROUP_REMOVE] = func() IMessage { return new(GroupRemove) }
	message_creators[MSG_GROUP_REMOVE_RESP] = func() IMessage { return new(SimpleResp) }
	message_creators[MSG_GROUP_QUIT] = func() IMessage { return new(GroupQuit) }
	message_creators[MSG_GROUP_QUIT_RESP] = func() IMessage { return new(SimpleResp) }
	message_creators[MSG_GROUP_DEL] = func() IMessage { return new(GroupDel) }
	message_creators[MSG_GROUP_DEL_RESP] = func() IMessage { return new(SimpleResp) }

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
	
	message_descriptions[MSG_SERVER_REGISTER] = "MSG_SERVER_REGISTER"
	
	message_descriptions[MSG_AUTH] = "MSG_AUTH"
	message_descriptions[MSG_AUTH_STATUS] = "MSG_AUTH_STATUS"
	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_ACK] = "MSG_ACK"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_PEER_ACK] = "MSG_PEER_ACK"
	message_descriptions[MSG_INPUTING] = "MSG_INPUTING"
	message_descriptions[MSG_SUBSCRIBE_ONLINE_STATE] = "MSG_SUBSCRIBE_ONLINE_STATE"
	message_descriptions[MSG_ONLINE_STATE] = "MSG_ONLINE_STATE"
	message_descriptions[MSG_PING] = "MSG_PING"
	message_descriptions[MSG_PONG] = "MSG_PONG"
	message_descriptions[MSG_AUTH_TOKEN] = "MSG_AUTH_TOKEN"
	message_descriptions[MSG_LOGIN_POINT] = "MSG_LOGIN_POINT"
	message_descriptions[MSG_RT] = "MSG_RT"
	message_descriptions[MSG_ENTER_ROOM] = "MSG_ENTER_ROOM"
	message_descriptions[MSG_LEAVE_ROOM] = "MSG_LEAVE_ROOM"
	message_descriptions[MSG_ROOM_IM] = "MSG_ROOM_IM"
	message_descriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	message_descriptions[MSG_UNREAD_COUNT] = "MSG_UNREAD_COUNT"
	message_descriptions[MSG_CUSTOMER_SERVICE] = "MSG_CUSTOMER_SERVICE"
	message_descriptions[MSG_VOIP_CONTROL] = "MSG_VOIP_CONTROL"
	message_descriptions[MSG_TRANSMIT_USER] = "MSG_TRANSMIT_USER"
	message_descriptions[MSG_TRANSMIT_GROUP] = "MSG_TRANSMIT_GROUP"
	message_descriptions[MSG_TRANSMIT_ROOM] = "MSG_TRANSMIT_ROOM"
	
	message_descriptions[MSG_CONTACT_ACCEPT] = "MSG_CONTACT_ACCEPT"
	message_descriptions[MSG_CONTACT_ACCEPT_RESP] = "MSG_CONTACT_ACCEPT_RESP"
	message_descriptions[MSG_CONTACT_INVITE] = "MSG_CONTACT_INVITE"
	message_descriptions[MSG_CONTACT_INVITE_RESP] = "MSG_CONTACT_INVITE_RESP"
	message_descriptions[MSG_CONTACT_REFUSE] = "MSG_CONTACT_REFUSE"
	message_descriptions[MSG_CONTACT_REFUSE_RESP] = "MSG_CONTACT_REFUSE_RESP"
	message_descriptions[MSG_CONTACT_DEL] = "MSG_CONTACT_DEL"
	message_descriptions[MSG_CONTACT_DEL_RESP] = "MSG_CONTACT_DEL_RESP"
	message_descriptions[MSG_CONTACT_BLACK] = "MSG_CONTACT_BLACK"
	message_descriptions[MSG_CONTACT_BLACK_RESP] = "MSG_CONTACT_BLACK_RESP"
	message_descriptions[MSG_CONTACT_UNBLACK] = "MSG_CONTACT_UNBLACK"
	message_descriptions[MSG_CONTACT_UNBLACK_RESP] = "MSG_CONTACT_UNBLACK_RESP"
	
	message_descriptions[MSG_GROUP_CREATE] = "MSG_GROUP_CREATE"
	message_descriptions[MSG_GROUP_CREATE_RESP] = "MSG_GROUP_CREATE_RESP"
	message_descriptions[MSG_GROUP_SELF_JOIN] = "MSG_GROUP_SELF_JOIN"
	message_descriptions[MSG_GROUP_SELF_JOIN_RESP] = "MSG_GROUP_SELF_JOIN_RESP"
	message_descriptions[MSG_GROUP_INVITE_JOIN] = "MSG_GROUP_INVITE_JOIN"
	message_descriptions[MSG_GROUP_INVITE_JOIN_RESP] = "MSG_GROUP_INVITE_JOIN_RESP"
	message_descriptions[MSG_GROUP_REMOVE] = "MSG_GROUP_REMOVE"
	message_descriptions[MSG_GROUP_REMOVE_RESP] = "MSG_GROUP_REMOVE_RESP"
	message_descriptions[MSG_GROUP_QUIT] = "MSG_GROUP_QUIT"
	message_descriptions[MSG_GROUP_QUIT_RESP] = "MSG_GROUP_QUIT_RESP"
	message_descriptions[MSG_GROUP_DEL] = "MSG_GROUP_DEL"
	message_descriptions[MSG_GROUP_DEL_RESP] = "MSG_GROUP_DEL_RESP"
	message_descriptions[MSG_SERVER_REGISTER_STORAGE] = "MSG_SERVER_REGISTER_STORAGE"
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

type IMessage interface {
	ToData() []byte
	FromData(buff []byte) bool
}

type IVersionMessage interface {
	ToData(version int) []byte
	FromData(version int, buff []byte) bool
}

type Message struct {
	cmd     int
	seq     int
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
		log.Infof("cmd: %d, %+v", cmd, c)
		r := c.FromData(buff)
		log.Infof("cmd: %d, %+v", cmd, c)
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

type ServerID struct {
	serverid string
}

func (id *ServerID) ToData() []byte {
	buffer := new(bytes.Buffer)
	buffer.Write([]byte(id.serverid))
	buf := buffer.Bytes()
	return buf
}

func (id *ServerID) FromData(buff []byte) bool {
	id.serverid = string(buff)
	return true
}

type RTMessage struct {
	sender   int64
	receiver int64
	content  string
}

func (message *RTMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (rt *RTMessage) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &rt.sender)
	binary.Read(buffer, binary.BigEndian, &rt.receiver)
	rt.content = string(buff[16:])
	return true
}

type IMMessage struct {
	sender    int64
	receiver  int64
	timestamp int32
	msgid     int32
	content   string
}

func (message *IMMessage) ToDataV0() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV0(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[20:])
	return true
}

func (message *IMMessage) ToDataV1() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.timestamp)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV1(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.timestamp)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[24:])
	return true
}

func (im *IMMessage) ToData(version int) []byte {
	if version == 0 {
		return im.ToDataV0()
	} else {
		return im.ToDataV1()
	}
}

func (im *IMMessage) FromData(version int, buff []byte) bool {
	if version == 0 {
		return im.FromDataV0(buff)
	} else {
		return im.FromDataV1(buff)
	}
}

type Authentication struct {
	uid int64
}

func (auth *Authentication) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.uid)
	buf := buffer.Bytes()
	return buf
}

func (auth *Authentication) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.uid)
	return true
}

type AuthenticationToken struct {
	token       string
	platform_id int8
	device_id   string
}

func (auth *AuthenticationToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.platform_id)

	l = int8(len(auth.token))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.token))

	l = int8(len(auth.device_id))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.device_id))

	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationToken) FromData(buff []byte) bool {
	var l int8
	if len(buff) <= 3 {
		return false
	}
	auth.platform_id = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	token := make([]byte, l)
	buffer.Read(token)

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	device_id := make([]byte, l)
	buffer.Read(device_id)

	auth.token = string(token)
	auth.device_id = string(device_id)
	return true
}

type AuthenticationStatus struct {
	status int32
	ip     int32 //兼容版本0
}

func (auth *AuthenticationStatus) ToData(version int) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	if version == 0 {
		binary.Write(buffer, binary.BigEndian, auth.ip)
	}
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationStatus) FromData(version int, buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.status)
	if version == 0 {
		if len(buff) < 8 {
			return false
		}
		binary.Read(buffer, binary.BigEndian, &auth.ip)
	}
	return true
}

type LoginPoint struct {
	up_timestamp int32
	platform_id  int8
	device_id    string
}

func (point *LoginPoint) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, point.up_timestamp)
	binary.Write(buffer, binary.BigEndian, point.platform_id)
	buffer.Write([]byte(point.device_id))
	buf := buffer.Bytes()
	return buf
}

func (point *LoginPoint) FromData(buff []byte) bool {
	if len(buff) <= 5 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &point.up_timestamp)
	binary.Read(buffer, binary.BigEndian, &point.platform_id)
	point.device_id = string(buff[5:])
	return true
}

type MessageACK struct {
	seq int32
}

func (ack *MessageACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.seq)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.seq)
	return true
}

type MessagePeerACK struct {
	sender   int64
	receiver int64
	msgid    int32
}

func (ack *MessagePeerACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.sender)
	binary.Write(buffer, binary.BigEndian, ack.receiver)
	binary.Write(buffer, binary.BigEndian, ack.msgid)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessagePeerACK) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.sender)
	binary.Read(buffer, binary.BigEndian, &ack.receiver)
	binary.Read(buffer, binary.BigEndian, &ack.msgid)
	return true
}

type MessageInputing struct {
	sender   int64
	receiver int64
}

func (inputing *MessageInputing) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, inputing.sender)
	binary.Write(buffer, binary.BigEndian, inputing.receiver)
	buf := buffer.Bytes()
	return buf
}

func (inputing *MessageInputing) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &inputing.sender)
	binary.Read(buffer, binary.BigEndian, &inputing.receiver)
	return true
}

type MessageUnreadCount struct {
	count int32
}

func (u *MessageUnreadCount) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, u.count)
	buf := buffer.Bytes()
	return buf
}

func (u *MessageUnreadCount) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &u.count)
	return true
}

type SystemMessage struct {
	notification string
}

func (sys *SystemMessage) ToData() []byte {
	return []byte(sys.notification)
}

func (sys *SystemMessage) FromData(buff []byte) bool {
	sys.notification = string(buff)
	return true
}

type CustomerServiceMessage struct {
	customer_id int64 //普通用户id
	sender      int64
	receiver    int64
	timestamp   int32
	content     string
}

func (cs *CustomerServiceMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, cs.customer_id)
	binary.Write(buffer, binary.BigEndian, cs.sender)
	binary.Write(buffer, binary.BigEndian, cs.receiver)
	binary.Write(buffer, binary.BigEndian, cs.timestamp)
	buffer.Write([]byte(cs.content))
	buf := buffer.Bytes()
	return buf
}

func (cs *CustomerServiceMessage) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)

	if len(buff) >= 28 {
		//兼容旧数据
		binary.Read(buffer, binary.BigEndian, &cs.customer_id)
	}

	binary.Read(buffer, binary.BigEndian, &cs.sender)
	binary.Read(buffer, binary.BigEndian, &cs.receiver)
	binary.Read(buffer, binary.BigEndian, &cs.timestamp)

	if len(buff) >= 28 {
		cs.content = string(buff[28:])
	} else {
		cs.content = string(buff[20:])
	}
	return true
}

type GroupNotification struct {
	notification string
}

func (notification *GroupNotification) ToData() []byte {
	return []byte(notification.notification)
}

func (notification *GroupNotification) FromData(buff []byte) bool {
	notification.notification = string(buff)
	return true
}

type Room int64

func (room *Room) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int64(*room))
	buf := buffer.Bytes()
	return buf
}

func (room *Room) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, (*int64)(room))
	return true
}

func (room *Room) RoomID() int64 {
	return int64(*room)
}

type RoomMessage struct {
	*RTMessage
}

type MessageOnlineState struct {
	sender int64
	online int32
}

func (state *MessageOnlineState) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, state.sender)
	binary.Write(buffer, binary.BigEndian, state.online)
	buf := buffer.Bytes()
	return buf
}

func (state *MessageOnlineState) FromData(buff []byte) bool {
	if len(buff) < 12 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &state.sender)
	binary.Read(buffer, binary.BigEndian, &state.online)
	return true
}

type MessageSubscribeState struct {
	uids []int64
}

func (sub *MessageSubscribeState) ToData() []byte {
	return nil
}

func (sub *MessageSubscribeState) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	var count int32
	binary.Read(buffer, binary.BigEndian, &count)
	sub.uids = make([]int64, count)
	for i := 0; i < int(count); i++ {
		binary.Read(buffer, binary.BigEndian, &sub.uids[i])
	}
	return true
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

type AppUserID struct {
	appid int64
	uid   int64
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

type AppRoomID struct {
	appid   int64
	room_id int64
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

type AppGroupMemberID struct {
	appid int64
	gid   int64
	uid   int64
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

//邀请好友
type ContactInvite struct {
	sender int64
	receiver int64
	reason string
}

func (contactInvite *ContactInvite) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactInvite.sender)
	binary.Write(buffer, binary.BigEndian, contactInvite.receiver)
	buffer.Write([]byte(contactInvite.reason))
	buf := buffer.Bytes()
	return buf
}

func (contactInvite *ContactInvite) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactInvite.sender)
	binary.Read(buffer, binary.BigEndian, &contactInvite.receiver)
	contactInvite.reason = string(buff[16:])
	
	return true
}

type ContactInviteResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactInviteResp *ContactInviteResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactInviteResp.status)
	binary.Write(buffer, binary.BigEndian, contactInviteResp.sender)
	binary.Write(buffer, binary.BigEndian, contactInviteResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactInviteResp *ContactInviteResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactInviteResp.status)
	binary.Read(buffer, binary.BigEndian, &contactInviteResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactInviteResp.receiver)
	
	return true
}

//接受好友请求
type ContactAccept struct {
	sender int64
	receiver int64
}

func (contactAccept *ContactAccept) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactAccept.sender)
	binary.Write(buffer, binary.BigEndian, contactAccept.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactAccept *ContactAccept) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactAccept.sender)
	binary.Read(buffer, binary.BigEndian, &contactAccept.receiver)
	
	return true
}

type ContactAcceptResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactAcceptResp *ContactAcceptResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactAcceptResp.status)
	binary.Write(buffer, binary.BigEndian, contactAcceptResp.sender)
	binary.Write(buffer, binary.BigEndian, contactAcceptResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactAcceptResp *ContactAcceptResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactAcceptResp.status)
	binary.Read(buffer, binary.BigEndian, &contactAcceptResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactAcceptResp.receiver)
	
	return true
}

//拒绝好友请求
type ContactRefuse struct {
	sender int64
	receiver int64
}

func (contactRefuse *ContactRefuse) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactRefuse.sender)
	binary.Write(buffer, binary.BigEndian, contactRefuse.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactRefuse *ContactRefuse) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactRefuse.sender)
	binary.Read(buffer, binary.BigEndian, &contactRefuse.receiver)
	
	return true
}

type ContactRefuseResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactRefuseResp *ContactRefuseResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactRefuseResp.status)
	binary.Write(buffer, binary.BigEndian, contactRefuseResp.sender)
	binary.Write(buffer, binary.BigEndian, contactRefuseResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactRefuseResp *ContactRefuseResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactRefuseResp.status)
	binary.Read(buffer, binary.BigEndian, &contactRefuseResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactRefuseResp.receiver)
	
	return true
}

//删除好友
type ContactDel struct {
	sender int64
	receiver int64
}

func (contactDel *ContactDel) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactDel.sender)
	binary.Write(buffer, binary.BigEndian, contactDel.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactDel *ContactDel) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactDel.sender)
	binary.Read(buffer, binary.BigEndian, &contactDel.receiver)
	
	return true
}

type ContactDelResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactDelResp *ContactDelResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactDelResp.status)
	binary.Write(buffer, binary.BigEndian, contactDelResp.sender)
	binary.Write(buffer, binary.BigEndian, contactDelResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactDelResp *ContactDelResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactDelResp.status)
	binary.Read(buffer, binary.BigEndian, &contactDelResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactDelResp.receiver)
	
	return true
}

type ContactBlack struct {
	sender int64
	receiver int64
}

func (contactBlack *ContactBlack) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactBlack.sender)
	binary.Write(buffer, binary.BigEndian, contactBlack.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactBlack *ContactBlack) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactBlack.sender)
	binary.Read(buffer, binary.BigEndian, &contactBlack.receiver)
	
	return true
}

type ContactBlackResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactBlackResp *ContactBlackResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactBlackResp.status)
	binary.Write(buffer, binary.BigEndian, contactBlackResp.sender)
	binary.Write(buffer, binary.BigEndian, contactBlackResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactBlackResp *ContactBlackResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactBlackResp.status)
	binary.Read(buffer, binary.BigEndian, &contactBlackResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactBlackResp.receiver)
	
	return true
}

type ContactUnBlack struct {
	sender int64
	receiver int64
}

func (contactUnBlack *ContactUnBlack) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactUnBlack.sender)
	binary.Write(buffer, binary.BigEndian, contactUnBlack.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactUnBlack *ContactUnBlack) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactUnBlack.sender)
	binary.Read(buffer, binary.BigEndian, &contactUnBlack.receiver)
	
	return true
}

type ContactUnBlackResp struct {
	status int32
	sender int64
	receiver int64
}

func (contactUnBlackResp *ContactUnBlackResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, contactUnBlackResp.status)
	binary.Write(buffer, binary.BigEndian, contactUnBlackResp.sender)
	binary.Write(buffer, binary.BigEndian, contactUnBlackResp.receiver)
	buf := buffer.Bytes()
	return buf
}

func (contactUnBlackResp *ContactUnBlackResp) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &contactUnBlackResp.status)
	binary.Read(buffer, binary.BigEndian, &contactUnBlackResp.sender)
	binary.Read(buffer, binary.BigEndian, &contactUnBlackResp.receiver)
	
	return true
}

type GroupCreate struct {
	is_private int32
	is_allow_invite int32
	members []int64
	title string
	desc string
}

func (groupCreate *GroupCreate) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupCreate.is_private)
	binary.Write(buffer, binary.BigEndian, groupCreate.is_allow_invite)
	
	var num int32
	num = int32(len(groupCreate.members))
	binary.Write(buffer, binary.BigEndian, num)
	for _, member := range groupCreate.members {
		binary.Write(buffer, binary.BigEndian, member)
	}
	
	var t_len int32
	t_len = int32(len(groupCreate.title))
	binary.Write(buffer, binary.BigEndian, t_len)
	var d_len int32
	d_len = int32(len(groupCreate.desc))
	binary.Write(buffer, binary.BigEndian, d_len)
	buffer.Write([]byte(groupCreate.title))
	buffer.Write([]byte(groupCreate.desc))
	
	buf := buffer.Bytes()
	return buf
}

func (groupCreate *GroupCreate) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupCreate.is_private)
	binary.Read(buffer, binary.BigEndian, &groupCreate.is_allow_invite)
	
	var num int32
	binary.Read(buffer, binary.BigEndian, &num)
	groupCreate.members = make([]int64, 0, 4)
	var i int32
	for i=0; i<num; i++ {
		var member int64
		binary.Read(buffer, binary.BigEndian, &member)
		groupCreate.members = append(groupCreate.members, member)
	}
	
	var t_len int32
	binary.Read(buffer, binary.BigEndian, &t_len)
	var d_len int32
	binary.Read(buffer, binary.BigEndian, &d_len)
	groupCreate.title = string(buff[28:(28+t_len)])
	groupCreate.desc = string(buff[(28+t_len):])
	
	return true
}

type GroupCreateResp struct {
	status int32
	gid int64
}

func (groupCreateResp *GroupCreateResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupCreateResp.status)
	binary.Write(buffer, binary.BigEndian, groupCreateResp.gid)
	
	buf := buffer.Bytes()
	return buf
}

func (groupCreateResp *GroupCreateResp) FromData(buff []byte) bool {
	if len(buff) < 12 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupCreateResp.status)
	binary.Read(buffer, binary.BigEndian, &groupCreateResp.gid)
	
	return true
}

type GroupSelfJoin struct {
	gid int64
}

func (groupSelfJoin *GroupSelfJoin) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupSelfJoin.gid)
	
	buf := buffer.Bytes()
	return buf
}

func (groupSelfJoin *GroupSelfJoin) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupSelfJoin.gid)
	
	return true
}

type GroupInviteJoin struct {
	gid int64
	members []int64
}

func (groupInviteJoin *GroupInviteJoin) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupInviteJoin.gid)
	
	var num int32
	num = int32(len(groupInviteJoin.members))
	binary.Write(buffer, binary.BigEndian, num)
	for _, member := range groupInviteJoin.members {
		binary.Write(buffer, binary.BigEndian, member)
	}
	
	buf := buffer.Bytes()
	
	return buf
}

func (groupInviteJoin *GroupInviteJoin) FromData(buff []byte) bool {
	if len(buff) < 12 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupInviteJoin.gid)
	
	var num int32
	binary.Read(buffer, binary.BigEndian, &num)
	groupInviteJoin.members = make([]int64, 0, 4)
	var i int32
	for i=0; i<num; i++ {
		var member int64
		binary.Read(buffer, binary.BigEndian, &member)
		groupInviteJoin.members = append(groupInviteJoin.members, member)
	}
	
	return true
}

type SimpleResp struct {
	status int32
}

func (simpleResp *SimpleResp) ToData() []byte {
	buffer := new(bytes.Buffer)
	
	binary.Write(buffer, binary.BigEndian, simpleResp.status)
	
	buf := buffer.Bytes()
	
	return buf
}

func (simpleResp *SimpleResp) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &simpleResp.status)
	
	return true
}

type GroupQuit struct {
	gid int64
}

func (groupQuit *GroupQuit) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupQuit.gid)
	
	buf := buffer.Bytes()
	return buf
}

func (groupQuit *GroupQuit) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupQuit.gid)
	
	return true
}

type GroupDel struct {
	gid int64
}

func (groupDel *GroupDel) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupDel.gid)
	
	buf := buffer.Bytes()
	return buf
}

func (groupDel *GroupDel) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupDel.gid)
	
	return true
}

type GroupRemove struct {
	gid int64
	uid int64
}

func (groupRemove *GroupRemove) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, groupRemove.gid)
	binary.Write(buffer, binary.BigEndian, groupRemove.uid)
	
	buf := buffer.Bytes()
	return buf
}

func (groupRemove *GroupRemove) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &groupRemove.gid)
	binary.Read(buffer, binary.BigEndian, &groupRemove.uid)
	
	return true
}

func WriteHeader(len int32, seq int32, cmd int32, version byte, buffer io.Writer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	binary.Write(buffer, binary.BigEndian, cmd)
	t := []byte{byte(version), 0, 0, 0}
	buffer.Write(t)
}

func ReadHeader(buff []byte) (int, int, int, int) {
	var length int32
	var seq int32
	var cmd int32
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	binary.Read(buffer, binary.BigEndian, &cmd)
	version, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd), int(version)
}

func WriteMessage(w *bytes.Buffer, msg *Message) {
	body := msg.ToData()
	WriteHeader(int32(len(body)), int32(msg.seq), int32(msg.cmd), byte(msg.version), w)
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
	buff := make([]byte, 16)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version := ReadHeader(buff)
	log.Infof("ReceiveMessage: length=%d, seq=%d, cmd=%d, version=%d", length, seq, cmd, version)
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
