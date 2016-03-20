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

import log "github.com/golang/glog"
import "unsafe"
import "sync"

type RoomClient struct {
	*Connection
	room_ids map[int64]struct{}
	room_mutex  sync.Mutex
}

func (client *RoomClient) Logout(route *Route) {
	client.room_mutex.Lock()
	defer client.room_mutex.Unlock()
	
	for room_id, _ := range client.room_ids {
		OpRemoveRoomMember(room_id, client.uid)
		delete(client.room_ids, room_id)
	}
}

func (client *RoomClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_ENTER_ROOM:
		client.HandleEnterRoom(msg.body.(*Room))
	case MSG_LEAVE_ROOM:
		client.HandleLeaveRoom(msg.body.(*Room))
	case MSG_ROOM_IM:
		client.HandleRoomIM(msg.body.(*RoomMessage), msg.seq)
	case MSG_TRANSMIT_ROOM:
		client.HandleTransmitRoom(msg.body.(*RoomMessage), msg.seq)
	}
}

func (client *RoomClient) HandleEnterRoom(room *Room){
	client.room_mutex.Lock()
	defer client.room_mutex.Unlock()
	
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	room_id := room.RoomID()
	log.Info("enter room id:", room_id)
	if room_id == 0{
		return
	}
	
	if _, ok := client.room_ids[room_id]; ok {
		return
	}

	client.room_ids[room_id] = struct{}{}
	
	OpAddRoomMember(room_id, client.uid)
}

func (client *RoomClient) Client() *Client {
	p := unsafe.Pointer(client.Connection)
	return (*Client)(p)
}

func (client *RoomClient) HandleLeaveRoom(room *Room) {
	client.room_mutex.Lock()
	defer client.room_mutex.Unlock()
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	room_id := room.RoomID()
	log.Info("leave room id:", room_id)
	if room_id == 0 {
		return
	}
	
	if _, ok := client.room_ids[room_id]; !ok {
		return
	}
	
	OpRemoveRoomMember(room_id, client.uid)
	
	delete(client.room_ids, room_id)
}

func (client *RoomClient) HandleRoomIM(room_im *RoomMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
	room_id := room_im.receiver
	if _, ok := client.room_ids[room_id]; !ok {
		log.Warningf("room id:%d is't client's room\n", room_id)
		return
	}

	m := &Message{cmd:MSG_ROOM_IM, body:room_im}

	amsg := &AppMessage{appid:client.appid, receiver:room_id, msg:m}
	channel := GetRouteChannel()
	channel.PublishRoom(amsg)

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
}

func (client *RoomClient) HandleTransmitRoom(room_im *RoomMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
	room_id := room_im.receiver
	if _, ok := client.room_ids[room_id]; !ok {
		log.Warningf("room id:%d is't client's room\n", room_id)
		return
	}

	m := &Message{cmd:MSG_TRANSMIT_ROOM, body:room_im}

	amsg := &AppMessage{appid:client.appid, receiver:room_id, msg:m}
	channel := GetRouteChannel()
	channel.PublishRoom(amsg)

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
}