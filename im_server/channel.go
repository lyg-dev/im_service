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

import "net"
import "time"
import "sync"
import log "github.com/golang/glog"

type Subscriber struct {
	uids map[int64]int
	room_ids map[int64]int
}

func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uids = make(map[int64]int)
	s.room_ids = make(map[int64]int)
	return s
}

type Channel struct {
	addr            string
	wt              chan *Message

	mutex           sync.Mutex
	subscribers     map[int64]*Subscriber

	dispatch        func(*AppMessage)

}

func NewChannel(addr string, f func(*AppMessage)) *Channel {
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

func (channel *Channel) Register() {
	msg := &ServerID{
		serverid : config.server_id,
	}
	
	m := &Message{}
	m.cmd = MSG_SERVER_REGISTER
	m.body = msg
	
	channel.wt <- m
}

func (channel *Channel) Publish(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) PublishMessage(msg *Message) {
	channel.wt <- msg
}

func (channel *Channel) PublishRoom(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH_ROOM, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closed_ch := make(chan bool)
	seq := 0

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closed_ch)
				return
			}
			log.Info("channel recv message:", Command(msg.cmd))
			if msg.cmd == MSG_PUBLISH {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch != nil {
					channel.dispatch(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closed_ch:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			log.Info("channel send message:", Command(msg.cmd))
			log.Infoln(msg.body)
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		if err != nil {
			log.Info("connect route server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
