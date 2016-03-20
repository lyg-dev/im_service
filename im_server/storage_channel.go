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

type StorageChannel struct {
	addr            string
	mutex           sync.Mutex
	dispatch  func(*Message)
	wt              chan *Message
}

func NewStorageChannel(addr string, f func(*Message)) *StorageChannel {
	channel := new(StorageChannel)
	channel.dispatch = f
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

func (sc *StorageChannel) Register() {
	msg := &ServerID{
		serverid : config.server_id,
	}
	
	m := &Message{}
	m.cmd = MSG_SERVER_REGISTER_STORAGE
	m.body = msg
	
	sc.wt <- m
}

func (sc *StorageChannel) RunOnce(conn *net.TCPConn) {
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
			log.Info("stroage channel recv message:", Command(msg.cmd))
			if sc.dispatch != nil {
				sc.dispatch(msg)
			}
		}
	}()

	for {
		select {
		case _ = <-closed_ch:
			log.Info("storage channel closed")
			return
		case msg := <-sc.wt:
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (sc *StorageChannel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", sc.addr)
		if err != nil {
			log.Info("connect server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("storage channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("storage channel connected")
		nsleep = 100
		sc.RunOnce(tconn)
	}
}


func (sc *StorageChannel) Start() {
	go sc.Run()
}