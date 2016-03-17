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

import "fmt"

import ots2 "github.com/GiterLab/goots"

type Storage struct {
	*ots2.OTSClient
	*PeerStorage
	*GroupStorage
}

func NewStorage(ots_endpoint string, ots_accessid string, ots_accesskey string, ots_instancename string) *Storage {
	ots2_client, err := ots2.New(ots_endpoint, ots_accessid, ots_accesskey, ots_instancename)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	ps := NewPeerStorage(ots2_client)
	gs := NewGroupStorage(ots2_client)
	return &Storage{ots2_client, ps, gs}
}
