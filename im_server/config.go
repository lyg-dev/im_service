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

import "strconv"
import "log"
import "strings"
import "github.com/richmonkey/cfg"

type Config struct {
	server_id		string
	port                int
	mysqldb_datasource  string
	mysqldb_appdatasource  string

	redis_address       string
	redis_password		string
	http_listen_address string
	socket_io_address   string

	storage_addrs       []string
	route_addrs         []string
}

func get_int(app_cfg map[string]string, key string) int {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.Atoi(concurrency)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}

func get_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	return concurrency
}

func read_cfg(cfg_path string) *Config {
	config := new(Config)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Println(err)
		return nil
	}

	config.server_id = get_string(app_cfg, "server_id")
	config.port = get_int(app_cfg, "port")
	config.http_listen_address = get_string(app_cfg, "http_listen_address")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.redis_password = get_string(app_cfg, "redis_password")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.mysqldb_appdatasource = get_string(app_cfg, "mysqldb_appsource")
	config.socket_io_address = get_string(app_cfg, "socket_io_address")

	str := get_string(app_cfg, "storage_pool")
    array := strings.Split(str, " ")
	config.storage_addrs = array
	if len(config.storage_addrs) == 0 {
		log.Println("storage pool config")
		return nil
	}

	str = get_string(app_cfg, "route_pool")
    array = strings.Split(str, " ")
	config.route_addrs = array
	if len(config.route_addrs) == 0 {
		log.Println("route pool config")
		return nil
	}

	return config
}
