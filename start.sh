#!/bin/bash

#杀死脚本
ps aux|grep im_server|awk '{print $2}'|xargs kill -9

ps aux|grep storage_server|awk '{print $2}'|xargs kill -9

ps aux|grep route_server|awk '{print $2}'|xargs kill -9

cd "${GOPATH}/src/im_service/im_server"

go install

cd "${GOPATH}/src/im_service/storage_server"

go install

cd "${GOPATH}/src/im_service/route_server"

go install

cd "${GOPATH}/bin"

chmod +x im_server
chmod +x storage_server
chmod +x route_server

nohup ./storage_server ims.cfg &
nohup ./route_server	imr.cfg &
nohup ./im_server	im.cfg &