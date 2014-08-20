/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cobar.frontend.server;

import java.nio.channels.SocketChannel;

import com.alibaba.cobar.frontend.server.handler.ServerPrepareHandler;
import com.alibaba.cobar.model.Server;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.factory.FrontendConnectionFactory;
import com.alibaba.cobar.startup.CobarServer;

/**
 * @author xianmao.hexm
 */
public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        Server sc = CobarServer.getInstance().getCobar().getServer();
        ServerConnection c = new ServerConnection(channel);
        c.setTxIsolation(sc.getTxIsolation());
        c.setHandler(new ServerAuthenticator(c));
        c.setPrivileges(new ServerPrivileges());
        c.setQueryHandler(new ServerQueryHandler(c));
        c.setPrepareHandler(new ServerPrepareHandler(c));
        c.setSession(new ServerSession(c));
        return c;
    }

}
