/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.resources;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.ClientConnection;
import poke.server.Server;
import poke.server.resources.Resource;
import poke.server.storage.ServerManagementUtil;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.Header.Routing;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;

public class DocumentResource implements Resource {

    protected static Logger logger = LoggerFactory.getLogger("server");
    public static final String sDriver = "jdbc.driver";
    public static final String sUrl = "jdbc.url";
    public static final String sUser = "jdbc.user";
    public static final String sPass = "jdbc.password";

    private int count=0;
    public DocumentResource() {
    }

    private Response createResponse(Request request) {
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.SUCCESS)
		.setReplyMsg(
			"File Uploaded")
		.setOriginator(request.getHeader().getToNode())
		.setToNode(
			request.getHeader().getOriginator())
		.setRoutingId(request.getHeader().getRoutingId())
		.build();

	PayloadReply pb = PayloadReply.newBuilder()
		.build();

	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    private Response createResponseForAlreadyExists(Request request) {
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.FAILURE)
		.setReplyMsg(
			"File Already Exists")
		.setOriginator(request.getHeader().getToNode())
		.setToNode(
			request.getHeader().getOriginator())
		.setRoutingId(request.getHeader().getRoutingId())
		.build();

	PayloadReply pb = PayloadReply.newBuilder()
		.build();

	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    @Override
    public Response process(Request request) {
    	ClientConnection.count++;
    	logger.info("Count of replication is : "+count);
	Properties p = System.getProperties();
	String key = "user.home";
	String userDir = (String) p.get(key);

	System.out.println("User directory--->>" + userDir);
	String origID = request.getHeader().getOriginator();
	String currentID = Server.getConf().getServer().getProperty("node.id");
	String namespace = request.getBody().getSpace().getName();

	String serverDir = userDir + "/" + Server.getConf().getServer().getProperty("node.id");
	File serverFolder = new File(serverDir);
	if (!serverFolder.exists()) {
	    if (serverFolder.mkdir()) {
		System.out.println("Directory is created!");
	    } else {
		System.out.println("Directory already exist");
	    }
	}
	String namespaceDir = serverDir + "/" + namespace;
	File namespaceFolder = new File(namespaceDir);
	if (!namespaceFolder.exists()) {
	    if (namespaceFolder.mkdir()) {
		System.out.println("Directory is created!");
	    } else {
		System.out.println("Directory already exist");
	    }
	}
	String filePath = namespaceDir + "/" + request.getBody().getDoc().getDocName();
	System.out.println("FlePath in server***" + filePath);
	String fileContent = request.getBody().getDoc().getChunkContent()
		.toStringUtf8();
	System.out.println("FleContent in server***" + fileContent);
	File file = new File(filePath);

	boolean fileExists = false;
	try {
	    if (!file.exists()) {
		System.out
			.println("File is created!#########################################################");
		file.createNewFile();
	    } else {
		fileExists = true;
		System.out
			.println("File already exists##################################################.");
		return createResponseForAlreadyExists(request);

	    }

	} catch (IOException e) {
	    e.printStackTrace();
	}
	PrintWriter writer;
	try {
	    writer = new PrintWriter(file, "UTF-8");
	    writer.println(fileContent);

	    writer.close();
	    String serverPort = ServerManagementUtil.getServerPort();
	    System.out
		    .println("The port number is **************** inside DOCUMENT RESOURCE  "
			    + serverPort);
	    if (fileExists) {
		ServerManagementUtil.getDatabaseStorage().updateDocument(request,
			filePath);
	    } else {
		ServerManagementUtil.getDatabaseStorage().addDocument(request,
			filePath, serverPort);
	    }
	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (UnsupportedEncodingException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	logger.info("document ********************* : "
		+ request.getBody().getDoc().getId());

	if (request.getHeader().getRoutingId().equals(Routing.DOCADD)) {
	    return createResponse(request);
	}

	return null;
    }
}
