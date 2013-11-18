package poke.resources;

/*
 * copyright 2013, gash
 *t poke. 
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

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ForwardQ;
import poke.server.ForwardedMessage;
import poke.server.Server;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.resources.Resource;
import poke.server.storage.ServerManagementUtil;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.RoutingPath;

/**
 * The forward resource is used by the ResourceFactory to send requests to a
 * destination that is not this server.
 * 
 * Strategies used by the Forward can include TTL (max hops), durable tracking,
 * endpoint hiding.
 * 
 * @author gash
 * 
 */
public class RemoveResource implements Resource {
    protected static Logger logger = LoggerFactory.getLogger("server");

    // protected static Logger logger = LoggerFactory.getLogger("server");
    public static final String sDriver = "jdbc.driver";
    public static final String sUrl = "jdbc.url";
    public static final String sUser = "jdbc.user";
    public static final String sPass = "jdbc.password";
    // private ServerConf cfg;
    public String adjacentNode = null;

    private ServerConf cfg;

    public ServerConf getCfg() {
	return cfg;
    }

    /**
     * Set the server configuration information used to initialized the server.
     * 
     * @param cfg
     */
    public void setCfg() {
	// this.cfg = cfg;
	this.cfg = Server.getConf();
    }

    private Request createRequest(Request request) {
	RoutingPath rp = RoutingPath.newBuilder()
		.setNode(Server.getConf().getServer().getProperty("node.id"))
		.setTime(System.currentTimeMillis()).build();

	System.out.println("Forward Resource Adding Route Path");
	System.out.println(rp);

	Header newHeader = Header
		.newBuilder(request.getHeader())
		.setTime(System.currentTimeMillis())
		.addPath(rp)
		.build();

	Request newRequest = Request.newBuilder(request).setHeader(newHeader)
		.build();

	for (RoutingPath rp1 : newHeader.getPathList()) {
	    System.out.println("added pathlist");
	    System.out.println(rp1);
	}

	return newRequest;
    }

    Response response;

    private Response createResponse(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.SUCCESS)
		.setReplyMsg("File Deleted")
		.setToNode(request.getHeader().getPath(0).getNode())
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.build();

	PayloadReply pb = PayloadReply.newBuilder()
		.build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    private Response createResponseFailure(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.SUCCESS)
		.setReplyMsg("File  Deleted")
		.setOriginator(request.getHeader().getToNode())
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.build();

	PayloadReply pb = PayloadReply.newBuilder()
		.build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    @Override
    public Response process(Request request) {

	setCfg();
	Properties p = System.getProperties();
	// Document retrievedFile = null;
	String fileToBeRetrieved = request.getBody().getDoc().getDocName();
	System.out.println("The name of the file to be removed is ==============> "
		+ fileToBeRetrieved);

	String filePath = (ServerManagementUtil.getDatabaseStorage())
		.removeDocumentfromDB(fileToBeRetrieved);
	// Response response = null;
	// eye.Comm.PayloadReply.Builder retrievePayload = null;

	// String filePath = ds.removeDocumentfromDB(fileToBeRetrieved);
	if (filePath != null) {

	    System.out.println("file to remove is" + filePath);
	    File file = new File(filePath);

	    if (file.delete()) {
		System.out.println(file.getName() + " is deleted!");
	    } else {
		System.out.println("Delete operation is failed.");
	    }
	    Request newReq = createRequest(request);
	    String next = determineForwardNode(request);
	    if (next != null) {
		System.out.println("RemoveResource next node/=" + next);
		ForwardedMessage msg = new ForwardedMessage(next, newReq);
		ForwardQ.enqueueRequest(msg);
	    } else {
		Response newRes = createResponse(request);
		return newRes;
	    }

	} else {
	    Request newReq = createRequest(request);
	    String next = determineForwardNode(request);
	    if (next != null) {
		System.out.println("RemoveResource next node/=" + next);
		ForwardedMessage msg = new ForwardedMessage(next, newReq);
		ForwardQ.enqueueRequest(msg);
	    } else {
		Response newRes = createResponse(request);
		return newRes;
	    }

	}

	return null;
    }

    /**
     * Find the nearest node that has not received the request.
     * 
     * TODO this should use the heartbeat to determine which node is active in
     * its list.
     * 
     * @param request
     * @return
     */
    private String determineForwardNode(Request request) {
	System.out.println("IN determineForwardNode start");
	List<RoutingPath> paths = request.getHeader().getPathList();
	Collection<NodeDesc> neighboursList = cfg.getNearest().getNearestNodes()
		.values();

	System.out.println("IN determineForwardNode1");
	if (paths == null || paths.size() == 0) {
	    System.out.println("paths==null, picking first nearest node");
	    System.out.println("NearestNode:"
		    + cfg.getNearest().getNearestNodes().values());
	    // pick first nearest
	   /* NodeDesc nd = cfg.getNearest().getNearestNodes().values()
		    .iterator().next();
	    if (nd == null)
		System.out.println("nodedesc is null");

	    System.out
		    .println("RemoveResource: if path==null" + nd.getNodeId());
	    return nd.getNodeId();*/
	    for (NodeDesc nd : cfg.getNearest().getNearestNodes().values()) {
		    if(Server.activeNodes.contains(nd.getNodeId())&&nd!=null)	
		    {
		    	System.out
			    .println("FowardResource: if path==null" + nd.getNodeId());
		    return nd.getNodeId();
		    }
		    else
		    	continue;
		    }
	} else {
	    System.out.println("RemoveResource:determine nextnode if path!=null");
	    // if this server has already seen this message return null

	    for (NodeDesc nd : neighboursList) {
		boolean found = true;
		for (RoutingPath rp : paths) {
		    if (rp.getNode().equalsIgnoreCase(nd.getNodeId())) {
			found = false;
			break;
		    }
		}
		if (found) {
		    //return nd.getNodeId();
			if(Server.activeNodes.contains(nd.getNodeId()))
		    	return nd.getNodeId();
		    else
		    	continue;
		}
	    }
	}
	return null;
    }

}
