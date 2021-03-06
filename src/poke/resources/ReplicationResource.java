/*
 * copyright 2013, gash
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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ForwardQ;
import poke.server.ForwardedMessage;
import poke.server.Server;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.resources.Resource;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;

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
public class ReplicationResource implements Resource {
    protected static Logger logger = LoggerFactory.getLogger("server");
    String nextNode;
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

    private Request createRequest(Request request, String NodeId) {
	
    	Request newRequest = null;
    	/*
	 * RoutingPath rp = RoutingPath.newBuilder()
	 * .setNode(Server.getConf().getServer().getProperty("node.id"))
	 * .setTime(System.currentTim    	if(request.getHeader().getOriginator().equals(iam))
eMillis()).build();
	 * 
	 * System.out.println("Forward Resource Adding Route Path");
	 * System.out.println(rp);
	 */
    	String toNode = request.getHeader().getToNode();
    	String replicationOriginator;
    	if (NodeId.equals("zero"))
    		replicationOriginator=toNode;
    	else
    		replicationOriginator=NodeId;
    		
	Header newHeader = Header
		.newBuilder(request.getHeader())
		.setTime(System.currentTimeMillis())
		.setToNode(NodeId).setOriginator(replicationOriginator)
		.build();

	 newRequest = Request.newBuilder(request).setHeader(newHeader)
		.build();
    			
	/*
	 * for (RoutingPath rp1 : newHeader.getPathList()) {
	 * System.out.println("added pathlist"); System.out.println(rp1); }
	 */

	
    			
    	return newRequest;
    }

    private Response createResponse(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	
    	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.FAILURE)
		.setReplyMsg(
			"File uploaded but not able to replicate to adjacent nodes")
		.setOriginator(request.getHeader().getToNode())
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.setRoutingId(request.getHeader().getRoutingId())
		.build();

	PayloadReply pb = PayloadReply.newBuilder()
		.build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }
    
    

    @Override
    public Response process(Request request) {
	setCfg();

	System.out.println("In ReplicationResource");
	GeneratedMessage m = null;

	DocumentResource dr = new DocumentResource();
	Response res = dr.process(request);

	// String nextNode = determineForwardNode(request);
	ArrayList<NodeDesc> neighboursList = new ArrayList(cfg.getNearest().getNearestNodes()
		.values());

	/*
	 * for(int i=0;i<neighboursList.size();i++) { nextNode =
	 * neighboursList.get(i).getNodeId();
	 * System.out.println("nextNode in replication resource=" + nextNode );
	 * GeneratedMessage msg = null;
	 * 
	 * if (nextNode != null ) { msg = createRequest(request,nextNode); }
	 * else { msg = createResponse(request); } ForwardedMessage fwdMessage =
	 * new ForwardedMessage(nextNode, msg);
	 * ForwardQ.enqueueRequest(fwdMessage); }
	 * 
	 * return res;
	 */

	if (!(res.getHeader().getReplyCode().equals(ReplyStatus.FAILURE))) {
	    System.out.println("((((((((((((Respone in Replication  Resource)))))))))))))"
		    + res.getHeader().getReplyCode());
	    
	    logger.info("originator and to node id : " + request.getHeader().getOriginator()+" " +request.getHeader().getToNode());
	    
	    if(request.getHeader().getOriginator().equals("zero"))
		{
	    	
	    for (int i = 0; i < neighboursList.size(); i++)
	    {
		nextNode = neighboursList.get(i).getNodeId();
		System.out.println("nextNode in replication resource=" + nextNode);
		GeneratedMessage msg = null;

		if (nextNode != null) {
		    msg = createRequest(request, nextNode);
		} else {
		   // msg = createResponse(request);
		}
		String iam = Server.getConf().getServer().getProperty("node.id");

    	
		ForwardedMessage fwdMessage = new ForwardedMessage(nextNode, msg);
		ForwardQ.enqueueRequest(fwdMessage);
    			}
	    }
	}

	//return res;

	/**
	 * Find the nearest node that has not received the request.
	 * 
	 * TODO this should use the heartbeat to determine which node is active
	 * in its list.
	 * 
	 * @param request
	 * @return
	 */

	 return null;
    }
}
