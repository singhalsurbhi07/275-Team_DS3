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
package poke.server.routing;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

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
import eye.Comm.Header.Routing;
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
public class ForwardResource implements Resource {
    protected static Logger logger = LoggerFactory.getLogger("server");

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
    
    private String determineDocRetrieveNode(Request request){
    	List<RoutingPath> rp=request.getHeader().getPathList();
    	System.out.println("Inside determine doc retrieve node in FORWARd RESOURCE ==============");
    	String current=Server.getConf().getServer().getProperty("node.id");
    	//boolean found=false;
    	int currentPosition = rp.indexOf(current);
    	RoutingPath nextPath = rp.get(currentPosition + 1);
    	String next = nextPath.getNode();
    	System.out.println("The value of next node in determine doc retrieve node is ===============" + next);
    	
    	return next;
    }

    private Request createRequest(Request request) {
	RoutingPath rp = RoutingPath.newBuilder().setNode(Server.getConf().getServer().getProperty("node.id"))
					.setTime(System.currentTimeMillis()).build();

	System.out.println("Forward Resource Adding Route Path");
	System.out.println(rp);

	Header newHeader = Header.newBuilder(request.getHeader()).setTime(System.currentTimeMillis())
						.setRemainingHopCount(request.getHeader().getRemainingHopCount()).addPath(rp)
						.build();

	Request newRequest = Request.newBuilder(request).setHeader(newHeader).build();

	for (RoutingPath rp1 : newHeader.getPathList()) {
	    System.out.println("added pathlist");
	    System.out.println(rp1);
	}

	return newRequest;
    }

    private Response createResponse(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.FAILURE)
		.setReplyMsg(
			"Not enough hop counts or not able to determine next node")
		.setToNode(request.getHeader().getOriginator())
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
	String nextNode;

	System.out.println("In ForwardResource");
	System.out.println("THE REQUEST IN FORWARD RESOURCE BEFORE DOCFIND IS  ======================" + request);
	if(request.getHeader().getRoutingId().equals(Routing.DOCFIND)){
		
		
		
		System.out.println("IN FORWARD RESOURCE INSIDE WHEN DOCFIND IS TRUE-----------------------");
		
		nextNode = determineDocRetrieveNode(request);
	}else{
		nextNode = determineForwardNode(request);
	}

	System.out.println("nextNode=" + nextNode);
	System.out.println("hopcount"
		+ request.getHeader().getRemainingHopCount());
	GeneratedMessage msg = null;
	ForwardedMessage fwdMessage = null;
	if (nextNode != null) {
	    msg = createRequest(request);
	    fwdMessage = new ForwardedMessage(nextNode, msg);
	} else {
	    msg = createResponse(request);
	    int rpCount = request.getHeader().getPathCount();

	    String next = request.getHeader().getPath(rpCount - 1).getNode();
	    fwdMessage = new ForwardedMessage(next, msg);

	}
	// ForwardedMessage fwdMessage = new ForwardedMessage(nextNode, msg);
	ForwardQ.enqueueRequest(fwdMessage);
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

	TreeMap<String, NodeDesc> neighboursList1 = (TreeMap<String, NodeDesc>) cfg.getNearest()
		.getNearestNodes();
	Collection<NodeDesc> neighboursList = cfg.getNearest().getNearestNodes()
		.values();

	for (NodeDesc eachNode : neighboursList) {
	    if (eachNode.getNodeId().equalsIgnoreCase(request.getHeader().getToNode())) {
		return request.getHeader().getToNode();
	    }
	}
	System.out.println("IN determineForwardNode1");
	if (paths == null || paths.size() == 0) {
	    System.out.println("pahs==null");

	    int neighboursCount = neighboursList.size();
	    System.out.println("ForwardResource neighbours c ount:" + neighboursCount);
	    Random randomno = new Random();
	    int randomNeighbour = randomno.nextInt(neighboursCount);
	    // Set<String,NodeDesc> nn = neighboursList1.entrySet();

	    System.out.println("ForwardResource random no" + randomNeighbour);
	    NodeDesc nn = null;
	    if (randomNeighbour == 0) {
		nn = cfg.getNearest().getNearestNodes().values()
			.iterator().next();
	    } else {
		// pick first nearest
		// NodeDesc nn = cfg.getNearest().getNearestNodes().values()
		// .iterator().next();

		// NodeDesc nd = neighboursList.get(randomNeighbour);
		// System.out.println("treemap" + nd.getNodeId());
		// NodeDesc it = neighboursList.iterator().next();
		int i = 0;

		for (NodeDesc nd : neighboursList) {
		    if (i != 0) {
			nn = nd;
			break;
		    } else {
			i++;
			System.out.println("i=" + i);
		    }
		}
	    }

	    if (nn == null) {
		nn = cfg.getNearest().getNearestNodes().values()
			.iterator().next();
		System.out.println("nodedesc is null");
	    }

	    System.out
		    .println("FowardResource: if path==null" + nn.getNodeId());
	    return nn.getNodeId();
	} else {
	    System.out.println("FowardResource: if path!=null");
	    String nextNode;
	    // if this server has already seen this message return null
	    for (NodeDesc eachNeighbour : neighboursList) {
		int i = 0;
		boolean found = true;
		while (i < paths.size()) {
		    if (paths.get(i).getNode().equals(eachNeighbour.getNodeId())) {
			found = false;
			break;
		    }

		    i++;

		}
		if (found) {
		    nextNode = eachNeighbour.getNodeId();
		}

	    }
	}

	return null;
    }
}
