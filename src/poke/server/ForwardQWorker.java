package poke.server;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelHandler;

import poke.server.management.HeartbeatData;
import poke.server.routing.ServerHandler;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Request;
import eye.Comm.Response;

public class ForwardQWorker extends Thread {

    public static HashMap<String, CreatePeerConnection> allPeerConnections = new HashMap<String, CreatePeerConnection>();
    protected static AtomicReference<ForwardQWorker> instance = new AtomicReference<ForwardQWorker>();

    // SimpleChannelHandler handler = new ServerHandler();
    // SimpleChannelHandler handler=new PeerHandler();

    /***
     * Create Connection here.
     * 
     * @param node
     */
    public void addNeighbouringNode(HeartbeatData node) {
	// PeerHandler handler = new PeerHandler();
	// handler.addListener(new PeerListener(node.getNodeId()));
	SimpleChannelHandler handler = new ServerHandler();
	// handler = new PeerHandler();

	CreatePeerConnection pc = new CreatePeerConnection(node.getHost(),
		node.getPort(), node.getMgmtport(), handler);
	allPeerConnections.put(node.getNodeId(), pc);
    }

    // ClientConnection conn;
    boolean forever = true;

    public ForwardQWorker() {
	if (ForwardQ.forwardingQ == null)
	    throw new RuntimeException("connection worker detected null queue");
    }

    public static ForwardQWorker getInstance() {
	instance.compareAndSet(null, new ForwardQWorker());
	return instance.get();
    }

    @Override
    public void run() {
	System.out.println("ForwardQWorker Working!!!!!!!!");

	while (forever) {
	    if (!forever && ForwardQ.forwardingQ.size() == 0)
		break;

	    try {
		System.out.println("Size: " + ForwardQ.forwardingQ.size());
		// block until a message is enqueued
		ForwardedMessage msg = ForwardQ.forwardingQ.take();
		System.out.println();
		GeneratedMessage req = msg.getMsg();
		String dest = msg.getToNode();

		System.out.println("ForwardQWorker:destination node:" + dest);
		CreatePeerConnection pc = allPeerConnections.get(dest);
		if (pc == null) {
		    System.out.println("ForwardQWorker:pc is null");
		} else {
		    System.out.println("ForwardQWorker:pc is not null");
		}
		Channel ch = null;

		if (req instanceof Request) {
		    ch = pc.connect();
		} else if (req instanceof Response) {
		    ch = pc.responseConnect();
		}

		if (ch != null && ch.isWritable()) {
		    System.out.println("Channel is writable");
		    ch.write(req);
		} else {
		    ForwardQ.forwardingQ.putFirst(msg);
		}
	    } catch (InterruptedException ie) {
		break;
	    } catch (Exception e) {
		e.printStackTrace();
		System.out.println("ForwardQWorker: error in writing in the cjannel");
		break;
	    }
	}

	if (!forever) {
	    System.out.println("ForwardQWorker: error ! forever loop");
	    // ClientConnection.logger.info("connection queue closing");
	}
    }
}
