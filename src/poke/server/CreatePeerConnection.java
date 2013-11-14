package poke.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.routing.ServerHandler;

//import poke.client.ClientConnection.OutboundWorker;

public class CreatePeerConnection {
    protected static Logger logger = LoggerFactory
	    .getLogger("CreatePeerConnection");

    private String host;
    private int port;
    private int mgmtPort;
    private ServerHandler handler;
    private ChannelFuture channel = null; // do not use directly call connect()!
    private ChannelFuture responseChannel = null; // do not use directly call
						  // connect()!
    private ClientBootstrap bootstrap;
    PeerDecoderPipeline clientPipeline;

    // private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>
    // forwardingQ;
    // private ForwardWorker worker;

    public CreatePeerConnection(String host, int port, int mgmtPort,
	    ServerHandler handler) {
	this.host = host;
	this.port = port;
	this.mgmtPort = mgmtPort;
	this.handler = handler;
	init();
    }

    public void release() {
	bootstrap.releaseExternalResources();
    }

    /**
     * add an application-level listener to receive messages from the server (as
     * in replies to requests).
     * 
     * @param listener
     */

    private void init() {
	// the queue to support client-side surging
	// forwardingQ = new
	// LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

	// Configure the client.
	bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
		Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

	bootstrap.setOption("connectTimeoutMillis", 10000);
	bootstrap.setOption("tcpNoDelay", true);
	bootstrap.setOption("keepAlive", true);

	// Set up the pipeline factory.
	clientPipeline = new PeerDecoderPipeline(this.handler);
	bootstrap.setPipelineFactory(clientPipeline);

	// start outbound message processor
	// worker = new OutboundWorker(this);
	// worker.start();
    }

    /**
     * create connection to remote server
     * 
     * @return
     */
    protected Channel connect() {
	// Start the connection attempt.
	if (channel == null) {
	    // System.out.println("---> connecting");
	    channel = bootstrap.connect(new InetSocketAddress(host, port));
	    // cleanup on lost connection
	}

	// wait for the connection to establish
	channel.awaitUninterruptibly();

	if (channel.isDone() && channel.isSuccess())
	    return channel.getChannel();
	else
	    throw new RuntimeException("Not able to establish connection to server");
    }

    /**
     * create connection to remote server
     * 
     * @return
     */
    protected Channel responseConnect() {
	// Start the connection attempt.
	if (responseChannel == null) {
	    // System.out.println("---> connecting");
	    responseChannel = bootstrap.connect(new InetSocketAddress(host, mgmtPort));
	    // cleanup on lost connection
	}

	// wait for the connection to establish
	responseChannel.awaitUninterruptibly();

	if (responseChannel.isDone() && responseChannel.isSuccess())
	    return responseChannel.getChannel();
	else
	    throw new RuntimeException("Not able to establish connection to server");
    }

    /**
     * queues outgoing messages - this provides surge protection if the client
     * creates large numbers of messages.
     * 
     * @author gash
     * 
     */
}
