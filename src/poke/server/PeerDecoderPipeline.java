package poke.server;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import poke.server.routing.ServerHandler;

//import poke.client.ClientHandler;
//import poke.client.ClientListener;

public class PeerDecoderPipeline implements ChannelPipelineFactory {

    private ServerHandler handler = null;

    public PeerDecoderPipeline(ServerHandler handler) {
	this.handler = handler;
    }

    public ChannelPipeline getPipeline() throws Exception {
	ChannelPipeline pipeline = Channels.pipeline();
	// use DebugFrameDecoder to look at the raw message
	// pipeline.addLast("frameDecoder", new DebugFrameDecoder(67108864, 0,
	// 4,0, 4));

	pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864,
		0, 4, 0, 4));
	pipeline.addLast("protobufDecoder",
		new ProtobufDecoder(eye.Comm.Response.getDefaultInstance()));

	pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
	pipeline.addLast("protobufEncoder", new ProtobufEncoder());

	/*
	 * if we had only java clients then this is what we can use
	 * pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
	 * pipeline.addLast("protobufDecoder", new
	 * ProtobufDecoder(eye.Comm.Response.getDefaultInstance()));
	 * pipeline.addLast("frameEncoder", new
	 * ProtobufVarint32LengthFieldPrepender());
	 * pipeline.addLast("protobufEncoder", new ProtobufEncoder());
	 */

	// our message processor
	pipeline.addLast("handler", this.handler);

	return pipeline;
    }

}
