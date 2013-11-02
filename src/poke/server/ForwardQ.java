package poke.server;

import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardQ {
  protected static Logger logger = LoggerFactory.getLogger("ForwardingQ");

  // protected static LinkedBlockingDeque<ManagementQueueEntry> inbound = new
  // LinkedBlockingDeque<ManagementQueueEntry>();
  protected static LinkedBlockingDeque<ForwardedMessage> forwardingQ = new LinkedBlockingDeque<ForwardedMessage>();

  // TODO static is problematic
  private static ForwardQWorker fwdQworker;
  // private static InboundMgmtWorker iworker;

  // not the best method to ensure uniqueness

  public static void startup() {
    if (fwdQworker != null)
      return;

    // System.out.println("TGROUP INSIDE MANAGEMENT QUEUE IS ------------------------->"
    // + tgroup);
    // fwdQworker = new ForwardQWorker();
    ForwardQWorker frwdWorker = ForwardQWorker.getInstance();
    frwdWorker.start();
    // System.out.println("TGROUP INSIDE MANAGEMENT QUEUE IS ------------------------->"
    // + tgroup);

    // oworker = new OutboundMgmtWorker(tgroup, 1);
    // oworker.start();
  }

  public static void shutdown(boolean hard) {
    // TODO shutdon workers
  }

  public static void enqueueRequest(ForwardedMessage msg) {
    try {
      System.out.println("Putting Request in forwardingQ");
      ForwardQ.forwardingQ.put(msg);
    } catch (InterruptedException e) {
      logger.error("message not enqueued for processing", e);
    }
  }

  public static void enqueueResponse(ForwardedMessage msg) {
    try {
      // ManagementQueueEntry entry = new ManagementQueueEntry(reply, ch, null);
      System.out.println("Putting Response in forwardingQ");
      ForwardQ.forwardingQ.put(msg);
    } catch (InterruptedException e) {
      logger.error("message not enqueued for reply", e);
    }
  }

}
