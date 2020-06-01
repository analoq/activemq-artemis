package org.apache.activemq.artemis.rest.test;
import static org.jboss.resteasy.test.TestPortProvider.generateURL;

import org.apache.activemq.artemis.rest.queue.QueueDeployment;
import org.apache.activemq.artemis.rest.util.LinkHeaderLinkStrategy;
import org.jboss.logging.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

public class SchedTTLTest extends MessageTestBase {
	private static final Logger log = Logger.getLogger(SchedTTLTest.class);

	@Test
	public void testWithId() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(false);
      deployment.setDurableSend(true);
      deployment.setName("testWithId");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testWithId"));
      ClientResponse<?> res = Util.head(request);
      log.warn("HELLO THERE!!!!");
      log.warn(res.toString());

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create-with-id");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, true);
      Link consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");

      // create message
      create.setHref(create.getHref().replace("{id}", "a-special-id"));
      res = create.request().body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = consumeNext.request().body("text/plain", "").post();
      consumeNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());

      // create dup message
      res = create.request().body("text/plain", "sooperdooper").put();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = consumeNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's NO message on the queue!
      Assert.assertEquals(503, res.getStatus());
	}
	
	@Test
	public void testPriority() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testPriority");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testPriority"));
      ClientResponse<?> res = Util.head(request);

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

      // create message
      res = create.request().queryParameter("priority", "9").body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());
	}
	
	@Test
	public void testIdle() throws Exception {
		// create queue
		QueueDeployment deployment = new QueueDeployment();
		deployment.setConsumerSessionTimeoutSeconds(1);
		deployment.setDuplicatesAllowed(true);
		deployment.setDurableSend(false);
		deployment.setName("testIdle");
		
		manager.getQueueManager().deploy(deployment);
		manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

		// get pull link
		ClientRequest request = new ClientRequest(generateURL("/queues/testIdle"));
		ClientResponse<?> res = Util.head(request);
		Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

		// call pull link
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure queue is empty!
      Assert.assertEquals(503, res.getStatus());
    }

	@Test
	public void testEnqueued() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testEnqueued");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testEnqueued"));
      ClientResponse<?> res = Util.head(request);

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

      // create message
      res = create.request().queryParameter("ttl", "1000").body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());
	}

	@Test
	public void testExpired() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testExpired");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testExpired"));
      ClientResponse<?> res = Util.head(request);

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

      // create message
      res = create.request().queryParameter("ttl", "1000").body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // wait for message to expire
      Thread.sleep(2000);

      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's NO message on the queue!
      Assert.assertEquals(503, res.getStatus());
	}
	
	@Test
	public void testNACK() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testNack");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testNack"));
      ClientResponse<?> res = Util.head(request);

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

      // create message
      res = create.request().queryParameter("ttl", "1000").body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());

      // negative acknowledgement!
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      res = ack.request().formParameter("acknowledge", "false").post();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());

      // call pull
      ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());
	}

	@Test
	public void testACK() throws Exception {
      // create queue
      QueueDeployment deployment = new QueueDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testAck");

      manager.getQueueManager().deploy(deployment);
      manager.getQueueManager().setLinkStrategy(new LinkHeaderLinkStrategy());

      // get create and pull link
      ClientRequest request = new ClientRequest(generateURL("/queues/testAck"));
      ClientResponse<?> res = Util.head(request);

      Link create = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create");
      Link pull = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "pull-consumers");
      res = Util.setAutoAck(pull, false);
      Link ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");

      // create message
      res = create.request().queryParameter("ttl", "1000").body("text/plain", "sooperdooper").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      // call pull link
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's a message on the queue!
      Assert.assertEquals(200, res.getStatus());

      // negative acknowledgement!
      Link ack = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledgement");
      res = ack.request().formParameter("acknowledge", "true").post();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());

      // call pull
      ackNext = getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "acknowledge-next");
      res = ackNext.request().body("text/plain", "").post();
      res.releaseConnection();
      // make sure there's NO message on the queue!
      Assert.assertEquals(503, res.getStatus());
	}
}
