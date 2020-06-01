package src.test.java.org.apache.activemq.artemis.utils;

import org.junit.Assert;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

import org.apache.activemq.artemis.utils.InetAddresses;

public class InetAddressesTest {
   @Test
   public void testForUriString() throws Throwable {
	   // check IPv4 address
	   Inet4Address addr4 = (Inet4Address)InetAddresses.forUriString("127.0.0.1");
	   Assert.assertEquals(addr4.getHostAddress(), "127.0.0.1");

	   // check IPv6 address
	   Inet6Address addr6 = (Inet6Address)InetAddresses
			   .forUriString("[0:1111:2222:3333:4444:5555:6666:7777]");
	   Assert.assertEquals(addr6.getHostAddress(),
			   "0:1111:2222:3333:4444:5555:6666:7777");
   }

   @Test
   public void testToAddrString() throws Throwable {
	   // check IPv4 address
	   Inet4Address addr4 = (Inet4Address)InetAddress.getByName("127.0.0.1");
	   Assert.assertEquals(InetAddresses.toAddrString(addr4), "127.0.0.1");
	   // check IPv6 address
	   Inet6Address addr6 = (Inet6Address)InetAddress.getByName("0:1111:2222:3333:4444:5555:6666:7777");
	   Assert.assertEquals(InetAddresses.toAddrString(addr6), "0:1111:2222:3333:4444:5555:6666:7777");
   }
}
