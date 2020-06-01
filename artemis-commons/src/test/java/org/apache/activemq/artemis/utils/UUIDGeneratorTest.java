package org.apache.activemq.artemis.utils;

import java.util.Random;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class UUIDGeneratorTest
{
   @Test
   public void testGenerateDummyAddress() {
      byte[] seed = new byte[]{0};
      Random rnd = new Random(0);
      UUIDGenerator generator = UUIDGenerator.getInstance();
      
      byte[] result = generator.generateDummyAddress(rnd);
      // ensure broadcast bit is set
      assertEquals(result[0] & (byte) 0x01, (byte)0x01);
      // check seed value
      byte[] compare = new byte[] {(byte)97,(byte)-76,(byte)32,
    		                       (byte)-69,(byte)56,(byte)81};
      assertArrayEquals(result, compare);
   }
}
