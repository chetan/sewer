package net.pixelcop.sewer.util;

import net.pixelcop.sewer.node.AbstractHadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

public class TestHdfsUtil extends AbstractHadoopTest {

  @Test
  public void testSelectCodec() {

    Configuration conf = new Configuration();
    conf.set(HdfsUtil.CONFIG_COMPRESSION, "default");
    assertEquals(DefaultCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "deflate");
    assertEquals(DefaultCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "gzip");
    assertEquals(GzipCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "gz");
    assertEquals(GzipCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "GzipCodec");
    assertEquals(GzipCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "org.apache.hadoop.io.compress.GzipCodec");
    assertEquals(GzipCodec.class, HdfsUtil.selectCodec(conf).getClass());

    conf.set(HdfsUtil.CONFIG_COMPRESSION, "bzip2");
    assertEquals(BZip2Codec.class, HdfsUtil.selectCodec(conf).getClass());

  }

}
