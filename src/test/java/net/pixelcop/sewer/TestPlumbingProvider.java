package net.pixelcop.sewer;

import java.io.IOException;

import net.pixelcop.sewer.node.AbstractNodeTest;
import net.pixelcop.sewer.node.ExitException;
import net.pixelcop.sewer.node.NodeConfig;
import net.pixelcop.sewer.node.TestableNode;

import org.junit.Test;

public class TestPlumbingProvider extends AbstractNodeTest {

    @Test(expected=ExitException.class)
    public void testRegisterFail() throws IOException {

        NodeConfig conf = loadTestConfig("-v");

        conf.set(NodeConfig.SOURCE, "null");
        conf.set(NodeConfig.SINK, "null");
        conf.set(NodeConfig.PLUGINS, "net.pixelcop.sewer.Foo");

        TestableNode node = new TestableNode(conf);
        fail("expected exception");
    }

    @Test
    public void testRegisterPass() throws IOException {

        NodeConfig conf = loadTestConfig("-v");

        conf.set(NodeConfig.SOURCE, "foo");
        conf.set(NodeConfig.SINK, "null");
        conf.set(NodeConfig.PLUGINS, FooSource.class.getCanonicalName());

        TestableNode node = new TestableNode(conf);
        assertNotNull(node);
        assertTrue(SourceRegistry.getRegistry().containsKey("foo"));
        assertNotNull(SourceRegistry.getRegistry().get("foo"));
    }

    @Test
    public void testEmptyPluginsList() throws IOException {

        NodeConfig conf = loadTestConfig("-v");

        conf.set(NodeConfig.SOURCE, "null");
        conf.set(NodeConfig.SINK, "null");

        TestableNode node = new TestableNode(conf);
        assertNotNull(node);
    }

}
