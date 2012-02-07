/**
 *
 */
package net.pixelcop.sewer;

import java.io.IOException;

public class FooSource extends Source implements PlumbingProvider {

    public FooSource(String[] args) {
    }

    @Override
    public Class<?> getEventClass() {
        return null;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void register() {
        SourceRegistry.register("foo", getClass());
    }

    @Override
    public void close() throws IOException {
    }

}