package net.pixelcop.sewer;

@SuppressWarnings("rawtypes")
public class SinkFactory extends SourceSinkFactory {

    public SinkFactory(Class clazz, Args args) {
        super(clazz, args);
    }
    
    public Sink buildSink() {
        return (Sink) build();
    }
    
}
