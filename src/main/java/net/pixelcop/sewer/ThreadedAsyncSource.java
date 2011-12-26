package net.pixelcop.sewer;

public abstract class ThreadedAsyncSource implements Source {
    
    private SinkFactory sinkFatory;

    public void setSinkFatory(SinkFactory sinkFatory) {
        this.sinkFatory = sinkFatory;
    }

    public SinkFactory getSinkFatory() {
        return sinkFatory;
    }
    
    public Sink createSink() {
        return sinkFatory.buildSink();
    }

}
