package net.pixelcop.sewer;

@SuppressWarnings("rawtypes")
public class SourceFactory extends SourceSinkFactory {
    
    public SourceFactory(Class clazz, Args args) {
        super(clazz, args);
    }
    
    public Source buildSource() {
        return (Source) build();
    }
    
}
