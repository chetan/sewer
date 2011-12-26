package net.pixelcop.sewer;


@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class SourceSinkFactory {
    
    public class Args {
        
        private String input;
        
        public Args(String input) {
            this.input = input;
        }

        public String getInput() {
            return input;
        }
    }
    
    private Class clazz;
    private Args args;
    
    public SourceSinkFactory(Class clazz, Args args) {
        this.clazz = clazz;
        this.args = args;
    }
    
    public Object build() {
        try {
            return clazz.getConstructor(String.class).newInstance(args.getInput());
            
        } catch (Exception e) {
        }
        
        return null; // TODO throw exception?
    }

}
