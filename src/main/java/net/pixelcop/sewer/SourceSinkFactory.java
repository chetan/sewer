package net.pixelcop.sewer;


@SuppressWarnings({"rawtypes", "unchecked"})
public class SourceSinkFactory<T> {

    private Class clazz;
    private String[] args;

    public SourceSinkFactory(Class clazz, String arg) {
        this.clazz = clazz;
        this.args = new String[]{ arg };
    }

    public SourceSinkFactory(Class clazz, String[] args) {
        this.clazz = clazz;
        this.args = args;
    }

    public T build() {
        try {
            return (T) clazz.getConstructor(String.class).newInstance((Object[]) args);

        } catch (Exception e) {
        }

        return null; // TODO throw exception?
    }

}
