package net.pixelcop.sewer;

public class PlumbingBuilder<T> {

  private Class clazz;
  private String[] args;

  public PlumbingBuilder(Class clazz, String[] args) {
    this.clazz = clazz;
    this.args = args;
  }

  public Object build() throws Exception {
    return (T) clazz.getConstructor(String[].class).newInstance((Object) args);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(clazz.getSimpleName());
    sb.append("(");
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(args[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  public Class getClazz() {
    return clazz;
  }

  public String[] getArgs() {
    return args;
  }
}