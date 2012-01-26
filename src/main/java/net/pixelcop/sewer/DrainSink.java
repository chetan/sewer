package net.pixelcop.sewer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.pixelcop.sewer.sink.durable.TransactionManager;

/**
 * Identifies a Sink as being a final endpoint as opposed to a wrapper or
 * decorator which adds some other functionality.
 *
 * <p>This is primarily used by the {@link TransactionManager} to process failed
 * Transactions.</p>
 *
 * @author chetan
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DrainSink {

}
