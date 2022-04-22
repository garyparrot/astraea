package org.astraea.metrics.kafka;

import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;

public interface HasValue extends HasBeanObject {
  default long value() {
    return (long) beanObject().getAttributes().getOrDefault("Value", 0);
  }

  static HasValue of(BeanObject beanObject) {
    return () -> beanObject;
  }
}
