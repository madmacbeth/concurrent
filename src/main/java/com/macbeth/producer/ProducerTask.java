package com.macbeth.producer;

import java.util.concurrent.Callable;

public interface ProducerTask<T> extends Callable<T> {
}
