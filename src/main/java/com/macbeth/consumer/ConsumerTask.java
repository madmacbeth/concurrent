package com.macbeth.consumer;

import java.util.concurrent.Callable;

public interface ConsumerTask<T> extends Callable<T> {
}
