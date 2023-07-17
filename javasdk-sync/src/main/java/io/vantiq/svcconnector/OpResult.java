package io.vantiq.svcconnector;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@NoArgsConstructor
@Data
public class OpResult<T> implements Iterator<T> {
    @Setter
    private T oneResult;
    @Setter
    private Throwable error;

    public OpResult(Throwable error) {
        this.error = error;
    }

    public OpResult(T result) {
        if (!(result instanceof List) && !(result instanceof Map) && !(result instanceof Integer)) {
            throw new UnsupportedOperationException("OpResult type must be one of List, Map, or Integer");
        }
        oneResult = result;
    }

    @Override
    public boolean hasNext() {
        return oneResult != null;
    }

    boolean failed() {
        return error != null;
    }

    Throwable cause() {
        return error;
    }

    @Override
    public T next() {
        if (oneResult == null) {
            throw new NoSuchElementException("no next result");
        }
        T result = oneResult;
        oneResult = null;
        return result;
    }
}
