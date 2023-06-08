package com.risingwave.functions;

import java.util.Iterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;

abstract class UserDefinedFunctionBatch {
    protected Schema inputSchema;
    protected Schema outputSchema;
    protected BufferAllocator allocator;

    public Schema getInputSchema() {
        return inputSchema;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    abstract Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch);
}

class ScalarFunctionBatch extends UserDefinedFunctionBatch {
    ScalarFunction function;
    Method method;

    ScalarFunctionBatch(ScalarFunction function, BufferAllocator allocator) {
        this.function = function;
        this.allocator = allocator;
        this.method = Reflection.getEvalMethod(function);
        this.inputSchema = TypeUtils.methodToInputSchema(this.method);
        this.outputSchema = TypeUtils.methodToOutputSchema(this.method);
    }

    @Override
    Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch) {
        var row = new Object[batch.getSchema().getFields().size()];
        var outputValues = new Object[batch.getRowCount()];
        for (int i = 0; i < batch.getRowCount(); i++) {
            for (int j = 0; j < row.length; j++) {
                row[j] = batch.getVector(j).getObject(i);
                if (row[j] instanceof Text) {
                    row[j] = row[j].toString();
                }
            }
            try {
                outputValues[i] = this.method.invoke(this.function, row);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        var outputVector = TypeUtils.createVector(this.outputSchema.getFields().get(0), this.allocator, outputValues);
        var outputBatch = VectorSchemaRoot.of(outputVector);
        return Collections.singleton(outputBatch).iterator();
    }

}

class TableFunctionBatch extends UserDefinedFunctionBatch {
    TableFunction<?> function;
    Method method;
    int chunk_size = 1024;

    TableFunctionBatch(TableFunction<?> function, BufferAllocator allocator) {
        this.function = function;
        this.allocator = allocator;
        this.method = Reflection.getEvalMethod(function);
        this.inputSchema = TypeUtils.methodToInputSchema(this.method);
        this.outputSchema = TypeUtils.tableFunctionToOutputSchema(function.getClass());
    }

    @Override
    Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch) {
        // TODO: incremental compute and output
        // due to the lack of generator support in Java, we can not yield from a
        // function call.
        var outputs = new ArrayList<VectorSchemaRoot>();
        var row = new Object[batch.getSchema().getFields().size()];
        var indexes = new ArrayList<Integer>();
        Runnable buildChunk = () -> {
            var indexVector = TypeUtils.createVector(this.outputSchema.getFields().get(0), this.allocator,
                    indexes.toArray());
            indexes.clear();
            var valueVector = TypeUtils.createVector(this.outputSchema.getFields().get(1), this.allocator,
                    this.function.take());
            var outputBatch = VectorSchemaRoot.of(indexVector, valueVector);
            outputs.add(outputBatch);
        };
        for (int i = 0; i < batch.getRowCount(); i++) {
            // prepare input row
            for (int j = 0; j < row.length; j++) {
                row[j] = batch.getVector(j).getObject(i);
                if (row[j] instanceof Text) {
                    row[j] = row[j].toString();
                }
            }
            // call function
            var size_before = this.function.size();
            try {
                this.method.invoke(this.function, row);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            var size_after = this.function.size();
            // add indexes
            for (int j = size_before; j < size_after; j++) {
                indexes.add(i);
            }
            // check if we need to flush
            if (size_after >= this.chunk_size) {
                buildChunk.run();
            }
        }
        if (this.function.size() > 0) {
            buildChunk.run();
        }
        return outputs.iterator();
    }
}

class Reflection {
    static Method getEvalMethod(Object obj) {
        var methods = new ArrayList<Method>();
        for (Method method : obj.getClass().getDeclaredMethods()) {
            if (method.getName().equals("eval")) {
                methods.add(method);
            }
        }
        if (methods.size() != 1) {
            throw new IllegalArgumentException(
                    "Exactly one eval method must be defined for class " + obj.getClass().getName());
        }
        return methods.get(0);
    }
}
