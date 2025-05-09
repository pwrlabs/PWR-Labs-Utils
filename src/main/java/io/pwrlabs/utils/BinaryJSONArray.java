package io.pwrlabs.utils;

import io.pwrlabs.concurrency.ConcurrentList;
import io.pwrlabs.util.encoders.Hex;
import org.json.JSONArray;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BinaryJSONArray {
    private final ConcurrentList<Object> values = new ConcurrentList<>();

    public BinaryJSONArray() {
    }

    public BinaryJSONArray(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        while(buffer.hasRemaining()) {
            byte type = buffer.get();
            switch (type) {
                case 1:
                    values.add(buffer.getInt());
                    break;
                case 2:
                    values.add(buffer.getLong());
                    break;
                case 3:
                    values.add(buffer.getShort());
                    break;
                case 4:
                    values.add(buffer.get() == 1);
                    break;
                case 5: //String
                    int valueLength = buffer.getInt();
                    byte[] valueBytes = new byte[valueLength];
                    buffer.get(valueBytes);
                    values.add(new String(valueBytes));
                    break;
                case 6: //byte[]
                    int valueLength2 = buffer.getInt();
                    byte[] valueBytes2 = new byte[valueLength2];
                    buffer.get(valueBytes2);
                    values.add(valueBytes2);
                    break;
                case 7: //BinaryJSONArray
                    int valueLength3 = buffer.getInt();
                    byte[] valueBytes3 = new byte[valueLength3];
                    buffer.get(valueBytes3);
                    values.add(new BinaryJSONArray(valueBytes3));
                    break;
                case 8: //BinaryJSONObject
                    int valueLength4 = buffer.getInt();
                    byte[] valueBytes4 = new byte[valueLength4];
                    buffer.get(valueBytes4);
                    values.add(new BinaryJSONObject(valueBytes4));
                    break;
                case 9: //Byte
                    values.add(buffer.get());
                    break;
                case 10: //BigInteger
                    int valueLength5 = buffer.getInt();
                    byte[] valueBytes5 = new byte[valueLength5];
                    buffer.get(valueBytes5);
                    values.add(new BigInteger(valueBytes5));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported value type");
            }
        }
    }

    public void add(Object value) {
        values.add(value);
    }

    public ConcurrentList<Object> getValues() {
        return values;
    }

    public int size() {
        return values.size();
    }

    public Object get(int index) {
        return values.get(index);
    }

    public byte[] getBytes(int index) {
        Object value = values.get(index);
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a byte array");
        }
    }

    public byte getByte(int index) {
        Object value = values.get(index);
        if (value instanceof Byte) {
            return (byte) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a byte");
        }
    }

    public int getInt(int index) {
        Object value = values.get(index);
        if (value instanceof Integer) {
            return (int) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not an integer");
        }
    }

    public long getLong(int index) {
        Object value = values.get(index);
        if (value instanceof Long) {
            return (long) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a long");
        }
    }

    public short getShort(int index) {
        Object value = values.get(index);
        if (value instanceof Short) {
            return (short) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a short");
        }
    }

    public boolean getBoolean(int index) {
        Object value = values.get(index);
        if (value instanceof Boolean) {
            return (boolean) value;
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a boolean");
        }
    }

    public String getString(int index) {
        Object value = values.get(index);
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof byte[]) {
            return new String((byte[]) value);
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a string");
        }
    }

    public BinaryJSONObject getBinaryJSONObject(int index) {
        Object value = values.get(index);
        if (value instanceof BinaryJSONObject) {
            return ((BinaryJSONObject) value);
        } else {
            throw new IllegalArgumentException("Value at index " + index + " is not a BinaryJSONObject");
        }
    }

    public byte[] toByteArray() throws IOException {
        if(values.size() == 0) {
            return new byte[0];
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for(Object value: values.getArrayListCopy()) {
            if (value instanceof Integer) {
                bos.write(1); // Type
                bos.write(ByteBuffer.allocate(4).putInt((int) value).array());
            } else if (value instanceof Long) {
                bos.write(2); // Type
                bos.write(ByteBuffer.allocate(8).putLong((long) value).array());
            } else if (value instanceof Short) {
                bos.write(3); // Type
                bos.write(ByteBuffer.allocate(2).putShort((short) value).array());
            } else if (value instanceof Boolean) {
                bos.write(4); // Type
                bos.write((byte) ((boolean) value ? 1 : 0));
            } else if (value instanceof String) {
                bos.write(5); // Type
                byte[] valueBytes = ((String) value).getBytes();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof byte[]) {
                bos.write(6); // Type
                byte[] valueBytes = (byte[]) value;
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof BinaryJSONArray) {
                bos.write(7); // Type
                byte[] valueBytes = ((BinaryJSONArray) value).toByteArray();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof BinaryJSONObject) {
                bos.write(8); // Type
                byte[] valueBytes = ((BinaryJSONObject) value).toByteArray();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof Byte) {
                bos.write(9); // Type
                bos.write((byte) value);
            } else if (value instanceof BigInteger) {
                bos.write(10); // Type
                byte[] valueBytes = ((BigInteger) value).toByteArray();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else {
                String typeName = value.getClass().getSimpleName();
                throw new IllegalArgumentException("Unsupported value type: " + typeName);

            }
        }

        return bos.toByteArray();
    }

    public JSONArray toJsonArray() {
        JSONArray jsonArray = new JSONArray();
        for(Object value: values.getArrayListCopy()) {
            if(value instanceof byte[]) {
                jsonArray.put(Hex.toHexString((byte[]) value));
            } else if (value instanceof BinaryJSONObject) {
                jsonArray.put(((BinaryJSONObject) value).toJsonObject());
            } else if (value instanceof BinaryJSONArray) {
                jsonArray.put(((BinaryJSONArray) value).toJsonArray());
            } else {
                jsonArray.put(value);
            }
        }

        return jsonArray;
    }

    @Override
    public String toString() {
        return toJsonArray().toString();
    }
}
