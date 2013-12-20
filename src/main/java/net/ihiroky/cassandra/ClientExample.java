package net.ihiroky.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

/**
 *
 */
public class ClientExample {

    public static void main(String[] args) throws Exception {
        TTransport transport = new TFramedTransport(new TSocket("localhost", 9160));
        TProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        try {
            client.set_keyspace("Demo");
            String columnFamily = "User";
            long timestamp = System.currentTimeMillis();
            String userId = "1";
            updateUser(client, columnFamily, timestamp, userId, "Duke", "18");
            readSingleColumn(client, columnFamily, userId, "name");
            readEntireRow(client, columnFamily, userId);
        } finally {
            transport.close();
        }
    }

    static void updateUser(Cassandra.Client client, String columnFamily,
            long timestamp, String userId, String name, String age) throws Exception {
        Column nameColumn = new Column(toByteBuffer("name"));
        nameColumn.setValue(toByteBuffer(name));
        nameColumn.setTimestamp(timestamp);

        Column ageColumn = new Column(toByteBuffer("age"));
        ageColumn.setValue(toByteBuffer(age));
        ageColumn.setTimestamp(timestamp);

        ColumnParent parent = new ColumnParent(columnFamily);
        client.insert(toByteBuffer(userId), parent, nameColumn, ConsistencyLevel.ONE);
        client.insert(toByteBuffer(userId), parent, ageColumn, ConsistencyLevel.ONE);
    }

    static void readSingleColumn(Cassandra.Client client, String columnFamily, String userId, String columnName)
            throws Exception {
        ColumnPath path = new ColumnPath(columnFamily);
        path.setColumn(toByteBuffer(columnName));
        ColumnOrSuperColumn result = client.get(toByteBuffer(userId), path, ConsistencyLevel.ONE);
        Column column = result.column;

        System.out.println("column name: " + toString(column.name));
        System.out.println("column value: " + toString(column.value));
        System.out.println("column timestamp: " + new Date(column.timestamp));
    }

    static void readEntireRow(Cassandra.Client client, String columnFamily, String userId) throws Exception {
        ColumnParent parent = new ColumnParent(columnFamily);
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
        predicate.setSlice_range(range);

        List<ColumnOrSuperColumn> results =
                client.get_slice(toByteBuffer(userId), parent, predicate, ConsistencyLevel.ONE);
        for (ColumnOrSuperColumn result : results) {
            Column column = result.column;
            System.out.println(toString(column.name) + " -> " + toString(column.value));
        }
    }

    static ByteBuffer toByteBuffer(String s) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(s));
    }

    static String toString(ByteBuffer b) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().decode(b).toString();
    }
}
