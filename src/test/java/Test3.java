import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;




public class Test3 {
    static class ValueData {
        private final String tableName;
        private final String key;
        private final byte[] value;

        public ValueData(String tableName, String key, byte[] value) {
            this.tableName = tableName;
            this.key = key;
            this.value = value;
        }

        public String getTableName() {
            return tableName;
        }

        public String getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }


    @Test
    public void perform_ReturnValidData() throws DatabaseException {

        Path dbRoot = Paths.get("/Users/ekaterinacerednicenko/Test");
        ExecutionEnvironment environment = new ExecutionEnvironmentImpl(new DatabaseConfig(dbRoot.toString()));
        InitializationContext initializationContext = new InitializationContextImpl(
                environment,
                null,
                null,
                null
        );
        Database database1 = DatabaseImpl.create("db1", dbRoot);
        Database database2 = DatabaseImpl.create("db2", dbRoot);
        Database database3 = DatabaseImpl.create("db3", dbRoot);

        database1.createTableIfNotExists("table1");
        database2.createTableIfNotExists("table2");
        database3.createTableIfNotExists("table3");

        List<ValueData> data1 = createTableData("table1", 100);
        List<ValueData> data2 = createTableData("table2", 150);
        List<ValueData> data3 = createTableData("table3", 200);

        for (int i = 0; i < 100; i++) {
            if (i == 99) {
                System.out.println("Last i");
            }
            database1.write(data1.get(i).getTableName(), data1.get(i).getKey(), data1.get(i).getValue());
        }

        for (int i = 0; i < 150; i++)
            database2.write(data2.get(i).getTableName(), data2.get(i).getKey(), data2.get(i).getValue());
        for (int i = 0; i < 200; i++)
            database3.write(data3.get(i).getTableName(), data3.get(i).getKey(), data3.get(i).getValue());

        DatabaseServerInitializer databaseServerInitializer = new DatabaseServerInitializer(
                new DatabaseInitializer(
                        new TableInitializer(
                                new SegmentInitializer()
                        )
                )
        );

        databaseServerInitializer.perform(initializationContext);

        String message = "Database not found in environment";
        assertTrue(message, environment.getDatabase("db1").isPresent());
        assertTrue(message, environment.getDatabase("db2").isPresent());
        assertTrue(message, environment.getDatabase("db3").isPresent());

        checkDatabase(environment.getDatabase("db1").get(), data1);
        checkDatabase(environment.getDatabase("db2").get(), data2);
        checkDatabase(environment.getDatabase("db3").get(), data3);
    }

    public void checkDatabase(Database database, List<ValueData> dataList) throws DatabaseException {
        String messageIsPresent = "Written data not found";
        String messageIsEqual = "Written data was found but not equal";

        for (var data : dataList) {
            assertTrue(messageIsPresent, database.read(data.getTableName(), data.getKey()).isPresent());
            assertArrayEquals(messageIsEqual, data.getValue(), database.read(data.getTableName(), data.getKey()).get());
        }
    }

    private List<ValueData> createTableData(String tableName, int n) {
        Random random = new Random();
        byte[] bytes = new byte[1000];
        return IntStream.range(0, n)
                .mapToObj(number -> {
                    random.nextBytes(bytes);
                    return new ValueData(
                            tableName,
                            "key" + number,
                            bytes
                    );
                })
                .collect(Collectors.toList());
    }
}
