package com.zhuinden.sparkexperiment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.zhuinden.sparkexperiment.model.Team;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(SpringRunner.class)
@SpringBootTest
@PropertySource("classpath:application.properties")
public class SparkExperimentApplicationTests {

    @Value("${spring.datasource.url}")
    private String urlOracle;
    @Value("${spring.datasource.username}")
    private String usernameOracle;
    @Value("${spring.datasource.password}")
    private String passwordOracle;

    @Autowired
    private Gson gson;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaSparkContext jsc;

    @Test
    public void contextLoads() {
        //String dbQuery = "SELECT * FROM ecommerce_prod.team";
        Dataset<Row> mysql =
                sparkSession.read().format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/ecommerce_prod")
                        .option("user", "root")
                        .option("password", "my-secret-pw")
                        .option("dbtable", "ecommerce_prod.team")
                        .load();
        List<String> teams = mysql.select("id", "name").map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        teams.forEach(System.out::println);
        // C2
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("user", "root");
//        connectionProperties.put("password", "my-secret-pw");
//        Dataset<Row> jdbcDF2 = sparkSession.read()
//                .jdbc("jdbc:mysql://localhost:3306/ecommerce_prod", "ecommerce_prod.team", connectionProperties);
//        System.out.println(jdbcDF2.col("name"));
    }

    @Test
    public void getAll() {
        Dataset<Row> mysql =
                sparkSession.read().format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/ecommerce_prod")
                        .option("user", "root")
                        .option("password", "my-secret-pw")
                        .option("dbtable", "ecommerce_prod.team")
                        .load();
        List<Row> teams = mysql.select("id", "name").collectAsList();
        Gson gson = new GsonBuilder().create();
        Team new_team;
        for (Row old_team : teams) {
            int id = (int) old_team.get(0);
            String name = (String) old_team.get(1);
            new_team = new Team(id, name);
            System.out.println(gson.toJson(new_team));
        }
    }

    @Test
    public void insertTeam() throws Exception {
        // INSERT INTO ecommerce_prod.team (id, name) VALUES(0, '');
        String dbQuery1 = "(INSERT INTO ecommerce_prod.team (id, name) VALUES(1, 'sang')) tmp";
        String dbQuery = "(SELECT table_name FROM information_schema.tables WHERE table_schema = '" + "ecommerce_prod.team" + "') tmp";
        Dataset<Row> mysql =
                sparkSession.read().format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/ecommerce_prod")
                        .option("user", "root")
                        .option("password", "my-secret-pw")
                        .option("dbtable", dbQuery1)
                        .load();
    }

    @Test
    public void updateTeam() {

    }

    @Test
    public void cloneTable() {
        //String dbQuery = "SELECT * FROM ecommerce_prod.team";
        Dataset<Row> mysql =
                sparkSession.read().format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/ecommerce_prod")
                        .option("user", "root")
                        .option("password", "my-secret-pw")
                        .option("dbtable", "ecommerce_prod.team")
                        .load();
        mysql.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/ecommerce_prod")
                .option("dbtable", "ecommerce_prod.company")
                .option("user", "root")
                .option("password", "my-secret-pw")
                .save();
    }

    @Test
    public void readFileExcel() {

    }

    @Test
    public void readFileCsv() {
    }

}
