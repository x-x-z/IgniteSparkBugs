package org.xxz.ignite_spark_bugs;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IgniteSparkBugsApplicationTests {

    private static final String CONFIG = "ignite.xml";
    private Ignite ignite;

    public IgniteSparkBugsApplicationTests() {
        this.ignite = Ignition.start(CONFIG);
    }

    /**
     * org.apache.spark.sql.Dataset#union(org.apache.spark.sql.Dataset) failed
     * with org.apache.ignite.spark.IgniteDataFrameSettings#OPTION_DISABLE_SPARK_SQL_OPTIMIZATION() = false (default)
     */
    @Test
    public void unionDatasetsTest() {
        CacheConfiguration<Long, Person> cacheCfg1 = new CacheConfiguration<>();
        cacheCfg1.setName("PERSON");
        cacheCfg1.setIndexedTypes(Long.class, Person.class);
        ignite.getOrCreateCache(cacheCfg1);

        CacheConfiguration<Long, Person2> cacheCfg2 = new CacheConfiguration<>();
        cacheCfg2.setName("PERSON2");
        cacheCfg2.setIndexedTypes(Long.class, Person2.class);
        ignite.getOrCreateCache(cacheCfg2);

        Dataset<Row> person1 = readTable("PERSON");
        Dataset<Row> person2 = readTable("PERSON2");
        Dataset<Row> union = person1.union(person2);
        union.show();
    }

    /**
     * org.apache.ignite.spark.impl.optimization.SimpleExpressions$#toString does not check String for null
     */
    @Test
    public void showTableWithNullValuesTest() {
        CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("PERSON");
        cacheCfg.setIndexedTypes(Long.class, Person.class);
        IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheCfg);

        Person person = new Person(1L, null); // name is null

        cache.put(1L, person);
        Dataset<Row> personTable = readTable("PERSON");
        personTable= personTable.withColumn("nullColumn", functions.lit(null).cast(DataTypes.StringType));
        personTable.show();
    }

    /*
    * org.apache.ignite.spark.impl.IgniteSQLRelation$.schema does not use the org.apache.ignite.cache.QueryEntity.aliases
    * that the user can specify using the annotation org.apache.ignite.cache.query.annotations.QuerySqlField
    */
    @Test
    public void readTableWithQuerySqlFieldTest() {
        CacheConfiguration<Long, PersonWithAlias> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("PERSON_WITH_ALIAS");
        cacheCfg.setIndexedTypes(Long.class, PersonWithAlias.class);
        IgniteCache<Long, PersonWithAlias> cache = ignite.getOrCreateCache(cacheCfg);

        SqlFieldsQuery fieldsQuery = new SqlFieldsQuery("SELECT PERSON_NAME FROM PERSON_WITH_ALIAS"); // PERSON_NAME field exists
        List<List<?>> all = cache.query(fieldsQuery).getAll();
        Assert.assertTrue(all.isEmpty());

        Dataset<Row> person = readTable("PERSON_WITH_ALIAS");
        List<Row> rows = person.select("PERSON_NAME").collectAsList(); // PERSON_NAME not found, because it`s 'name' in schema
        Assert.assertTrue(rows.isEmpty());
    }

    private Dataset<Row> readTable(String tableName) {
        return SparkSingleton.INSTANCE.get().read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_TABLE(), tableName)
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .load();
    }

}
