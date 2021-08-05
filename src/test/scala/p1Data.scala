import org.apache.spark.sql.SparkSession

object p1Data {
  def main(args: Array[String]): Unit = {

    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    //Comment out any of these to select which scenerios to run

    createDatabase(spark)
    scenerio1(spark)
    scenerio2(spark)
    scenerio3(spark)
    scenerio4(spark)
    scenerio5(spark)
    scenerio6(spark)
    finding1(spark)
    finding2(spark)
  }
  //Create tables and load data into them
  def createDatabase(spark: SparkSession): Unit = {
    spark.sql("create table bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
    spark.sql("create table bev_count(beverage String,count Int) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/bev_branches.txt' INTO TABLE bev_branches")
    spark.sql("LOAD DATA LOCAL INPATH 'input/bev_Conscount.txt' INTO TABLE bev_count")
  }
  //Count of how many beverages consumed at Branches 1 and 2
  def scenerio1(spark: SparkSession): Unit = {
    spark.sql("Select sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch1'").show()
    spark.sql("Select sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch = 'Branch2'").show()
  }
  //Most Consumed from Branch1 Least Consumed from Branch2 Average From Branch2
  def scenerio2(spark: SparkSession): Unit = {
    spark.sql("Select b.beverage, sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch='Branch1' group by b.beverage order by sum(c.count) desc limit 1").show()
    spark.sql("Select b.beverage, sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch='Branch2' group by b.beverage order by sum(c.count) asc limit 1").show()
    spark.sql("Select avg(cf) from (Select b.beverage, sum(c.count) as cf from bev_branches b join bev_count c on b.beverage=c.beverage where b.branch='Branch2' group by b.beverage) as counts").show()
  }
  //Get distinct beverages offered at locations
  def scenerio3(spark: SparkSession): Unit = {
    spark.sql("Select distinct beverage from bev_branches where branch ='Branch10' or branch='Branch8' or branch='Branch1'").show()
    spark.sql("Select distinct beverage from bev_branches where branch='Branch4' INTERSECT Select distinct beverage from bev_branches where branch='Branch7'").show()
  }
  //Create partition, view for scenerio 3
  def scenerio4(spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS bev_branch")
    spark.sql("CREATE TABLE bev_branch (beverage STRING) PARTITIONED BY ( branch STRING)")
    spark.sql("FROM bev_branches INSERT OVERWRITE TABLE bev_branch PARTITION(branch) SELECT * DISTRIBUTE BY branch")
    //Comment this out if not first time running scenerio
    spark.sql("CREATE VIEW B1081 As Select distinct beverage from bev_branch where branch ='Branch10' or branch='Branch8' or branch='Branch1'")
    spark.sql("SELECT * FROM B1081").show()
  }
  //Add comment to Table properties of bev_branch table
  def scenerio5(spark: SparkSession): Unit = {
    spark.sql("ALTER TABLE bev_branch SET TBLPROPERTIES ('comment' = 'comment for scenario 5' )")
    spark.sql("DESCRIBE EXTENDED bev_branch").show()
  }
  //Remove entry from query
  def scenerio6(spark: SparkSession): Unit ={
    spark.sql("SELECT * FROM bev_branch order by branch desc, beverage desc").show()
    spark.sql("SELECT * FROM bev_branch WHERE beverage not like 'Triple_Coffee' order by branch desc, beverage desc").show()
  }
  //Branches that offer Special Coffee
  def finding1(spark: SparkSession): Unit ={
    spark.sql("SELECT Distinct branch from bev_branch where beverage = 'Special_Coffee' order by branch asc").show()
  }
  //Branch most purchased from
  def finding2(spark: SparkSession): Unit ={
    spark.sql("Select b.branch, sum(c.count) from bev_branches b join bev_count c on b.beverage=c.beverage group by b.branch order by sum(c.count) desc limit 1").show()
  }
}
