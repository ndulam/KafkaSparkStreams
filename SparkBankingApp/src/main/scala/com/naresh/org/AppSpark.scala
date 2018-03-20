package com.naresh.org
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
//http://lisp.vse.cz/pkdd99/berka.htm
object AppSpark
{

  case class accounts(account_id:Int,district_id:Int,frequency:String,date:java.sql.Timestamp)

  object accounts
  {
    def apply(account_id:Int,district_id:Int,frequency:String,date:Long): accounts =
    {
      new accounts(account_id,district_id,frequency, new java.sql.Timestamp(date))
    }
  }

  case class card(card_id:Int,disp_id:Int,card_type:String,issued:String)

  case class client(client_id:Int,birth_number:String,district_id:Int)

  case class disposition(disp_id:Int,client_id:Int,account_id:Int,ownner_type:String)

  case class district(A1:Int,A2:String,A3:String,A4:String,A5:String,A6:String,A7:String,A8:String,A9:String,A10:String,A11:String,A12:String,A13:String,A14:String,A15:String,A16:String)

  case class loan(loan_id:Int,account_id:Int,date:java.sql.Timestamp,amount:Double,duration:Int,payments:Double,status:String)
  object loan
        {
          def apply(loan_id:Int,account_id:Int,date:Long,amount:Double,duration:Int,payments:Double,status:String):loan =
          {
            new loan(loan_id,account_id,new java.sql.Timestamp(date),amount,duration,payments,status)
          }

        }

  case class order(order_id:Int,account_id:Int,bank_to:String,account_to:Int,amount:Double,k_symbol:String)

  case class transaction(trans_id:Int,account_id:Int,date:java.sql.Timestamp,tran_type:String,operation:String,amount:Double,balance:Double,k_symbol:String,bank:String,
                         account:String)
  object transaction
  {
    def apply(trans_id: Int, account_id: Int, date: Long, tran_type: String, operation: String, amount: Double, balance: Double, k_symbol: String, bank: String,
              account: String): transaction = new transaction(trans_id, account_id, new java.sql.Timestamp(date), tran_type, operation, amount, balance, k_symbol, bank, account)
  }

  def main(args: Array[String]): Unit =
  {


    val spark = SparkSession.builder().appName("Spark App").master("local[*]").getOrCreate()
    import spark.implicits._
    var accountrawds  = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/account.csv").as[accounts]
    var cardds = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/card.csv").as[card]
    var clientds  = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/client.csv").as[client]
    var dispds = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/disp.csv").as[disposition]
    var districtds = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/district.csv").as[district]
    var loands = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/loan.csv").as[loan]
    var orderrawds = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/order.csv").as[order]
    var transrawds = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv("/Users/nd2629/Desktop/workspace/KafkaSparkStreams/SparkBankingApp/resources/trans.csv").as[transaction]

    var account= accountrawds.withColumn("frequency", when(col("frequency") === "POPLATEK MESICNE" ,"monthly").when(col("frequency") === "POPLATEK TYDNE" ,"weekly").when(col("frequency") === "POPLATEK PO OBRATU" ,"after transaction").otherwise(col("frequency")) )
    var orders = orderrawds.withColumn("k_symbol",when(col("k_symbol") === "POJISTNE" ,"insurrance payment").when(col("k_symbol") === "SIPO","household payment").when(col("k_symbol") === "LEASING","leasing").when(col("k_symbol") === "UVER","loan payment").otherwise(col("k_symbol"))  )
    var trans = transrawds.withColumn("k_symbol",when(col("k_symbol") === "POJISTNE" ,"insurrance payment").when(col("k_symbol") === "SLUZBY","payment for statement").when(col("k_symbol") === "UROK","interest credited").when(col("k_symbol") === "SANKC. UROK","interest if negative balance").when(col("k_symbol") === "SIPO","household").when(col("k_symbol") === "DUCHOD","old-age pension").when(col("k_symbol") === "UVER","loan payment").otherwise(col("k_symbol"))).withColumn("operation",when(col("operation") === "VYBER KARTOU","credit card withdrawal").when(col("operation") === "VKLAD","credit in cash").when(col("operation") === "PREVOD Z UCTU","collection from another bank").when(col("operation") === "VYBER","withdrawal in cash").when(col("operation") === "PREVOD NA UCET","remittance to another bank").otherwise(col("operation"))).withColumn("tran_type",when(col("tran_type") === "PRIJEM","credit").when(col("tran_type") === "VYDAJ","withdrawal").when(col("tran_type") === "VYBER","withdrawal in cash").otherwise(col("tran_type")) )

    var temp  = cardds.join(dispds,Seq("disp_id")).join(clientds,Seq("client_id"))
    var clientsdetail=temp.join(districtds,temp("district_id")===districtds("A1"))

    var loanaccount = loands.join(account,Seq("account_id"))

    var loanwithdemograph = loanaccount.join(districtds,loanaccount("district_id")===districtds("A1"))

    var transwithaccdemo = trans.as("tr").join(broadcast(account).as("acc")).where($"tr.account_id" === $"acc.account_id").as("res").join(broadcast(districtds).as("dist")).where($"res.district_id"===$"dist.A1").drop(col("A1"))

    var pivotdata = transwithaccdemo.groupBy("district_id").pivot("tran_type").agg(sum("amount")).take(10).foreach(println) //pivot /transaction grouped by dist sum of transaction type

    transwithaccdemo.groupBy("district_id").agg(count("tran_type")).collect().foreach(println) // total trans group by district
    loanwithdemograph.groupBy("district_id").agg(count("loan_id")).collect().foreach(println) // load by demo graph wise

    //accounts without loan
    account.join(loands,Seq("account_id"),"left_outer").filter(col("loan_id").isNull).count

    transwithaccdemo.filter(col("bank").isNotNull).groupBy("district_id","bank").pivot("tran_type").agg(sum("balance"))

    /*joins
    card->disp->client-->demograph
    loan-->account-->demograph
    trans->account-->demograph

    pivot:
    transaction grouped by dist sum of transaction type
    account ->[loan,card,trans]

    demgraph wise loans
    demograph wise transactions

     */


  }

  /*
  delimiter
    account.write.parquet("/Users/nd2629/Downloads/parquet/account")
    card.write.parquet("/Users/nd2629/Downloads/parquet/card")
    client.write.parquet("/Users/nd2629/Downloads/parquet/client")
    disp.write.parquet("/Users/nd2629/Downloads/parquet/disp")
    district.write.parquet("/Users/nd2629/Downloads/parquet/district")
    loan.write.parquet("/Users/nd2629/Downloads/parquet/loan")
    order.write.parquet("/Users/nd2629/Downloads/parquet/order")
    trans.write.parquet("/Users/nd2629/Downloads/parquet/trans")
   */

}
