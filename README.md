# FunHBaseLoaderExamples
Just for Fun do not use in the real world.  :)

<H1>Building</H1>

mvn clean compile assembly:single

<H1>Running</H1>

[*] hadoop jar target/HBaseUtilMain.jar com.cloudera.sa.hbaseloadexamples.DataGenerator "/mvp/ingest/staging" 1000000 4
[*] hadoop jar target/HBaseUtilMain.jar com.cloudera.sa.hbaseloadexamples.HBaseSinglePut "/mvp/ingest/staging" "christoph_gamer" "i"
