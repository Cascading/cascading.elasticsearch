(defproject cascading-elasticsearch "0.1.0"
  :description "Elasticsearch integration for Cascading and Cascalog."
  :java-source-path "src"
  :jvm-opts ["-Xmx768m" "-server"]
  :dependencies [[cascading/cascading-core "1.2.4"
                  :exclusions [org.codehaus.janino/janino
                               thirdparty/jgrapht-jdk1.6
                               riffle/riffle]]
                 [thirdparty/jgrapht-jdk1.6 "0.8.1"]
                 [riffle/riffle "0.1-dev"]
                 [log4j/log4j "1.2.16"]
                 [org.elasticsearch/elasticsearch "0.16.0"]
                 [org.codehaus.jackson/jackson-mapper-asl "1.5.2"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
