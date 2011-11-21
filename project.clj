(defproject cascalog-elasticsearch "0.1.0"
  :description "Elasticsearch integration for Cascading and Cascalog."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :jvm-opts ["-Xmx768m" "-server"]
  :repositories {"sonatype"
                 "https://oss.sonatype.org/content/repositories/releases"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [cascalog "1.8.4"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.4.0-SNAPSHOT"]])
