# LINDA
**Rule Mining on Distributed Data:**  A distributed rule mining system based on Spark which mines non monotonic rules.

Over the recent years, the advances in information extraction has resulted in the proliferation of large Knowledge Bases (KBs), which  is a machine readable collection of knowledge. The KBs are inevitably bound to be incomplete. Logic rules are being used to predict new facts based on the existing ones. Horn rules are particular rule-like form which gives it useful properties for use in logic programming and are one of the popular form of rules mined. However, Horn rules do not take into account possible exceptions, so that predicting facts via such rules introduces errors. Another major challenge is to  perform scalable analysis of large scale KBs to facilitate rule mining. Hence we address these two problems in this work and present two rule mining approaches implemented for a cluster based environment - (1) Exception Enriched Rule Mining Approach, which aims at adding exception (negated body elements) in order to refine rules; (2) Decision Tree Rule Mining, a rule miner which can mine both horn rules and non-monotonic rules. Both these approaches are implemented for a distributed environment. We apply our method to discover rules with exceptions from real-world KBs.

## Getting Started

### Prerequisites

1. **Spark** - The instructions to install Spark can be found [here](https://spark.apache.org/docs/latest/spark-standalone.html).
2. **Maven** - The instructions to install Maven can be found [here](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html).
3. **Java**
4. **HDFS** - The instructions to install HDFS can be found [here] (https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/)


### Installing
1. Clone the project.

2. Build the project using 

```
mvn clean package
```
3. [Start](https://spark.apache.org/docs/latest/cluster-overview.html) HDFS and spark in the cluster

3. Submit jobs in spark by going to the spark directory and executing the following

**EWS Rule Miner**

```
./bin/spark-submit --class org.aksw.dice.linda.Utils.DatasetParser --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>" "<Input dataset exact path>”
```
```
./bin/spark-submit --class org.aksw.dice.linda.EWS.EWSRuleMiner --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```

```
./bin/spark-submit --class org.aksw.dice.linda.EWS.FactGenerator --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```


**Decision Tree Rule Miner**



Comment out the Horn Rule Miner from Dataset Parser since it is not required.

```
./bin/spark-submit --class org.aksw.dice.linda.Utils.DatasetParser --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>" "<Input dataset exact path>”
```

Execute the LIBSVM Writer
```
python libsvmwriter.py
```
Put the new data files created in the location `HDFSClusterURL\DatasetNAME\LIBSVMDATA`.


```
./bin/spark-submit --class org.aksw.dice.linda.classificationRuleMining.DTRuleMiner --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```

```
./bin/spark-submit --class org.aksw.dice.linda.classificationRuleMining.DTFactGenerator --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```


## Built With
* [Maven](https://maven.apache.org/) - Dependency Management
* [Spark](https://spark.apache.org/docs/latest/index.html) - Cluster Computing Framework


## Author

[Kunal Jha](https://github.com/Kunal-Jha)

## License


## Acknowledgments

* Tommaso Soru
* Gezim Sejdiu 
* Ivan Ermilov
* Dr. Hajira Jabeen
* Prof. Dr. Axel-C. Ngonga Ngomo
* Prof. Dr. Jens Lehmann

