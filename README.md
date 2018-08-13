# LINDA
Thesis: Rule Mining on Distributed Data



A distributed rule mining system based on Spark which mines non monotonic rules. 

## Getting Started

### Prerequisites

1. Spark 
2. Maven
3. Java

### Installing

Submit jobs in spark by going to the spark directory and executing the following

```
./bin/spark-submit --class org.aksw.dice.linda.Utils.DatasetParser --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>" "<Input dataset exact path>”
```
```
./bin/spark-submit --class org.aksw.dice.linda.EWS.EWSRuleMiner --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```

```
./bin/spark-submit --class org.aksw.dice.linda.EWS.FactGenerator --master <Master url> <location to jar >/LINDA-0.1.0.jar <dataset name> "<hdfs cluster url>"
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With
* [Maven](https://maven.apache.org/) - Dependency Management
* [Spark]() - Cluster Computing Framework


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

