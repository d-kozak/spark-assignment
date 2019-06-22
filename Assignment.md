# Spark assignment

* Author: David Kozak
* Email: dkozak94@gmail.com

The goal of this assignment was to implement 12 data analysis tasks in using [Apache Spark](https://spark.apache.org/).
The structure of the report is as follows. First, the dataset is introduces. Afterwards follows the description and the solution for each of the tasks. 
In the end, a conclusion is given.

## Dataset
As in the first [mongo assignment](https://github.com/d-kozak/mongo-assignment), I chose to use the dataset [employee reviews](https://www.kaggle.com/petersunga/google-amazon-facebook-employee-reviews/version/2),
which contains reviews from employees of 6 IT companies - Google, Facebook, Amazon, Apple and Netflix. The reviews are store in this [csv file](./employee_reviews.csv).
The structure of one review can be determined by checking the header line.
```
head -n 1 employee_reviews.csv
```
Which results in:
```
id,company,location,dates,job-title,summary,pros,cons,advice-to-mgmt,overall-ratings,work-balance-stars,culture-values-stars,carrer-opportunities-stars,comp-benefit-stars,senior-mangemnet-stars,helpful-count,link
``` 
The size is 67529 reviews.
```
tail -n 1
```
```
67529,microsoft,none," Dec 14, 2010",Former Employee - Sen ....
```
## Run the code
The whole repository is gradle module with source code written in [Kotlin](http://kotlinlang.org), which can be compiled into jar using the following command.
```
gralde jar
```
This jar can then be submitted to locally installed apache spark instance.
```
${SPARK_EXEC} --class io.dkozak.estg.spark.assignment.MainKt --master "local[4]" ${JAR_FILE} ${INPUT_FILE} ${OUTPUT_DIR}
```
* SPARK_EXEC is the location of the spark-submit file in the bin folder inside the spark directory
* JAR_FILE is jar build the previous step
* INPUT_FILE is a path to the input csv file
* OUTPUT_DIR is a directory into which the output should be written

To allow for easier execution, a [shell script](./execute.sh) is prepared. This script takes one to two arguments. 
* location where ApacheSpark is installed.
* [optional] a single task that should be run (default is all tasks)

## Tasks
In the section description and solutions for individual tasks will be given

### 1)Lookup collection [code](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/1.kt)
The goal of this task was to replace one enumeration column in the dataset with a numeric value. The mapping between the numeric value and the original enumeration value
should be saved in a separate lookup collection.

For this task I decided to replace the company column in the original dataset.
