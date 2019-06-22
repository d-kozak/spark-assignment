# Spark assignment

* Author: David Kozak
* Email: dkozak94@gmail.com

The goal of this assignment was to implement 12 data analysis tasks using [Apache Spark](https://spark.apache.org/).
The structure of the report is as follows. First, the dataset is introduced. Afterwards the description and the solution for each of the tasks are given. 


## Dataset
As in the first [mongo assignment](https://github.com/d-kozak/mongo-assignment), I decided to use the dataset [employee reviews](https://www.kaggle.com/petersunga/google-amazon-facebook-employee-reviews/version/2),
which contains reviews from employees of 6 IT companies - Google, Facebook, Amazon, Apple and Netflix. The reviews are stored in a [csv file](./employee_reviews.csv).
The structure of one review can be determined by checking the header line.
```
head -n 1 employee_reviews.csv
```
Which results in:
```
id,company,location,dates,job-title,summary,pros,cons,advice-to-mgmt,overall-ratings,
work-balance-stars,culture-values-stars,carrer-opportunities-stars,comp-benefit-stars
,senior-mangemnet-stars,helpful-count,link
``` 
The size is 67529 reviews.
```
tail -n 1
```
```
67529,microsoft,none," Dec 14, 2010",Former Employee - Sen ....
```
## Run the code
The whole repository is a gradle module with source code written in [Kotlin](http://kotlinlang.org), which can be compiled into jar using the following command.
```
gralde jar
```
This jar can then be submitted to locally installed apache spark instance.
```
${SPARK_EXEC} --class io.dkozak.estg.spark.assignment.MainKt \ 
--master "local[4]" ${JAR_FILE} ${INPUT_FILE} ${OUTPUT_DIR}
```
* SPARK_EXEC is the location of the spark-submit file in the bin folder inside the spark directory
* JAR_FILE is jar build the previous step
* INPUT_FILE is a path to the input csv file
* OUTPUT_DIR is a directory into which the output should be written

To allow for easier execution, a [execute.sh](./execute.sh) shell script is prepared. This script takes one to two arguments. 
* location where ApacheSpark is installed.
* [optional] a single task that should be run (default is all tasks)

The output can be found in the ./output directory, each task results are stored in a subfolder identified 
by the number of the task.

## Tasks
In this section description and solutions for individual tasks will be given

### 1)[Lookup collection](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/1.kt)
The goal of this task was to replace one enumeration column in the dataset with a numeric value. The mapping between the numeric value and the original enumeration value
should be saved in a separate lookup collection.

For this task I decided to replace the company column in the original dataset.

### 2) [Oversampling](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/2.kt)

Since the amount of reviews per company is different, it might be important for some types of analysis to transform the dataset in a way to 
make sure that all the categories are represented equally. Therefore, for the oversampling task, I decided to even out the amount of reviews per company. 
I achieved this by inserting some reviews multiple times.

### 3) [Undersampling](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/3.kt)
As in the case of mongodb, undersampling was easier to implement, because there is a sample operator that 
takes a randomly selected subset from the dataset. Again, I used undersampling to ensure that the amount of tasks per company is equal.

### 4) [Discretizing](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/4.kt)
For this task I decided to discretize the overall rating column. Originally it contained values from the interval <0.0,5.0> 
and transformed it into values from {1,2,3,4,5} by rounding the floating point numbers up to their nearest integer.

### 5) [Probabilistic analysis](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/5.kt)
For this task I decided to calculate the probabilities of a review belonging to a company. 

### 6) [Tf-idf](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/6.kt)
As in the mongodb task, I decided to calculate the tf-idf for the word 'work' in the summary column. 

### 7) [Index](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/7.kt)
Since the column containing longest strings is the summary, I decided to create inverted index for the one.
It could be used to add search engine capability for the dataset.

### 8) [k-fold cross validation](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/8.kt)
For this task I decided to split the dataset into 5 disjoint datasets that can be used for example to test
that a prediction model is not overfitted.

### 9) [Normalization](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/9.kt)
I decided to normalize the values in overall-ratings, which originally were form the interval <0.0,5.0>. 

### 10) [Remove noise](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/10.kt)
I decided to remove all reviews that were older than 1.1.2017, because they are outdated and 
therefore they are less valuable for people deciding where to go now.

### 11) [Fill missing values](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/11.kt)
Again I encountered the problem that I could not find missing values. However, to fulfil the task, 
I decided to create a code that would insert average rating into reviews where overall-rating would be missing.

### 12) [Pivot table](./src/main/kotlin/io/dkozak/estg/spark/assignment/tasks/11.kt)
For this tasks, I decided to compute the average value per company for the following columns.
* overall-ratings
* work-balance-stars
* culture-values-stars
* carrer-opportunities-stars  
  

