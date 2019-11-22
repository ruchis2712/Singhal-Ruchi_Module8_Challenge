# Module 8 Challenge

## This challenge is focussed on developing a general function that takes in 3 different movies related datasets: wiki movies, kaggle metadata and movie ratings from kaggle. It cleans, merges and transforms this data into a useable form and also creates a POSTGRES connection and uploads the cleaned movies data and ratings data into sql tables

## Assumptions used for cleaning and transforming the dataset:

- The code keeps only columns with atleast 90% of values i.e. it drops the columns that have more than 10% NULL or Blank values. After that, data cleansing is performed on some specified columns such as Box office, Budget, Running time. This is done assuming that these columns were not dropped and had more than 90% values that were not null/not blank

- The regex expressions used for data cleansing are based on the values observed in the dataset used during the code development. It is assumed that the new data set passed to the function will have similar structured expressions and that these will constitute 90% or more of the values within that column

- The data structures i.e. columns will be the same as the ones used / observed in the dataset used to develop the code

- When wiki and kaggle metadata sets are merged, an assumption is made on common columns around which column (wiki/kaggle) has better data and which column should be dropped. It is assumed that this selection will be correct and true even for the new dataset

- The dependencies pyhton pacakages needed to connect to POSTGRES are installed correctly

- There is a config.py file with the database password

- The POSTGRES Database movies_data exists
