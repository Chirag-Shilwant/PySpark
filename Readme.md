# DS Assignment 5

### Modules to be downloaded in the system
- pyspark
- pandas
- numpy

### Command for executing the code
**In Windows:**
```sh
python pyspark_problemnumber.py noOfCPUs inputFile.format outputFileName
```

**In Ubuntu:**
```sh
python3 pyspark_problemnumber.py noOfCPUs inputFile.format outputFileName
```

### Assumptions
- The outfile file format will always be txt.
- In all problems, the output file contains only values and does not contains header.
- In problem 1, stored the Country name and its respective counts of airports in output file.
- In problem 2, assumed that there would be only 1 Country having the highest number of airports. If there are multiple Country having the highest number of airports then only 1 will be displayed to the output. In this problem stored the country name and its count of airport in output file.
- In problem 3, stored the entire tuple satisfying the condition in the output file.