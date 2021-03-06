# HPC Twitter Parser

In the last decade, social media has become more prevalent and pervasive in society. This boom has resulted in an exponential growth in the volume of data captured by each social media platform, but also the value of the data. Social media provides a unique and (predominantly) uncensored insight into the behaviour and trends of consumers. Accordingly, this data has become a powerful tool for firms to use to help understand the market. Before the raw data generated by the social media platforms can be used by firms, it must first be processed into a form with higher utility. Due to the sheer volume of data generated, this is not a trivial problem to solve.  

This repository contains an application for a university assignment to parse large collections of Twitter data utilising the University of Melbourne's high performance computing cluster **Spartan**. A report is also provided that demonstrates and discuss the advantages and disadvantages of using a parallelised solution for processing large social media datasets. A large Twitter data-set for the city of Sydney was processed to find the top ten:
* Most frequently occurring hashtags
* Most commonly used languages for tweeting

Final mark: 10/10

## Execution
The slurm scripts provided were designed to be used within the Spartan facility.  
To execute locally:
```bash
pip3 install -r requirements.txt
srun -n <number_of_processes> python3 app.py <path_to_twitter>
```
