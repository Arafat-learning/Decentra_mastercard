### Contents

1. **Introduction**
2. **File Descriptions**
3. **Preprocessing**
4. **Model**
5. **Conclusion**

### File Descriptions

In the following table you can find short information about each file

|**File**                 |**Description**                                                 |
|:------------------------|:---------------------------------------------------------------|
|eda.ipynb                |Exploratory Data Analysis                                       |
|preprocessing.py         |Preprocessing of data (More on Preprocessing segment)           |
|model.py                 |Model and comparison (More on Model segment)                    |
|Customer Segments.pptx   |Contains presentation of the project                            |
|requirements.txt         |List of python dependencies used in the project                 |

Data files

In this table we present short information about data files found in data directory

|**File**                 |**Description**                                                 |
|:------------------------|:---------------------------------------------------------------|
|DECENTRATHON_3.0.parquet |Original Data                                                   |
|clustreing_data.parquet  |Processed original data preprered for fitting in the model      |
|data_dictionary.csv      |Description of columns of clustering_data.parquet               |
|results.parquet          |Segmentation Results                                            |

### Preprocessing

Preprecessing is done with *preprocessing.py*  
It takes the original data and turns it into useable form for the *model.py*  
DECENTRATHON_3.0.parquet -> clustering_data.parquet

| **Column name** | **Description**|
|:----------------|:---------------|
|ID               | Id of the card |