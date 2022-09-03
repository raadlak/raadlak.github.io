# Time Series Analysis

<!-- vscode-markdown-toc -->
	* 1. [A. Reading TimeSeries Data in Pandas from Files](#A.ReadingTimeSeriesDatainPandasfromFiles)
		* 1.1. [1. Reading data from CSVs and other delimited files](#ReadingdatafromCSVsandotherdelimitedfiles)
		* 1.2. [2. Reading Data from Excel Files](#ReadingDatafromExcelFiles)
		* 1.3. [3. Reading data from GitHub](#ReadingdatafromGitHub)
		* 1.4. [4. Reading data from public S3 bucket](#ReadingdatafrompublicS3bucket)
		* 1.5. [4.1 Reading data from private S3 Bucket](#ReadingdatafromprivateS3Bucket)
		* 1.6. [5. Reading data from HTML](#ReadingdatafromHTML)
		* 1.7. [6. Reading data from SAS dataset](#ReadingdatafromSASdataset)
	* 2. [B. Reading Time Series Data from Databases](#B.ReadingTimeSeriesDatafromDatabases)
		* 2.1. [B.1 Reading Data from relational databases](#B.1ReadingDatafromrelationaldatabases)
		* 2.2. [B.4 Reading data from time series database (InfluxDB)](#B.4ReadingdatafromtimeseriesdatabaseInfluxDB)
	* 3. [C. Writing timeseries data to files](#C.Writingtimeseriesdatatofiles)
	* 4. [D. Writing time series data to Databases](#D.WritingtimeseriesdatatoDatabases)
	* 5. [E. Working with Date and Time in Python](#E.WorkingwithDateandTimeinPython)
		* 5.1. [E.1 DatetimeIndex](#E.1DatetimeIndex)
		* 5.2. [E.2  DateTime Formatting](#E.2DateTimeFormatting)
		* 5.3. [E.3 Tranforming a pandas DataFrame to a time series DataFrame](#E.3TranformingapandasDataFrametoatimeseriesDataFrame)
		* 5.4. [E.4 Working with Time Deltas](#E.4WorkingwithTimeDeltas)
		* 5.5. [E.5 Converting Datetime with TimeZone information](#E.5ConvertingDatetimewithTimeZoneinformation)
		* 5.6. [E.6 Working with date offsets](#E.6Workingwithdateoffsets)
		* 5.7. [E.6 Custom business days](#E.6Custombusinessdays)
		* 5.8. [E.7 Custom Business Day hours](#E.7CustomBusinessDayhours)
	* 6. [F. Handling Missing Data](#F.HandlingMissingData)
		* 6.1. [F.1 Handling missing data with imputation using Pandas](#F.1HandlingmissingdatawithimputationusingPandas)
		* 6.2. [F.1.2 Handling missing data with univariate imputation using scikit-learn](#F.1.2Handlingmissingdatawithunivariateimputationusingscikit-learn)
		* 6.3. [F.1.3 Handling missing data with multivariate imputation](#F.1.3Handlingmissingdatawithmultivariateimputation)
		* 6.4. [F.1.4 Handling missing data with Interpolation](#F.1.4HandlingmissingdatawithInterpolation)
	* 7. [G. Outlier Detection Using Statistical Methods](#G.OutlierDetectionUsingStatisticalMethods)
		* 7.1. [G.1 Resampling time series data](#G.1Resamplingtimeseriesdata)
		* 7.2. [G.2 Detecting Outliers using Visualization](#G.2DetectingOutliersusingVisualization)
		* 7.3. [G.3 Detecting Outliers with Tukey Method](#G.3DetectingOutlierswithTukeyMethod)
		* 7.4. [G.4 Detecting outliers using a z-score](#G.4Detectingoutliersusingaz-score)
		* 7.5. [G.5 Detecting Outliers using a modified z-score](#G.5DetectingOutliersusingamodifiedz-score)
	* 8. [H. Exploratory Data Analysis and Diagnosis](#H.ExploratoryDataAnalysisandDiagnosis)
		* 8.1. [H.1 Plotting time series data using Pandas](#H.1PlottingtimeseriesdatausingPandas)
		* 8.2. [H.2 Plotting time series data with interactive visualization using hvPlot](#H.2PlottingtimeseriesdatawithinteractivevisualizationusinghvPlot)
		* 8.3. [H.3 Decomposing time series data](#H.3Decomposingtimeseriesdata)
		* 8.4. [H.4 Detecting time series stationarity](#H.4Detectingtimeseriesstationarity)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

###  1. <a name='A.ReadingTimeSeriesDatainPandasfromFiles'></a>A. Reading TimeSeries Data in Pandas from Files    

Often we see that time series Data is represented with Indexes. Reason in general to represent DataFrame with Index is to make slicing and dicing operations very intuitive.<br> DatetimeIndex is used for time series as Index which unlocks many features and useful functions esential while working with time series data.

Following are the recipes to efficiently read time series data into a DataFrame.

####  1.1. <a name='ReadingdatafromCSVsandotherdelimitedfiles'></a>1. Reading data from CSVs and other delimited files


```python
import pandas as pd
from pathlib import Path
```


```python
_base_location = '/Users/rahuladlakha/Documents/Github/Code_books/TimeSeriesAnalysis_Python/Datasets/Time-Series-Analysis-with-Python-Cookbook./datasets/'
chapter = 'Ch2'
filename = '/movieboxoffice.csv'
filepath = Path(_base_location+chapter+filename)
```

Since the first column of the dataset is Date column, we can convert it to DateTimeIndex and parse it as Date while reading it into a pandas DataFrame.


```python
ts = pd.read_csv(filepath,
            header=0, # by Default it is infer, If csv doesn't contain header, then header=None. 
            #If we prefer to supply custom col names as header, then supply header=0 and overwrite it by providing a list of col to names argument.
            parse_dates=['Date'], #can take list with col positions as well col names. 
            index_col=0, #can take position as well as name of the col. can be used to create MultiIndex, using list of position of col or string as cols.
            infer_datetime_format=True,
            usecols=['Date',
                     'DOW',
                     'Daily',
                     'Forecast',
                     'Percent Diff'])# can take positional indices)
```


```python
ts.sample(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DOW</th>
      <th>Daily</th>
      <th>Forecast</th>
      <th>Percent Diff</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-08-08</th>
      <td>Thursday</td>
      <td>$11.46</td>
      <td>$20.49</td>
      <td>-44.07%</td>
    </tr>
    <tr>
      <th>2021-07-22</th>
      <td>Monday</td>
      <td>$132.52</td>
      <td>$229.51</td>
      <td>-42.26%</td>
    </tr>
    <tr>
      <th>2021-06-19</th>
      <td>Wednesday</td>
      <td>$353.04</td>
      <td>$608.22</td>
      <td>-41.96%</td>
    </tr>
    <tr>
      <th>2021-06-21</th>
      <td>Friday</td>
      <td>$398.94</td>
      <td>$576.32</td>
      <td>-30.78%</td>
    </tr>
    <tr>
      <th>2021-08-12</th>
      <td>Monday</td>
      <td>$4.80</td>
      <td>$7.54</td>
      <td>-36.33%</td>
    </tr>
  </tbody>
</table>
</div>



There are cases where the parse_dates parameter doesn't work, this is where the **date_parser** parameter can be useful.
date_parser will contain a lambda function to convert the date column using pandas **to_datetime** function. <br><br>
Format is added to the date parser to point out the existing format of the date column. <br>
%d represents, day of the month - 21<br>
%b represents, month as short, ex. Jan <br>
%y represents, two digit year, 90
<br> <br>
%Y represents, year four digit, 2022 <br>
%B represents, month in full, April <br>
%m represents, date as two digits, 12 <br>


```python
date_parser = lambda x: pd.to_datetime(x, format="%d-%b-%y")
```


```python
ts2=pd.read_csv(filepath, 
                parse_dates=[0],
                index_col=0,
                date_parser=date_parser,
                usecols=[0,1,3,7,6])
```


```python
ts2.sample(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DOW</th>
      <th>Daily</th>
      <th>Forecast</th>
      <th>Percent Diff</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-07-25</th>
      <td>Thursday</td>
      <td>$125.49</td>
      <td>$204.31</td>
      <td>-38.58%</td>
    </tr>
    <tr>
      <th>2021-08-13</th>
      <td>Tuesday</td>
      <td>$4.68</td>
      <td>$7.36</td>
      <td>-36.38%</td>
    </tr>
    <tr>
      <th>2021-05-28</th>
      <td>Tuesday</td>
      <td>$1,652.02</td>
      <td>$2,337.15</td>
      <td>-29.31%</td>
    </tr>
    <tr>
      <th>2021-04-27</th>
      <td>Saturday</td>
      <td>$99,374.01</td>
      <td>$197,622.55</td>
      <td>-49.72%</td>
    </tr>
    <tr>
      <th>2021-07-28</th>
      <td>Sunday</td>
      <td>$111.00</td>
      <td>$166.88</td>
      <td>-33.49%</td>
    </tr>
  </tbody>
</table>
</div>




```python
ts.info()
```

    <class 'pandas.core.frame.DataFrame'>
    DatetimeIndex: 128 entries, 2021-04-26 to 2021-08-31
    Data columns (total 4 columns):
     #   Column        Non-Null Count  Dtype 
    ---  ------        --------------  ----- 
     0   DOW           128 non-null    object
     1   Daily         128 non-null    object
     2   Forecast      128 non-null    object
     3   Percent Diff  128 non-null    object
    dtypes: object(4)
    memory usage: 5.0+ KB


Note: Date column is now of type DateTimeIndex. Column Daily and Forecast is of dtype *'object'*, to correct the dtype we would need to remove $ sign and convert them to astype float.


```python
c_df = ts[['Daily', 'Forecast']].apply(lambda x: x.str.replace('[^\d]', '', regex=True)) # replace $ and change dtype
```


```python
c_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Daily</th>
      <th>Forecast</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-04-26</th>
      <td>12578989</td>
      <td>23503646</td>
    </tr>
    <tr>
      <th>2021-04-27</th>
      <td>9937401</td>
      <td>19762255</td>
    </tr>
    <tr>
      <th>2021-04-28</th>
      <td>8220316</td>
      <td>11699126</td>
    </tr>
    <tr>
      <th>2021-04-29</th>
      <td>3353026</td>
      <td>6665265</td>
    </tr>
    <tr>
      <th>2021-04-30</th>
      <td>3010524</td>
      <td>3482819</td>
    </tr>
  </tbody>
</table>
</div>




```python
ts[['Daily', 'Forecast']] = c_df.astype(float)
```


```python
ts.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>DOW</th>
      <th>Daily</th>
      <th>Forecast</th>
      <th>Percent Diff</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-04-26</th>
      <td>Friday</td>
      <td>12578989.0</td>
      <td>23503646.0</td>
      <td>-46.48%</td>
    </tr>
    <tr>
      <th>2021-04-27</th>
      <td>Saturday</td>
      <td>9937401.0</td>
      <td>19762255.0</td>
      <td>-49.72%</td>
    </tr>
    <tr>
      <th>2021-04-28</th>
      <td>Sunday</td>
      <td>8220316.0</td>
      <td>11699126.0</td>
      <td>-29.74%</td>
    </tr>
    <tr>
      <th>2021-04-29</th>
      <td>Monday</td>
      <td>3353026.0</td>
      <td>6665265.0</td>
      <td>-49.69%</td>
    </tr>
    <tr>
      <th>2021-04-30</th>
      <td>Tuesday</td>
      <td>3010524.0</td>
      <td>3482819.0</td>
      <td>-13.56%</td>
    </tr>
  </tbody>
</table>
</div>



####  1.2. <a name='ReadingDatafromExcelFiles'></a>2. Reading Data from Excel Files

Excels contain multiple worksheets, so it is essential to explore different options to read Excels.In read_excel(), we will use engine parameter to specify, which library to use for processing Excels. <br>
Ex: openpyxl, xlrd


```python
filename = '/sales_trx_data.xlsx'
```


```python
filepath = Path(_base_location+chapter+filename)
```


```python
excel_ts = pd.read_excel(filepath,
                        engine='openpyxl',
                        index_col=1,
                        sheet_name=[0,1], #can use sheet names, sheet positions or combination of both
                        parse_dates=True)

# ExcelFile - Function provides additional methods and properties, can be used to find sheetname
```


```python
excel_ts[0].info()
```

    <class 'pandas.core.frame.DataFrame'>
    DatetimeIndex: 36764 entries, 2017-01-01 to 2017-12-31
    Data columns (total 4 columns):
     #   Column              Non-Null Count  Dtype 
    ---  ------              --------------  ----- 
     0   Line_Item_ID        36764 non-null  int64 
     1   Credit_Card_Number  36764 non-null  int64 
     2   Quantity            36764 non-null  int64 
     3   Menu_Item           36764 non-null  object
    dtypes: int64(3), object(1)
    memory usage: 1.4+ MB


For the current excel, the two dataframes within the dictionary are identical in terms of schema, we can stack them into one DataFrame with single DateTimeIndex


```python
ts_combined = pd.concat([excel_ts[0], excel_ts[1]])
```


```python
ts_combined.info()
```

    <class 'pandas.core.frame.DataFrame'>
    DatetimeIndex: 74124 entries, 2017-01-01 to 2018-12-31
    Data columns (total 4 columns):
     #   Column              Non-Null Count  Dtype 
    ---  ------              --------------  ----- 
     0   Line_Item_ID        74124 non-null  int64 
     1   Credit_Card_Number  74124 non-null  int64 
     2   Quantity            74124 non-null  int64 
     3   Menu_Item           74124 non-null  object
    dtypes: int64(3), object(1)
    memory usage: 2.8+ MB


####  1.3. <a name='ReadingdatafromGitHub'></a>3. Reading data from GitHub

To read data from github we need url to the raw content, like (https://media.githubusercontent.com/media/PacktPublishing/Time-Series-Analysis-with-Python-Cookbook./main/datasets/Ch2/AirQualityUCI.csv)


```python
url = 'https://media.githubusercontent.com/media/PacktPublishing/Time-Series-Analysis-with-Python-Cookbook./main/datasets/Ch2/AirQualityUCI.csv'
```


```python
date_parser = lambda x: pd.to_datetime(x, format="%d/%m/%Y")
```


```python
df = pd.read_csv(url,
                delimiter=';',
                index_col = 'Date',
                date_parser=date_parser
                )
```


```python
df.iloc[:3, 1:4]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CO(GT)</th>
      <th>PT08.S1(CO)</th>
      <th>NMHC(GT)</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2004-03-10</th>
      <td>2,6</td>
      <td>1360</td>
      <td>150</td>
    </tr>
    <tr>
      <th>2004-03-10</th>
      <td>2</td>
      <td>1292</td>
      <td>112</td>
    </tr>
    <tr>
      <th>2004-03-10</th>
      <td>2,2</td>
      <td>1402</td>
      <td>88</td>
    </tr>
  </tbody>
</table>
</div>



####  1.4. <a name='ReadingdatafrompublicS3bucket'></a>4. Reading data from public S3 bucket

To read data from public S3 bucket, you don't need to specify the region (us-east-1), we can directly read it using s3 url.


```python
url = 'https://s3.amazonaws.com/tscookbook/AirQualityUCI.xlsx'
```


```python
df_s3 = pd.read_excel(url,
                    index_col='Date',
                    parse_dates=True
                    )
```


```python
df_s3.sample(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Time</th>
      <th>CO(GT)</th>
      <th>PT08.S1(CO)</th>
      <th>NMHC(GT)</th>
      <th>C6H6(GT)</th>
      <th>PT08.S2(NMHC)</th>
      <th>NOx(GT)</th>
      <th>PT08.S3(NOx)</th>
      <th>NO2(GT)</th>
      <th>PT08.S4(NO2)</th>
      <th>PT08.S5(O3)</th>
      <th>T</th>
      <th>RH</th>
      <th>AH</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2005-01-28</th>
      <td>16:00:00</td>
      <td>1.3</td>
      <td>891.000000</td>
      <td>-200</td>
      <td>3.909047</td>
      <td>705.666667</td>
      <td>192.9</td>
      <td>948.666667</td>
      <td>137.9</td>
      <td>860.00</td>
      <td>600.00</td>
      <td>3.366667</td>
      <td>50.699999</td>
      <td>0.399114</td>
    </tr>
    <tr>
      <th>2004-09-23</th>
      <td>05:00:00</td>
      <td>0.5</td>
      <td>846.666667</td>
      <td>-200</td>
      <td>2.502260</td>
      <td>619.666667</td>
      <td>84.0</td>
      <td>1085.333333</td>
      <td>58.0</td>
      <td>1339.00</td>
      <td>670.00</td>
      <td>22.933333</td>
      <td>57.500000</td>
      <td>1.587902</td>
    </tr>
    <tr>
      <th>2004-08-07</th>
      <td>22:00:00</td>
      <td>1.9</td>
      <td>1030.000000</td>
      <td>-200</td>
      <td>10.285388</td>
      <td>988.250000</td>
      <td>84.0</td>
      <td>686.000000</td>
      <td>95.0</td>
      <td>1750.50</td>
      <td>828.75</td>
      <td>27.250000</td>
      <td>49.400001</td>
      <td>1.760218</td>
    </tr>
    <tr>
      <th>2004-07-04</th>
      <td>02:00:00</td>
      <td>1.1</td>
      <td>1006.500000</td>
      <td>-200</td>
      <td>7.171164</td>
      <td>864.250000</td>
      <td>65.0</td>
      <td>816.000000</td>
      <td>71.0</td>
      <td>1638.75</td>
      <td>987.25</td>
      <td>22.599999</td>
      <td>51.000000</td>
      <td>1.380526</td>
    </tr>
    <tr>
      <th>2005-04-02</th>
      <td>21:00:00</td>
      <td>0.8</td>
      <td>885.250000</td>
      <td>-200</td>
      <td>1.772570</td>
      <td>566.750000</td>
      <td>116.3</td>
      <td>1020.500000</td>
      <td>86.7</td>
      <td>872.25</td>
      <td>428.25</td>
      <td>13.550000</td>
      <td>39.400001</td>
      <td>0.608906</td>
    </tr>
  </tbody>
</table>
</div>



Reading the same file using s3url


```python
s3uri = 's3://tscookbook/AirQualityUCI.csv'
```

####  1.5. <a name='ReadingdatafromprivateS3Bucket'></a>4.1 Reading data from private S3 Bucket


```python
import configparser
config = configparser.ConfigParser()
config.read('aws.cfg')

AWS_ACCESS_KEY = config['AWS']['aws_access_key']
AWS_SECRET_KEY = config['AWS']['aws_secret_key']
```

To use AWS config Access Key and Secret Key we need to pass them in the read_csv function as storage_options


```python
df = pd.read_csv(s3uri,
                 index_col = 'Date',
                 parse_dates = True,
                 storage_options = {
                 'key' : AWS_ACCESS_KEY,
                 'secret' : AWS_SECRET_KEY})
```

Alternatively, you can use the AWS SDK for Python (Boto3) to achieve similar results.


```python
import boto3 #Boto3 offers two levels of APIs: client and resource.
bucket = "tscookbook-private"
client = boto3.client("s3",
                      aws_access_key_id = AWS_ACCESS_KEY,
                      aws_secret_access_key = AWS_SECRET_KEY) #client object has many methods specific to the AWS S3 service for creating, deleting and retrieving bucket info, etc.

```

The **client** is a low level service access interface that gives you more granular control, ex. boto3.client("s3). <br>
The **resource** is a high level object-oriented interface (an abstraction layer), for example, boto3.resource("s3")


```python
data = client.get_object(Bucket=bucket, Key='AirQuality.csv')
df = pd.read_csv(data['Body'], # when we call get_object, we receive a key-value pair response, however we are interested in the response body.
                index_col='Date',
                parse_dates=True)
```

####  1.6. <a name='ReadingdatafromHTML'></a>5. Reading data from HTML

We can read HTML tables using pandas.read_html() function. We will https://en.wikipedia.org/wiki/COVID-19_pandemic_by_country_and_territory link to access HTML Wikipedia data.


```python
url = "https://en.wikipedia.org/wiki/COVID-19_pandemic_by_country_and_territory"
result = pd.read_html(url)
```


```python
print(len(result)) #returned 70 dataframes
```

    70



```python
df_html = result[15] #Dataframe at index 15 contains summary of COVID-19 cases
```


```python
df_html.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Region[29]</th>
      <th>Total cases</th>
      <th>Total deaths</th>
      <th>Cases per million</th>
      <th>Deaths per million</th>
      <th>Current weekly cases</th>
      <th>Current weekly deaths</th>
      <th>Population millions</th>
      <th>Vaccinated %[30]</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>European Union</td>
      <td>158263205</td>
      <td>1116388</td>
      <td>353803</td>
      <td>2496</td>
      <td>1743773</td>
      <td>4439</td>
      <td>447</td>
      <td>74.9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>North America</td>
      <td>94411188</td>
      <td>1067866</td>
      <td>255991</td>
      <td>2895</td>
      <td>928336</td>
      <td>3180</td>
      <td>369</td>
      <td>74.3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Other Europe</td>
      <td>54061026</td>
      <td>485902</td>
      <td>231385</td>
      <td>2080</td>
      <td>572668</td>
      <td>1592</td>
      <td>234</td>
      <td>60.9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>South America</td>
      <td>61630291</td>
      <td>1288867</td>
      <td>143368</td>
      <td>2998</td>
      <td>402909</td>
      <td>2296</td>
      <td>430</td>
      <td>80.6</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Russia and Central Asia</td>
      <td>22414473</td>
      <td>423836</td>
      <td>94658</td>
      <td>1790</td>
      <td>71486</td>
      <td>252</td>
      <td>237</td>
      <td>50.8</td>
    </tr>
  </tbody>
</table>
</div>



Most of the pandas functions accept URL as a path, examples includes following: <br>

pandas.read_csv() <br>
pandas.read_excel() <br>
pandas.read_parquet() <br>
pandas.read_table() <br>
pandas.read_pickle() <br>
pandas.read_orc() <br>
pandas.read_stata() <br>
pandas.read_sas() <br>
pandas.read_json() <br>

The URL needs to be one of the valid URL schemes that pandas supports, which includes http and https, ftp, s3, gs, or the file protocol.
			

####  1.7. <a name='ReadingdatafromSASdataset'></a>6. Reading data from SAS dataset

We can use pandas.read_sas() function to read SAS7BDAT files.


```python
filename = '/DCSKINPRODUCT.sas7bdat'
```


```python
path = Path(_base_location+chapter+filename)
```


```python
df = pd.read_sas(path, chunksize=10000) 
type(df) # returned object is a SAS7BDATReader object. we can retrieve first chunk using the next() method.
```




    pandas.io.sas.sas7bdat.SAS7BDATReader



We will need to iterate through the chunks to do some computations. Since the data in most SAS7BDAT files is huge, this is the way forward. 


```python
results = []
for chunk in df:
    results.append(chunk)
len(results) #There are 16 chunks(DataFrame) in total.
```




    16




```python
df_all = pd.concat(results)
df_all.shape
```




    (152130, 5)




```python
df = pd.read_sas(path, chunksize=10000) #Re-read the chunks, grouping by Date and aggregating using sum and count.
results = []
for chunk in df:
    results.append(
        chunk.groupby('DATE')['Revenue']
             .agg(['sum', 'count']))
len(results)
```




    16




```python
results[0].loc['2013-02-10']
```




    sum      923903.0
    count        91.0
    Name: 2013-02-10 00:00:00, dtype: float64



Pandas is a single core framework, doesn't offer parallel computation. There are specialized libraries that offer parallel processing, ex. Dask.<br> It creates computational graphs and parallelizes small tasks and improves speed and reduces memory overheads, which are generally high if we use just Pandas. <br><br>

There is a Modin library which acts as a wrapper against the Dask/Ray library, it optimizes the pandas code without adding another framework into the code. 


```python
import memory_profiler
filename = '/large_file.csv'
```


```python
%load_ext memory_profiler
l_path = Path(_base_location+chapter+filename)
```


```python
%%time
%memit pd.read_csv(l_path).groupby('label_source').count()

```

    peak memory: 307.48 MiB, increment: 93.58 MiB
    CPU times: user 227 ms, sys: 46.9 ms, total: 274 ms
    Wall time: 717 ms


###  2. <a name='B.ReadingTimeSeriesDatafromDatabases'></a>B. Reading Time Series Data from Databases

Reading TimeSeries Data from different databases with DatetimeIndex. <br>

Reading data from a relational database <br>
Reading data from Snowflake <br>
Reading data from a document database (MongoDB) <br>
Reading third-party financial data using APIs <br>
Reading data from a time series database (InfluxDB) <br>


####  2.1. <a name='B.1ReadingDatafromrelationaldatabases'></a>B.1 Reading Data from relational databases

To read data from relational databases we will start with Postgre SQL. We will be exploring two different methods to connect to PSQL. <br>
1. psycopg2 - python connector for PSQL.
2. SQL Alchemy - an object-relational mapper (ORM), with Pandas.

##### B.1.1 Using psycopg2


```python
import psycopg2
import pandas as pd
params = {"host": "127.0.0.1",
          "database": "postgres",
          "user": "postgres",
          "password": "password"} #parameter values to 
```

Connection can be established by passing the parameters to the .connect() method. object for cursor function can be used to execute SQL queries. The cursor object provides several attributes and methods, including execute() and fetchall().


```python
conn = psycopg2.connect(**params)
cursor = conn.cursor()
```


```python
cursor.execute("""SELECT date, last, volume
                FROM yen_tbl,
                ORDER BY date;""")
cursor.rowcount
```

Note: The returned result set after the query execution will not include a header.


```python
cursor.description # To grab the column names from the cursor object using description attribute.
columns = [col[0] for col in cursor.description]
columns
```


```python
data = cursor.fetchall()
df = pd.DataFrame(data, columns=columns)
df.info()
```


Another way to grab the table with column name is by instructing the cursor to return a **RealDictRow** class to the cursor_factory parameter


```python
from psycopg2.extras import RealDictCursor
cursor = conn.cursor(cursor_factory=RealDictCursor)
cursor.execute("SELECT * from yen_tbl;")
data = cursor.fetchall()
df = pd.DataFrame(data)
cursor.close()
conn.close()
```

psycopg2 connections and cursors can be used with Python's with statement for exceptions handling. when commiting an transaction. The cursor object provides three different fetching functions that is fetchall(), fetchmany() and fetchone(). Fetchone() returns a single tuple.


```python
import psycopg2
url = 'postgresql://postgres:password@localhost:5432'
with psycopg2.connect(url) as conn:
    with conn.cursor() as cursor:
        cursor.excecute("SELECT * FROM yen_tbl")
        data = cursor.fetchall()

```

##### B.1.2 Using SQL Alchemy

SQL Alchemy is ORM (object relational mapper), which provides abstraction layer to use OOP to interact with relational database.<br>
SQL Alchemy integrates well with pandas, and pandas SQL reader and writer functions depend on SQL Alchemy as abstraction layer.<br><br>

Some of the pandas reader function that rely on SQLAlchemy include, **pandas.read_sql(), pandas.read_sql_query() and pandas.read_sql_table()**


```python
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432") #SQLAlchemy uses the dialect and the driver (DBAPI).
query = "SELECT * FROM yen_tbl"
df = pd.read_sql(query,
                 engine,
                 index_col='date',
                 parse_dates={'date': '%Y-%m-%d'})

df['last'].tail(3)
```

The same can be accomplished by using pandas.read_sql_query() function.


```python
df = pd.read_sql_query(query,
                       engine,
                       index_col='date',
                       parse_dates={'date':'%Y-%m-%d'})
df['last'].tail(3)
```

Pandas has another SQL reader function called pandas.read_sql_table() that does noth take a SQL query, instead taking a table name. This can be considered as SELECT * FROM sometable query.


```python
df = pd.read_sql_table('yen_tbl',
                        engine,
                        index_col='date')
df.index[0]
```

AWS Redshift is based on PostgreSQL, hence the same connection (read, dialect and DBAPI) can be used to connect to AWS Redshift warehouse.


```python
from configparser import ConfigParser
config = ConfigParser()
config.read('snow.cfg')
config.sections()

params = dict(config['AWS'])
```


```python
import pandas as pd
from sqlalchemy import create_engine
host = params['host']
port = 5439
database = 'dev'
username = params['username']
chunksize = 1000
password = params['password']
query = "SELECT * FROM yen_tbl"

aws_engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:\
                   {port}/{database}")

df = pd.read_sql(query,
                 aws_engine,
                 index_col='date',
                 parse_dates=True,
                 chunksize=chunksize)

next(df)
```

##### B.2 Reading Data from Snowflake

Installation required: <br>

“conda install -c conda-forge snowflake-sqlalchemy snowflake-connector-python” <br>
“pip install "snowflake-connector-python[pandas]" <br>
pip install --upgrade snowflake-sqlalchemy”



```python
import pandas as pd
from snowflake import connector
from configparser import ConfigParser
```


```python
connector.paramstyle='qmark'
config = ConfigParser()
config.read('snow.cfg')
config.sections()
params = dict(config['SNOWFLAKE'])

```


```python
con = connector.connect(**params)
cursor = con.cursor()
```


```python
query = "select * from ORDERS;"
cursor.execute(query)

df = cursor.fetch_pandas_all()
df.info()
```

From the preceding output, you can see that the DataFrame's Index is just a sequence of numbers and that the O_ORDERDATE column is not a Date field. <br> This can be fixed by parsing the O_ORDERDATE column with a DatetimeIndex and setting it as an index for the DataFrame:<br>




```python
df_ts = (df.set_index(pd.to_datetime(df['O_ORDERDATE']))
            .drop(columns='O_ORDERDATE'))
df_ts.iloc[0:3, 1:5]
```

##### B.3 Reading data from third-party financial data using APIs

pandas-datareader, which provides remote data access to extract data from multiple data sources, including Yahoo Finance, Quandl, and Alpha Vantage, etc. <br>
The library not only fetches the data but also returns the data as a pandas DataFrame and the index as a DatetimeIndex”



```python
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import pandas_datareader.data as web
```


```python
start_date = (datetime.datetime.today() -
        datetime.timedelta(weeks=52*10)).strftime('%Y-%m-%d')
end_date = datetime.datetime.today().strftime('%Y-%m-%d')
tickers = ['MSFT','AAPL']

```


```python
dt = web.DataReader(name=tickers,
                    data_source='yahoo',
                    start=start_date,
                    end=end_date)['Adj Close']
dt.tail(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Symbols</th>
      <th>MSFT</th>
      <th>AAPL</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-01</th>
      <td>260.399994</td>
      <td>157.960007</td>
    </tr>
    <tr>
      <th>2022-09-02</th>
      <td>256.059998</td>
      <td>155.809998</td>
    </tr>
  </tbody>
</table>
</div>




```python
dt = web.get_data_yahoo(tickers)['Adj Close']
dt.tail(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Symbols</th>
      <th>MSFT</th>
      <th>AAPL</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-01</th>
      <td>260.399994</td>
      <td>157.960007</td>
    </tr>
    <tr>
      <th>2022-09-02</th>
      <td>256.059998</td>
      <td>155.809998</td>
    </tr>
  </tbody>
</table>
</div>



Additionally, the library provides other high-level functions for many of the data sources, as follows:

    get_data_quandl

    get_data_tiingo

    get_data_alphavantage

    get_data_fred

    get_data_stooq

    get_data_moex



####  2.2. <a name='B.4ReadingdatafromtimeseriesdatabaseInfluxDB'></a>B.4 Reading data from time series database (InfluxDB)

A time serie DB, a type of NoSQL database, is optimized for time-stamped or time series data for improved performance, especially when working with large datasets containing IoT data or sensor data. 

Installation required; InfluxDB python SDK

“conda install -c conda-forge influxdb-client”


```python
from influxdb_client import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
```


```python
token = "WXT1Hkn-Hg3FGvKChg4UQ2IW2c2_zprqbj63A1GntGVVQIZ2wZP8egDSD91MH_56sM8LbheJ1WZjM1iNI_60NQ=="
org = "my-org"
bucket = "noaa"
```


```python
client = InfluxDBClient(url="http://localhost:8086", 
                        token=token,
                        org=org)
```


```python
query_api = client.query_api()
```


```python
query = '''
        from(bucket: "noaa")
            |> range(start: 2019-09-01T00:00:00Z)
            |> filter(fn: (r) => r._measurement == "h2o_temperature")
            |> filter(fn: (r) => r.location == "coyote_creek")
            |> filter(fn: (r) => r._field == "degrees")
            |> movingAverage(n: 120)
        '''
result = client.query_api().query_data_frame( query=query)
```


```python
result.info()
```


```python
result.loc[0:5, '_time':'_value']
```


```python
result.set_index('_time')['_value'].plot()
plt.show()
```


```python
result = query_api.query_data_frame(query=query,                                 
                                    data_frame_index=['_time'])
result['_value'].head()
```

query_api gives us additional methods to interact with our bucket:
			
    query() returns the result as a FluxTable.

    query_csv() returns the result as a CSV iterator (CSV reader).

    query_data_frame() returns the result as a pandas DataFrame.

    query_data_frame_stream() returns a stream of pandas DataFrames as a generator.

    query_raw() returns the result as raw unprocessed data in s string format.

    query_stream() is similar to query_data_frame_stream but returns a stream of FluxRecord as a generator.


###  3. <a name='C.Writingtimeseriesdatatofiles'></a>C. Writing timeseries data to files

###  4. <a name='D.WritingtimeseriesdatatoDatabases'></a>D. Writing time series data to Databases

###  5. <a name='E.WorkingwithDateandTimeinPython'></a>E. Working with Date and Time in Python

####  5.1. <a name='E.1DatetimeIndex'></a>E.1 DatetimeIndex

It is critical to understand DatetimeIndex to work with Date and Time datasets.

**pandas.to_datetime()** - it returns Timestamp object, is a powerful function that can intelligently parse different date representations from strings.


```python
import datetime as dt
import numpy as np
```


```python
dates = ['2021-1-1', '2021-1-2']
dates_pd = pd.to_datetime(dates)
```


```python
dates_pd #produces sequence of type DatetimeIndex.
```




    DatetimeIndex(['2021-01-01', '2021-01-02'], dtype='datetime64[ns]', freq=None)



The DatetimeIndex object gives access to many useful properties and methods to extract additional date and time properties. As an example, you can extract <br>
day_name, <br>
month, <br>
year, <br>
days_in_month, <br> 
quarter, <br>
is_quarter_start, <br> 
is_leap_year, <br>
is_month_start, <br>
is_month_end, and <br>
is_year_start.”<br>



```python
dates = ['2021-01-01', # date str format %Y-%m-%d
         '2/1/2021', # date str format %m/%d/%Y
         '03-01-2021', # date  str format %m-%d-%Y
         'April 1, 2021', # date  str format %B %d, %Y
         '20210501', # date str format %Y%m%d
          np.datetime64('2021-07-01'), # numpy datetime64
          dt.datetime(2021, 8, 1), # python datetime
          pd.Timestamp(2021,9,1) # pandas Timestamp
          ]
```


```python
parsed_dates = pd.to_datetime(
                 dates,
                 infer_datetime_format=True,
                 errors='coerce' #pandas.to_datetime() contains error parameter which sets any value it could not parse as NaT indicating a missing value.
                 )

print(parsed_dates)
```

    DatetimeIndex(['2021-01-01', '2021-02-01', '2021-03-01', '2021-04-01',
                   '2021-05-01', '2021-07-01', '2021-08-01', '2021-09-01'],
                  dtype='datetime64[ns]', freq=None)


Error parameter in **pandas.to_datetime** can take one of the three valid string option:
1. raise - which means it will raise exception (error out).
2. coerce - will not cause exception. Instead, it will just replace pd.NaT, indicating a missing datetime value.
3. ignore - will not cause exception, it will pass the original value.


```python
example = pd.to_datetime(['something 2021', 'Jan 1, 2021'], errors='ignore')
example
```




    Index(['something 2021', 'Jan 1, 2021'], dtype='object')




```python
print(f'Name of Day : {parsed_dates.day_name()}')
print(f'Month : {parsed_dates.month}')
print(f'Year : {parsed_dates.year}')
print(f'Days in Month : {parsed_dates.days_in_month}')
print(f'Quarter {parsed_dates.quarter}')
print(f'Quarter Start : {parsed_dates.is_quarter_start}')
print(f'Leap Year : {parsed_dates.is_leap_year}')
print(f'Month Start : {parsed_dates.is_month_start}')
print(f'Month End : {parsed_dates.is_month_end}')
print(f'Year Start : {parsed_dates.is_year_start}')
```

    Name of Day : Index(['Friday', 'Monday', 'Monday', 'Thursday', 'Saturday', 'Thursday',
           'Sunday', 'Wednesday'],
          dtype='object')
    Month : Int64Index([1, 2, 3, 4, 5, 7, 8, 9], dtype='int64')
    Year : Int64Index([2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021], dtype='int64')
    Days in Month : Int64Index([31, 28, 31, 30, 31, 31, 31, 30], dtype='int64')
    Quarter Int64Index([1, 1, 1, 2, 2, 3, 3, 3], dtype='int64')
    Quarter Start : [ True False False  True False  True False False]
    Leap Year : [False False False False False False False False]
    Month Start : [ True  True  True  True  True  True  True  True]
    Month End : [False False False False False False False False]
    Year Start : [ True False False False False False False False]


pandas.date_range() - It is an alternate way to generate DatetimeIndex. <br>
It requires three parameters:  <br>
start, <br>
end,<br>
period, and <br>
freq <br>


```python
pd.date_range(start='2021-01-01', periods=3, freq='D')
```




    DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03'], dtype='datetime64[ns]', freq='D')




```python
pd.date_range(start='2021-01-01', periods=3, freq='D')
```




    DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03'], dtype='datetime64[ns]', freq='D')




```python
pd.date_range(start='2021-01-01',
               end='2021-01-03',
               freq='D')
```




    DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03'], dtype='datetime64[ns]', freq='D')




```python
pd.date_range(start='2021-01-01',
               periods=3)
```




    DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03'], dtype='datetime64[ns]', freq='D')



####  5.2. <a name='E.2DateTimeFormatting'></a>E.2  DateTime Formatting

At times date column is stored as string format, to have more control, to ensure date is parsed correctly as date/datetime object we use strptime() method from datetime module.

We will parse four different representations of January 1, 2022 that will produce the same output – datetime.datetime(2022, 1, 1, 0, 0)



```python
dt.datetime.strptime('1/1/2022', '%m/%d/%Y')
```




    datetime.datetime(2022, 1, 1, 0, 0)




```python
dt.datetime.strptime('1/1/2022', '%m/%d/%Y').date()
```




    datetime.date(2022, 1, 1)




```python
dt.datetime.strptime('1 January, 2022', '%d %B, %Y').date()
```




    datetime.date(2022, 1, 1)




```python
dt.datetime.strptime('1-Jan-2022', '%d-%b-%Y').date()
```




    datetime.date(2022, 1, 1)




```python
dt.datetime.strptime('Saturday, January 1, 2022', '%A, %B %d, %Y').date()
```




    datetime.date(2022, 1, 1)



####  5.3. <a name='E.3TranformingapandasDataFrametoatimeseriesDataFrame'></a>E.3 Tranforming a pandas DataFrame to a time series DataFrame


```python
df = pd.DataFrame(
        {'Date': ['January 1, 2022', 'January 2, 2022', 'January 3, 2022'],
         'Sales': [23000, 19020, 21000]}
            )
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>January 1, 2022</td>
      <td>23000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>January 2, 2022</td>
      <td>19020</td>
    </tr>
    <tr>
      <th>2</th>
      <td>January 3, 2022</td>
      <td>21000</td>
    </tr>
  </tbody>
</table>
</div>




```python
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True)
df.info() #since Date is now index
```

    <class 'pandas.core.frame.DataFrame'>
    DatetimeIndex: 3 entries, 2022-01-01 to 2022-01-03
    Data columns (total 1 columns):
     #   Column  Non-Null Count  Dtype
    ---  ------  --------------  -----
     0   Sales   3 non-null      int64
    dtypes: int64(1)
    memory usage: 48.0 bytes


####  5.4. <a name='E.4WorkingwithTimeDeltas'></a>E.4 Working with Time Deltas


```python
df = pd.DataFrame(
        {       
        'item': ['item1', 'item2', 'item3', 'item4', 'item5', 'item6'],
        'purchase_dt': pd.date_range('2021-01-01', periods=6, freq='D', tz='UTC')
        }
)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2021-01-01 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-02 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-03 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-04 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-05 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>item6</td>
      <td>2021-01-06 00:00:00+00:00</td>
    </tr>
  </tbody>
</table>
</div>



The **Timedelta** class makes it possible to derive new datetime objects by adding or subtracting at different ranges or increments, such as seconds, daily, and weekly. This includes time zone-aware calculations



```python
df['expiration_dt'] = df['purchase_dt'] + pd.Timedelta(days=30)
```


```python
df['extended_dt'] = df['purchase_dt'] +\
                pd.Timedelta('35 days 12 hours 30 minutes')
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
      <th>expiration_dt</th>
      <th>extended_dt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2021-01-01 00:00:00+00:00</td>
      <td>2021-01-31 00:00:00+00:00</td>
      <td>2021-02-05 12:30:00+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-02 00:00:00+00:00</td>
      <td>2021-02-01 00:00:00+00:00</td>
      <td>2021-02-06 12:30:00+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-03 00:00:00+00:00</td>
      <td>2021-02-02 00:00:00+00:00</td>
      <td>2021-02-07 12:30:00+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-04 00:00:00+00:00</td>
      <td>2021-02-03 00:00:00+00:00</td>
      <td>2021-02-08 12:30:00+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-05 00:00:00+00:00</td>
      <td>2021-02-04 00:00:00+00:00</td>
      <td>2021-02-09 12:30:00+00:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>item6</td>
      <td>2021-01-06 00:00:00+00:00</td>
      <td>2021-02-05 00:00:00+00:00</td>
      <td>2021-02-10 12:30:00+00:00</td>
    </tr>
  </tbody>
</table>
</div>



Time zone conversion from UTC to local time zone of Los Angeles.


```python
df.iloc[:,1:] = df.iloc[: ,1:].apply(
            lambda x: x.dt.tz_convert('US/Pacific')
                )
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
      <th>expiration_dt</th>
      <th>extended_dt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2020-12-31 16:00:00-08:00</td>
      <td>2021-01-30 16:00:00-08:00</td>
      <td>2021-02-05 04:30:00-08:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-01 16:00:00-08:00</td>
      <td>2021-01-31 16:00:00-08:00</td>
      <td>2021-02-06 04:30:00-08:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-02 16:00:00-08:00</td>
      <td>2021-02-01 16:00:00-08:00</td>
      <td>2021-02-07 04:30:00-08:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-03 16:00:00-08:00</td>
      <td>2021-02-02 16:00:00-08:00</td>
      <td>2021-02-08 04:30:00-08:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-04 16:00:00-08:00</td>
      <td>2021-02-03 16:00:00-08:00</td>
      <td>2021-02-09 04:30:00-08:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>item6</td>
      <td>2021-01-05 16:00:00-08:00</td>
      <td>2021-02-04 16:00:00-08:00</td>
      <td>2021-02-10 04:30:00-08:00</td>
    </tr>
  </tbody>
</table>
</div>




```python
df['exp_ext_diff'] = (
         df['extended_dt'] - df['expiration_dt']
        )
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
      <th>expiration_dt</th>
      <th>extended_dt</th>
      <th>exp_ext_diff</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2020-12-31 16:00:00-08:00</td>
      <td>2021-01-30 16:00:00-08:00</td>
      <td>2021-02-05 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-01 16:00:00-08:00</td>
      <td>2021-01-31 16:00:00-08:00</td>
      <td>2021-02-06 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-02 16:00:00-08:00</td>
      <td>2021-02-01 16:00:00-08:00</td>
      <td>2021-02-07 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-03 16:00:00-08:00</td>
      <td>2021-02-02 16:00:00-08:00</td>
      <td>2021-02-08 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-04 16:00:00-08:00</td>
      <td>2021-02-03 16:00:00-08:00</td>
      <td>2021-02-09 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>item6</td>
      <td>2021-01-05 16:00:00-08:00</td>
      <td>2021-02-04 16:00:00-08:00</td>
      <td>2021-02-10 04:30:00-08:00</td>
      <td>5 days 12:30:00</td>
    </tr>
  </tbody>
</table>
</div>



##### E.4.1 Time Delta

pandas.Timedelta is used for capturing the difference between two date or time objects. <br>
pandas.Timedelta and datetime.timedelta are equivalent.


```python
week_td = pd.Timedelta('1W')
print(pd.to_datetime('1 JAN 2022') + week_td)
print(pd.to_datetime('1 JAN 2022') + 2*week_td)
```

    2022-01-08 00:00:00
    2022-01-15 00:00:00



```python
df = pd.DataFrame(
    {
        'item': ['item1', 'item2', 'item3', 'item4', 'item5', 'item6'],
        'purchase_dt': pd.date_range('2021-01-01', periods=6, freq='D', tz='UTC')
    }
)
```


```python
df['1 week'] = pd.Timedelta('1W')
df['1 week_more'] = df['purchase_dt'] + df['1 week']
df['1_week_less'] = df['purchase_dt'] - df['1 week']
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
      <th>1 week</th>
      <th>1 week_more</th>
      <th>1_week_less</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2021-01-01 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-08 00:00:00+00:00</td>
      <td>2020-12-25 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-02 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-09 00:00:00+00:00</td>
      <td>2020-12-26 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-03 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-10 00:00:00+00:00</td>
      <td>2020-12-27 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-04 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-11 00:00:00+00:00</td>
      <td>2020-12-28 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-05 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-12 00:00:00+00:00</td>
      <td>2020-12-29 00:00:00+00:00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>item6</td>
      <td>2021-01-06 00:00:00+00:00</td>
      <td>7 days</td>
      <td>2021-01-13 00:00:00+00:00</td>
      <td>2020-12-30 00:00:00+00:00</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.timedelta_range('1W 2 days', periods=5)
```




    TimedeltaIndex(['9 days', '10 days', '11 days', '12 days', '13 days'], dtype='timedelta64[ns]', freq='D')




```python
df = pd.DataFrame(
        {       
        'item': ['item1', 'item2', 'item3', 'item4', 'item5'],
        'purchase_dt': pd.date_range('2021-01-01', periods=5, freq='D', tz='UTC'),
        'time_deltas': pd.timedelta_range('1W 2 days 6 hours', periods=5)
        }

)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item</th>
      <th>purchase_dt</th>
      <th>time_deltas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>item1</td>
      <td>2021-01-01 00:00:00+00:00</td>
      <td>9 days 06:00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>item2</td>
      <td>2021-01-02 00:00:00+00:00</td>
      <td>10 days 06:00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>item3</td>
      <td>2021-01-03 00:00:00+00:00</td>
      <td>11 days 06:00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>item4</td>
      <td>2021-01-04 00:00:00+00:00</td>
      <td>12 days 06:00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>item5</td>
      <td>2021-01-05 00:00:00+00:00</td>
      <td>13 days 06:00:00</td>
    </tr>
  </tbody>
</table>
</div>



####  5.5. <a name='E.5ConvertingDatetimewithTimeZoneinformation'></a>E.5 Converting Datetime with TimeZone information 

Time-series data requires attention to different time zones, when developing data pipelines, building a data warehouse, or integrating data between systems, dealing with time zones requires attention

There are multiple packages in python to deal with time zone: <br>
pytz <br>
dateutil <br>
zoneinfo <br>


```python
df = pd.DataFrame(
        {       
        'Location': ['Los Angeles', 
                     'New York',
                     'Berlin', 
                     'New Delhi', 
                     'Moscow', 
                     'Tokyo', 
                     'Dubai'],
        'tz': ['US/Pacific', 
               'US/Eastern', 
               'Europe/Berlin', 
               'Asia/Kolkata', 
               'Europe/Moscow', 
               'Asia/Tokyo',
               'Asia/Dubai'],
        'visit_dt': pd.date_range(start='22:00',periods=7, freq='45min'),
        }).set_index('visit_dt')
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-03 22:00:00</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
    </tr>
    <tr>
      <th>2022-09-03 22:45:00</th>
      <td>New York</td>
      <td>US/Eastern</td>
    </tr>
    <tr>
      <th>2022-09-03 23:30:00</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
    </tr>
    <tr>
      <th>2022-09-04 00:15:00</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
    </tr>
    <tr>
      <th>2022-09-04 01:00:00</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
    </tr>
    <tr>
      <th>2022-09-04 01:45:00</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
    </tr>
    <tr>
      <th>2022-09-04 02:30:00</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
    </tr>
  </tbody>
</table>
</div>



To convert this DataFrame to same time zone as in Tokyo. We can use DataFrame.tz_convert() against the DataFrame, but will get TypeError exception if you do this. <br>
That is because your time-series DataFrame is not time zone-aware. <br>
So, you need to localize it first using tz_localize() to make it time-zone aware. 


```python
df = df.tz_localize('UTC')
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-03 22:00:00+00:00</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
    </tr>
    <tr>
      <th>2022-09-03 22:45:00+00:00</th>
      <td>New York</td>
      <td>US/Eastern</td>
    </tr>
    <tr>
      <th>2022-09-03 23:30:00+00:00</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
    </tr>
    <tr>
      <th>2022-09-04 00:15:00+00:00</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
    </tr>
    <tr>
      <th>2022-09-04 01:00:00+00:00</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
    </tr>
    <tr>
      <th>2022-09-04 01:45:00+00:00</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
    </tr>
    <tr>
      <th>2022-09-04 02:30:00+00:00</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_hq = df.tz_convert('Asia/Tokyo')
df_hq
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-04 07:00:00+09:00</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
    </tr>
    <tr>
      <th>2022-09-04 07:45:00+09:00</th>
      <td>New York</td>
      <td>US/Eastern</td>
    </tr>
    <tr>
      <th>2022-09-04 08:30:00+09:00</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
    </tr>
    <tr>
      <th>2022-09-04 09:15:00+09:00</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
    </tr>
    <tr>
      <th>2022-09-04 10:00:00+09:00</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
    </tr>
    <tr>
      <th>2022-09-04 10:45:00+09:00</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
    </tr>
    <tr>
      <th>2022-09-04 11:30:00+09:00</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
    </tr>
  </tbody>
</table>
</div>




```python
df['local_dt'] = df.index
df['local_dt'] = df.apply(lambda x: pd.Timestamp.tz_convert(x['local_dt'], x['tz']), axis=1)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
      <th>local_dt</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-03 22:00:00+00:00</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
      <td>2022-09-03 15:00:00-07:00</td>
    </tr>
    <tr>
      <th>2022-09-03 22:45:00+00:00</th>
      <td>New York</td>
      <td>US/Eastern</td>
      <td>2022-09-03 18:45:00-04:00</td>
    </tr>
    <tr>
      <th>2022-09-03 23:30:00+00:00</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
      <td>2022-09-04 01:30:00+02:00</td>
    </tr>
    <tr>
      <th>2022-09-04 00:15:00+00:00</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
      <td>2022-09-04 05:45:00+05:30</td>
    </tr>
    <tr>
      <th>2022-09-04 01:00:00+00:00</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
      <td>2022-09-04 04:00:00+03:00</td>
    </tr>
    <tr>
      <th>2022-09-04 01:45:00+00:00</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
      <td>2022-09-04 10:45:00+09:00</td>
    </tr>
    <tr>
      <th>2022-09-04 02:30:00+00:00</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
      <td>2022-09-04 06:30:00+04:00</td>
    </tr>
  </tbody>
</table>
</div>



It is important to standardize on UTC if you are dealing with different time zones and daylight saving, since UTC is always consistent and never changes (regardless of where you are or if daylight saving time is applied or not). <br>
Once in UTC, converting to other time zones is very straightforward.


```python
df = pd.DataFrame(
        {       
        'Location': ['Los Angeles', 
                     'New York',
                     'Berlin', 
                     'New Delhi', 
                     'Moscow', 
                     'Tokyo', 
                     'Dubai'],
        'tz': ['US/Pacific', 
               'US/Eastern', 
               'Europe/Berlin', 
               'Asia/Kolkata', 
               'Europe/Moscow', 
               'Asia/Tokyo',
               'Asia/Dubai'],
        'visit_dt': pd.date_range(start='22:00',periods=7, freq='45min'),
        }).set_index('visit_dt').tz_localize('UTC').tz_convert('Asia/Tokyo')
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-04 07:00:00+09:00</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
    </tr>
    <tr>
      <th>2022-09-04 07:45:00+09:00</th>
      <td>New York</td>
      <td>US/Eastern</td>
    </tr>
    <tr>
      <th>2022-09-04 08:30:00+09:00</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
    </tr>
    <tr>
      <th>2022-09-04 09:15:00+09:00</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
    </tr>
    <tr>
      <th>2022-09-04 10:00:00+09:00</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
    </tr>
    <tr>
      <th>2022-09-04 10:45:00+09:00</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
    </tr>
    <tr>
      <th>2022-09-04 11:30:00+09:00</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.index = df.index.strftime('%Y-%m-%d %H:%M %p') # to format datetime to morning AM or evening PM
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Location</th>
      <th>tz</th>
    </tr>
    <tr>
      <th>visit_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2022-09-04 07:00 AM</th>
      <td>Los Angeles</td>
      <td>US/Pacific</td>
    </tr>
    <tr>
      <th>2022-09-04 07:45 AM</th>
      <td>New York</td>
      <td>US/Eastern</td>
    </tr>
    <tr>
      <th>2022-09-04 08:30 AM</th>
      <td>Berlin</td>
      <td>Europe/Berlin</td>
    </tr>
    <tr>
      <th>2022-09-04 09:15 AM</th>
      <td>New Delhi</td>
      <td>Asia/Kolkata</td>
    </tr>
    <tr>
      <th>2022-09-04 10:00 AM</th>
      <td>Moscow</td>
      <td>Europe/Moscow</td>
    </tr>
    <tr>
      <th>2022-09-04 10:45 AM</th>
      <td>Tokyo</td>
      <td>Asia/Tokyo</td>
    </tr>
    <tr>
      <th>2022-09-04 11:30 AM</th>
      <td>Dubai</td>
      <td>Asia/Dubai</td>
    </tr>
  </tbody>
</table>
</div>



####  5.6. <a name='E.6Workingwithdateoffsets'></a>E.6 Working with date offsets

Offsets are useful in transforming dates into something more meaningful and relatable.


```python
np.random.seed(10)
df = pd.DataFrame(
        {       
        'purchase_dt': pd.date_range('2021-01-01', periods=6, freq='D'),
        'production' : np.random.randint(4, 20, 6)
        }).set_index('purchase_dt')
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>13</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>17</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>8</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>19</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>




```python
df['day'] = df.index.day_name() # While getting the day name is useful, better would be to know working days information.
```

pandas.offsets.BDay() - can be used to get weekend/holiday information


```python
df['BusinessDay'] = df.index + pd.offsets.BDay(0) #to populate the last business day
df['BDay Name'] = df['BusinessDay'].dt.day_name() 
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
      <th>day</th>
      <th>BusinessDay</th>
      <th>BDay Name</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>13</td>
      <td>Friday</td>
      <td>2021-01-01</td>
      <td>Friday</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>17</td>
      <td>Saturday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>8</td>
      <td>Sunday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>19</td>
      <td>Monday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
      <td>Tuesday</td>
      <td>2021-01-05</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>5</td>
      <td>Wednesday</td>
      <td>2021-01-06</td>
      <td>Wednesday</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.reset_index().groupby(['purchase_dt', 'day']).sum()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>production</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th>day</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <th>Friday</th>
      <td>13</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <th>Saturday</th>
      <td>17</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <th>Sunday</th>
      <td>8</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <th>Monday</th>
      <td>19</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <th>Tuesday</th>
      <td>4</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <th>Wednesday</th>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.groupby(['BusinessDay', 'BDay Name']).sum() #Monday seems to be the most productive day
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>production</th>
    </tr>
    <tr>
      <th>BusinessDay</th>
      <th>BDay Name</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <th>Friday</th>
      <td>13</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <th>Monday</th>
      <td>44</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <th>Tuesday</th>
      <td>4</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <th>Wednesday</th>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



To track production monthly (MonthEnd) and quarterly (QuarterEnd). We can use pandas.offsets


```python
df['QuarterEnd'] = df.index + pd.offsets.QuarterEnd(0)
df['MonthEnd'] = df.index + pd.offsets.MonthEnd(0)
df['BusinessDay'] = df.index + pd.offsets.BDay(0)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
      <th>day</th>
      <th>BusinessDay</th>
      <th>BDay Name</th>
      <th>QuarterEnd</th>
      <th>MonthEnd</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>13</td>
      <td>Friday</td>
      <td>2021-01-01</td>
      <td>Friday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>17</td>
      <td>Saturday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>8</td>
      <td>Sunday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>19</td>
      <td>Monday</td>
      <td>2021-01-04</td>
      <td>Monday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
      <td>Tuesday</td>
      <td>2021-01-05</td>
      <td>Tuesday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>5</td>
      <td>Wednesday</td>
      <td>2021-01-06</td>
      <td>Wednesday</td>
      <td>2021-03-31</td>
      <td>2021-01-31</td>
    </tr>
  </tbody>
</table>
</div>



Date offsets made it possible to increment, decrement, and transform your dates to a new date range following specific rules. There are several offsets provided by pandas, each with its own rules, which can be applied to your dataset. Here is a list of the common offsets available in pandas:


    BusinessDay or Bday
    MonthEnd
    BusinessMonthEnd or BmonthEnd
    CustomBusinessDay or Cday
    QuarterEnd
    FY253Quarter


BusinessDay (BDay) offset that it did not account for the New Year's Day holiday (January 1). So, what can be done to account for both the New Year's Day holiday and weekends? 

To accomplish this, pandas provides two approaches to handle standard holidays. The first is by defining a custom holiday. <br> The second approach uses an existing holiday offset.


```python
from pandas.tseries.holiday import (
    USFederalHolidayCalendar
)

df = pd.DataFrame(
        {       
        'purchase_dt': pd.date_range('2021-01-01', periods=6, freq='D'),
        'production' : np.random.randint(4, 20, 6)
        }).set_index('purchase_dt')

df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>15</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>16</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>13</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>17</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>17</td>
    </tr>
  </tbody>
</table>
</div>



First : Existing offset.<br> For this example, dealing with New Year, you can use the USFederalHolidayCalendar class, which has standard holidays such as New Year, Christmas, and other holidays specific to the United States.



```python
USFederalHolidayCalendar.rules
```




    [Holiday: New Years Day (month=1, day=1, observance=<function nearest_workday at 0x7f839c17b160>),
     Holiday: Martin Luther King Jr. Day (month=1, day=1, offset=<DateOffset: weekday=MO(+3)>),
     Holiday: Presidents Day (month=2, day=1, offset=<DateOffset: weekday=MO(+3)>),
     Holiday: Memorial Day (month=5, day=31, offset=<DateOffset: weekday=MO(-1)>),
     Holiday: July 4th (month=7, day=4, observance=<function nearest_workday at 0x7f839c17b160>),
     Holiday: Labor Day (month=9, day=1, offset=<DateOffset: weekday=MO(+1)>),
     Holiday: Columbus Day (month=10, day=1, offset=<DateOffset: weekday=MO(+2)>),
     Holiday: Veterans Day (month=11, day=11, observance=<function nearest_workday at 0x7f839c17b160>),
     Holiday: Thanksgiving (month=11, day=1, offset=<DateOffset: weekday=TH(+4)>),
     Holiday: Christmas (month=12, day=25, observance=<function nearest_workday at 0x7f839c17b160>)]




```python
df['USFederalHolidays'] = df.index + pd.offsets.CDay(calendar=USFederalHolidayCalendar())
df
```

    /Users/rahuladlakha/opt/anaconda3/lib/python3.9/site-packages/pandas/core/arrays/datetimes.py:741: PerformanceWarning: Non-vectorized DateOffset being applied to Series or DatetimeIndex
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
      <th>USFederalHolidays</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>15</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>16</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>13</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>17</td>
      <td>2021-01-05</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
      <td>2021-01-06</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>17</td>
      <td>2021-01-07</td>
    </tr>
  </tbody>
</table>
</div>




```python
from pandas.tseries.holiday import (
			Holiday,
			nearest_workday,
			USFederalHolidayCalendar)

newyears = Holiday("New Years", month=1, day=1, observance=nearest_workday)
newyears
```




    Holiday: New Years (month=1, day=1, observance=<function nearest_workday at 0x7f839c17b160>)




```python
from calendar import calendar


df['NewYearsHoliday'] = df.index+pd.offsets.CDay(calendar=newyears)
df
```

    /Users/rahuladlakha/opt/anaconda3/lib/python3.9/site-packages/pandas/core/arrays/datetimes.py:741: PerformanceWarning: Non-vectorized DateOffset being applied to Series or DatetimeIndex
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>production</th>
      <th>USFederalHolidays</th>
      <th>NewYearsHoliday</th>
    </tr>
    <tr>
      <th>purchase_dt</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2021-01-01</th>
      <td>15</td>
      <td>2021-01-04</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-02</th>
      <td>16</td>
      <td>2021-01-04</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-03</th>
      <td>13</td>
      <td>2021-01-04</td>
      <td>2021-01-04</td>
    </tr>
    <tr>
      <th>2021-01-04</th>
      <td>17</td>
      <td>2021-01-05</td>
      <td>2021-01-05</td>
    </tr>
    <tr>
      <th>2021-01-05</th>
      <td>4</td>
      <td>2021-01-06</td>
      <td>2021-01-06</td>
    </tr>
    <tr>
      <th>2021-01-06</th>
      <td>17</td>
      <td>2021-01-07</td>
      <td>2021-01-07</td>
    </tr>
  </tbody>
</table>
</div>



####  5.7. <a name='E.6Custombusinessdays'></a>E.6 Custom business days


```python
dubai_uae_workdays = "Sun Mon Tue Wed Thu"

# UAE national day
nationalDay = [pd.to_datetime('2021-12-2')]


dubai_uae_bday = pd.offsets.CDay(
    holidays=nationalDay,
    weekmask=dubai_uae_workdays,
)
```


```python
df = pd.DataFrame({'Date': pd.date_range('12-1-2021', periods=10, freq=dubai_uae_bday )})
df['Day_name'] = df.Date.dt.day_name()
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Day_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2021-12-01</td>
      <td>Wednesday</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2021-12-05</td>
      <td>Sunday</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2021-12-06</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2021-12-07</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2021-12-08</td>
      <td>Wednesday</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2021-12-09</td>
      <td>Thursday</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2021-12-12</td>
      <td>Sunday</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2021-12-13</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2021-12-14</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2021-12-15</td>
      <td>Wednesday</td>
    </tr>
  </tbody>
</table>
</div>



Working with date offsets but focuses on customizing offsets. pandas provides several offsets that can take a custom calendar, holiday, and weekmask. 

These include the following:

    CustomBusinessDay or Cday
    CustomBusinessMonthEnd or CBMonthEnd
    CustomBusinessMonthBegin or CBMonthBegin
    CustomBusinessHour

####  5.8. <a name='E.7CustomBusinessDayhours'></a>E.7 Custom Business Day hours


```python
b_hours = pd.offsets.BusinessHour()
b_hours
```




    <BusinessHour: BH=09:00-17:00>




```python
cust_hours = pd.offsets.CustomBusinessHour(
    start="8:30",
    end="15:30",
    holidays=nationalDay,
    weekmask=dubai_uae_workdays)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Day_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2021-12-01</td>
      <td>Wednesday</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2021-12-05</td>
      <td>Sunday</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2021-12-06</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2021-12-07</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2021-12-08</td>
      <td>Wednesday</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2021-12-09</td>
      <td>Thursday</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2021-12-12</td>
      <td>Sunday</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2021-12-13</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2021-12-14</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2021-12-15</td>
      <td>Wednesday</td>
    </tr>
  </tbody>
</table>
</div>




```python
df['Date'] + cust_hours * 16
```

    /Users/rahuladlakha/opt/anaconda3/lib/python3.9/site-packages/pandas/core/arrays/datetimes.py:741: PerformanceWarning: Non-vectorized DateOffset being applied to Series or DatetimeIndex
      warnings.warn(





    0   2021-12-06 10:30:00
    1   2021-12-07 10:30:00
    2   2021-12-08 10:30:00
    3   2021-12-09 10:30:00
    4   2021-12-12 10:30:00
    5   2021-12-13 10:30:00
    6   2021-12-14 10:30:00
    7   2021-12-15 10:30:00
    8   2021-12-16 10:30:00
    9   2021-12-19 10:30:00
    Name: Date, dtype: datetime64[ns]



###  6. <a name='F.HandlingMissingData'></a>F. Handling Missing Data

Missing data and outliers are two common problems that need to be dealt with during data cleaning and preparation. <br>
Time series data is no different, and before plugging the data into any analysis or modeling workflow, you must investigate the data first.<br>We will explore techniques to handle missing data through imputation and interpolation.


Steps to be followed for handling missing data: ingest the data into a DataFrame, **identify missing data, impute missing data, evaluate it against the original data**, and finally, visualize **and compare the different imputation techniques**.



```python
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.dates as mdates

import matplotlib
import sklearn
import statsmodels as sm
```


```python
def read_dataset(folder, file, date_col=None):
    '''
    Reading the dataset
    folder: is a Path object
    file: the CSV filename in that Path object. 
    date_col: specify a date_col to use as index_col 
    
    returns: a pandas DataFrame with a DatetimeIndex
    '''
    df = pd.read_csv(folder / file, 
                     index_col=date_col, 
                     parse_dates=[date_col])
    return df
```

We will be ploting two DataFrames: one original with no missing data and other with missing / imputed dataframe to compare against.<br>
The function creates multiple time series subplots using the specified response column (col). We will be plotting each imputation technique for visual comparison.


```python
def plot_dfs(df1, df2, col, title=None, xlabel=None, ylabel=None):

    '''	df1: original dataframe without missing data
    df2: dataframe with missing data
    col: column name that contains missing data'''  

    df_missing = df2.rename(columns={col: 'missing'})

    columns = df_missing.loc[:, 'missing':].columns.tolist()
    subplot_size = len(columns)
    fig, ax = plt.subplots(subplot_size+1, 1, sharex=True)
    plt.subplots_adjust(hspace=0.25)
    fig.suptitle = title
    df1[col].plot(ax=ax[0], figsize=(10, 12))
    ax[0].set_title('Original Dataset')
    ax[0].set_xlabel(xlabel)
    ax[0].set_ylabel(ylabel)

    for i, colname in enumerate(columns):
        df_missing[colname].plot(ax=ax[i+1])
        ax[i+1].set_title(colname.upper())
    plt.show()

```

RMSE Score - In addition to compare the the non-missing DataFrame with imputed DataFrame we will need statistical measure.


```python
def rmse_score(df1, df2, col=None):
    '''
    df1: original dataframe without missing data
    df2: dataframe with missing data
    col: column name that contains missing data

    returns: a list of scores
    '''
    df_missing = df2.rename(columns={col: 'missing'})
    columns = df_missing.loc[:, 'missing':].columns.tolist()
    scores = []
    dict = {}
    for comp_col in columns[1:]:
        rmse = np.sqrt(np.mean((df1[col] - df_missing[comp_col])**2))
        scores.append(rmse)
        print(f'RMSE for {comp_col}: {rmse}')
        min_rmse = min(scores)
        dict[comp_col] = [rmse]
        func = min(dict, key=dict.get)
    print(f'Mininum RMSE belongs to {func} is {min_rmse}')
    
    return scores
```

Missing data is inevitable. It is important to formulate right strategy to deal with missing data. <br>

**One approach** could be to remove the missing data, drop the observations. This may not be the best strategy if you have limited data in first place. And if done prematurely then the drawback could be, you'll never know if the missing data was due to censoring or due to bias.

**Second approach** could be to tag the rows with missing data by adding a column, describing or labeling the missing data.

**Third approach** could be to estimate the missing data values. The method could range from simple to more complex techniques leveraging ML and stat models.

One of the most important piece is to measure the accuracy of the estimated values for data missing in the first place.<br>
There are different approaches, emphasizing a through evaluation and validation process to select ideal way for each situation.

**RMSE - Root Mean Squared Error** - To evaluate different imputation techniques

The process to calculate the RMSE can be broken down into a few simple steps: first, computing the error, which is the difference between the actual values and the predicted or estimated values. 
This is done for each observation. <br> Since the errors may be either negative or positive, and to avoid having a zero summation, the errors (differences) are squared. <br>
Finally, all the errors are summed and divided by the total number of observations to compute the mean. This gives you the **Mean Squared Error (MSE)**. RMSE is just the square root of the MSE.

$RMSE$ = $\huge\sqrt{\frac{\sum_{i=1}^{N}(x-\widehat{x_i})^2}{N}}$

$x$ - original value; <br>
$\widehat{x_i}$ - imputed value <br>
$N$ - Number of observations



RMSE is commonly used to measure the performance of predictive models.<br>
A lower RMSE is desirable; it tells us that the model can fit the dataset. It tells us tht the average distance (error) between the predicted value and the actual value. We want this distance minimized for the a well fit model.

When comparing different imputation methods, we want our imputed values to resemble (as close as possible) to the actual data, which contains random effects. This means we are not seeking a perfect prediction and thus a lower RMSE score does not necessarily indicate a better imputation method. <br><br>
RMSE compared with visualization of original and imputation to understand different techniques compare and work.


```python
chapter='ch7/'
```


```python
filename='/co2_missing.csv'
```


```python
filename1='/clicks_missing.csv'
```


```python
folder1 = Path(_base_location+chapter+filename1)
```


```python
folder = Path(_base_location+chapter+filename)
```


```python
co2_df = pd.read_csv(folder, parse_dates=['year'])
ecom_df = pd.read_csv(folder1, parse_dates=['date'])
```


```python
ecom_df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>price</th>
      <th>location</th>
      <th>clicks</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2008-04-01</td>
      <td>43.155647</td>
      <td>2</td>
      <td>18784.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2008-04-02</td>
      <td>43.079056</td>
      <td>1</td>
      <td>24738.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2008-04-03</td>
      <td>43.842609</td>
      <td>2</td>
      <td>15209.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2008-04-04</td>
      <td>43.312376</td>
      <td>1</td>
      <td>14018.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2008-04-05</td>
      <td>43.941176</td>
      <td>1</td>
      <td>11974.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2008-04-06</td>
      <td>44.403936</td>
      <td>1</td>
      <td>11007.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2008-04-07</td>
      <td>43.995888</td>
      <td>2</td>
      <td>15214.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2008-04-08</td>
      <td>43.373773</td>
      <td>1</td>
      <td>11333.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2008-04-09</td>
      <td>43.320312</td>
      <td>1</td>
      <td>7026.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2008-04-10</td>
      <td>43.154738</td>
      <td>5</td>
      <td>15677.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
co2_df.isna().sum()
```




    year     0
    co2     25
    dtype: int64




```python
ecom_df.isnull().sum()
```




    date         0
    price        0
    location     0
    clicks      16
    dtype: int64




```python
co2_df.describe(include='all', datetime_is_numeric=True)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>co2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>226</td>
      <td>201.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>1906-11-27 01:29:12.212389376</td>
      <td>1.590015</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1750-01-01 00:00:00</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>1851-04-02 06:00:00</td>
      <td>0.076400</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1907-07-02 12:00:00</td>
      <td>0.935100</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>1963-10-01 18:00:00</td>
      <td>2.807600</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2020-01-01 00:00:00</td>
      <td>4.907900</td>
    </tr>
    <tr>
      <th>std</th>
      <td>NaN</td>
      <td>1.644182</td>
    </tr>
  </tbody>
</table>
</div>




```python
ecom_df.describe(include='all', datetime_is_numeric=True)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>price</th>
      <th>location</th>
      <th>clicks</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>135</td>
      <td>135.000000</td>
      <td>135.000000</td>
      <td>119.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>2008-06-07 00:00:00</td>
      <td>43.478978</td>
      <td>1.696296</td>
      <td>9530.336134</td>
    </tr>
    <tr>
      <th>min</th>
      <td>2008-04-01 00:00:00</td>
      <td>42.207018</td>
      <td>1.000000</td>
      <td>2044.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>2008-05-04 12:00:00</td>
      <td>43.045714</td>
      <td>1.000000</td>
      <td>6438.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>2008-06-07 00:00:00</td>
      <td>43.487069</td>
      <td>1.000000</td>
      <td>8391.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2008-07-10 12:00:00</td>
      <td>43.886875</td>
      <td>2.000000</td>
      <td>11363.500000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2008-08-13 00:00:00</td>
      <td>45.801613</td>
      <td>5.000000</td>
      <td>29505.000000</td>
    </tr>
    <tr>
      <th>std</th>
      <td>NaN</td>
      <td>0.608467</td>
      <td>1.114853</td>
      <td>4687.587507</td>
    </tr>
  </tbody>
</table>
</div>




```python
test = pd.read_csv(folder1, parse_dates=['date'], na_values={'?'}) #specifying the na values based on the summary statistics 
```

####  6.1. <a name='F.1HandlingmissingdatawithimputationusingPandas'></a>F.1 Handling missing data with imputation using Pandas

There are two approaches to impute missing data:

    univariate imputation
    multivariate imputation

##### F.1.1 Univariate Imputation

In Univariate Imputation, we will use non-missing values in a single variable (think a column or feature) to impute the missing values for that variable. <br>

For Ex. If have a sales column in the dataset with some missing values, you can use a univariate imputation method to impute missing sales observations using average sales


**Basic Univariate Imputation Techniques** <br>
Imputing using the mean<br>
Imputing using the last observation forward (**forward fill - ffill**). This also referred as Last Observed Carried Forward (LOCF) <br>
Imputing using the next observation backward (**backward fill - bfill**). This also referred as Next Observation Carried Backward (NOCB)


```python
folder = Path(_base_location+chapter)
```


```python
co2_original = read_dataset(folder, 'co2_original.csv', 'year')
co2_missing = read_dataset(folder, 'co2_missing_only.csv', 'year')
clicks_original = read_dataset(folder, 'clicks_original.csv', 'date')
clicks_missing = read_dataset(folder, 'clicks_missing.csv', 'date')
```


```python
plot_dfs(co2_original,
         co2_missing,
         'co2',
         title="Annual co2 emission/capita",
         xlabel="Years",
         ylabel="x100 million tons")
```


    
![png](TimeSeries_files/TimeSeries_246_0.png)
    



```python
co2_missing[co2_missing['co2'].isna()]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>co2</th>
    </tr>
    <tr>
      <th>year</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1847-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1848-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1849-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1850-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1851-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1852-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1853-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1854-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1855-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1856-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1927-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1928-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1929-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1930-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1931-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1932-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1933-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1934-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1935-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1936-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1937-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1974-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1975-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1976-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1977-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1978-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1979-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1980-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1981-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1982-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1983-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1984-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1985-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1986-01-01</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1987-01-01</th>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
plot_dfs(clicks_original,
         clicks_missing,
         'clicks',
         title="Page Clicks per Day",
         xlabel="date",
         ylabel="No. of clicks")
```


    
![png](TimeSeries_files/TimeSeries_248_0.png)
    



```python
clicks_missing[clicks_missing['clicks'].isna()]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>price</th>
      <th>location</th>
      <th>clicks</th>
    </tr>
    <tr>
      <th>date</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2008-05-15</th>
      <td>42.517755</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-16</th>
      <td>44.011009</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-17</th>
      <td>42.530303</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-18</th>
      <td>42.565698</td>
      <td>5</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-19</th>
      <td>43.892996</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-20</th>
      <td>43.247706</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-21</th>
      <td>44.106289</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-22</th>
      <td>43.810714</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-23</th>
      <td>43.620802</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-24</th>
      <td>43.854031</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-25</th>
      <td>43.586710</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-26</th>
      <td>43.670659</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-27</th>
      <td>42.850184</td>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-28</th>
      <td>43.702147</td>
      <td>5</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-29</th>
      <td>43.872321</td>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2008-05-30</th>
      <td>44.558011</td>
      <td>5</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Let's perform **imputation** using **fillna() method**, DataFrame.fillna() is the simplest imputation method. The function can be used in two ways depending on which parameter you are using:

The **value parameter**, where you can pass a scalar value (numeric or string) to use to fill for all missing values <br>
The method parameter, which takes specific string values: <br>
**Backward filling: backfill or bfill**, uses the next observation, after the missing spot(s) and fills the gaps backward <br>
**Forward filling: ffill or pad**, uses the last value before the missing spot(s) and fills the gaps forward” <br>



```python
co2_missing['ffill'] = co2_missing['co2'].fillna(method='ffill')
co2_missing['bfill'] = co2_missing['co2'].fillna(method='bfill')
co2_missing['mean'] = co2_missing['co2'].fillna(co2_missing['co2'].mean())
```


```python
rmse_score(co2_original, co2_missing, 'co2')
```

    RMSE for ffill: 0.05873012599267133
    RMSE for bfill: 0.05550012995280968
    RMSE for mean: 0.7156383637041684
    Mininum RMSE belongs to bfill is 0.05550012995280968





    [0.05873012599267133, 0.05550012995280968, 0.7156383637041684]



bfill and ffill produce better results that when using the mean, both techniques have favorable RMSE scores.


```python
plot_dfs(co2_original, co2_missing, 'co2')
```


    
![png](TimeSeries_files/TimeSeries_254_0.png)
    



```python
clicks_missing['ffil'] = clicks_missing['clicks'].fillna(method='ffill')
clicks_missing['bfill'] = clicks_missing['clicks'].fillna(method='bfill')
clicks_missing['mean'] = clicks_missing['clicks'].fillna(clicks_missing['clicks'].mean())
```


```python
_ = rmse_score(clicks_original, 
                    clicks_missing, 
                    'clicks')
```

    RMSE for ffil: 1034.1210689204554
    RMSE for bfill: 2116.6840489225033
    RMSE for mean: 997.7600138929953
    Mininum RMSE belongs to mean is 997.7600138929953



```python
plot_dfs(clicks_original, clicks_missing, 'clicks')
```


    
![png](TimeSeries_files/TimeSeries_257_0.png)
    


####  6.2. <a name='F.1.2Handlingmissingdatawithunivariateimputationusingscikit-learn'></a>F.1.2 Handling missing data with univariate imputation using scikit-learn

**SimpleImputer class from scikit-learn** accepts different values for the strategy parameter, including mean, median and most_frequent.


```python
co2_original = read_dataset(folder, 
                            'co2_original.csv', 'year')
co2_missing = read_dataset(folder, 
                           'co2_missing_only.csv', 'year')
clicks_original = read_dataset(folder, 
                               'clicks_original.csv', 'date')
clicks_missing = read_dataset(folder, 
                              'clicks_missing.csv', 'date')
```


```python
from sklearn.impute import SimpleImputer
```


```python
strategy = [
    ('Mean Strategy', 'mean'),
    ('Median Strategy', 'median'),
    ('Most Frequent Strategy', 'most_frequent')]
```

SimpleImputer accepts a Numpy array, so you will need to use the Series.values property followed by the .reshape(-1,1) method to create a 2D NumPy array.


```python
col2_vals = co2_missing['co2'].values.reshape(-1, 1)
click_vals = clicks_missing['clicks'].values.reshape(-1, 1)

for s_name, s in strategy:
    co2_missing[s_name] = (
        SimpleImputer(strategy=s).fit_transform(col2_vals))
    clicks_missing[s_name] = (SimpleImputer(strategy=s).fit_transform(click_vals))
```


```python
_ = rmse_score(co2_original, co2_missing, 'co2')
```

    RMSE for Mean Strategy: 0.7156383637041684
    RMSE for Median Strategy: 0.8029421606859859
    RMSE for Most Frequent Strategy: 1.1245663822743381
    Mininum RMSE belongs to Mean Strategy is 0.7156383637041684



```python
_ = rmse_score(clicks_original, clicks_missing, 'clicks')
```

    RMSE for Mean Strategy: 997.7600138929953
    RMSE for Median Strategy: 959.3580492530756
    RMSE for Most Frequent Strategy: 1097.6425985146868
    Mininum RMSE belongs to Median Strategy is 959.3580492530756



```python
plot_dfs(co2_original, co2_missing, 'co2')
```


    
![png](TimeSeries_files/TimeSeries_267_0.png)
    



```python
plot_dfs(clicks_original, clicks_missing, 'clicks')
```


    
![png](TimeSeries_files/TimeSeries_268_0.png)
    


We used SimpleImputer class to implement three simple strategies to impute missing values: mean, median and most frequent (mode). This is a univariate technique, since we are only using one feature/column to impute/compute mean, median and mode.

SimpleImputer will impute all occurrences of the missing_values, which you can update with pandas.NA, an integer, float, or a string value.

**strategy**, which defaults to mean, and takes string values. <br>
**fill_value** can be used to replace all instances from missing_values with a specific value. This can either be a string or a numeric value. If the Strategy was set to constant, then you will need to provide your custom fill_value.”



```python
avg = co2_missing['co2'].mean()
co2_missing['pands_fillna'] = co2_missing['co2'].fillna(avg)
```


```python
co2_missing
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>co2</th>
      <th>Mean Strategy</th>
      <th>Median Strategy</th>
      <th>Most Frequent Strategy</th>
      <th>pands_fillna</th>
    </tr>
    <tr>
      <th>year</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1750-01-01</th>
      <td>0.0125</td>
      <td>0.0125</td>
      <td>0.0125</td>
      <td>0.0125</td>
      <td>0.0125</td>
    </tr>
    <tr>
      <th>1760-01-01</th>
      <td>0.0128</td>
      <td>0.0128</td>
      <td>0.0128</td>
      <td>0.0128</td>
      <td>0.0128</td>
    </tr>
    <tr>
      <th>1770-01-01</th>
      <td>0.0150</td>
      <td>0.0150</td>
      <td>0.0150</td>
      <td>0.0150</td>
      <td>0.0150</td>
    </tr>
    <tr>
      <th>1780-01-01</th>
      <td>0.0169</td>
      <td>0.0169</td>
      <td>0.0169</td>
      <td>0.0169</td>
      <td>0.0169</td>
    </tr>
    <tr>
      <th>1790-01-01</th>
      <td>0.0206</td>
      <td>0.0206</td>
      <td>0.0206</td>
      <td>0.0206</td>
      <td>0.0206</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2016-01-01</th>
      <td>4.7496</td>
      <td>4.7496</td>
      <td>4.7496</td>
      <td>4.7496</td>
      <td>4.7496</td>
    </tr>
    <tr>
      <th>2017-01-01</th>
      <td>4.7595</td>
      <td>4.7595</td>
      <td>4.7595</td>
      <td>4.7595</td>
      <td>4.7595</td>
    </tr>
    <tr>
      <th>2018-01-01</th>
      <td>4.8022</td>
      <td>4.8022</td>
      <td>4.8022</td>
      <td>4.8022</td>
      <td>4.8022</td>
    </tr>
    <tr>
      <th>2019-01-01</th>
      <td>4.7582</td>
      <td>4.7582</td>
      <td>4.7582</td>
      <td>4.7582</td>
      <td>4.7582</td>
    </tr>
    <tr>
      <th>2020-01-01</th>
      <td>4.4654</td>
      <td>4.4654</td>
      <td>4.4654</td>
      <td>4.4654</td>
      <td>4.4654</td>
    </tr>
  </tbody>
</table>
<p>226 rows × 5 columns</p>
</div>




```python
cols = ['co2', 'Mean Strategy', 'pands_fillna']
_ = rmse_score(co2_original, co2_missing[cols], 'co2')
```

    RMSE for Mean Strategy: 0.7156383637041684
    RMSE for pands_fillna: 0.7156383637041684
    Mininum RMSE belongs to Mean Strategy is 0.7156383637041684


####  6.3. <a name='F.1.3Handlingmissingdatawithmultivariateimputation'></a>F.1.3 Handling missing data with multivariate imputation

In multivariate imputation we use multiple variables within the dataset to impute missing values. Having more variables within the dataset,chime in to improve the predictability of missing values.

We will use scikit-learn IterativeImputer class for multivariate imputation, as we get to pass a regressor to predict the missing values from other variables


```python
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.ensemble import ExtraTreesRegressor, BaggingRegressor
from sklearn.linear_model import ElasticNet, LinearRegression, BayesianRidge
from sklearn.neighbors import KNeighborsRegressor
```

With IterativeImputer we can test different estimators and compare results. <br>
Create a list of regressors to be used in IterativeImputer


```python
estimators = [
             ('bayesianRidge', BayesianRidge()),
             ('extra_trees', ExtraTreesRegressor(n_estimators=10)),
             ('bagging', BaggingRegressor(n_estimators=10)),
             ('elastic_net', ElasticNet()),
             ('linear_regression', LinearRegression()),
             ('knn', KNeighborsRegressor(n_neighbors=3))
             ]
```

Loop through the estimators and train on the dataset using .fit(), and building different models, and finally apply the imputation using .transform() on variable with missing data.


```python
clicks_vals = clicks_missing.iloc[:, 0:3].values
```


```python
for e_name, e in estimators:
    est = IterativeImputer(
                random_state=15,
                estimator=e).fit(clicks_vals)
    clicks_missing[e_name] = est.transform(clicks_vals)[: , 2]
```

    /Users/rahuladlakha/opt/anaconda3/lib/python3.9/site-packages/sklearn/impute/_iterative.py:685: ConvergenceWarning: [IterativeImputer] Early stopping criterion not reached.
      warnings.warn("[IterativeImputer] Early stopping criterion not"
    /Users/rahuladlakha/opt/anaconda3/lib/python3.9/site-packages/sklearn/impute/_iterative.py:685: ConvergenceWarning: [IterativeImputer] Early stopping criterion not reached.
      warnings.warn("[IterativeImputer] Early stopping criterion not"



```python
_ = rmse_score(clicks_original, clicks_missing, 'clicks')
```

    RMSE for Mean Strategy: 997.7600138929953
    RMSE for Median Strategy: 959.3580492530756
    RMSE for Most Frequent Strategy: 1097.6425985146868
    RMSE for bayesianRidge: 949.4393973455852
    RMSE for extra_trees: 1853.2704226332992
    RMSE for bagging: 1435.306256311665
    RMSE for elastic_net: 945.4075209343099
    RMSE for linear_regression: 938.9419831427186
    RMSE for knn: 1336.8798392251822
    Mininum RMSE belongs to linear_regression is 938.9419831427186



```python
plot_dfs(clicks_original, clicks_missing, 'clicks')
```


    
![png](TimeSeries_files/TimeSeries_284_0.png)
    


The RMSE could be misleading because we did not seek the best score (smallest value) since we are not scoring a prediction model but rather an imputation model to fill for missing data. <br>
We may use the data (with imputed values) to build another model for making predictions (forecasting). <br>
Thus, we do not mind some imperfections to better resemble real data. Additionally, since we may not know the true nature of the missing data, the goal is to get a decent estimate

**Multivariate Imputation by Chained Equation** implementation in InterativeImputer class

The basic idea is to treat each variable with missing values as the dependent variable in a regression, with some or all of the remaining variables as its predictors. <br> 
The MICE procedure cycles through these models, fitting each in turn, then uses a procedure called **“predictive mean matching” (PMM)** to generate random draws from the predictive distributions determined by the fitted models. <br>
These random draws become the imputed values for one imputed data set.

By default, each variable with missing variables is modeled using a linear regression with main effects for all other variables in the data set. <br> Note that even when the imputation model is linear, the PMM procedure preserves the domain of each variable. <br> Thus, for example, if all observed values for a given variable are positive, all imputed values for the variable will always be positive. <br> The user also has the option to specify which model is used to produce imputed values for each variable.


```python
from statsmodels.imputation.mice import MICE, MICEData, MICEResults
import statsmodels.api as sm
```


```python
# create a MICEData object
fltr = ['price', 'location','clicks']
mice_data = MICEData(clicks_missing[fltr], 
                     perturbation_method='gaussian')
# 20 iterations
mice_data.update_all(n_iter=20)

mice_data.set_imputer('clicks', formula='~ price + location', model_class=sm.OLS)
```


```python
clicks_missing['MICE']  = mice_data.data['clicks'].values.tolist()
```


```python
_ = rmse_score(clicks_original, clicks_missing, 'clicks')
```

    RMSE for Mean Strategy: 997.7600138929953
    RMSE for Median Strategy: 959.3580492530756
    RMSE for Most Frequent Strategy: 1097.6425985146868
    RMSE for bayesianRidge: 949.4393973455852
    RMSE for extra_trees: 1853.2704226332992
    RMSE for bagging: 1435.306256311665
    RMSE for elastic_net: 945.4075209343099
    RMSE for linear_regression: 938.9419831427186
    RMSE for knn: 1336.8798392251822
    RMSE for MICE: 1768.0716258325265
    Mininum RMSE belongs to linear_regression is 938.9419831427186



```python
cols = ['clicks','bayesianRidge', 'bagging', 'knn', 'MICE']
plot_dfs(clicks_original, clicks_missing[cols], 'clicks')
```


    
![png](TimeSeries_files/TimeSeries_292_0.png)
    



```python
_ = mice_data.plot_imputed_hist('clicks')
```


    
![png](TimeSeries_files/TimeSeries_293_0.png)
    



```python
mice_data = MICEData(clicks_missing[fltr], 
                     perturbation_method='gaussian')
mice_data.update_all(n_iter=20)
_ = mice_data.plot_fit_obs('clicks')
```


    
![png](TimeSeries_files/TimeSeries_294_0.png)
    


Overall, multivariate imputation techniques generally produce better results than univariate methods.<br> This is true when working with more complex time-series datasets in terms of the number of features (columns) and records. <br> Though univariate imputers are more efficient in terms of speed and simplicity to interpret, there is a need to balance complexity, quality, and analytical requirements.

####  6.4. <a name='F.1.4HandlingmissingdatawithInterpolation'></a>F.1.4 Handling missing data with Interpolation

Commonly used technique for imputing missing values is interpolation. The pandas library provides the `DataFrame.interpolate()` method for more complex univariate imputation strategies. <br>
Each interpolation method will have a different mathematical operation to determine how to fill in for the missing data.

**Techniques in Interpolation:** <br>
**Linear Interpolation**<br>
Linear interpolation can be used to impute missing data by drawing a straight line between the two points surrounding the missing value. <br>
  In time series, this means for a missing data point, it looks at a prior past value and the next future value to draw a line between them.

**Polynomial Interpolation** <br>
Polynimial Interpolation will attempt to draw a curved line between the two points.

We will use the pandas DataFrame.interpolate() function to examine different interpolation methods, including linear, polynomial, quadratic, nearest, and spline.


```python
clicks_missing = clicks_missing.iloc[:,0:3]
co2_missing = co2_missing.iloc[:,0:1]
```


```python
interpolation = ['linear',
                 'quadratic',
                 'nearest',
                 'cubic']
```


```python
'''Loop through each of the interpolation method in the list and use interpolate()'''
for intp in interpolation:
    co2_missing[intp] = co2_missing['co2'].interpolate(method=intp)
    clicks_missing[intp] = clicks_missing['clicks'].interpolate(method=intp)
```

    /var/folders/fn/gllvw1z179nb4gb993sqkg9h0000gn/T/ipykernel_74813/65019824.py:3: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      co2_missing[intp] = co2_missing['co2'].interpolate(method=intp)


There are two additional methods that it would be interesting to test: *spline* and *polynomial*. To use these methods, we need to provide the order parameter.


```python
co2_missing['spline'] = co2_missing['co2'].interpolate(method='spline', order=2)
clicks_missing['spline'] = clicks_missing['clicks'].interpolate(method='spline', order=2)
co2_missing['poly'] = co2_missing['co2'].interpolate(method='polynomial', order=5)
clicks_missing['poly'] = clicks_missing['clicks'].interpolate(method='polynomial', order=5)
```


```python
_ = rmse_score(co2_original, co2_missing, 'co2')
```

    RMSE for linear: 0.05507291327761665
    RMSE for quadratic: 0.08367561505614346
    RMSE for nearest: 0.05385422309469095
    RMSE for cubic: 0.08373627305833138
    RMSE for spline: 0.1878602347541416
    RMSE for poly: 0.06728323553134932
    Mininum RMSE belongs to nearest is 0.05385422309469095



```python
_ = rmse_score(clicks_original, clicks_missing, 'clicks')
```

    RMSE for linear: 1329.1448378562811
    RMSE for quadratic: 5224.641260626974
    RMSE for nearest: 1706.1853705030173
    RMSE for cubic: 6199.304875782833
    RMSE for spline: 5222.922993448641
    RMSE for poly: 56757.29323647128
    Mininum RMSE belongs to linear is 1329.1448378562811



```python
cols = ['co2', 'linear', 'nearest', 'poly']
```


```python
plot_dfs(co2_original, co2_missing[cols], 'co2')
```


    
![png](TimeSeries_files/TimeSeries_309_0.png)
    



```python
cols = ['clicks', 'linear', 'nearest', 'poly', 'spline']
plot_dfs(clicks_original, clicks_missing[cols], 'clicks')
```


    
![png](TimeSeries_files/TimeSeries_310_0.png)
    


###  7. <a name='G.OutlierDetectionUsingStatisticalMethods'></a>G. Outlier Detection Using Statistical Methods

Types of Outliers: <br>
**Point Outlier** - A data point deviates from the rest of the population—sometimes referred to as a global outlier.<br>
**Contextual Outlier** -  When an observation is considered an outlier based on a particular condition or context, such as deviation from neighboring data points. However, the same observation may not be considered an outlier if the context changes<br>
**Collective Outlier** - groups of observations, differ from the population and don't follow the expected pattern<br>


Outlier Detection and Change point detection are different. <br>

**Change Point Detection (CPD)**, the goal is to anticipate abrupt and impactful fluctuations (increasing or decreasing) in the time series data.
CPD covers specific techniques, such as **CUMSUM** and **Bayesian online change point detection (BOCPD)**. <br>
Example: A machine may break if the internal temperature reaches a certain point or if you're trying to understand whether the discounted price did increase sales or not.


**Statistical Techniques for Outlier Detection** <br>
1. Resampling time series data
2. Detecting outliers using visualization
3. Detecting outliers using the Tukey method
4. Detecting outliers using a z-score
5. Detecting outliers using a modified z-score


```python
import warnings
warnings.filterwarnings('ignore')
plt.rcParams["figure.figsize"] = [16, 3]
```


```python
chapter = 'Ch8'
filename = 'nyc_taxi.csv'
path = Path(_base_location+chapter)
```


```python
nyc_taxi = read_dataset(path, filename, 'timestamp')
```


```python
nyc_taxi.index.freq = '30T' #setting 30 min freq for the index
```


```python
nyc_dates = ["2014-11-01",
             "2014-11-27",
             "2014-12-25",
             "2015-01-01",
             "2015-01-27"]
```


```python
nyc_taxi.plot(title='NYC Taxi', alpha=0.6); #Timeseries with 30 min frequency
```


    
![png](TimeSeries_files/TimeSeries_320_0.png)
    



```python
def plot_outliers(outliers, data, method='KNN',
                  halignment = 'right',
                  valignment = 'bottom',
                  labels = False):

    ax = data.plot(alpha=0.6)

    if labels:
        for i in outliers['value'].items():
            plt.plot(i[0], i[1], 'rx')
            plt.text(i[0], i[1], f'{i[0].date()}', horizontalalignment = halignment,
                                                   verticalalignment = valignment)
    else:
        data.loc[outliers.index].plot(ax=ax, style='rx')

    plt.title(f'NYC Taxi - {method}')
    plt.xlabel('date'); plt.ylabel('# of passengers')
    plt.legend(['nyc taxi', 'outliers'])
    plt.show()

```

Outliers can indicate bad data due to a random variation in the process, known as noise, or due to data entry error, faulty sensors, bad experiment, or natural variation. Outliers are usually undesirable if they seem synthetic, for example, bad data. On the other hand, if outliers are a natural part of the process, you may need to rethink removing them and opt to keep these data points.

Generally, outliers can cause side effects when building a model based on strong assumptions on the data distribution; for example, the data is from a Gaussian (normal) distribution. Statistical methods and tests based on assumptions of the underlying distribution are referred to as parametric methods.

Sometimes you may need to test your model with outliers and again without outliers to understand the overall impact on your analysis. In other words, not all outliers are created, nor should they be treated equally.


There are many well-known methods for outlier detection.

In **statistical methods**, you have different tools that you can leverage:
1. Use of visualizations (boxplots, QQ-plots, histograms, and scatter plots), 
2. z-score, 
3. interquartile range (IQR) and Tukey fences,
4.  and statistical tests such as Grubb's test, the Tietjen-Moore test, or the generalized Extreme Studentized Deviate (ESD) test. 

These are basic, easy to interpret, and effective methods.

####  7.1. <a name='G.1Resamplingtimeseriesdata'></a>G.1 Resampling time series data

Resampling implies changing the frequency or level of granularity of the data.

The need for resampling depends upon the requirement of analysis granularity. Sometimes we need to aggregate the data from daily level to weekly level, i.e. called as **downsampling**; for this we need some aggregation function such as mean, sum, min, or max. etc.

However, in some situations we need to resample from daily level to hourly, i.e. called as **upsampling**; this results in null rows, which can be filled using imputation or interpolation.

Resampling is done using **DataFrame.resample()** function.

For downsampling, **D** stands for **Day**, **3B** for **3*Business day**, **W** for **Week**, **M** for **Month end**, **MS** for **Month Start**

For Upsampling, **15T** stands for **15 minutes** and rest same for downsampling


```python
df_downsampled = nyc_taxi.resample('D').mean() #daily downsampling
```


```python
df_downsampled.index.freq # to check frequency of the index
```




    <Day>




```python
df_downsampled = nyc_taxi.resample('3D').sum() #3 day frequency
```


```python
df_downsampled.index.freq
```




    <3 * Days>




```python
df_downsampled = nyc_taxi.resample('3B').sum() #frequency to 3 Business days
```


```python
nyc_taxi.resample('15T').mean().head() #'T' is used for minutes
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
    </tr>
    <tr>
      <th>timestamp</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-07-01 00:00:00</th>
      <td>10844.0</td>
    </tr>
    <tr>
      <th>2014-07-01 00:15:00</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2014-07-01 00:30:00</th>
      <td>8127.0</td>
    </tr>
    <tr>
      <th>2014-07-01 00:45:00</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2014-07-01 01:00:00</th>
      <td>6210.0</td>
    </tr>
  </tbody>
</table>
</div>



**NaN** rows created by upsampling, we need to give instruction on how to fill the NaN rows. 


```python
nyc_taxi.resample('15T').fillna('ffill').head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
    </tr>
    <tr>
      <th>timestamp</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-07-01 00:00:00</th>
      <td>10844</td>
    </tr>
    <tr>
      <th>2014-07-01 00:15:00</th>
      <td>10844</td>
    </tr>
    <tr>
      <th>2014-07-01 00:30:00</th>
      <td>8127</td>
    </tr>
    <tr>
      <th>2014-07-01 00:45:00</th>
      <td>8127</td>
    </tr>
    <tr>
      <th>2014-07-01 01:00:00</th>
      <td>6210</td>
    </tr>
  </tbody>
</table>
</div>



With downsampling we can provide list of aggregation function, this will create a new col for each aggregation method.


```python
nyc_taxi.resample('W').agg(['mean', 'sum', 'max', 'median', 'max']).head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }

    .dataframe thead tr:last-of-type th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th colspan="5" halign="left">value</th>
    </tr>
    <tr>
      <th></th>
      <th>mean</th>
      <th>sum</th>
      <th>max</th>
      <th>median</th>
      <th>max</th>
    </tr>
    <tr>
      <th>timestamp</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-07-06</th>
      <td>13361.350694</td>
      <td>3848069</td>
      <td>29985</td>
      <td>14428.0</td>
      <td>29985</td>
    </tr>
    <tr>
      <th>2014-07-13</th>
      <td>15365.928571</td>
      <td>5162952</td>
      <td>26873</td>
      <td>17085.0</td>
      <td>26873</td>
    </tr>
    <tr>
      <th>2014-07-20</th>
      <td>15524.455357</td>
      <td>5216217</td>
      <td>27167</td>
      <td>17093.5</td>
      <td>27167</td>
    </tr>
    <tr>
      <th>2014-07-27</th>
      <td>15685.339286</td>
      <td>5270274</td>
      <td>26688</td>
      <td>17420.0</td>
      <td>26688</td>
    </tr>
    <tr>
      <th>2014-08-03</th>
      <td>15071.163690</td>
      <td>5063911</td>
      <td>25969</td>
      <td>16692.0</td>
      <td>25969</td>
    </tr>
  </tbody>
</table>
</div>



####  7.2. <a name='G.2DetectingOutliersusingVisualization'></a>G.2 Detecting Outliers using Visualization

There are two general approaches for using statistical techniques to detect outliers: **parametric** and **non-parametric** methods. <br> Parametric methods assume you know the underlying distribution of the data. For example, if your data follows a normal distribution. <br> On the other hand, in non-parametric methods, you make no such assumptions.

Histograms and box plots are basic non-parametric techniques that can provide insight into the distribution of the data and the presence of outliers. <br>
More specifically, box plots, also known as box and whisker plots, provide a five-number summary: the minimum, first quartile (25th percentile), median (50th percentile), third quartile (75th percentile), and the maximum. <br>
There are different implementations for how far the whiskers extend, for example, the whiskers can extend to the minimum and maximum values.The whiskers extend to what is called Tukey's lower and upper fences. Any data point outside these boundaries is considered an outlier.


```python
import seaborn as sns
```


```python
tx = nyc_taxi.resample('D').mean() # Downsampling the data to daily frequency.
```


```python
known_outliers = tx.loc[nyc_dates]
```


```python
plot_outliers(known_outliers, tx, 'Known Outliers')
```


    
![png](TimeSeries_files/TimeSeries_345_0.png)
    



```python
sns.histplot(tx)
```




    <AxesSubplot:ylabel='Count'>




    
![png](TimeSeries_files/TimeSeries_346_1.png)
    



```python
# sns.displot(tx, kind='hist', height=3, aspect=4)
```


```python
sns.boxplot(tx['value'])
```




    <AxesSubplot:xlabel='value'>




    
![png](TimeSeries_files/TimeSeries_348_1.png)
    


The width of the box (Q1 to Q3) is called **interquartile range (IQR)** calculated as the difference between the 75th and 25th percentiles **(Q3 – Q1)**. <br> The lower fence is calculated as Q1 - (1.5 x IQR), and the upper fence as Q3 + (1.5 x IQR). Any observation less than the lower boundary or greater than the upper boundary is considered a potential outlier.

There are two more variations for box plots in seaborn **(boxenplot and violinplot)**.

**Boxenplot** is letter-value plot, better suited when working with larger datasets


```python
sns.boxenplot(tx['value']) #The boxen plot, which in literature is referred to as a letter-value plot, better suited when working with larger datasets
```




    <AxesSubplot:xlabel='value'>




    
![png](TimeSeries_files/TimeSeries_351_1.png)
    


In seaborn, this parameter is called k_depth, which can take a numeric value, or you can specify different methods such as tukey, proportion, trustworthy, or full.


```python
for k in ["tukey", "proportion", "trustworthy", "full"]:
    sns.boxenplot(tx['value'], k_depth=k)
    plt.title(k)
    plt.show()
```


    
![png](TimeSeries_files/TimeSeries_353_0.png)
    



    
![png](TimeSeries_files/TimeSeries_353_1.png)
    



    
![png](TimeSeries_files/TimeSeries_353_2.png)
    



    
![png](TimeSeries_files/TimeSeries_353_3.png)
    


Final variation is the violin plot, which you can display using the violinplot function.


```python
sns.violinplot(tx["value"])
```




    <AxesSubplot:xlabel='value'>




    
![png](TimeSeries_files/TimeSeries_355_1.png)
    


**Violen plot** that is a hybrid between a box plot and a **kernel density estimation (KDE)**.

A kernel is a function that estimates the probability density function, the larger peaks (wider area), for example, show where the majority of the points are concentrated. This means that there is a higher probability that a data point will be in that region as opposed to the much thinner regions showing much lower probability.

The number of peaks; in this case, we have one peak, which makes it a unimodal distribution. If there is more than one peak, we call it a multimodal distribution, which should trigger a further investigation into the data.

**Lag Plot** - It is a Scatter plot, but instead of plotting two variables to observe correlation, as an example, we plot the same variable against its lagged version.


```python
from pandas.plotting import lag_plot
lag_plot(tx)
```




    <AxesSubplot:xlabel='y(t)', ylabel='y(t + 1)'>




    
![png](TimeSeries_files/TimeSeries_359_1.png)
    


####  7.3. <a name='G.3DetectingOutlierswithTukeyMethod'></a>G.3 Detecting Outliers with Tukey Method

The box plot showed the quartiles with whiskers extending to the upper and lower fences. These boundaries or fences were calculated using the Tukey method.


```python
percentiles = [0, 0.05, .10, .25, .5, .75, .90, .95, 1]
tx.describe(percentiles = percentiles)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>215.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>15137.569380</td>
    </tr>
    <tr>
      <th>std</th>
      <td>1937.391020</td>
    </tr>
    <tr>
      <th>min</th>
      <td>4834.541667</td>
    </tr>
    <tr>
      <th>0%</th>
      <td>4834.541667</td>
    </tr>
    <tr>
      <th>5%</th>
      <td>11998.181250</td>
    </tr>
    <tr>
      <th>10%</th>
      <td>13043.854167</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>14205.197917</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>15299.937500</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>16209.427083</td>
    </tr>
    <tr>
      <th>90%</th>
      <td>17279.300000</td>
    </tr>
    <tr>
      <th>95%</th>
      <td>18321.616667</td>
    </tr>
    <tr>
      <th>100%</th>
      <td>20553.500000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>20553.500000</td>
    </tr>
  </tbody>
</table>
</div>



Quartiles divide your distribution into four segments (hence the name) marked as Q1 (25th percentile), Q2 (50th percentile or Median), and Q3 (75th percentile). <br> Percentiles, on the other hand, can take any range from 0 to 100 (in pandas from 0 to 1, while in NumPy from 0 to 100), but most commonly refer to when the distribution is partitioned into 100 segments. These segments are called quantiles.


```python
percentiles = [0, 5, 10, 25, 50, 75, 90, 95, 100]
np.percentile(tx, percentiles)
```




    array([ 4834.54166667, 11998.18125   , 13043.85416667, 14205.19791667,
           15299.9375    , 16209.42708333, 17279.3       , 18321.61666667,
           20553.5       ])



IQR is calculated as the difference between Q3 and Q1 (IQR = Q3 – Q1), which determines the width of the box in the box plot. <br> These upper and lower fences are known as Tukey's fences, and more specifically, they are referred to as inner boundaries. <br> The outer boundaries also have lower Q1 - (3.0 x IQR) and upper Q3 + (3.0 x IQR) fences.


```python
def iqr_outliers(data):
    q1, q3 = np.percentile(data, [25, 75])
    IQR = q3 - q1
    lower_fence = q1 - (1.5 * IQR)
    upper_fence = q3 + (1.5 * IQR)
    return data[(data.value > upper_fence) | (data.value < lower_fence)]
```


```python
outliers = iqr_outliers(tx)
print(outliers)
```

                       value
    timestamp               
    2014-11-01  20553.500000
    2014-11-27  10899.666667
    2014-12-25   7902.125000
    2014-12-26  10397.958333
    2015-01-26   7818.979167
    2015-01-27   4834.541667



```python
plot_outliers(outliers, tx, "Outliers using IQR with Tukey's Fences")
```


    
![png](TimeSeries_files/TimeSeries_368_0.png)
    



```python
known_outliers
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>value</th>
    </tr>
    <tr>
      <th>timestamp</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-11-01</th>
      <td>20553.500000</td>
    </tr>
    <tr>
      <th>2014-11-27</th>
      <td>10899.666667</td>
    </tr>
    <tr>
      <th>2014-12-25</th>
      <td>7902.125000</td>
    </tr>
    <tr>
      <th>2015-01-01</th>
      <td>14383.479167</td>
    </tr>
    <tr>
      <th>2015-01-27</th>
      <td>4834.541667</td>
    </tr>
  </tbody>
</table>
</div>




```python
plot_outliers(known_outliers, tx, "known outliers")
```


    
![png](TimeSeries_files/TimeSeries_370_0.png)
    


This simple method did a great job at identifying four of the five known outliers. In addition, Tukey's method identified two additional outliers on 2014-12-26 and 2015-01-26.

“The use of 1.5x(IQR) is common when it comes to defining outliers, you can change the default 1.5 value by updating the whis parameter in the boxplot function. <br> **The choice of 1.5 makes the most sense when the data follows a Gaussian distribution (normal)**, but this is not always the case. <br> Generally, the larger the value, the fewer outliers you will capture as you expand your boundaries (fences). Similarly, the smaller the value, the more non-outliers will be defined as outliers, as you are shrinking the boundaries (fences).


```python
def iqr_outliers(data, p):
    q1, q3 = np.percentile(data, [25, 75])
    IQR = q3 - q1
    lower_fence = q1 - (p * IQR)
    upper_fence = q3 + (p * IQR)
    return data[(data.value > upper_fence) | (data.value < lower_fence)]
```


```python
for p in [1.3, 1.5, 2.0, 2.5,  3.0]:
    print(f'with p={p}')
    print(iqr_outliers(tx, p))
    print('-'*15)
```

    with p=1.3
                       value
    timestamp               
    2014-07-04  11511.770833
    2014-07-05  11572.291667
    2014-07-06  11464.270833
    2014-09-01  11589.875000
    2014-11-01  20553.500000
    2014-11-08  18857.333333
    2014-11-27  10899.666667
    2014-12-25   7902.125000
    2014-12-26  10397.958333
    2015-01-26   7818.979167
    2015-01-27   4834.541667
    ---------------
    with p=1.5
                       value
    timestamp               
    2014-11-01  20553.500000
    2014-11-27  10899.666667
    2014-12-25   7902.125000
    2014-12-26  10397.958333
    2015-01-26   7818.979167
    2015-01-27   4834.541667
    ---------------
    with p=2.0
                       value
    timestamp               
    2014-11-01  20553.500000
    2014-12-25   7902.125000
    2015-01-26   7818.979167
    2015-01-27   4834.541667
    ---------------
    with p=2.5
                      value
    timestamp              
    2014-12-25  7902.125000
    2015-01-26  7818.979167
    2015-01-27  4834.541667
    ---------------
    with p=3.0
                      value
    timestamp              
    2014-12-25  7902.125000
    2015-01-26  7818.979167
    2015-01-27  4834.541667
    ---------------


####  7.4. <a name='G.4Detectingoutliersusingaz-score'></a>G.4 Detecting outliers using a z-score

**z-score** is a common transformation for standardizing the data, common when you want to compare different datasets.

The z-score standardizes the data to be centered around a zero mean and the units represent standard deviations away from the mean.

$\huge{z}$ = $\huge\frac{(x-\mu)}{\sigma}$

z-score is a lossless transformation, which means you will not lose information such as its distribution (shape of data) or the relationship between the observation.

**Shortcoming** - It is a parameteric statistical method, since it assumes a gaussian distribution. In case the data is not normal then we will use modified z-score.


```python
def zscore(df, degree=3):
    data = df.copy()
    data['zscore'] = (data - data.mean())/data.std()
    outliers = data[(data['zscore'] <= -degree) | (data['zscore'] >= degree)]
    return outliers['value'], data
```


```python
threshold = 2.5
outliers, transformed = zscore(tx, threshold)
```


```python
transformed.hist()
```




    array([[<AxesSubplot:title={'center':'value'}>,
            <AxesSubplot:title={'center':'zscore'}>]], dtype=object)




    
![png](TimeSeries_files/TimeSeries_380_1.png)
    



```python
print(outliers)
```

    timestamp
    2014-11-01    20553.500000
    2014-12-25     7902.125000
    2015-01-26     7818.979167
    2015-01-27     4834.541667
    Name: value, dtype: float64



```python
plot_outliers(outliers, tx, "Outliers using z-score")
```


    
![png](TimeSeries_files/TimeSeries_382_0.png)
    


z-score managed to capture 3 out of 5 known outliers.


```python
def plot_zscore(data, d=3):
    n = len(data)
    plt.figure(figsize=(8,8))
    plt.plot(data, 'k^')
    plt.plot([0, n], [d,d], 'r--')
    plt.plot([0, n], [-d,-d], 'r--')    
```


```python
data = transformed['zscore'].values
plot_zscore(data, d=2.5)
```


    
![png](TimeSeries_files/TimeSeries_385_0.png)
    


The z-scores are interpreted as standard deviation units away from the mean, which is the center of the distribution.

There are several test in statsmodels library to test if the data is normal distributed.
1. **Kolmogorov-Smirnov Test** - The null hypothesis is that the data comes from Normal Distribution. The test returns the statistics and a **p-value**, if the p-value is less than 0.05, then you can reject the null hypothesis (data is not normally distributed). Otherwise, we fail to reject null hypothesis (data is normally distributed).

    **ktest_normal - statslibrary**


```python
from statsmodels.stats.diagnostic import kstest_normal

def test_normal(df):
    t_test, p_value = kstest_normal(df)
    if p_value < 0.05:
        print(f"Reject null hypothesis since p_value is {p_value}. Data is not normal")
    else:
        print(f"Fail to reject null hypothesis since p_value is {p_value}. Data is normal")
```


```python
test_normal(tx)
```

    Reject null hypothesis since p_value is 0.0009999999999998899. Data is not normal


####  7.5. <a name='G.5DetectingOutliersusingamodifiedz-score'></a>G.5 Detecting Outliers using a modified z-score

Modified version of z-score comes to work when data is not normally distributed. The main difference between the regular z-score and modified z-score is that we replace the mean with median.

$\large{Modified  Z}$ = $\huge\frac{0.6745(x_i - \tilde{x})} {MAD}$

$\tilde{x}$ - median of the dataset

$MAD$ - Median absolute deviation

$MAD$ = $median(abs(x_i - \tilde{x}))$

$0.6745$ - is the standard deviation unit that corresponds to the 75th percentile (Q3) in gaussian distribution and is used as a normalization factor. It is used to approximate the standard deviation.


```python
import scipy.stats as stats

def modified_zscore(df, degree=3):
    data = df.copy()
    s = stats.norm.ppf(0.75) # percent point function, known as inverse cumulative distribution function - gives quantile for the percentile
    numerator = s*(data - data.median())
    MAD = np.abs(data - data.median()).median()
    data['m_zscore'] = numerator/MAD
    outliers = data[(data['m_zscore']>degree) | (data['m_zscore'] < -degree)]
    return outliers['value'], data
```


```python
threshold = 3
outliers, transformed = modified_zscore(tx, threshold)
transformed.hist()
```




    array([[<AxesSubplot:title={'center':'value'}>,
            <AxesSubplot:title={'center':'m_zscore'}>]], dtype=object)




    
![png](TimeSeries_files/TimeSeries_393_1.png)
    



```python
plot_outliers(outliers, tx, "Outliers using Modified Z-score")
```


    
![png](TimeSeries_files/TimeSeries_394_0.png)
    



```python
data = transformed['m_zscore'].values
plot_zscore(data, d=2.5)
```


    
![png](TimeSeries_files/TimeSeries_395_0.png)
    


**QQ - Plot (Quantile-Quantile plot)** - Designed to test normality and sometimes help outliers.


```python
import scipy
import scipy.stats as stats
```


```python
res = scipy.stats.probplot(tx.values.reshape(-1), plot=plt)
```


    
![png](TimeSeries_files/TimeSeries_398_0.png)
    


The solid line represents a reference line for what normally distributed data would look like. <br> If the data you are comparing is normally distributed, all the points will lie on that straight line. <br>
 We can see that the distribution is almost normal (not perfect), and we see issues toward the distribution's tails. Majority of the outliers are at the bottom tail end (less than -2 standard deviation).

###  8. <a name='H.ExploratoryDataAnalysisandDiagnosis'></a>H. Exploratory Data Analysis and Diagnosis

For time series data exploratory data analysis includes some of the time series specific characteristics, such as stationarity, effects of trends and seasonality, autocorrelation etc.

Focus will be on the following:

1. Plotting time series data using Pandas
2. Plotting time series data using hvplot
3. Decomposing time series data
4. Detecting time series stationarity
5. Applying power transformation
6. Testing for autocorrelation in time series

####  8.1. <a name='H.1PlottingtimeseriesdatausingPandas'></a>H.1 Plotting time series data using Pandas


```python
from statsmodels.datasets import co2, get_rdataset
plt.rcParams["figure.figsize"] = [12, 5] 
```


```python
chapter = 'ch9/'
filename = 'closing_price.csv'
path = Path(_base_location+chapter+filename)
```


```python
closing_price = pd.read_csv(path,
                            index_col='Date',
                            parse_dates=True)
```


```python
co2_df = co2.load_pandas().data
co2_df = co2_df.ffill()
air_passenger = get_rdataset("AirPassengers")
airp_df = air_passenger.data
airp_df.index = pd.date_range('1949', '1961', freq='M')
airp_df.drop(columns=['time'], inplace=True)
```


```python
closing_price.plot(kind="line")
```




    <AxesSubplot:xlabel='Date'>




    
![png](TimeSeries_files/TimeSeries_407_1.png)
    


To check price fluctuations in comparison to each other, we need to normalize the data, by dividing the stock price by the first-day price (first row of each stock). This will make all the stocks have same starting point.


```python
closing_price_n = closing_price.div(closing_price.iloc[0])
closing_price_n.plot()
```




    <AxesSubplot:xlabel='Date'>




    
![png](TimeSeries_files/TimeSeries_409_1.png)
    



```python
closing_price_n.plot(subplots=True);
```


    
![png](TimeSeries_files/TimeSeries_410_0.png)
    



```python
closing_price_n.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>AAPL</th>
      <th>MSFT</th>
      <th>IBM</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2019-11-01</th>
      <td>1.000000</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>2019-11-04</th>
      <td>1.006567</td>
      <td>1.005775</td>
      <td>1.015790</td>
    </tr>
    <tr>
      <th>2019-11-05</th>
      <td>1.005121</td>
      <td>1.005149</td>
      <td>1.017413</td>
    </tr>
    <tr>
      <th>2019-11-06</th>
      <td>1.005551</td>
      <td>1.002366</td>
      <td>1.023980</td>
    </tr>
    <tr>
      <th>2019-11-07</th>
      <td>1.017156</td>
      <td>1.003757</td>
      <td>1.027937</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Further customization of plots

start_date = '2019'
end_date = '2021'
plot = closing_price.plot(
            title=f'Closing Prices from {start_date} - {end_date}',
            ylabel= 'Closing Price')
# plt.savefig('images/fig_9.1.png', bbox_inches='tight')
```


    
![png](TimeSeries_files/TimeSeries_412_0.png)
    


There are many plotting styles that you can use within pandas simply by providing a value to the kind argument. For example, you can specify the following:
			
    line - line charts commonly used to display time series
    bar or barh (horizontal) - bar plots
    hist - histogram plots
    box - boxplots
    kde or density - kernel density estimation plots
    area - area plots
    pie - pie plots
    scatter - scatter plots 
    hexbin - hexagonal bin plots

####  8.2. <a name='H.2PlottingtimeseriesdatawithinteractivevisualizationusinghvPlot'></a>H.2 Plotting time series data with interactive visualization using hvPlot

####  8.3. <a name='H.3Decomposingtimeseriesdata'></a>H.3 Decomposing time series data

There are 3 major component of any time series analysis process:
1. Trend
2. Seasonality
3. Residual

These components help make informed decisions during the modeling process.

**Trend** - Gives a sense of long-term direction of time series and can be upward, downward, or horizontal. <br>
**Seasonality** - These are repeated patterns over time. For Ex. a time series of sales data might show an increase in sales around Christmas. <br>
**Residual** - The residual is simply the remaining or unexplained portion once we extract trend and seasonality. <br>

The decomposition of time series data is the process of extracting these three components and representing them as their models. The modeling of the decomposed components can be either additive or multiplicative.

**Additive Model** when the original time series can be reconstructed by adding all three components:

$\huge{y_t} = T_t + S_t + R_t$

An additive model is reasonable when the seasonal variations do not change over time.

**Multiplicative Model** - If the time series can be constructed by multiplying all three components, you have a multiplicative model.

$\huge{y_t} = T_t * S_t * R_t$

A multiplicative model is suitable when the seasonal variation fluctuates over time.

**Seasonality** and **Trend** can be considered as **predictable components** (these are consistent, repeating patterns that can be captured and modeled).

Whereas, unpredictable components, those that shows irregularity, often called noise, can be referred as **Residual** in context of decomposition.

Different Decomposition Techniques:

1. Seasonal Decompose
2. Seasonal-Trend Decomposition with LOESS (STL)
3. hp_filter 

These methods are available in statsmodels library.


```python
from statsmodels.tsa.seasonal import seasonal_decompose, STL

plt.rcParams["figure.figsize"] = (10, 3)
```


```python
co2_df.plot()
```




    <AxesSubplot:>




    
![png](TimeSeries_files/TimeSeries_422_1.png)
    


The **co2_df** data shows a long-term linear trend, with a repeated seasonal pattern at a constant rate. (Seasonal Variation). This indicates that the CO2 dataset is an additive model.


```python
airp_df['value'].plot()
```




    <AxesSubplot:>




    
![png](TimeSeries_files/TimeSeries_424_1.png)
    


The **airp_df** data shows long-term linear upward trend and seasonality. However, the seasonality fluctuation seems to be increasing as well, indicating a multiplicative model.


```python
plt.rcParams["figure.figsize"] = (10,5)
```


```python
air_decomposed = seasonal_decompose(airp_df, model='multiplicative')
air_decomposed.plot(); plt.show()
```


    
![png](TimeSeries_files/TimeSeries_427_0.png)
    


Breaking down the air passenger data decomposition:
1. The trend component shows an upward direction. The trend indicates whether there is positive (increasing or upward), negative (decreasing or downward) or constant (no-trend or horizontal) long term movement.
2. The seasonal component shows the seasonality effect and the repeating pattern of highs and lows.
3. The residue component shows the random variations in the data after applying the model.


```python
co2_decomposed = seasonal_decompose(co2_df, model = 'additive')
```


```python

co2_decomposed.plot(); plt.show()
```


    
![png](TimeSeries_files/TimeSeries_430_0.png)
    



```python
(air_decomposed.trend *
air_decomposed.seasonal *
air_decomposed.resid).plot(); #We can reconstruct the timeseries by multiplying the component, since the data was of multiplicative model.
```


    
![png](TimeSeries_files/TimeSeries_431_0.png)
    


There is another decomposition option within statsmodel is STL, the STL option (class) requires additional parameters than the seasonal_decompose function. The two other parameters you will use are seasonal and robust. <br> The first one is **seasonal parameter**, is for the seasonal smoother and can only take odd integer values greater than or equal to 7. Similarly, the STL function has a trend smoother (the trend parameter).

The second parameter is **robust**, which takes a Boolean value (True or False). Setting robust=True helps remove the impact of outliers on seasonal and trend components when calculated.


```python
co2_stl = STL(co2_df,
              seasonal=13,
              robust=True).fit()
co2_stl.plot(); plt.show()
```


    
![png](TimeSeries_files/TimeSeries_433_0.png)
    


The STL class uses the LOESS seasonal smoother, which stands for Locally Estimated Scatterplot Smoothing. STL is more robust than seasonal_decompose for measuring non-linear relationships. <br> On the other hand, STL assumes additive composition, so you do not need to indicate a model, unlike with seasonal_decompose.

The **Hodrick-Prescott filter** is a smoothing filter that can be used to separate short-term fluctuations (cyclic variations) from long-term trends. This is implemented as **hp_filter** in the statsmodels library.

STL and seasonal_decompose returned three components (trend, seasonal, and residual). On the other hand, hp_filter returns two components: a cyclical component and a trend component.


```python
from statsmodels.tsa.filters.hp_filter import hpfilter
plt.rcParams["figure.figsize"] = (20, 3)
co2_cyclic, co2_trend = hpfilter(co2_df)

fig, ax = plt.subplots(1,2, figsize=(16, 4))
co2_cyclic.plot(ax=ax[0], title='CO2 Cyclic Component')
co2_trend.plot(ax=ax[1], title='CO2 Trend Component')
```




    <AxesSubplot:title={'center':'CO2 Trend Component'}>




    
![png](TimeSeries_files/TimeSeries_437_1.png)
    


####  8.4. <a name='H.4Detectingtimeseriesstationarity'></a>H.4 Detecting time series stationarity

A **stationary** time series implies that specific statistical properties do not vary over time and remain steady, making the processes easier to model and predict. <br> On the other hand, a **non-stationary** process is more complex to model due to the dynamic nature and variations over time.

A stationary time series is defined as a time series with a **constant mean**, a **constant variance**, and a **consistent covariance** (or **autocorrelation**) between identical distanced periods (lags). <br> Having the mean and variance as constants simplifies modeling since you are not solving for them as functions of time.

Generally, a time series with trend or seasonality can be considered non-stationary. Usually, spotting trends or seasonality visually in a plot can help you determine whether the time series is stationary or not. It is not always easy to identify stationarity or lack if it, visually. <br>

We do have some statistical tests, to help identify stationary or non-stationary time series numerically.

**Statistical Tests to identify stationarity**

1. Augmented Dicker-Fuller **(ADF)** Test
2. Kwiatkowski-Phillips-Schnidt-Shin **(KPSS)** Test

Both these, test for unit roots in a univariate time series process. Generally presence of unit roots indicates non-stationarity. <br>

ADF and KPSS test are based on Linear Regression and are a type of statistical hypothesis test. <br>
For Ex. null hypothesis for ADF states that there is a unit root in the time series, and thus it is non-stationary. <br>
For KPSS, the null hypothesis is opposite of ADF, which assumes the time series is stationary. We will need to interpret the test results to determine whether you can reject or fail the null hypothesis.

Generally, you can rely on the p-values returned to decide whether you reject or fail to reject the null hypothesis.


```python
from statsmodels.tsa.stattools import adfuller, kpss
```


```python
def print_results(output, test='adf'):
    '''
    The function takes output from adfuller and kpss functions and returns a dictionary that add labels to the output
    '''
    pval = output[1]
    test_score = output[0]
    lags = output[2]
    
    decision = 'Non-Stationary'
    if test == 'adf':
        critical = output[4]
        if pval < 0.05:
            decision = 'Stationary'
    elif test=='kpss':
        critical = output[3]
        if pval >= 0.05:
            decision = 'Stationary'
            
    output_dict = {
    'Test Statistic': test_score,
    'p-value': pval,
    'Numbers of lags': lags,
    'decision': decision
    }
    for key, value in critical.items():
        output_dict["Critical Value (%s)" % key] = value
       
    return pd.Series(output_dict, name=test)
```


```python
adf_output = adfuller(co2_df)
kpss_output = kpss(co2_df)
```


```python
pd.concat([
    print_results(adf_output, 'adf'),
    print_results(kpss_output, 'kpss')
], axis=1)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>adf</th>
      <th>kpss</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Test Statistic</th>
      <td>0.046051</td>
      <td>8.183188</td>
    </tr>
    <tr>
      <th>p-value</th>
      <td>0.962179</td>
      <td>0.01</td>
    </tr>
    <tr>
      <th>Numbers of lags</th>
      <td>27</td>
      <td>27</td>
    </tr>
    <tr>
      <th>decision</th>
      <td>Non-Stationary</td>
      <td>Non-Stationary</td>
    </tr>
    <tr>
      <th>Critical Value (1%)</th>
      <td>-3.433252</td>
      <td>0.739</td>
    </tr>
    <tr>
      <th>Critical Value (5%)</th>
      <td>-2.862822</td>
      <td>0.463</td>
    </tr>
    <tr>
      <th>Critical Value (10%)</th>
      <td>-2.567452</td>
      <td>0.347</td>
    </tr>
    <tr>
      <th>Critical Value (2.5%)</th>
      <td>NaN</td>
      <td>0.574</td>
    </tr>
  </tbody>
</table>
</div>



For *ADF*, the p-value is at 0.96, which is greater than 0.05, so you cannot reject the null hypothesis, and therefore, the time series is non-stationary.<br> For *KPSS*, the p-value is at 0.01, which is less than 0.05, so you reject the null hypothesis, and therefore, the time series is non-stationary.

1. The **Test Statistic** value is 0.046 for ADF and 8.18 for KPSS, which are above the 1% critical value threshold. This indicates that the time series is non-stationary. It confirms that you cannot reject the null hypothesis. The critical values for ADF come from a Dickey-Fuller table. 
2. The **p-value** result is associated with the test statistic. Generally, you can reject the null hypothesis if the p-value is less than 0.05 (5%). Again, when using ADF, KPSS, or other stationarity tests, make sure to understand the null hypothesis to accurately interpret the results.
3. **Number of lags** represents the number of lags used in the autoregressive process in the test (ADF and KPSS). In both tests, 27 lags were used. Since our CO2 data is weekly, a lag represents 1 week back. So, 27 lags represent 27 weeks in our data.
4. The number of observations used is the number of data points, excluding the number of lags.
5. The maximized info criteria are based on the **autolag** parameter. The default is **autolag="aic"** for the **Akaike information criterion**. Other acceptable autolag parameter values are bic for the **Bayesian information criterion** and t-stat.

Essentially stationarity can be achieved by removing trend (detrending) and seasonality effects.

**6 Main techniques to make time series stationary**, such as transformations and differencing.

1. First-Order Differencing
2. Second-Order Differencing
3. Subtracting Moving Average
4. Log Transformation
5. Decomposition
6. Hodrick-Prescott Fiter

For each transformation technique, we will run the stationarity tests and compare the results between the different techniques.

To do so, we will create two functions:
1. check_stationarity
2. plot_comparison


```python
def check_stationarity(df):

    '''
    takes a DataFrame, performs both KPSS and ADF tests, and returns the outcome.
    '''

    kps = kpss(df)
    adf = adfuller(df)

    kpss_pval, adf_pval = kps[1], adf[1]
    kpssh, adfh = 'Stationary', 'Non-Stationary'

    if adf_pval < 0.05:
        #Reject the null hypothesis
        adfh = 'Stationary'
    if kpss_pval < 0.05:
        kpssh = 'Non-Stationary'
    return(kpssh, adfh)

```


```python
def plot_comparison(methods, plot_type='line'):

    '''
    Takes a list of methods and compares their plots. The function takes plot_type, so you can explore a line chart and a histogram.
    The function calls check_stationarity function to capture the results for the subplot titles.
    '''

    n = len(methods)//2
    fig, ax = plt.subplots(n, 2, sharex=True, figsize=(20, 10))
    for i, method in enumerate(methods):
        method.dropna(inplace=True)
        name = [n for n in globals() if globals()[n] is method]
        v, r = i//2, i%2
        kpss_s, adf_s = check_stationarity(method)
        method.plot(kind=plot_type,
                    ax=ax[v,r],
                    legend=False,
                    title=f'{name[0]} --> KPSS: {kpss_s}, ADF {adf_s}')
        ax[v,r].title.set_size(20)
        method.rolling(52).mean().plot(ax=ax[v,r], legend=False)
```

Implementing methods to transform time series stationary or extracting stationary component.

**1. First Order Differencing**: This is also known as de-trending, calculated by subtracting an observation at time $t$ from the previous observation at time $t-1$, $(y_t - y_{t-1})$. <br>
This can be done using **.diff()** function which default period=1. <br>
Differenced data will contain one less data point (row) than the original data, hence the use of the .dropna() method.


```python
first_order_diff = co2_df.diff().dropna()
```

**2. Second Order Differencing**: This is useful if seasonality exists or if the first order differencing was insufficient. This is essentially differencing twice - differencing to remove seasonality followed by differencing to remove trend.


```python
differencing_twice = co2_df.diff(52).diff().dropna()
```

**3. Substracting moving average**: rolling window from the time series using **DataFrame.rolling(window=52).mean()** since it is weekly data:


```python
rolling = co2_df.rolling(window=52).mean()
substract_rolling_mean = co2_df - rolling
```

**4. Log Transformation**: Using **np.log()** is a common technique to stabilize the variance in a time series and sometimes enough to make the time series stationary. Simply, all it does is replace each observation with its log value 


```python
log_transform = np.log(co2_df)
```

**5. Decomposition**: Using decomposition to remove the trend component, such as **seasonal_decompose**. If the decomposition component is additive then the default parameter in seasonal_decompose is additive.


```python
decom = seasonal_decompose(co2_df)
sd_detrend = decom.observed - decom.trend
```

**6. Using Hodrick-Prescott** filter to remove the trend component


```python
cyclic, trend = hpfilter(co2_df)
```


```python
methods = [first_order_diff, differencing_twice, substract_rolling_mean, log_transform, sd_detrend, cyclic]
```


```python
plot_comparison(methods) #Plotting the different methods to make the CO2 time series stationary
```


    
![png](TimeSeries_files/TimeSeries_467_0.png)
    


Over-differenting time series as some studies have shown that models are less accurate. <br> 
For example, first_order_diff already made the time series stationary, and thus there was no need to difference it any further. In other words, differencing_twice was not needed. <br> Additionally, notice how **log_transform** is still non-stationary.


```python

```
