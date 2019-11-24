# This your assignment design report

it is a free form. you can add:

* your design
* your test result
* etc.


I want to compute max tips per 15 mins computed in every 5 mins, keyed by location id

When tip was maximum customer behavior



Incremental Aggegation to be more efficient, rather than buffering up events in a window

## Part 1: Design for Streaming Analytics


**Q1:** *Select a dataset suitable for streaming analytics for a customer as a running example (thus the basic unit of the data should be a discrete record/event data). Explain the dataset and at least two different analytics for the customer: (i) a streaming analytics which analyzes streaming data from the customer (customerstreamapp) and (ii) a batch analytics which analyzes historical results outputted by the streaming analytics.* 

**Answer:** 

I used the New York City (NYC) Taxi [dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) published by NYC Taxi and Limousine Corporation (TLC). 

Particularly, I have used the dataset for yellow taxi cabs for the month *January, 2019*. The data is available in CSV format, where each row of the CSV file represents a taxi ride event in NYC. 
The dataset that I have used here contains in total 18 fields of which we are mostly interested in the following fields - 

* *tpep_pickup_datetime* - When the meter is started
* *tpep_dropoff_datetime* - When the meter was disengaged
* *DOLocationID* - Dropoff location ID
* *tip_amount* - The amount of tip given by the passenger by credit card payment

(Note: A list of the explanation of each individal fields can be found [here](https://data.cityofnewyork.us/api/views/t29m-gskq/files/89042b9b-8280-4339-bda2-d68f428a7499?download=true&filename=data_dictionary_trip_records_yellow.pdf))

Previously, this dataset used to include the latitude and longitude of the pickup and dropoff locations, but in the latest versions they have removed these attributes. Instead they are providing locationID (1 through 263) from the pickup and dropoff locations as they have divided the whole NYC into 263 locations.

**Streaming analytics:**

The taxi drivers might be interested in finding the areas where customers usually gives more *tips*. So, here I am going to develop a streaming analytics for the taxi drivers (my customers), where they will be able to see the status quo of total tip amount paid by the passengers for the rides which started in these pickup location. So that, if they want they can go to such places in the hope of earning more. 

To be more clear, the taxi drivers will be able see the list of pickup locations updating in realtime .....

**Batch analytics from the historical output of Streaming analytics:**

For the batch analytics part, the drivers can see the pickup locations which resulted into highest tips by hourly, daily, weekly or even monthly......



**Q2:** *Customers will send data through message brokers/messaging systems which become data stream sources. Discuss and explain the following aspects for the streaming analytics: (i) should the analytics handle keyed or non-keyed data streams for the customer data, and (ii) which types of delivery guarantees should be suitable.*

**Answer** 
i. The analytics should handle *keyed* data stream. Because, the analytics is going to calculate total tips of all the trips starting from a particular pickup location. So, to calculate this we must  




    






