# Blood test appointment booking via UHCW's web portal #


## Context

The [University Hospitals - Coventry &
Warwickshire](https://www.uhcw.nhs.uk) (UHCW) has set up a [web
portal](https://www.swiftqueue.co.uk/uhcw.php) to facilitate the
booking of blood test appointments in Coventry (UK) from 18
participating test centers.

<p align="center">
b<img src="UHCW-swift-screenshot.png" alt="UHCW online appointment system" width="400"/>
</p>


## Goals
Collecting data about the available appointments from the portal (see a
section below), in this project we aim to address the following types
of questions.

1. Which test centers have the longest or shortest wait time to the
     next available appointment?  
1. What features, such as distance to city center, are more likely to
     predict which centers are booked quicker?  
1. Are there any salient patterns in the booking and cancellation
   behavior of the patients?

**Note.** This project is in its preliminary phase.  For speed, we
work with a relatively small subset of the full dataset.

## Preliminary phase: understanding the UHCW dataset
Details about the datasets, `attendance.csv` and `centers.csv`, are given below.  

Following is a summary of preliminary findings obtained from the
Jupyter notebook `UHCW_EDA.ipynb`.

- There are:  
  - 18 test centers,  
  - serving 2 age groups ("adult" and "child"),  
  - delivering collectively a total of 5 different test types,
    labeled:  
     - 'Blood Test'  
     - 'INR Test',  
     - 'Fasting Bloods',  
     - 'Non Fasting Bloods',  
     - 'Glucose Tolerance Test'  
- Each test center serves only 1 age group.  
- Each center delivers 1, 2, or 3 different types of test.  
- Most centers which deliver tests of multiple types put the
  appointments for all types at the same time.  
- The test centers offer a varying number of appointments.  This fact
  combine with the variability of the number of days these
  appointments are available for online booking means that the number
  of records varies across test centers, as depicted in the figure
  below.  
  <p align="center"> 
  <img src="number-of-records-per-center-distribution.png" alt="Distribution,
  across test centers, of number of records" width="1000"/>
  </p>
- The centers open their appointments for booking with a varying
  number of days ahead of time, between 52 and 125.  The distribution,
  across the test centers, is depicted in the figure below.  
  <p align="center"> 
  <img src="online-posting-period-distribution.png" alt="Distribution,
  across test centers, of number of days appointments go online prior to
  appointment date" width="1000"/>
  </p>




## Case study with center 10188
Let's take a look for example at the data for center 10188.  A blue
dot on the table below indicates an available appointment at the time
of scraping.  (Given the scale of the plot, all appointments for a
given day appear as one blue dot.)

<p align="center">
<img src="center-10188-Blood-Test-schedule.png" alt="Timeline of available appointments at center 10188" width="1000"/>
</p>

It is not very easy to read, but it illustrates some of the features
of the dataset.

- The data was collected over a period of roughly 24 hours.  Also, the
  data was not collected at regular times: during the night, data was
  collected around each hour.  During the day, data was collected
  mostly regularly, with short gaps.  This is due to the fact that the
  data was collected on my personal laptop, which goes to sleep when
  not used, or which I had to close when moving places.  These
  technical challenges are by now mostly overcome, as I am now
  collecting the data on a separate laptop dedicated for this
  purpose.  
- The platform does not display all appointments in the
  future. Rather, only the appointments up to about 9 March 2019 are
  open for booking.  In fact, a closer look reveals that new
  appointments on 9 Mar 2019 are open for booking starting at
  midnight on 16 Jan 2019.  This suggests that appointments for a new
  day are made available online at the same time, at midnight on a
  fixed number of days ahead of the appointment day.  This hypothesis
  is tested more thoroughly in `UHCW_EDA.ipynb` for all test centers
  and all test types.  



The figure above displays raw datat (filtered for a specific test
  center and test type), from which we wish to extract meaningful and
  accurate information from this data.     

### Reconstructing the booking history
The code in the notebook `UHCW_reconstruction.ipynb` **reconstructs**
the booking history of the appointments at all test centers.  For
example, the code produces a representation of the booking history
such as in the image below, where a blue cross represents a booking
and an orange cross represents a cancellation at center 10188 (test
type "Blood Test").


<p align="center">
<img src="center-10188-Blood-Test-history.png" alt="Booking history for center 10188 (Blood Test)"/>
</p>

## More on the data


The dataset `centers.csv` contains the following information on each *test center*:  

1. `name`  
1. `id` number  
1. postal `address`  
1. geocoordinates (`latitude` and `longitude`)  
1. center's `web` address  
1. `appointments URL`  
1. `age group` served (children or adults)  

The datasets `sample_appointments.csv` and `appointments.csv`
(available on
[kaggle](https://www.kaggle.com/antoinechoffrut/uhcwappointments))
contain the following information on each *appointment*:

1. `appointment timestamp` (date and time of appointment)  
1. `grab timestamp` (date and time data is collected)  
1. `center id`  
1. `age group` served (children or adults)  
1. `test type` (blood test, INR test, fasting bloods, non-fasting
     bloods, glucose tolerance test)  

## Scripts and notebooks

- The script `UHCW_scraping.py` downloads the data hourly (most of the
  time).  
- The Jupyter notebook `UHCW_EDA.ipynb` helps to understand the UHCW
  dataset.  
- The Jupyter notebook `UHCW_reconstruction.ipynb` reconstructs the
  booking history of the appointments from the dataset.  
- The file `UHCW.py` contains custom functions and classes used in the
  notebooks.  
