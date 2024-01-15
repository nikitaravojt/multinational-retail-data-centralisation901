# AiCore MRDC Project

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#filestructure)

## Description
This project provides tools to perform data extraction and cleaning on the MRDC customer, card, store, products orders and date events datasets. 
Contains methods to connect to remote and local AWS RDS instance, S3 bucket, as well as read data from a PDF file, given credentials provided as
a .yaml file. Contains functionality to send HTTP requests to pull data from remote endpoint. Data cleaning and validation is performed mainly 
through the use of regex.

## Installation
To access the methods, follow these steps:
  1. Clone the repository to your local machine:
      
    git clone https://github.com/nikitaravojt/multinational-retail-data-centralisation901.git
  
  2. Navigate to the project directory:
     
    cd multinational-retail-data-centralisation901
  
  3. Create a new file and import your desired class objects.

## Usage
After importing the project libraries, you can create a class instance of DataExtractor() from data_extraction.py, 
DatabaseConnector() from database_utils.py and DataCleaning() from data_cleaning.py, depending on your requirements. 
You must create a .yaml file containing the remote and local database credentials to allow you to pull data from
remote as well as upload clean data to local. It is recommended to add this credentials file to your .gitignore if
you are planning to upload to GitHub for security reasons.

## File Structure
The project contains three main files:
  1. database_utils.py - provides class methods for database operations such as reading credentials from a .yaml file,
     generating sqlalchemy engines for connecting to remote and uploading clean dataframes, as well as listing
     tables found in a target database.
  2. data_extraction.py - provides class methods to pull data from a AWS RDS instance, S3 bucket or a PDF file. Provides
     a method to send HTTP requests to pull data from an endpoint.
  3. data_cleaning.py - provides class methods to clean and validate the customer, card, store, products orders and date
     events datasets. These methods make use of private helper methods defined in the DataCleaning() class to perform
     cleaning operations on individual columns of the aforementioned datasets. 
