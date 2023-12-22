Summary
Customers using DynamoDB have the need to insert data into existing DynamoDB tables using AWS Command Line Interface (CLI) because their organizations do not allow direct access to AWS Management Console due to security reasons.
Customers, many a times have data into a CSV file that needs to be inserted into DynamoDB table. You cannot directly insert data from CSV file into an existing DynamoDB table.
This pattern provides step by step process on how to convert a CSV file containing data into DynamoDB format JSON file that can be directly imported into an existing DynamoDB table.
Create Python script in Visual Studio Code:-
Copy the Python code provided with this pattern and create a python script named csvtojsondynamodb.py
Execute the Python script:-
Open Mac terminal or Windows CMD and enter the following command to execute the script:-
python3.10 csvtojsondynamodb.py
Above command assumes that you have Python version 3.10 installed and that you are executing the script from the same directory where the python script exists.
If you have other version of Python installed, you can check the same using the following command:-
python —version
During runtime, python code accepts the following mandatory inputs:-
Path where CSV file exists
Path where you want the JSON file to be generated.
Name of the existing DynamoDB table where you want to ingest the data
Post execution of the Python script:-
After  successful execution of the python script, JSON file in DynamoDB format is generated and ingested into DynamoDB. Since a single batch write supports 25 items to be written into DynamoDB, multiple JSON files are created with 25 items each. Details about the generation of JSON files and ingestions into DynamoDB are generated after the execution of the script.
