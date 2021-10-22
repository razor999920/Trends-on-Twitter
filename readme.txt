How to run part A of application:


First to run twitter_app.py:
1) In terminal, create a container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash

2) After creating the docker run the following command:
    ~ pip install tweepy   

3) To start the twitter app, cd to the directory where the python scripts exist and run the following command:
    ~ python twitter_app_A.py

4) In a new terminal, create a new container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --name twitter_plot -w /app python bash

5) To start the dashboard portion, cd to the directory where the python scripts exist and run the following command:
    ~ python app.py

6) In another new terminal, create a new container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --link twitter:twitter --link twitter_plot:twitter_plot eecsyorku/eecs4415

7) To start the spark portion of the application, run the folloing command:
    ~ spark-submit spark_app_A.py


How to run Part B application:

First to run twitter_app.py:
1) In terminal, create a container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash

2) After creating the docker run the following command:
    ~ pip install tweepy   

3) To start the twitter app, cd to the directory where the python scripts exist and run the following command:
    ~ python twitter_app_B.py

4) In a new terminal, create a new container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --name twitter_plot -w /app python bash

5) To start the dashboard portion, cd to the directory where the python scripts exist and run the following command:
    ~ pip install flask
    ~ python app.py

6) In another new terminal, create a new container using docker by running the following command:
    ~ docker run -it -v $PWD:/app --link twitter:twitter --link twitter_plot:twitter_plot eecsyorku/eecs4415

7) Install all nlt packages by running the following commands
    ~ apt-get update
    ~ apt-get install gcc
    ~ apt-get install python-dev python3-dev
    ~ apt-get install python-nltk

8) Type 'python' in terminal and click enter

9) To install the neccessary libraries type in the following commands:
    ~ import nltk
    ~ nltk.downloader.download('vader_lexicon')

10) To start the spark portion of the application, run the folloing command:
    ~ spark-submit spark_app_B.py
    






