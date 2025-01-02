## Skynet Saviors
<br> Ray Zeng <br> Swastik Chowdhury <br> Zhijie Xu <br> Aditya Narendra Kolpe <br> Siddharth Shah <br>

## Installation

Clone:
```
$ git clone https://github.com/mlip-cmu/group-project-f24-skynetsaviors.git
$ cd group-project-f24-skynetsaviors
```
Create & Activate virtual env then install dependency:

```
$ python -m venv env  # use `virtualenv env` for Python2, use `python3 ...` for Python3 on Linux & macOS
$ source env/bin/activate  # use `env\Scripts\activate` on Windows
$ pip install -r requirements.txt
```

## Environment Variables

To configure the necessary environment variables, follow these steps:
<br>
Create a .env file in the root directory of your project (if not already created). On the terminal.
```
touch .env
```
<br>

Add the tmdb api to your .env file

```
TMDB_API_KEY = "<api-key>"
```

## Model Training and Prediction
Enter the model diretory
```
cd recommendation-model
```
Random Forest Regressor
```
python3 recommendation-model/random_forest_model.py train
python3 recommendation-model/random_forest_model.py predict
```


## How to create get a TMDB API

Follow this link : https://www.youtube.com/watch?v=FlFyrOEz2S4&t=20s 


