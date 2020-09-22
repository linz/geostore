# Geospatial Data Lake
Central storage, management and access for important geospatial datasets
Developed by [Land Information New Zealand](https://github.com/linz)


## Installation
* Create and activate a virtual environment.

```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
```

* Upgrade pip and install the required dependencies

```bash
$ pip install --upgrade pip
$ pip install -r requirements.txt
```

* Install Data Lake

```bash
$ python ./setup.py install
```


## Development
* Install development dependencies

```bash
$ pip install -r requirements-dev.txt
```

* Install commit-msg git hook

```bash
$ pre-commit install --hook-type commit-msg
```

* Install automatic Pylint code checks for your editor or run it by hand
```
$ pylint <DIRECTORY-PATH>
```

* Install automatic Black code formatting for your editor or run it by hand
```
$ black . --check --diff
```
