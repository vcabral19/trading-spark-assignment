# Trading Data Spark Assignment

### Note
I've used pyspark==3.5.1 instead of pyspark==2.3.0 as suggested in the requirements.txt just because **2.3.0 installation is broken on poetry**. As it would be more convenient for me to use poetry to set up the project structure and development environment I've decided to upgrade it so I would not need to work on any crazy workarounds.
The code should run ok in pyspark==2.3.0 anyway as I'm using no newly introduced features.

Poetry also isolates the environment so you can install this version without fear of interfering with your own setup.

## How to install

[**Install Poetry**](https://python-poetry.org/docs/)
I Strongly recommend using pipx to do it!

and then:

```bash
make install
```

## How to execute the pipeline
Simple run:
```bash
make run
```

Alternatively, you can run main.py directly if you want:
```bash
spark-submit src/app/main.py
```

## How to develop

After updating the code:

Run the tests to check if you haven't broken anything
```bash
make test
```

If everything is all right with the tests, for linting:
```bash
make format
```
