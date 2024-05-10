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

## Solution Evaluation

> how it would scale to 100,000s events over multiple days and 100s of ids

I've wrote the application thinking about this scenario. I believe the Fill() should be ok as it is.

It could become particularly complicated to scale the pivot() function given the amount of columns and the fact that you need to get the state of every other column to build the last line.

In the current approach of applying a window function for every column generated forward fill the values, the definition of the window would be too broad if we did not limited it somehow and we would end up losing all spark paralelism. I choose to partition the data by day as an assumption that it would matter more to have all the states of a given operation day, not necessarily of all days. And of course, assuming that the data of a full day fits into a single worker. Those assumptions can be false, in this case this strategy would need to be reconsidered.

Additional tunning could be made by experimenting with spark configurations.
