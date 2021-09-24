This project is for a small distributed data processing pipeline in the apache beam framework.

- `src/main.py` contains the pipeline code. This file has incomplete sections marked with `TODO` that you need to fill in.
- `src/main__test.py` contains test code for the pipeline.
- `src/schema.sql` contains the postgres sql used to define a schema and insert some data.

Go through these steps to get started:

- Install `docker`: https://docs.docker.com/get-docker/
- Install `docker-compose`: https://docs.docker.com/compose/install/
- Run `docker-compose up` in this folder. The first time you run this command, a docker image defined by `Dockerfile` will be built, which provides a python environment to run the pipeline. A postgres database will also be started that will be initialized with `src/schema.sql`.
- In a separate shell, run `./test.sh`. This script runs the `pytest` command inside the python docker container currently running through `docker-compose`.

If you stop the `docker-compose up` command, and then run `docker-compose down`, it will reset your database to the initial state.

You will likely find these resources useful:

- Documentation for python's `date` type: https://docs.python.org/3/library/datetime.html#date-objects
- Beam transform reference: https://beam.apache.org/documentation/transforms/python/overview/
- Beam programming guide (don't read this entire thing, just enough to understand what's going on): https://beam.apache.org/documentation/programming-guide/

Also, putting `-k name_of_test_function` at the end of the `pytest` call in `./test.sh` will allow you to only run one test at a time.
