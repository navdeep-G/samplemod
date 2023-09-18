# POC

# Setup
- obtain they `keys.json` file
- create a `.env` file which is a copy of `.env.example` and fill in the values
- run `make install`
- run `make run`

# Plan
## Step 1

- read yaml file into typescript script
- connect to data warehouse
- connect to vector db
- parse yaml file to create
    - table model
    - to array of string
    - convert to embeddings
- write it to vector db

## Step 2

- add the ability to run it cron style

## Step 3

- add logic to only embed modified rows (to embed diff)

## Step 4

- Ability to run on my own cloud (self hosted)

## Step 5

- front end to parse yaml file
- visualize
- ability o write to yaml and export

## Step 6

- ability to run locally for testing