# Question 1

```
docker build --help
```

Answer: `--iidfile string`

# Question 2

```
docker run -it --entrypoint bash python:3.9
pip list
```

Answer: `3`

# Question 3

```
SELECT COUNT(*)
FROM green_taxi_trips
WHERE DATE_TRUNC('day', lpep_pickup_datetime) = '2019-01-15 00:00:00'
AND DATE_TRUNC('day', lpep_dropoff_datetime) = '2019-01-15 00:00:00'
```

Answer: `20530`

# Question 4

```
SELECT DATE_TRUNC('day', lpep_pickup_datetime)
FROM green_taxi_trips
WHERE trip_distance = 
	(SELECT MAX(trip_distance)
	FROM green_taxi_trips)
```

Answer: `2019-01-15`

# Question 5

```
SELECT COUNT(*)
FROM green_taxi_trips
WHERE DATE_TRUNC('day', lpep_pickup_datetime) = '2019-01-01 00:00:00'
AND passenger_count = 2
```


```
SELECT COUNT(*)
FROM green_taxi_trips
WHERE DATE_TRUNC('day', lpep_pickup_datetime) = '2019-01-01 00:00:00'
AND passenger_count = 3
```
Answer: `2: 1282; 3:254`

# Question 6

```
WITH max_tip_Astoria_amt AS
	(SELECT MAX(green_taxi_trips.tip_amount)
	FROM green_taxi_trips JOIN taxi_zones ON green_taxi_trips."PULocationID" = taxi_zones."LocationID"
	WHERE taxi_zones."Zone" = 'Astoria')

SELECT taxi_zones."Zone"
FROM green_taxi_trips JOIN taxi_zones ON green_taxi_trips."DOLocationID" = taxi_zones."LocationID"
WHERE green_taxi_trips.tip_amount = (SELECT * FROM max_tip_Astoria_amt)
```

Answer: `Long Island City/Queens Plaza`