-- Exploring the data
select * from pass_in_trip;
select * from passenger;
select * from trip;

select count(distinct trip_no) from pass_in_trip;
select count(distinct ID_psg) from passenger;
select count(distinct trip_no) from trip;

select count(trip_no) from pass_in_trip;
select count(ID_psg) from passenger;
select count(trip_no) from trip;

SELECT * FROM passenger p
LEFT JOIN pass_in_trip pt ON p.ID_psg = pt.ID_psg
LEFT JOIN trip t ON pt.trip_no = t.trip_no;

select count(*) from pass_in_trip pt 
left join passenger p on pt.ID_psg = p.ID_psg
left join trip t on t.trip_no = pt.trip_no;
-- =========================================================================================================== --
-- Find the Top 3 Most Frequent Passengers
select p.name, count(pt.trip_no) as flight_count from  pass_in_trip pt 
left join passenger p on p.ID_psg = pt.ID_psg
group by p.name
order by count(pt.trip_no) desc
limit 3;

-- List All Passengers Who Took at Least One Trip from 'London' to 'Singapore'
select p.name, count(p.name) as num_of_flights from passenger p 
inner join pass_in_trip pt on p.ID_psg = pt.ID_psg
inner join trip t on t.trip_no = pt.trip_no
where t.town_from = 'London' and t.town_to = 'Singapore'
group by p.name
having count(pt.trip_no) >= 1;


-- Find Passengers Who Flew More Than Once on the Same Trip
select p.name, pt.trip_no, count(pt.trip_no) as flight_count from pass_in_trip pt
left join passenger p on p.ID_psg = pt.ID_psg
group by p.name, pt.trip_no
having count(pt.trip_no) > 1;

-- List Passengers Who Have Never Taken a Trip
select p.name, pt.trip_no from passenger p 
left join pass_in_trip pt on p.ID_psg = pt.ID_psg
where pt.ID_psg is NULL;

-- Find the Longest Duration Flight and Its Info
SELECT trip_no, time_out, time_in,
    TIMESTAMPDIFF(
        MINUTE, time_out, 
        IF(time_in < time_out, DATE_ADD(time_in, INTERVAL 1 DAY), time_in)
    ) AS duration_minutes
FROM trip ORDER BY duration_minutes DESC;


-- List All Dates Where ‘Bruce Willis’ and ‘George Clooney’ Were on the Same Trip
select pt1.trip_no, pt1.date from pass_in_trip pt1
join pass_in_trip pt2 on pt2.trip_no = pt1.trip_no and pt1.date = pt2.date
join passenger p1 on p1.ID_psg = pt1.ID_psg
join passenger p2 on p2.ID_psg = pt2.ID_psg
where p1.name in ('Bruce Willis', 'George Clooney');

-- Count Unique Trips Per Plane Type
select plane, count(distinct trip_no) as unique_trips from trip
group by plane
order by count(distinct trip_no) desc;

-- For Each Passenger, Show Their First and Last Trip Dates
select p.name, min(pt.date) as first_date, max(pt.date) as last_date from passenger p 
join pass_in_trip pt on p.ID_psg = pt.ID_psg
group by p.name;

-- List Passengers Who Have Been on Trips in at Least 2 Different Cities
select p.name, count(distinct town_to) as num_of_different_cities from passenger p 
join pass_in_trip pt on p.ID_psg = pt.ID_psg
join trip t on t.trip_no = pt.trip_no
group by p.name
having count(distinct t.town_to) > 2;

-- Find the Trip with the Highest Number of Passengers
select pt.trip_no, count( distinct p.name) as total_passengers from  pass_in_trip pt 
left join passenger p on p.ID_psg = pt.ID_psg
group by pt.trip_no
order by count(distinct pt.ID_psg) desc;


-- Find passengers who flew the same route (same from/to cities):
SELECT t.town_from, t.town_to,
COUNT(DISTINCT p.ID_psg) AS passenger_count,
GROUP_CONCAT(DISTINCT p.name ORDER BY p.name) AS passengers
FROM Passenger p
JOIN Pass_in_trip pit ON p.ID_psg = pit.ID_psg
JOIN Trip t ON pit.trip_no = t.trip_no
GROUP BY t.town_from, t.town_to
HAVING passenger_count > 1
ORDER BY passenger_count DESC;
 
-- Find passengers who flew the same route more than once
SELECT p.name, t.town_from, t.town_to, COUNT(*) AS times_flew
FROM Passenger p
JOIN Pass_in_trip pit ON p.ID_psg = pit.ID_psg
JOIN Trip t ON pit.trip_no = t.trip_no
GROUP BY p.name, t.town_from, t.town_to
HAVING COUNT(*) > 1
ORDER BY times_flew DESC;

-- Get the Top 3 Passengers by Flight Count and Their Ranking
with PassengerFlights  as (
select p.name, count(pt.trip_no) as flight_count from passenger p
left join pass_in_trip pt on pt.ID_psg = p.ID_psg
group by p.name
), 
RankedPassangers as (select *, dense_rank() over(order by flight_count desc) as rnk from PassengerFlights
) select * from RankedPassangers where rnk <= 3;

-- For Each Passenger, Show the Next Passenger (by ID_psg) With Higher Flight Count
WITH flightCounts as (
select p.name, p.ID_psg, count(pt.trip_no) as flight_count from passenger p 
left join pass_in_trip pt on p.ID_psg = pt.ID_psg
group by p.name, p.ID_psg
order by count(pt.trip_no) desc
) select *, lead(name) over(order by flight_count desc) as nextPassenger from flightCounts
;

WITH PassengerFlights AS (
    SELECT p.ID_psg, p.name, COUNT(pt.trip_no) AS flight_count
    FROM Passenger p
    LEFT JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
    GROUP BY p.ID_psg, p.name
),
NextHigher AS (
    SELECT a.ID_psg AS current_id, a.name AS current_name, a.flight_count,
           (
               SELECT name
               FROM PassengerFlights b
               WHERE b.flight_count > a.flight_count AND b.ID_psg > a.ID_psg
               ORDER BY b.ID_psg
               LIMIT 1
           ) AS next_higher_passenger
    FROM PassengerFlights a
)
SELECT * FROM NextHigher;

-- Return the Passenger(s) With the Same Number of Flights as Someone Whose Name Starts With 'A'
WITH FlightPassengers AS (
  SELECT p.name, COUNT(pt.trip_no) AS flight_count
  FROM passenger p
  LEFT JOIN pass_in_trip pt ON pt.ID_psg = p.ID_psg
  WHERE p.name LIKE 'A%'
  GROUP BY p.name
)
SELECT distinct fp1.name, fp1.flight_count
FROM FlightPassengers fp1
JOIN FlightPassengers fp2 
  ON fp1.flight_count = fp2.flight_count
 AND fp1.name != fp2.name;

-- Who Had the Most Flights in Each Year?
WITH flights as (
select p.name, year(pt.date) as year_date, count(pt.trip_no) as flight_count from passenger p 
left join pass_in_trip pt on pt.ID_psg = p.ID_psg
left join trip t on t.trip_no = pt.trip_no
where year(pt.date) is not null
group by p.name, year(pt.date) 
), 
maxFlights as (
select *, rank() over(partition by year_date order by flight_count desc) as max_flight from flights
)
select * from maxFlights where max_flight = 1;
 
-- Find the Passenger Who Traveled the Most Unique Routes
with PassengerRoutes as (
select pt.ID_psg,  count(distinct concat(town_from, ' ', town_to)) as uniqueFlight from pass_in_trip pt
inner join trip t on t.trip_no = pt.trip_no
group by pt.ID_psg), 
rankedRoutes as (
select *, 
rank() over(order by uniqueFlight desc) as rnk
from PassengerRoutes
)
select p.name, rr.ID_psg, uniqueFlight, rnk from rankedRoutes rr
inner join passenger p on p.ID_psg = rr.ID_psg
where rnk = 1;

-- For Each Passenger, Show Whether They Have Ever Traveled Back and Forth (A→B then B→A)
with flights as (
select pt.ID_psg, t.town_from, t.town_to from pass_in_trip pt
inner join trip t on t.trip_no = pt.trip_no
), pairs as (
select a.ID_psg, a.town_from, a.town_to from flights a
join flights b 
on a.ID_psg = b.ID_psg and a.town_from = b.town_to and b.town_from = a.town_to
order by ID_psg
), pairs2 as (
select distinct p.name, concat(town_from, '-', town_to) as cities, 
row_number() over(partition by p.name order by concat(town_from, ' ', town_to)) as rn from pairs
join passenger p on p.ID_psg = pairs.ID_psg
) select distinct name from pairs2;


-- Find Passengers Who Flew With Every Available Trip
with tripCounts as (
select count(distinct t.trip_no) as total_trips from trip t
), passengerTripCount as (
SELECT pt.ID_psg, COUNT(DISTINCT pt.trip_no) AS trips_taken
FROM Pass_in_trip pt
GROUP BY pt.ID_psg
) 
SELECT p.ID_psg, p.name
FROM passengerTripCount ptc
JOIN tripCounts tc ON ptc.trips_taken = tc.total_trips
JOIN Passenger p ON p.ID_psg = ptc.ID_psg;

SELECT p.ID_psg, p.name
FROM Passenger p
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
GROUP BY p.ID_psg, p.name
HAVING COUNT(DISTINCT pt.trip_no) = (
    SELECT COUNT(DISTINCT trip_no) FROM Trip
);


-- For Each Passenger, Show First and Last City They Ever Visited
with flights as (
select pt.ID_psg, pt.trip_no,  pt.date, t.town_from,
row_number() over(partition by pt.ID_psg order by pt.date) as first_city,
t.town_to,   
row_number() over(partition by pt.ID_psg order by pt.date desc) as last_city 
from pass_in_trip pt
inner join trip t on t.trip_no = pt.trip_no)
select p.name, 
min(case when first_city = 1 then town_from end) as firstCity, 
max(case when last_city = 1 then town_to end) as lastCity
from passenger p 
join flights f on f.ID_psg = p.ID_psg
group by p.name; 

-- Find All Cities That Were Final Destinations for More Than 90% of Passengers
with RankedFlights as (
select pt.ID_psg, pt.date, t.town_from, t.town_to, 
row_number() over(partition by ID_psg order by pt.date desc) rn
from pass_in_trip pt
inner join trip t on t.trip_no = pt.trip_no),
FinalDestination as (
select ID_psg, town_to, date as finalDestDate from RankedFlights
where rn = 1),
Counting as (
select town_to, count(ID_psg) as numOfPassenger from FinalDestination
group by town_to),
Percentage as (
select town_to, numOfPassenger from Counting),
TotalPassengers AS (
SELECT COUNT(DISTINCT ID_psg) AS total
FROM Pass_in_trip
)
SELECT c.town_to, c.numOfPassenger, tp.total, ROUND((c.numOfPassenger / tp.total) * 100) as percentage
FROM Counting c
inner JOIN TotalPassengers tp ON 1 = 1
where ROUND((c.numOfPassenger / tp.total) * 100) > 20;



-- For Each Passenger, Show the Average Time Between Flights
WITH RankedFlights as (
select pt.ID_psg, pt.date, 
lag(pt.date) over(partition by pt.ID_psg order by pt.date desc) as last_date
from pass_in_trip pt
inner join trip t on t.trip_no = pt.trip_no), 
flightDiff as (
SELECT ID_psg, timestampdiff(day, date, last_date) AS days_between
FROM RankedFlights
WHERE last_date IS NOT NULL)
select p.name, fd.ID_psg, round(avg(fd.days_between), 2) as avg_days_between from flightDiff fd
inner join passenger p on p.ID_psg = fd.ID_psg
group by fd.ID_psg, p.name;

-- Find the Passenger Who Visited the Most Unique Cities (from town_from and town_to)
With CitiesVisited as (
select pt.ID_psg, t.town_from from pass_in_trip pt 
inner join trip t on t.trip_no = pt.trip_no
union 
select pt.ID_psg, t.town_to from pass_in_trip pt 
inner join trip t on t.trip_no = pt.trip_no
order by ID_psg
),
UniqueVisits AS (
    SELECT ID_psg, COUNT(DISTINCT town_from) AS city_count
    FROM CitiesVisited
    GROUP BY ID_psg
),
TopTraveler AS (
SELECT *, RANK() OVER (ORDER BY city_count DESC) AS rnk
FROM UniqueVisits)
SELECT p.name, ut.city_count
FROM TopTraveler ut
JOIN Passenger p ON ut.ID_psg = p.ID_psg
WHERE rnk = 1;

-- Task: 
/* For a sequence of passengers ordered by ID_psg, determine who has made the most flights 
(one or more passengers can meet this criteria), as well as those who are in the sequence immediately before and after them.
For the first passenger in the sequence, the last one will be the previous one, and for the last passenger, 
the first one will be the next one.
For each passenger meeting the condition, display:
●	Name
●	Name of the previous passenger
●	Name of the next passenger  */

WITH PassengerFlights AS (
    SELECT p.ID_psg, p.name, COUNT(pt.trip_no) AS flight_count
    FROM Passenger p
    LEFT JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
    LEFT JOIN trip t on t.trip_no = pt.trip_no
    GROUP BY p.ID_psg, p.name
),
MaxFlights AS (
    SELECT MAX(flight_count) AS max_flight_count
    FROM PassengerFlights
),
TopPassengers AS (
    SELECT pf.ID_psg, pf.name, pf.flight_count
    FROM PassengerFlights pf
    JOIN MaxFlights mf ON pf.flight_count = mf.max_flight_count
),
RankedPassengers AS (
    SELECT pf.ID_psg, pf.name,
           LAG(pf.name) OVER (ORDER BY pf.ID_psg) AS previous_passenger,
           LEAD(pf.name) OVER (ORDER BY pf.ID_psg) AS next_passenger
    FROM PassengerFlights pf
)
SELECT tp.name,
       COALESCE(rp.previous_passenger, (SELECT name FROM RankedPassengers ORDER BY ID_psg DESC LIMIT 1)) AS previous_passenger,
       COALESCE(rp.next_passenger,     (SELECT name FROM RankedPassengers ORDER BY ID_psg ASC  LIMIT 1)) AS next_passenger
FROM TopPassengers tp
JOIN RankedPassengers rp ON tp.ID_psg = rp.ID_psg;