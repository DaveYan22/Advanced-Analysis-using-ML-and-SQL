-- Find the Top 3 Most Frequent Passengers
select p.name, count(pt.trip_no) as flight_count from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
group by p.name
order by count(pt.trip_no) desc
limit 3;

-- List All Passengers Who Took at Least One Trip from 'London' to 'Singapore'
select p.name, count(pt.trip_no) as flight_count from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
where t.town_from = 'London' and  t.town_to = 'Singapore'
group by p.name
having count(pt.trip_no) > 1;

-- Find Passengers Who Flew More Than Once on the Same Trip
select p.name, count(pt.trip_no) as trips_taken, pt.trip_no as flight_count from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
group by p.name, pt.trip_no
having count(pt.trip_no) > 1;

-- List Passengers Who Have Never Taken a Trip
select p.name, pt.trip_no from passenger p 
left join pass_in_trip pt on pt.ID_psg = p.ID_psg
left join trip t on t.trip_no = pt.trip_no
where pt.trip_no is null;

-- Find the Longest Duration Flight and Its Info
select p.name, pt.trip_no, t.time_in, t.time_out, 
timestampdiff(minute, t.time_in, if(t.time_out < t.time_in, DATE_ADD(time_in, INTERVAL 1 day), t.time_out)) as time_diff
from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no;

-- List All Dates Where ‘Bruce Willis’ and ‘George Clooney’ Were on the Same Trip
select pt1.trip_no, pt1.date from pass_in_trip pt1
inner join pass_in_trip pt2 on pt1.trip_no = pt2.trip_no and pt1.date = pt2.date
inner join passenger p1 on p1.ID_psg = pt1.ID_psg
inner join passenger p2 on p2.ID_psg = pt2.ID_psg
where p1.name in ('Bruce Willis', 'George Clooney');

-- Count Unique Trips Per Plane Type
select plane, count(distinct trip_no) as uniqueTrips from trip
group by plane;

-- For Each Passenger, Show Their First and Last Trip Dates
select p.name, min(pt.date) as first_date, max(pt.date) as last_date from passenger p 
inner join pass_in_trip pt on p.ID_psg = pt.ID_psg
group by p.name;

-- List Passengers Who Have Been on Trips in at Least 2 Different Cities
select p.name, count(distinct t.town_to) from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
group by p.name
having count(distinct t.town_to) > 1;

-- Find the Trip with the Highest Number of Passengers
select pt.trip_no, count(distinct p.name) as NumOfPass from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
group by pt.trip_no
order by count(distinct p.name) desc;

-- Find passengers who flew the same route (same from/to cities):
select p.name, pt.trip_no, t.town_from, t.town_to, count(pt.trip_no) as flights from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
group by p.name, pt.trip_no, t.town_from, t.town_to
having count(pt.trip_no) > 1;

-- Find passengers who flew the same route (same from/to cities):
select distinct t.town_from, t.town_to, count(distinct p.name) as NumOfPassengers FROM Passenger p
JOIN Pass_in_trip pit ON p.ID_psg = pit.ID_psg
JOIN Trip t ON pit.trip_no = t.trip_no
group by t.town_from, t.town_to 
having count(distinct p.name) > 1
order by count(distinct p.name) desc;

-- =================================================================================================================
-- Get the Top 3 Passengers by Flight Count and Their Ranking
select * from (
select p.name, count(pt.trip_no) as flights, 
dense_rank() over(order by count(pt.trip_no) desc) as ranking
from passenger p 
inner join pass_in_trip pt on pt.ID_psg = p.ID_psg
inner join trip t on t.trip_no = pt.trip_no
group by p.name
) a 
where ranking <= 3;

-- For Each Passenger, Show the Next Passenger (by ID_psg) With Higher Flight Count
WITH CTE as (
select p.name, p.ID_psg, count(pt.trip_no) as flight_counts, 
dense_rank() over(order by count(pt.trip_no) desc) as ranks FROM Passenger p
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
group by p.name, p.ID_psg
order by count(pt.trip_no) asc
) select *, lead(name) over(order by flight_counts asc) as next_passenger from CTE;

-- Return the Passenger(s) With the Same Number of Flights as Someone Whose Name Starts With 'A'
WITH NumFlights as (
select p.name, p.ID_psg, count(pt.trip_no) as flight_count
FROM Passenger p
LEFT JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
where p.name like 'A%'
group by p.name, p.ID_psg)
select distinct NF1.name, NF1.flight_count from NumFlights NF1 
JOIN NumFlights NF2 on NF1.flight_count = NF2.flight_count
AND NF1.name != NF2.name;


-- Who Had the Most Flights in Each Year?
with MostFlights as (
select p.name, p.ID_psg, year(pt.date) as flight_year, count(pt.trip_no) as flight_count, 
dense_rank() over(partition by year(pt.date) order by count(pt.trip_no) desc) as rnk
FROM Passenger p
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
group by p.name, p.ID_psg, year(pt.date)
)
select * from MostFlights
where rnk = 1;

with OrderedFlights as (
select p.name, p.ID_psg, year(pt.date) as flight_year, count(pt.trip_no) as flight_count
FROM Passenger p
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
group by p.name, p.ID_psg, year(pt.date)
), 
MaxFlights as (
select *, dense_rank() over(partition by flight_year order by flight_count desc) as rnk
from OrderedFlights)
select * from MaxFlights
where rnk = 1;

-- Find the Passenger Who Traveled the Most Unique Routes
WITH flightPassengers as (
select p.name, p.ID_psg, count(distinct concat(t.town_from, ' ', t.town_to)) as unique_flights
FROM Passenger p 
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
join trip t on t.trip_no = pt.trip_no
group by p.name, p.ID_psg
), UniqueRoutes as (
select *, 
rank() over(order by unique_flights desc) as ranking from flightPassengers
order by unique_flights desc)
select * from UniqueRoutes 
where ranking = 1;

-- For Each Passenger, Show Whether They Have Ever Traveled Back and Forth (A→B then B→A)
With tripPassengers as (
select p.name, t.town_from, t.town_to
FROM Passenger p 
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
join trip t on t.trip_no = pt.trip_no), 
pair_flights as (
select tp1.name, tp1.town_from, tp1.town_to  
from tripPassengers tp1
inner join tripPassengers tp2 
on tp1.town_from = tp2.town_to and tp1.town_to = tp2.town_from and tp1.name = tp2.name)
select name, town_from, town_to, count(*) as NumOfFlights from pair_flights
group by name, town_from, town_to
order by count(*) desc;

-- Find Passengers Who Flew 10% of Every Available Trip
WITH totalTrips AS (
SELECT COUNT(DISTINCT trip_no) AS total_trip_count
FROM Trip) 
SELECT p.ID_psg, p.name, COUNT(DISTINCT pt.trip_no) AS passenger_trip_count
FROM Passenger p
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
GROUP BY p.ID_psg, p.name
HAVING COUNT(DISTINCT pt.trip_no) >= (
SELECT total_trip_count / 10 FROM totalTrips);

-- For Each Passenger, Show First and Last City They Ever Visited
WITH tripDates as (
select p.name, t.town_from, t.town_to, pt.date
FROM Passenger p 
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
join trip t on t.trip_no = pt.trip_no), 
FirstLastCities as (
select name, date, town_from, town_to,
row_number() over(partition by name order by date desc) as lastCity,
row_number() over(partition by name order by date asc) as firstCity
from tripDates)
select name,
max(case 
	when lastCity = 1 then town_from end) as firstCity,
min(case 
	when lastCity = 1 then town_to end) as lastCity
from FirstLastCities
group by name;

-- Find All Cities That Were Final Destinations for More Than 90% of Passengers
WITH LastVisit AS (
  SELECT p.name, t.town_to, pt.date, 
  ROW_NUMBER() OVER (PARTITION BY p.name ORDER BY pt.date DESC) AS rnk
  FROM Passenger p 
  JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
  JOIN Trip t ON t.trip_no = pt.trip_no
), 
Visits AS (
SELECT town_to, COUNT(name) AS NumOfTrips
FROM LastVisit
WHERE rnk = 1
GROUP BY town_to
),
TotalPassengers AS (
SELECT COUNT(DISTINCT name) AS totalPass
FROM Passenger
)
select v.town_to, v.NumOfTrips, tp.totalPass, ((NumOfTrips * 100) / tp.totalPass) as percentage from Visits v
cross join TotalPassengers tp
where ((NumOfTrips * 100) / tp.totalPass) > 10;

-- For Each Passenger, Show the Average Time Between Flights
SELECT p.name, t.time_out, t.time_in, 
TIMESTAMPDIFF(MINUTE, t.time_out, 
    CASE 
      WHEN t.time_in < t.time_out THEN ADDTIME(t.time_in, '24:00:00') 
      ELSE t.time_in 
    END
  ) AS minute_diff
FROM Passenger p 
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
JOIN Trip t ON t.trip_no = pt.trip_no;

WITH minutesDiff as (
SELECT p.name, t.time_out, t.time_in,
  TIMESTAMPDIFF(MINUTE, t.time_out, 
    IF(t.time_in < t.time_out, ADDTIME(t.time_in, '24:00:00'), t.time_in)
  ) AS minute_diff
FROM Passenger p 
JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
JOIN Trip t ON t.trip_no = pt.trip_no)
select distinct name, avg(minute_diff) over(partition by name) as avgFlightTime from minutesDiff;

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
SELECT p.name, p.ID_psg, count(pt.trip_no) as flight_count
FROM Passenger p 
LEFT JOIN Pass_in_trip pt ON p.ID_psg = pt.ID_psg
LEFT JOIN Trip t ON t.trip_no = pt.trip_no
group by p.name, p.ID_psg), 
MaxFlights as (
select max(flight_count) as max_flight_count from PassengerFlights),
TopPassengers as (
select pf.name, pf.ID_psg, pf.flight_count from PassengerFlights pf
inner join MaxFlights mf on mf.max_flight_count = pf.flight_count),
RankedPassengers as (
select pf.name, pf.ID_psg,
lag(pf.name) over(order by pf.ID_psg) as previousPassenger,
lead(pf.name) over(order by pf.ID_psg) as nextPassenger
from PassengerFlights pf
) select  
tp.name,
coalesce(rp.previousPassenger, (select name from RankedPassengers order by ID_psg desc limit 1)) as previousPassenger, 
coalesce(rp.nextPassenger, (select name from RankedPassengers order by ID_psg asc limit 1)) as nextPassenger
from RankedPassengers rp
inner join TopPassengers tp on rp.ID_psg = tp.ID_psg;


-- =================================================================================

-- Triggers are used to automatically react to events like INSERT, UPDATE, DELETE.

-- Create a trigger to log when a passenger's name changes

CREATE TABLE Passenger_Audit (
    audit_id INT AUTO_INCREMENT PRIMARY KEY,
    id_psg INT,
    old_name VARCHAR(100),
    new_name VARCHAR(100),
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Now create the trigger
DELIMITER //
CREATE TRIGGER after_passenger_update
AFTER UPDATE ON Passenger
FOR EACH ROW
BEGIN
    IF OLD.name != NEW.name THEN
        INSERT INTO Passenger_Audit (id_psg, old_name, new_name)
        VALUES (OLD.ID_psg, OLD.name, NEW.name);
    END IF;
END //
DELIMITER ;


-- Create a trigger that blocks deleting a trip if passengers are booked on it.
DELIMITER //

CREATE TRIGGER before_trip_delete
BEFORE DELETE ON Trip
FOR EACH ROW
BEGIN
    DECLARE passenger_count INT;
    
    SELECT COUNT(*) INTO passenger_count
    FROM Pass_in_trip
    WHERE trip_no = OLD.trip_no;
    
    IF passenger_count > 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Cannot delete trip: Passengers still booked.';
    END IF;
END //
DELIMITER ;

-- =========================================================================
-- Indexes are used to speed up queries on large tables

-- Speed up searches for passengers' trips.
CREATE INDEX idx_passenger_id ON Pass_in_trip(ID_psg);
SELECT * FROM Pass_in_trip WHERE ID_psg IN (12345, 67890);

-- Speed up searching by routes.
CREATE INDEX idx_route ON Trip(town_from, town_to);

SELECT * FROM Trip 
WHERE town_from = 'Moscow' AND town_to = 'Saint Petersburg';

-- =============================================================================
-- Stored procedures are predefined SQL blocks you can call easily — better for complex logic.

DELIMITER //

CREATE PROCEDURE BookPassengerOnTrip(IN passenger_id INT, IN trip_id INT)
BEGIN
    INSERT INTO Pass_in_trip (ID_psg, trip_no, date)
    VALUES (passenger_id, trip_id, CURRENT_DATE());
END //

DELIMITER ;

CALL BookPassengerOnTrip(101, 3050);


-- A procedure that lists trips for a given passenger.
DELIMITER //
CREATE PROCEDURE GetPassengerTrips(IN passenger_name VARCHAR(100))
BEGIN
    SELECT t.trip_no, t.town_from, t.town_to, pit.date
    FROM Passenger p
    JOIN Pass_in_trip pit ON p.ID_psg = pit.ID_psg
    JOIN Trip t ON pit.trip_no = t.trip_no
    WHERE p.name = passenger_name
    ORDER BY pit.date;
END //

DELIMITER ;

CALL GetPassengerTrips('Bruce Willis');

-- ===========================================================
/* 
A SQL function is a reusable block of code that accepts input parameters, performs operations 
(such as calculations, data lookups, or formatting), and returns a single value.
Functions are mainly used inside SQL statements (like SELECT, WHERE, ORDER BY) to simplify and modularize complex logic.
*/

-- Create a function that takes trip_no and returns a nice formatted route like 'Moscow -> Rostov'.

DELIMITER //

CREATE FUNCTION GetRoute(tripId INT)
RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
    DECLARE from_city VARCHAR(100);
    DECLARE to_city VARCHAR(100);
    DECLARE route VARCHAR(255);
    
    SELECT town_from, town_to
    INTO from_city, to_city
    FROM Trip
    WHERE trip_no = tripId;
    
    SET route = CONCAT(from_city, ' -> ', to_city);
    
    RETURN route;
END //
DELIMITER ;

SELECT GetRoute(1123);  

-- Create a function that returns how many trips a passenger (by ID) has booked.


DELIMITER //

CREATE FUNCTION GetPassengerTripCount(passengerId INT)
RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE trip_count INT;
    
    SELECT COUNT(*) INTO trip_count
    FROM Pass_in_trip
    WHERE ID_psg = passengerId;
    
    RETURN trip_count;
END //

DELIMITER ;

SELECT GetPassengerTripCount(101);


