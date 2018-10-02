-------------------------------------------------------------------------------
-- P03_1 - Seat_validation ----------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fSeatCeck();
DROP TRIGGER IF EXISTS tSeatCheck ON Bookings;

-- Create a trigger on the Bookings table that throws a descriptive error 
-- if the given seat number does not exist in the corresponding venue.

CREATE OR REPLACE FUNCTION fSeatCheck()
RETURNS TRIGGER
AS $$ 
BEGIN
	-- Raises exception when seat_id is negative
	IF (NEW.seat_id < 1) THEN
		RAISE EXCEPTION 'The seat number can not be negative or zero' USING ERRCODE = '45000';
	END IF;

	-- Raises exception when seat_id is larger than number of seats in the venue
	IF (NEW.seat_id > (SELECT v.number_of_seats 
						FROM EventSchedules s
						JOIN Venues v ON v.id = s.venue_id
						WHERE s.id = NEW.schedule_id)
		) THEN
			RAISE EXCEPTION 'The seat number does not exsist in this venue' USING ERRCODE = '45001';
	END IF;
	
	RETURN NEW;
END; 
$$ LANGUAGE plpgsql;

CREATE TRIGGER tSeatCheck
BEFORE INSERT OR UPDATE OF seat_id
ON Bookings
FOR EACH ROW EXECUTE PROCEDURE fSeatCheck();

 
-------------------------------------------------------------------------------
-- P03_2 - Scheduleing_conflict_avoidance -------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fScheduleCeck();
DROP TRIGGER IF EXISTS tScheduleCheck ON EventSchedules;

-- Create a trigger on the EventSchedules table that makes sure that (i) each venue can only 
-- be booked once a day, and (ii) each event can only be scheduled once a day. If those rules 
-- are violated the trigger should throw a descriptive error and cancel the insertion.

CREATE OR REPLACE FUNCTION fScheduleCeck()
RETURNS TRIGGER
AS $$ 
BEGIN
	-- Raises exception when a venue is being booked twice in one day
	IF EXISTS (
			SELECT *
			FROM EventSchedules s
			WHERE s.venue_id = NEW.venue_id
			AND EXTRACT(YEAR FROM s.event_time) = EXTRACT(YEAR FROM NEW.event_time)
			AND EXTRACT(MONTH FROM s.event_time) = EXTRACT(MONTH FROM NEW.event_time)
			AND EXTRACT(DAY FROM s.event_time) = EXTRACT(DAY FROM NEW.event_time)
		) THEN
		RAISE EXCEPTION 'Venue can only be booked once per day' USING ERRCODE = '45002';
	END IF;

	-- Raises exception when a event is being scheduled twice in one day
	IF EXISTS (
			SELECT *
			FROM EventSchedules s
			WHERE s.event_id = NEW.event_id
			AND EXTRACT(YEAR FROM s.event_time) = EXTRACT(YEAR FROM NEW.event_time)
			AND EXTRACT(MONTH FROM s.event_time) = EXTRACT(MONTH FROM NEW.event_time)
			AND EXTRACT(DAY FROM s.event_time) = EXTRACT(DAY FROM NEW.event_time)
		) THEN
		RAISE EXCEPTION 'Events can only be schedule once per day' USING ERRCODE = '45003';
	END IF;
	
	RETURN NEW;
END; 
$$ LANGUAGE plpgsql;

CREATE TRIGGER tScheduleCheck
BEFORE INSERT OR UPDATE OF event_id, venue_id, event_time
ON EventSchedules
FOR EACH ROW EXECUTE PROCEDURE fScheduleCeck();


-------------------------------------------------------------------------------
-- P03_3 - Seat_alocation -----------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fGetNextSeatAvailable(IN schedule_id INT);

-- Create a function fGetNextSeatAvailable that takes a schedule ID 
-- as input parameter and returns the next available seat number on that event.

CREATE OR REPLACE FUNCTION fGetNextSeatAvailable(IN schedule_id INT)
RETURNS INT
AS $$ 
BEGIN
	RETURN (SELECT * 
			-- A list of all seat_ids
			FROM generate_series(1,(SELECT v.number_of_seats
									FROM EventSchedules s
									JOIN Venues v ON v.ID = s.venue_id
									WHERE s.id = $1)) AS n(available_seat)
			WHERE NOT EXISTS (
			-- All booked seat_ids
				SELECT b.seat_id
				FROM EventSchedules s
				JOIN Bookings b ON b.schedule_id = s.id AND b.seat_id = n.available_seat
				WHERE s.id = $1)
			-- Only shows the firs available seat_id
			LIMIT 1
			);
END; 
$$ LANGUAGE plpgsql;


-------------------------------------------------------------------------------
-- P03_4 - Seat_avalibility ---------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fGetNumberOfFreeSeats(IN schedule_id INT);

-- Create a  function  fGetNumberOfFreeSeats  that  takes a  schedule ID 
-- as input parameter and returns the current number of free seats on that scheduled event.

CREATE OR REPLACE FUNCTION fGetNumberOfFreeSeats(IN schedule_id INT)
RETURNS INT
AS $$ 
BEGIN
	RETURN ((SELECT v.number_of_seats		-- The number_of_seats for the schedule_id
			FROM EventSchedules s
			JOIN Venues v ON v.ID = s.venue_id 
			AND s.id = schedule_id)
			-
			(SELECT s.number_of_bookedSeats	-- Minus the number_of_bookedSeats
			FROM EventSchedules s
			WHERE s.id = schedule_id));
END; 
$$ LANGUAGE plpgsql;


-------------------------------------------------------------------------------
-- P03_5 - Counting_seats -----------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fSeatCount();
DROP TRIGGER IF EXISTS tSeatCount ON Bookings;

--Create a trigger on the Bookings table that maintains the number_of_bookedSeats
--counter in the EventSchedules table. This counter shows how many seats have been
--booked at that given event at each time.

CREATE OR REPLACE FUNCTION fSeatCount()
RETURNS TRIGGER
AS $$ 
BEGIN
	-- Increments the number_of_bookedSeats
	IF (TG_OP = 'INSERT') THEN
		UPDATE EventSchedules
		SET number_of_bookedSeats = number_of_bookedSeats + 1
		WHERE id = NEW.schedule_id;
	END IF;

	-- Detriments the number_of_bookedSeats
	IF (TG_OP = 'DELETE') THEN
		UPDATE EventSchedules
		SET number_of_bookedSeats = number_of_bookedSeats - 1
		WHERE id = OLD.schedule_id;
	END IF;
	
	RETURN NEW;
END; 
$$ LANGUAGE plpgsql;

CREATE TRIGGER tSeatCount
AFTER INSERT OR DELETE
ON Bookings
FOR EACH ROW EXECUTE PROCEDURE fSeatCount();


-------------------------------------------------------------------------------
-- P03_6 - Consecutive_seating ------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fFindConsecutiveSeats(IN schedule_id INT, IN number_of_consecutiveSeats INT);

-- Create a function fFindConsecutiveSeats that takes as input parameters a scheduled event 
-- ID and the number of consecutive seats it should find. The function returns the first 
-- (lowest) seat number where there are sufficiently many free seats in a row. If no sequence
-- of sufficiently many adjacent free seats exists, the function should throw a descriptive error

CREATE OR REPLACE FUNCTION fFindConsecutiveSeats(IN schedule_id INT, IN number_of_consecutiveSeats INT)
RETURNS INT
AS $$ 
DECLARE
	i RECORD;
	c INT := 1;
	s INT := 0;
	l INT := 0;
	
BEGIN
	l := fGetNextSeatAvailable($1);
	
	FOR i IN (
		SELECT * 
		-- A list of all seat_ids
		FROM generate_series(1,(SELECT v.number_of_seats
								FROM EventSchedules s
								JOIN Venues v ON v.ID = s.venue_id
								WHERE s.id = $1)) AS n(a)
		WHERE NOT EXISTS (
		-- All booked seat_ids
			SELECT b.seat_id
			FROM EventSchedules s
			JOIN Bookings b ON b.schedule_id = s.id AND b.seat_id = n.a
			WHERE s.id = $1)
		)
	LOOP
		IF (l + 1 = i.a) THEN 
			c := c + 1;
		ELSE 
			c := 1;
			s := i.a;
		END IF;
		l := i.a;
		IF(c = number_of_consecutiveSeats) THEN
			RETURN s;
		END IF;
	END LOOP;
		RAISE EXCEPTION 'No consecutive seats found' USING ERRCODE = '45004'
	RETURN;
END; 
$$ LANGUAGE plpgsql;

-- Testing ********************************************************************
SELECT fFindConsecutiveSeats(1,4);

SELECT * 
-- A list of all seat_ids
FROM generate_series(0,(SELECT v.number_of_seats
						FROM EventSchedules s
						JOIN Venues v ON v.ID = s.venue_id
						WHERE s.id = 1)) AS n(a)
WHERE NOT EXISTS (
-- All booked seat_ids
	SELECT b.seat_id
	FROM EventSchedules s
	JOIN Bookings b ON b.schedule_id = s.id AND b.seat_id = n.a
	WHERE s.id = 1);

-------------------------------------------------------------------------------
-- P03_7 - Multi_Booking ------------------------------------------------------
-------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS fBookSeat();
DROP FUNCTION IF EXISTS fBookManySeats();

-- Create a procedure, fBookManySeats that takes 4 input parameters (schedule ID, 
-- customer ssn, first seat number and how many seats it should book) and makes a booking 
-- for the given number of seats, starting on the first seat number provided and booking 
-- consecutive seats to the given customer.
/*
CREATE OR REPLACE FUNCTION fSeatCount(IN schedule_id INT
									, IN seat_id INT
									, IN people_ssn CHAR(10))
RETURNS VOID
AS $$ 
BEGIN
	-- Checking if the EventSchedule exist
	IF ($1 IS NOT IN (SELECT id FROM EventSchedules)) THEN 
		RAISE EXCEPTION 'Schedule does not exists' USING ERRCODE = '45005';
	END IF;
	
	-- The seat_id is validated by the tSeatCheck Trigger
	
	-- Checking if the 
	IF ($3 IS NOT IN (SELECT ssn FROM People)) THEN 
		RAISE EXCEPTION 'Person does not exists' USING ERRCODE = '45006';
	END IF;
	
	-- Increments the number_of_bookedSeats
	INSERT INTO Bookings VALUES($1, $2, $3);

END; 
$$ LANGUAGE plpgsql;
*/

CREATE OR REPLACE FUNCTION fBookManySeats(IN schedule_id INT
										, IN people_ssn CHAR(10)
										, IN seat_id INT
										, IN number_of_seats INT)
RETURNS VOID
AS $$ 
DECLARE
	i INT := 0;
BEGIN	
	LOOP
		EXIT WHEN i = $4;
		INSERT INTO Bookings VALUES($1, ($3 + i), $2);
		i  := i + 1;
	END LOOP;

END; 
$$ LANGUAGE plpgsql;

-- Testing ********************************************************************
BEGIN TRANSACTION;
DO $$ BEGIN
	PERFORM fBookManySeats(1,'1201585899',4,4);
END $$;
ROLLBACK;
-------------------------------------------------------------------------------
-- P03_8 - Multi_Book_Avalible ------------------------------------------------
-------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fFindAndBookSeats();

-- Create a procedure, fFindAndBookSeats that takes number of consecutive seats that 
-- should be booked, the schedule ID and customer ssn. This procedure books the next 
-- available seat row with given amount of seats in a row to the given schedule to the given 
-- person. If seat row is not found, a descriptive error should be thrown describing the problem.

CREATE OR REPLACE FUNCTION fFindAndBookSeats(IN number_of_seats INT
											, IN schedule_id INT
											, IN people_ssn CHAR(10))
RETURNS VOID
AS $$ 
DECLARE
	start_seat_id INT;
BEGIN
	start_seat_id := fFindConsecutiveSeats($3,$1);
	PERFORM fBookManySeats($2,$3, start_seat_id, $1);
END; 
$$ LANGUAGE plpgsql;


-------------------------------------------------------------------------------
-- P03_9 - Show_Shows ---------------------------------------------------------
-------------------------------------------------------------------------------
DROP VIEW IF EXISTS vGetShowList;

-- Create a view vGetShowList that returns a list of schedules that have not already passed. 
-- The list should show ID of schedule, ID of event, time of event, name of event, name of 
-- venue, total number of seats and how many seats are still available

CREATE OR REPLACE VIEW vGetShowList(schedule_id, 
									event_id, 
									event_time, 
									event_name, 
									venue_name,
									number_of_seats,
									number_of_availableSeats)
AS
SELECT s.ID AS schedule_id, s.event_id, s.event_time, e.name AS event_name, 
		v.name AS venue_name, v.number_of_seats, (v.number_of_seats - s.number_of_bookedSeats)
FROM EventSchedules s
JOIN Events e ON e.id = s.event_id
JOIN Venues v ON v.id = s.venue_id
WHERE CURRENT_TIMESTAMP < s.event_time;

-- Testing ********************************************************************
BEGIN TRANSACTION;
UPDATE EventSchedules
SET event_time = '2020-10-10 18:00:00'
WHERE id = 3;
SELECT * FROM vGetShowList;
ROLLBACK;
-------------------------------------------------------------------------------
-- P03_10 - Socialites --------------------------------------------------------
-------------------------------------------------------------------------------
DROP VIEW IF EXISTS vListOfVipPeople;

-- Create a view vListOfVipPeople that shows ssn, name and email of all people that have 
-- booked every event that has been scheduled in the current year. 

CREATE OR REPLACE VIEW vListOfVipPeople(ssn, name, email)
AS
SELECT p.ssn, p.name, p.email
FROM EventSchedules s
JOIN Bookings b ON b.schedule_id = s.id
JOIN People p ON p.ssn = b.people_ssn
JOIN Events e ON e.id = s.event_id
WHERE EXTRACT(YEAR FROM s.event_time) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY p.ssn
HAVING COUNT(DISTINCT e.name) = (
	SELECT COUNT(DISTINCT e.name)
	FROM EventSchedules s
	JOIN Events e ON e.id = s.event_id 
	AND EXTRACT(YEAR FROM s.event_time) = EXTRACT(YEAR FROM CURRENT_DATE)
);

-- TESTING *******************************************************************
BEGIN TRANSACTION;
INSERT INTO Bookings VALUES(1, fGetNextSeatAvailable(1) ,'1201585899');
INSERT INTO Bookings VALUES(2, fGetNextSeatAvailable(2) ,'1201585899');
INSERT INTO Bookings VALUES(3, fGetNextSeatAvailable(3) ,'1201585899');
INSERT INTO Bookings VALUES(4, fGetNextSeatAvailable(4) ,'1201585899');

SELECT * FROM Bookings WHERE people_ssn = '1201585899';
SELECT * FROM EventSchedules;
SELECT * FROM vListOfVipPeople;

DELETE FROM Bookings WHERE people_ssn = '1201585899' AND seat_id = 0;