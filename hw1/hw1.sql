DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, helper;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  where weight>300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  where namefirst ~ ' '
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), count(*)
  FROM master
  GROUP BY birthyear
  ORDER BY birthyear ASC
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), count(*)
  FROM master
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, h.playerid, yearid
  FROM master m, halloffame h
  WHERE m.playerid = h.playerid
  AND h.inducted = 'Y'
  ORDER BY yearid
  DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, q.playerid, c.schoolid, q.yearid
  FROM q2i q, collegeplaying c, schools s
  WHERE q.playerid = c.playerid
  AND s.schoolid = c.schoolid
  AND s.schoolstate = 'CA'
  ORDER BY yearid DESC, schoolid ASC, q.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT q.playerid, namefirst, namelast, c.schoolid
  FROM q2i q LEFT OUTER JOIN collegeplaying c
  	ON q.playerid = c.playerid
  ORDER BY q.playerid DESC, c.schoolid ASC
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT m.playerid, namefirst, namelast, yearid, CAST(h + h2b + 2*h3b + 3*hr AS FLOAT)/CAST(ab AS FLOAT) as slg
  FROM master m, batting b
  WHERE m.playerid = b.playerid
  AND ab > 50
  ORDER BY slg DESC, yearid ASC, m.playerid ASC
  limit 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  WITH temp(playerid, sumh, sumab) AS
  (SELECT b.playerid, sum(b.h+b.h2b+2*b.h3b+3*b.hr), sum(b.ab)
  	FROM batting b
  	GROUP BY b.playerid)
  SELECT m.playerid, m.namefirst, m.namelast, CAST(b.sumh AS FLOAT)/CAST(b.sumab AS FLOAT) as lslg
  FROM master m, temp b
  WHERE m.playerid = b.playerid 
  GROUP BY m.playerid, b.sumh, b.sumab
  HAVING b.sumab > 50
  ORDER BY lslg DESC, m.playerid ASC
  limit 10
;

CREATE VIEW helper(playerid, namefirst, namelast, lslg)
AS
  WITH temp(playerid, sumh, sumab) AS
  (SELECT b.playerid, sum(b.h+b.h2b+2*b.h3b+3*b.hr), sum(b.ab)
  	FROM batting b
  	GROUP BY b.playerid)
  SELECT m.playerid, m.namefirst, m.namelast, CAST(b.sumh AS FLOAT)/CAST(b.sumab AS FLOAT) as lslg
  FROM master m, temp b
  WHERE m.playerid = b.playerid 
  GROUP BY m.playerid, b.sumh, b.sumab
  HAVING b.sumab > 50
  ORDER BY lslg DESC, m.playerid ASC
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
	WITH willie(playerid, lslg) AS
	(SELECT playerid, CAST(sum(h+h2b+2*h3b+3*hr) AS FLOAT)/CAST(sum(ab) AS FLOAT) as lslg
	 FROM batting
	 WHERE playerid = 'mayswi01'
	 GROUP BY playerid
	 HAVING sum(ab) > 50)
  SELECT m.namefirst, m.namelast, q.lslg
  FROM master m, helper q, willie w
  WHERE q.lslg > w.lslg
  AND m.playerid = q.playerid
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT s.yearid, min(salary), max(salary), avg(salary), stddev(salary)
  FROM salaries s
  GROUP BY yearid
  ORDER BY yearid ASC

;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
	WITH temp(low, high, binwidth) AS
	(SELECT min(salary), max(salary), (max(salary)-min(salary))/10 as binwidth
	 FROM salaries
	 WHERE yearid = 2016)
  SELECT CASE WHEN FLOOR((salary-t.low)/binwidth) >= 10 THEN 9
              ELSE FLOOR((salary-t.low)/binwidth) END AS binid,
  CASE WHEN FLOOR((salary-t.low)/binwidth) = 0 THEN t.low
  													WHEN FLOOR((salary-t.low)/binwidth) = 1 THEN t.low +binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 2 THEN t.low + binwidth*2
  													WHEN FLOOR((salary-t.low)/binwidth) = 3 THEN t.low + binwidth*3
  													WHEN FLOOR((salary-t.low)/binwidth) = 4 THEN t.low + binwidth *4
  													WHEN FLOOR((salary-t.low)/binwidth) = 5 THEN t.low + binwidth*5
  													WHEN FLOOR((salary-t.low)/binwidth) = 6 THEN t.low + binwidth*6 
  													WHEN FLOOR((salary-t.low)/binwidth) = 7 THEN t.low + binwidth*7 
  													WHEN FLOOR((salary-t.low)/binwidth) = 8 THEN t.low+ binwidth*8 
  													WHEN FLOOR((salary-t.low)/binwidth) >= 9 THEN t.low+ binwidth*9  END as low, 
  													CASE WHEN FLOOR((salary-t.low)/binwidth) = 0 THEN t.low + 1*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 1 THEN t.low + 2*binwidth 
  													WHEN FLOOR((salary-t.low)/binwidth) = 2 THEN t.low + 3*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 3 THEN t.low + 4*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 4 THEN t.low + 5*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 5 THEN t.low + 6*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 6 THEN t.low + 7*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 7 THEN t.low + 8*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) = 8 THEN t.low + 9*binwidth
  													WHEN FLOOR((salary-t.low)/binwidth) <= t.high THEN t.low  + 10*binwidth END as high, count(*)
  FROM temp t, salaries s
  WHERE s.yearid = 2016
  GROUP BY binid, CASE WHEN FLOOR((salary-t.low)/binwidth) = 0 THEN t.low
                            WHEN FLOOR((salary-t.low)/binwidth) = 1 THEN t.low +binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 2 THEN t.low + binwidth*2
                            WHEN FLOOR((salary-t.low)/binwidth) = 3 THEN t.low + binwidth*3
                            WHEN FLOOR((salary-t.low)/binwidth) = 4 THEN t.low + binwidth *4
                            WHEN FLOOR((salary-t.low)/binwidth) = 5 THEN t.low + binwidth*5
                            WHEN FLOOR((salary-t.low)/binwidth) = 6 THEN t.low + binwidth*6 
                            WHEN FLOOR((salary-t.low)/binwidth) = 7 THEN t.low + binwidth*7 
                            WHEN FLOOR((salary-t.low)/binwidth) = 8 THEN t.low+ binwidth*8 
                            WHEN FLOOR((salary-t.low)/binwidth) >= 9 THEN t.low+ binwidth*9 END, 
                            CASE WHEN FLOOR((salary-t.low)/binwidth) = 0 THEN t.low + 1*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 1 THEN t.low + 2*binwidth 
                            WHEN FLOOR((salary-t.low)/binwidth) = 2 THEN t.low + 3*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 3 THEN t.low + 4*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 4 THEN t.low + 5*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 5 THEN t.low + 6*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 6 THEN t.low + 7*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 7 THEN t.low + 8*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) = 8 THEN t.low + 9*binwidth
                            WHEN FLOOR((salary-t.low)/binwidth) <= t.high THEN t.low  + 10*binwidth END, binwidth
  ORDER BY binid ASC
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH valueset(vyearid, vmin, vmax, vavg) AS
  (SELECT yearid, min(salary), max(salary), avg(salary)
  	FROM salaries
  	GROUP BY yearid)
  SELECT s.yearid, min(s.salary - v.vmin) as mindiff, max(s.salary-v.vmax) as maxdiff, avg(s.salary)-v.vavg as avgdiff
  FROM salaries s, valueset v WHERE s.yearid-1=v.vyearid
  GROUP BY s.yearid, v.vavg
  ORDER BY s.yearid ASC
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT m.playerid, m.namefirst, m.namelast, max(s.salary), s.yearid
  FROM master m , salaries s
  WHERE m.playerid = s.playerid AND s.salary = (SELECT MAX(s2.salary) FROM salaries s2 where s2.yearid = 2000)
  GROUP BY s.yearid, m.playerid
  HAVING s.yearid = 2000
  UNION
  SELECT m.playerid, m.namefirst, m.namelast, max(s.salary), s.yearid
  FROM master m , salaries s
  WHERE m.playerid = s.playerid AND s.salary = (SELECT MAX(s2.salary) FROM salaries s2 where s2.yearid = 2001)
  GROUP BY s.yearid, m.playerid
  HAVING s.yearid = 2001
;

