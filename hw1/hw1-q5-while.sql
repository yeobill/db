-- Q5 Iteration

DROP TABLE IF EXISTS q5_extended_paths;
CREATE TABLE q5_extended_paths(src, dest, length, path)
AS
	WITH temp(src, dest, length, path) AS
	(SELECT DISTINCT p.src, p.dest, p.length, p.path
	 FROM q5_edges e, q5_paths_to_update p
	 WHERE e.src =p.src
	)
	SELECT t.src, e.dest, t.length + e.length, array_append(t.path, e.dest)
	FROM q5_edges e, temp t
	WHERE t.dest = e.src AND t.src != e.dest
;

CREATE TABLE q5_new_paths(src, dest, length, path)
AS
	WITH temp(src, dest, length) AS
	(SELECT src, dest, length
	 FROM q5_paths)
	SELECT e.src, e.dest, e.length, e.path
	FROM q5_extended_paths e, temp t
	WHERE e.src = t.src AND e.dest = t.dest AND e.length < t.length
	UNION ALL
	SELECT e.src, e.dest, e.length, e.path
	FROM q5_extended_paths e
	WHERE ARRAY[e.src, e.dest] NOT IN 
	(SELECT ARRAY[src, dest] 
	 FROM q5_paths)
;


CREATE TABLE q5_better_paths(src, dest, length, path)
AS 
    SELECT *
    FROM q5_new_paths 
    UNION ALL
    SELECT *
    FROM q5_paths
    WHERE ARRAY[src, dest] NOT IN 
    (SELECT ARRAY[n.src, n.dest] 
     FROM q5_new_paths n) 
;

DROP TABLE q5_paths;
ALTER TABLE q5_better_paths RENAME TO q5_paths;

DROP TABLE q5_paths_to_update;
ALTER TABLE q5_new_paths RENAME TO q5_paths_to_update;

SELECT COUNT(*) AS path_count,
       CASE WHEN 0 = (SELECT COUNT(*) FROM q5_paths_to_update) 
            THEN 'FINISHED'
            ELSE 'RUN AGAIN' END AS status
  FROM q5_paths;
