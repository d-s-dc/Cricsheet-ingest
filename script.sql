use assgnmnt;

-- SELECT count(*) FROM match_results;

CREATE VIEW team_table AS
SELECT `date`, `year`, team_1 AS team, gender, 
	(
	CASE
		WHEN winner = team_1 THEN team_1
		ELSE NULL
	END) AS winner
FROM match_results
WHERE winner IS NOT NULL AND method IS NULL

UNION

SELECT `date`, `year`, team_2 AS team, gender, 
	(
	CASE
		WHEN winner = team_2 THEN team_2
		ELSE NULL
	END) AS winner
FROM match_results
WHERE winner IS NOT NULL AND method IS NULL;

-- Q2.a
-- The win records (percentage win and total wins) for each team by year and gender, 
-- excluding ties, matches with no result, and matches decided by the DLS method 
-- in the event that, for whatever reason, the planned innings can’t be completed.
SELECT team, `year`, gender, count(*) AS total_matches_played,
		count(winner) AS matches_won, count(winner) / count(*) * 100 AS winning_percentage
FROM team_table
GROUP BY `year`, gender, team
ORDER BY team, `year`, gender;

-- Q2.b
-- Which male and female teams had the highest win percentages in 2019
WITH scores_2019 AS
	(
     SELECT team, gender, count(winner) / count(*) * 100 AS winning_percentage
	 FROM team_table
     WHERE `year` = 2019
	 GROUP BY gender, team
    )
SELECT team, gender, winning_percentage
FROM scores_2019
WHERE winning_percentage IN
	(SELECT MAX(winning_percentage)
	 FROM scores_2019
     GROUP BY gender
	);

-- Q2.c
WITH strike_rate_2019 AS
	(
    SELECT batsman, gender, SUM(runs) / COUNT(ball) * 100 AS strike_rate
	FROM ball_data
	WHERE `year` = 2019
	GROUP BY batsman, gender
    )
(SELECT batsman, gender, strike_rate
FROM strike_rate_2019
WHERE gender = 'male'
ORDER BY strike_rate DESC
LIMIT 3)

UNION

(SELECT batsman, gender, strike_rate
FROM strike_rate_2019
WHERE gender = 'female'
ORDER BY strike_rate DESC
LIMIT 3);

SELECT * FROM ball_data;