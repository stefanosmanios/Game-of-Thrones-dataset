SELECT c.characterid, c.ch_name, COUNT(DISTINCT a.episodeid) AS episode_count
FROM  appearsin a, character c
WHERE a.characterid=c.characterid
GROUP BY c.characterid, c.ch_name
ORDER BY episode_count DESC
LIMIT 20;

SELECT DISTINCT e.episodeid, e.episodetitle, e.length
FROM locations l, episodes e, scenes s
WHERE l.locationid= s.locationid AND e.episodeid=s.episodeid
AND s.sublocationname = 'Winterfell'
UNION 
SELECT e.episodeid, e.episodetitle, e.length
FROM episodes e, openingsequencelocation o
WHERE e.episodeid=o.episodeid
AND openingsequencelocation  = 'Winterfell';

SELECT DISTINCT c.characterid, c.ch_name, c.housename, a.actorname
FROM character c, actorname a, relationships r
WHERE c.characterid=a.characterid_fk
AND c.characterid=r.characterid 
AND r.relation = 'killed';

SELECT COUNT(DISTINCT c.characterid) AS stark_count
FROM character c, relationships r, character c1
WHERE c.housename = 'Stark' 
OR (r.characterid = c.characterid 
AND r.relatedto = c1.ch_name 
AND r.relation 
IN ('married to', 'serves') 
AND c1.housename = 'Stark');

BEGIN;

  DELETE FROM openingsequencelocation;

  INSERT INTO openingsequencelocation (episodeid, orderofsequence, openingsequencelocation)
  SELECT e.episodeid,
         1 AS orderofsequence,
         'kavala' AS openingsequencelocation
  FROM episodes e;

COMMIT;