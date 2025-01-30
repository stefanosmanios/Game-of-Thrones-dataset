CREATE TABLE character (
    characterid INT PRIMARY KEY,
    ch_name VARCHAR(100),
    housename VARCHAR(100),
    nickname VARCHAR(100),
    royal BOOLEAN
);

CREATE TABLE locations (
    locationid INT PRIMARY KEY,
    locationname VARCHAR(100)
);

CREATE TABLE seasons (
    seasonnum INT PRIMARY KEY,
    seasonlength INT
);

CREATE TABLE episodes (
    episodeid INT PRIMARY KEY,
    seasonnum INT,
    episodenum INT,
    episodetitle VARCHAR(150),
    episodeairdate DATE,
    length INT,
    episodedescription TEXT,
    FOREIGN KEY (seasonnum) REFERENCES seasons(seasonnum)
);

CREATE TABLE scenes (
    sceneid INT PRIMARY KEY,
    scenenumber INT,
    scenestart TIME,
    sceneend TIME,
    locationid INT,
    sublocationname VARCHAR(100),
    episodeid INT,
    FOREIGN KEY (locationid) REFERENCES locations(locationid),
    FOREIGN KEY (episodeid) REFERENCES episodes(episodeid)
);

CREATE TABLE sublocation (
    locationid INT,
    sublocationname VARCHAR(100),
    PRIMARY KEY (locationid, sublocationname),
    FOREIGN KEY (locationid) REFERENCES locations(locationid)
);

CREATE TABLE openingsequencelocation (
    episodeid INT,
    orderofsequence INT,
    openingsequencelocation VARCHAR(100),
    PRIMARY KEY (episodeid, orderofsequence),
    FOREIGN KEY (episodeid) REFERENCES episodes(episodeid)
);

CREATE TABLE actorname (
    characterid INT,
    actorname VARCHAR(100),
    PRIMARY KEY (characterid, actorname),
    FOREIGN KEY (characterid) REFERENCES character(characterid)
);

CREATE TABLE appearsin (
    characterid INT,
    episodeid INT,
    sceneid INT,
    PRIMARY KEY (characterid, episodeid, sceneid),
    FOREIGN KEY (characterid) REFERENCES character(characterid),
    FOREIGN KEY (episodeid) REFERENCES episodes(episodeid),
    FOREIGN KEY (sceneid) REFERENCES scenes(sceneid)
);

CREATE TABLE relationships (
    relationid INT PRIMARY KEY,
    characterid INT,
    relatedto VARCHAR,
    relation VARCHAR(100),
    FOREIGN KEY (characterid) REFERENCES character(characterid),
    FOREIGN KEY (relatedto) REFERENCES character(characterid)
);
