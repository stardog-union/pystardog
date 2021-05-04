CREATE TABLE Artist (
	id 	        INT,
	name        VARCHAR(30),
	description VARCHAR(1024),
	type        INT,
	PRIMARY KEY (id)
);

CREATE TABLE Album (
	id 	         INT,
	name         VARCHAR(30),
	release_date DATE,
	artist       INT,
	PRIMARY KEY (id),
	FOREIGN KEY (artist) REFERENCES Artist (id)
);

CREATE TABLE Membership (
	artist INT,
	band INT,
	FOREIGN KEY (band) REFERENCES Artist (id),
	FOREIGN KEY (artist) REFERENCES Artist (id)
);

CREATE TABLE Track (
	id INT,
	name VARCHAR(30),
	album INT,
	length INT,
	PRIMARY KEY (id),
	FOREIGN KEY (album) REFERENCES Album (id)
);

CREATE TABLE Songwriter (
	song INT,
	writer INT,
	FOREIGN KEY (writer) REFERENCES Artist (id),
	FOREIGN KEY (song) REFERENCES Track (id)
);
