create databases projects;
USE projects;
CREATE TABLE users
(
    username VARCHAR(20) BINARY NOT NULL,
    password VARCHAR(20) BINARY NOT NULL,
    admin INT(1),
    UNIQUE(username)
);
INSERT INTO users (username, password, admin) VALUES ("admin", "password", 1);