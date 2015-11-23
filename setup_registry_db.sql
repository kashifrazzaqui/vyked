CREATE TABLE services(
   service_name VARCHAR(100) NOT NULL,
   version VARCHAR(100) NOT NULL,
   ip INET NULL, --can be made TEXT if INET does not work
   port INTEGER NULL,
   protocol VARCHAR(5) NULL,    --can be made ENUM for (TCP, HTTP, WS, ...)
   node_id VARCHAR(100) NULL,
   is_pending BOOLEAN DEFAULT TRUE,
   PRIMARY KEY (node_id)
);

CREATE TABLE subscriptions(
   subscriber_name VARCHAR(100) NOT NULL,
   subscriber_version VARCHAR(100) NOT NULL,
   subscribee_name VARCHAR(100) NOT NULL,
   subscribee_version VARCHAR(100) NOT NULL,
   event_name VARCHAR(100) NOT NULL,
   strategy VARCHAR(100) NOT NULL, -- can be made enum for (DESIGNATION, LEADER, RANDOM)
   PRIMARY KEY (subscriber_name, subscriber_version, subscribee_name, subscribee_version)
);

CREATE TABLE dependencies(
   child_name VARCHAR(100) NOT NULL,
   child_version VARCHAR(100) NOT NULL,
   parent_name VARCHAR(100) NOT NULL,
   parent_version VARCHAR(100) NOT NULL,
   PRIMARY KEY (child_name, child_version, parent_name, parent_version)
);

CREATE TABLE uptimes(
   node_id VARCHAR(100) NOT NULL,
   event_type VARCHAR(50) NOT NULL, -- can be made enum for (UPTIME, DOWNTIME)
   event_time INTEGER NOT NULL, -- change to timestamp if required
   PRIMARY KEY (node_id, event_type, event_time)
);
