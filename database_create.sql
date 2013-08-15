
CREATE TABLE `location` (
  `ip_addr` varchar(15) NOT NULL,
  `city` varchar(100) NOT NULL,
  `country_code` char(2) NOT NULL COMMENT ' /* comment truncated */ /*2-letter country code*/',
  `latitude` double DEFAULT NULL,
  `longitude` int(11) DEFAULT NULL,
  PRIMARY KEY (`ip_addr`),
  UNIQUE KEY `ip_addr_UNIQUE` (`ip_addr`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




CREATE TABLE `nodes` (
  `node_id` int(11) NOT NULL AUTO_INCREMENT,
  `node_key` text NOT NULL,
  `node_port` int(11) NOT NULL,
  `node_type` varchar(15) NOT NULL DEFAULT 'unknown',
  `ip_addr` varchar(15) NOT NULL,
  `last_ip_change` datetime NOT NULL,
  `last_seen` datetime NOT NULL,
  PRIMARY KEY (`node_id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;



CREATE TABLE `vessels` (
  `node_id` int(11) NOT NULL,
  `vessel_name` varchar(5) NOT NULL,
  `acquirable` boolean DEFAULT TRUE,
  PRIMARY KEY (`node_id`,`vessel_name`),
  CONSTRAINT `node_id` FOREIGN KEY (`node_id`) REFERENCES `nodes` (`node_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;





CREATE TABLE `userkeys` (
  `node_id` int(11) NOT NULL,
  `vessel_name` varchar(10) NOT NULL,
  `userkey` text NOT NULL,
  PRIMARY KEY (`node_id`),
  KEY `vessel_idx` (`node_id`,`vessel_name`),
  CONSTRAINT `userkeys_foreignkey` FOREIGN KEY (`node_id`, `vessel_name`) REFERENCES `vessels` (`node_id`, `vessel_name`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;




CREATE TABLE `vesselports` (
  `node_id` int(11) NOT NULL,
  `vessel_name` varchar(45) NOT NULL,
  `port` varchar(45) NOT NULL,
  PRIMARY KEY (`node_id`, `vessel_name`, `port`),
  KEY `vesselport_foreignkey_idx` (`node_id`,`vessel_name`),
  CONSTRAINT `vesselport_foreignkey` FOREIGN KEY (`node_id`, `vessel_name`) REFERENCES `vessels` (`node_id`, `vessel_name`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


