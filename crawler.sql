/* 
Raw Database creation code. Future versions should be more clever in there use of indexes (esp. for lastfm_annotations.)
*/

CREATE DATABASE /*!32312 IF NOT EXISTS*/`crawler_lastfm` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `crawler_lastfm`;

DROP TABLE IF EXISTS `lastfm_annotations`;
CREATE TABLE `lastfm_annotations` (`user_id` bigint(20) NOT NULL,`item_url` varchar(767) NOT NULL,`tag_name` varchar(2000) NOT NULL,`tag_date` date NOT NULL, index user_id (user_id)) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `lastfm_errorqueue`;
CREATE TABLE `lastfm_errorqueue` (`user_id` bigint(20) DEFAULT NULL,`error_type` varchar(11) DEFAULT NULL,`tag_name` varchar(2000) DEFAULT NULL,`retry_count` int(11) DEFAULT NULL, index user_id (user_id), index error_type (error_type), index retry_count (retry_count)) ENGINE=InnoDB DEFAULT CHARSET=latin1;
# future versions should rename the "retry_count" column, as it is not being used as such anymore
# we should also change tag_name to something more generic like "desc", as it is used for more than just tag names (i.e scrobble timestamps)

DROP TABLE IF EXISTS `lastfm_friendlist`;
CREATE TABLE `lastfm_friendlist` (`friend_id1` bigint(20) NOT NULL,`friend_id2` bigint(20) NOT NULL,`sanity_check_id` varchar(767) NOT NULL,PRIMARY KEY (`sanity_check_id`), index friend_id1 (friend_id1), index friend_id2 (friend_id2)) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `lastfm_userlist`;
CREATE TABLE `lastfm_userlist` (`user_id` bigint(20) NOT NULL,`user_name` varchar(767) NOT NULL, PRIMARY KEY (`user_id`), INDEX user_name (user_name)) ENGINE=InnoDB DEFAULT CHARSET=latin1;

drop table if exists `lastfm_crawlqueue`;
CREATE TABLE `lastfm_crawlqueue` (
	`user_name` VARCHAR(767) NOT NULL,
	`crawl_flag` INT(1) NULL DEFAULT '0',
	`time_stamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`friends_fixed` INT(1) NOT NULL DEFAULT '1',
	`loved_tracks` INT(1) NULL DEFAULT '0',
	`banned_tracks` INT(1) NULL DEFAULT '0',
	INDEX `friends_fixed` (`friends_fixed`),
	INDEX `loved_tracks` (`loved_tracks`)
	PRIMARY KEY (`user_name`),
	INDEX `crawl_flag` (`crawl_flag`)
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB;

DROP TABLE IF EXISTS `lastfm_scrobbles`;
CREATE TABLE `lastfm_scrobbles` (
	`user_id` BIGINT(20) NOT NULL,
	`item_url` VARCHAR(767) NOT NULL,
	`scrobble_time` TIMESTAMP NOT NULL,
	UNIQUE INDEX `user_id_item_url_scrobble_time` (`user_id`, `item_url`, `scrobble_time`),
	INDEX `user_id` (`user_id`),
	INDEX `item_url` (`item_url`)
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB;

DROP TABLE IF EXISTS `lastfm_extended_user_info`;
CREATE TABLE `lastfm_extended_user_info` (
	`user_name` VARCHAR(767) NULL DEFAULT NULL,
	`user_id` BIGINT(20) NOT NULL DEFAULT '0',
	`country` VARCHAR(767) NULL DEFAULT NULL,
	`age` INT(20) NULL DEFAULT NULL,
	`gender` VARCHAR(50) NULL DEFAULT NULL,
	`subscriber` TINYINT(1) NULL DEFAULT NULL,
	`playcount` BIGINT(20) NULL DEFAULT NULL,
	`playlists` BIGINT(20) NULL DEFAULT NULL,
	`bootstrap` BIGINT(20) NULL DEFAULT NULL,
	`registered` DATETIME NULL DEFAULT NULL,
	`type` VARCHAR(50) NULL DEFAULT NULL,
	`anno_count` BIGINT(20) NULL DEFAULT NULL,
	PRIMARY KEY (`user_id`),
	INDEX (`user_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `lastfm_lovedtracks`;
CREATE TABLE `lastfm_lovedtracks` (
	`user_id` BIGINT(20) NOT NULL,
	`item_url` VARCHAR(767) NOT NULL,
	`love_time` TIMESTAMP NOT NULL DEFAULT NULL,
	UNIQUE INDEX `user_id_item_url` (`user_id`, `item_url`),
	INDEX `item_url` (`item_url`),
	INDEX `user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `lastfm_bannedtracks`;
CREATE TABLE `lastfm_bannedtracks` (
	`user_id` INT(10) NULL,
	`item_url` VARCHAR(767) NULL,
	`ban_time` TIMESTAMP NULL,
	UNIQUE INDEX `user_id_item_url` (`user_id`, `item_url`),
	INDEX `user_id` (`user_id`),
	INDEX `item_url` (`item_url`)
)
COLLATE='latin1_general_ci'
ENGINE=InnoDB;



/*
This procedured handles calls to "update error queue". Currently we're handling an error partially with this function, and partially with other methods. Future versions should be consistent.
*/

DROP PROCEDURE IF EXISTS `updateerrorqueue`;

DELIMITER $$

CREATE
    /*[DEFINER = { user | CURRENT_USER }]*/
    PROCEDURE `crawler_lastfm`.`updateerrorqueue`(IN userId BIGINT, IN errorType VARCHAR(25), IN tagName VARCHAR(2000), OUT retryCount INT)
    /*LANGUAGE SQL
    | [NOT] DETERMINISTIC
    | { CONTAINS SQL | NO SQL | READS SQL DATA | MODIFIES SQL DATA }
    | SQL SECURITY { DEFINER | INVOKER }
    | COMMENT 'string'*/
    BEGIN
	SELECT retry_count FROM lastfm_errorqueue WHERE user_id=userId AND error_type=errorType AND tag_name=tagName INTO retryCount;
	IF retryCount IS NULL THEN
		INSERT INTO lastfm_errorqueue (user_id, error_type, tag_name, retry_count) VALUES (userId, errorType, tagName, 0);
		IF errorType='friends' THEN
			DELETE FROM lastfm_friendlist WHERE friend_id1=userId OR friend_id2=userId;
		ELSEIF errorType='annotations' THEN
			DELETE FROM lastfm_annotations WHERE user_id=userId AND tag_name=tagname;
		END IF; 
	END IF; 
    END$$

DELIMITER ;

/*
This handles updates to the crawl queue.
*/

DELIMITER //
CREATE DEFINER=`root`@`localhost` PROCEDURE `updatecrawlerqueue`(OUT `userName` VARCHAR(1000))
BEGIN
	SELECT user_name from lastfm_crawlqueue where crawl_flag = 0 LIMIT 1 INTO userName;
	UPDATE lastfm_crawlqueue SET crawl_flag = 1, time_stamp= current_timestamp where user_name = userName;
    END//
DELIMITER ;






