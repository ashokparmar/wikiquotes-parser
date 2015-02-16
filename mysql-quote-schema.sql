CREATE TABLE `quote` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `quote_md5` varchar(128) NOT NULL,
  `page_id` varchar(256) NOT NULL,
  `page_title` text,
  `quote_group` text,
  `quote` text,
  `categories` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `quote_md5` (`quote_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
